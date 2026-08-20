"""Real Redis Sentinel and Cluster provider integration tests."""

from __future__ import annotations

import os
import subprocess
import time
import uuid

import pytest
import redis

from tributo_broker_redis.config import RedisBrokerConfig
from tributo_broker_redis.consumer import RedisTaskConsumer
from tributo_broker_redis.redis_client import create_redis_client
from tributo_broker_redis.reporter import RedisEventReporter

pytestmark = [pytest.mark.integration, pytest.mark.slow]


def _sentinel_map(values: list[str]) -> dict[str, str]:
    return dict(zip(values[::2], values[1::2], strict=True))


def _compose_args() -> list[str]:
    project = os.environ.get("BROKER_TOPOLOGY_PROJECT")
    compose_file = os.environ.get("BROKER_TOPOLOGY_COMPOSE_FILE")
    if not project or not compose_file:
        pytest.fail("topology compose environment is required")
    return [
        "docker",
        "compose",
        "--project-name",
        project,
        "--file",
        compose_file,
    ]


def test_sentinel_reads_tasks_and_survives_master_failover() -> None:
    suffix = uuid.uuid4().hex
    sentinel_probe = redis.Redis(
        host=os.environ.get("BROKER_SENTINEL_HOST", "127.0.0.1"),
        port=int(os.environ.get("BROKER_SENTINEL_PORT", "26380")),
        decode_responses=True,
    )
    master_info = _sentinel_map(
        sentinel_probe.execute_command("SENTINEL", "MASTER", "mymaster")
    )
    replica_info = _sentinel_map(
        sentinel_probe.execute_command("SENTINEL", "REPLICAS", "mymaster")[0]
    )
    sentinel_probe.close()
    sentinel_address_map = {
        f"{master_info['ip']}:{master_info['port']}": ("127.0.0.1", 16380),
        f"{replica_info['ip']}:{replica_info['port']}": ("127.0.0.1", 16381),
    }
    config = RedisBrokerConfig(
        mode="sentinel",
        sentinel_hosts=[
            (
                os.environ.get("BROKER_SENTINEL_HOST", "127.0.0.1"),
                int(os.environ.get("BROKER_SENTINEL_PORT", "26380")),
            )
        ],
        sentinel_service="mymaster",
        sentinel_address_map=sentinel_address_map,
        task_stream_key=f"it:sentinel:tasks:{suffix}",
        event_stream_prefix=f"it:sentinel:events:{suffix}",
        consumer_group=f"it-sentinel-{suffix}",
        consumer_name=f"sentinel-{suffix}",
        group_start_id="0-0",
        block_ms=100,
        claim_idle_ms=0,
    )
    client = create_redis_client(config)
    try:
        client.ping()
        task_id = client.xadd(
            config.task_stream_key,
            {"job_id": "sentinel-job", "payload": "{}"},
        )
        consumer = RedisTaskConsumer(client, config)
        message = consumer.poll(100)
        assert message is not None
        assert message.delivery_id == task_id
        consumer.ack(message)

        compose = _compose_args()
        subprocess.run([*compose, "stop", "sentinel-master"], check=True)
        deadline = time.monotonic() + 30
        failover_key = f"it:sentinel:key:{suffix}"
        failover_value = f"{suffix}-after-failover"
        while time.monotonic() < deadline:
            try:
                client.set(failover_key, failover_value)
                if client.get(failover_key) == failover_value:
                    break
            except Exception:
                time.sleep(0.5)
        else:
            pytest.fail("Sentinel did not promote a replica before timeout")
    finally:
        client.close()


def test_cluster_handles_task_event_and_cancel_keys() -> None:
    suffix = uuid.uuid4().hex
    config = RedisBrokerConfig(
        mode="cluster",
        cluster_startup_nodes=[
            (
                os.environ.get("BROKER_CLUSTER_HOST", "127.0.0.1"),
                int(os.environ.get("BROKER_CLUSTER_PORT", "17000")),
            )
        ],
        cluster_address_remap_host="127.0.0.1",
        task_stream_key=f"it:cluster:tasks:{suffix}",
        event_stream_prefix=f"it:cluster:events:{suffix}",
        cancel_key_prefix=f"it:cluster:cancel:{suffix}",
        consumer_group=f"it-cluster-{suffix}",
        consumer_name=f"cluster-{suffix}",
        group_start_id="0-0",
        block_ms=100,
        claim_idle_ms=0,
    )
    client = create_redis_client(config)
    consumer = RedisTaskConsumer(client, config)
    try:
        client.ping()
        task_id = client.xadd(
            config.task_stream_key,
            {"job_id": "cluster-job", "payload": "{}"},
        )
        message = consumer.poll(100)
        assert message is not None
        assert message.delivery_id == task_id
        consumer.ack(message)

        event_key = config.event_stream_key("cluster-job")
        reporter = RedisEventReporter(client, config, "cluster-job")
        reporter.report_phase("cluster-job", "QUEUED")
        assert reporter.report_failed_with_code("cluster-job", "controlled failure")
        assert reporter.report_cancelled("cluster-job")
        cancel_key = config.cancel_key("cluster-job")
        client.set(cancel_key, "1")
        assert client.exists(cancel_key) == 1
        assert client.xlen(event_key) == 2
    finally:
        consumer.close()
