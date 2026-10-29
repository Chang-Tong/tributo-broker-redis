"""Real Standalone Redis -> provider -> Ray -> result/event matrix."""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import pytest
import redis
from ray.job_submission import JobStatus, JobSubmissionClient
from tributo.exporting import load_bundle

from tributo_broker_redis.config import OperationType, RedisBrokerConfig
from tributo_broker_redis.runtime import RedisBrokerRuntime

pytestmark = [pytest.mark.integration, pytest.mark.slow]
_TERMINAL_EVENTS = {"COMPLETED", "FAILED", "CANCELLED"}
_SECRET = "integration-secret-must-not-leak"


@pytest.fixture(scope="module")
def redis_client() -> Iterator[redis.Redis]:
    client = redis.Redis.from_url(
        os.environ.get("BROKER_REDIS_URL", "redis://127.0.0.1:16379/0"),
        decode_responses=True,
    )
    client.ping()
    yield client
    client.close()


@pytest.fixture(scope="module")
def provider_config() -> RedisBrokerConfig:
    suffix = uuid.uuid4().hex
    wheel_name = os.environ["BROKER_PROVIDER_WHEEL_NAME"]
    core_root = os.environ["BROKER_PROJECT_ROOT"]
    return RedisBrokerConfig.model_validate(
        {
            "broker_id": "tributo-redis",
            "api_version": 1,
            "transport": {
                "mode": "standalone",
                "url": os.environ.get("BROKER_REDIS_URL", "redis://127.0.0.1:16379/0"),
                "driver_url": os.environ.get(
                    "BROKER_REDIS_DRIVER_URL",
                    "redis://host.docker.internal:16379/0",
                ),
                "block_ms": 100,
                "claim_idle_ms": 0,
            },
            "channels": {
                "training": {
                    "task_stream_key": f"it:training:tasks:{suffix}",
                    "event_stream_prefix": f"it:training:events:{suffix}",
                    "cancel_key_prefix": f"it:training:cancel:{suffix}",
                    "consumer_group": f"it:training:group:{suffix}",
                    "consumer_name": f"it-training-{suffix}",
                    "group_start_id": "0-0",
                    "outer_identity_field": "request_id",
                },
                "batch_inference": {
                    "task_stream_key": f"it:inference:tasks:{suffix}",
                    "event_stream_prefix": f"it:inference:events:{suffix}",
                    "cancel_key_prefix": f"it:inference:cancel:{suffix}",
                    "consumer_group": f"it:inference:group:{suffix}",
                    "consumer_name": f"it-inference-{suffix}",
                    "group_start_id": "0-0",
                },
            },
            "protocol": {
                "profile": "tributo-generic-v1",
                "protocol_version": "1.0",
            },
            "operations": {
                "training": {"execution_profiles": ["single_worker", "distributed"]},
                "batch_inference": {
                    "execution_profiles": ["single_worker", "distributed"]
                },
            },
            "execution": {
                "ray_dashboard_url": os.environ.get(
                    "BROKER_RAY_DASHBOARD_URL", "http://127.0.0.1:18265"
                ),
                "project_root": core_root,
                "runtime_pip_packages": [f"/provider/{wheel_name}"],
                "cancel_poll_interval_seconds": 0.25,
                "entrypoint_num_cpus": 1,
            },
        }
    )


@pytest.fixture(scope="module")
def runtime(
    provider_config: RedisBrokerConfig,
    redis_client: redis.Redis,
) -> Iterator[RedisBrokerRuntime]:
    value = RedisBrokerRuntime(provider_config, redis_client=redis_client)
    value.start()
    yield value
    value.close()


@pytest.fixture(scope="module")
def inference_input() -> str:
    host_root = Path(os.environ["BROKER_RAY_STORAGE_DIR"])
    table = pacsv.read_csv(Path(__file__).parent / "fixtures" / "broker_train.csv")
    features = pa.table({"x1": table["x1"], "x2": table["x2"]})
    host_path = host_root / "inference-input.parquet"
    pq.write_table(features, host_path)
    return "/tmp/ray_results/inference-input.parquet"


def _events(
    client: redis.Redis,
    config: RedisBrokerConfig,
    operation_type: str,
    operation_id: str,
) -> list[dict[str, Any]]:
    channel = config.channels.for_operation(cast(OperationType, operation_type))
    values: list[dict[str, Any]] = []
    for _event_id, fields in client.xrange(channel.event_stream_key(operation_id)):
        assert fields[channel.outer_identity_field] == operation_id
        values.append(json.loads(fields["payload"]))
    return values


def _request(
    operation_id: str,
    operation_type: str,
    execution_profile: str,
    spec: dict[str, Any],
) -> str:
    return json.dumps(
        {
            "protocol_profile": "tributo-generic-v1",
            "protocol_version": "1.0",
            "operation_id": operation_id,
            "operation_type": operation_type,
            "execution_profile": execution_profile,
            "run_id": f"run-{operation_id}",
            "attempt_id": "attempt-1",
            "request_digest": hashlib.sha256(operation_id.encode("utf-8")).hexdigest(),
            "spec": spec,
        },
        separators=(",", ":"),
    )


def _admit(
    runtime: RedisBrokerRuntime,
    client: redis.Redis,
    config: RedisBrokerConfig,
    operation_type: str,
    operation_id: str,
    payload: str,
) -> dict[str, Any]:
    channel = config.channels.for_operation(cast(OperationType, operation_type))
    client.xadd(
        channel.task_stream_key,
        {channel.outer_identity_field: operation_id, "payload": payload},
    )
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if runtime.run_once(timeout_ms=100):
            break
    else:
        pytest.fail(f"provider did not consume {operation_id}")
    pending = client.xpending(channel.task_stream_key, channel.consumer_group)
    assert pending["pending"] == 0
    events = _events(client, config, operation_type, operation_id)
    assert events and events[-1]["event_type"] in {"ACCEPTED", "CANCELLED", "FAILED"}
    queued_cancel = (
        events[-1]["event_type"] == "CANCELLED"
        and events[-1]["submission_id"] is None
        and events[-1]["phase"] == "QUEUED"
    )
    # Cancellation precedes payload parsing, so its fallback run identity comes
    # from the outer envelope even when the payload contains another run_id.
    assert events[-1]["run_id"] == (
        operation_id if queued_cancel else f"run-{operation_id}"
    )
    return events[-1]


def _wait_terminal(
    client: redis.Redis,
    config: RedisBrokerConfig,
    operation_type: str,
    operation_id: str,
    *,
    timeout: float = 480,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        events = _events(client, config, operation_type, operation_id)
        terminals = [
            event for event in events if event["event_type"] in _TERMINAL_EVENTS
        ]
        if terminals:
            return terminals[-1], events
        time.sleep(1)
    pytest.fail(f"operation {operation_id} did not publish a terminal event")


def _training_spec(
    operation_id: str,
    num_workers: int,
    *,
    path: str | None = None,
    rounds: int = 2,
    credential_ref: str | None = None,
) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "algorithm": "xgboost",
        "config": {
            "ingestion": {
                "source": {
                    "type": "csv",
                    "path": path or os.environ["BROKER_TRAIN_FIXTURE"],
                },
                "engine": "ray",
            },
            "feature_names": ["x1", "x2"],
            "label_name": "label",
            "worker_count": num_workers,
            "model": {"objective": "binary:logistic", "max_depth": 2, "eta": 0.3},
            "training": {
                "num_rounds": rounds,
                "train_ratio": 1.0,
                "validation_ratio": 0.0,
                "test_ratio": 0.0,
                "seed": 7,
            },
            "storage_path": f"/tmp/ray_results/{operation_id}/ray",
            "bundle_uri": f"/tmp/ray_results/{operation_id}/bundles",
        },
    }
    if credential_ref is not None:
        spec["credential_ref"] = credential_ref
    return spec


def _inference_spec(
    operation_id: str,
    bundle_uri: str,
    input_uri: str,
    concurrency: int,
) -> dict[str, Any]:
    return {
        "profile": "bundle-backed",
        "request": {
            "schema_version": 1,
            "model": {"kind": "bundle", "uri": bundle_uri},
            "input": {
                "source": {"type": "parquet", "path": input_uri},
                "engine": "ray",
            },
            "input_binding": {
                "tensors": [
                    {
                        "tensor_name": "float_input",
                        "columns": ["x1", "x2"],
                        "dtype": "float32",
                    }
                ]
            },
            "output_binding": {
                "tensors": [
                    {
                        "tensor_name": "label",
                        "column": "prediction",
                        "semantic": "label",
                        "squeeze_singleton": True,
                    },
                    {
                        "tensor_name": "probabilities",
                        "column": "score",
                        "semantic": "probability",
                    },
                ]
            },
            "result_sink": {
                "sink_id": "parquet-v1",
                "uri": f"/tmp/ray_results/{operation_id}/output",
            },
            "execution": {
                "executor_id": "ray-map-batches-v1",
                "batch_size": 2,
                "concurrency": concurrency,
                "num_cpus_per_actor": 1.0,
                "num_gpus_per_actor": 0.0,
            },
        },
    }


def test_four_real_healthy_paths_and_event_result_contracts(
    runtime: RedisBrokerRuntime,
    redis_client: redis.Redis,
    provider_config: RedisBrokerConfig,
    inference_input: str,
) -> None:
    bundle_uris: list[str] = []
    for profile, workers in (("single_worker", 1), ("distributed", 2)):
        operation_id = f"training-{profile}-{uuid.uuid4().hex}"
        spec = _training_spec(
            operation_id,
            workers,
            credential_ref=(
                "env:TRIBUTO_TEST_SECRET" if profile == "single_worker" else None
            ),
        )
        payload = _request(operation_id, "training", profile, spec)
        accepted = _admit(
            runtime,
            redis_client,
            provider_config,
            "training",
            operation_id,
            payload,
        )
        terminal, events = _wait_terminal(
            redis_client, provider_config, "training", operation_id
        )
        assert terminal["event_type"] == "COMPLETED", json.dumps(events, sort_keys=True)
        assert terminal["submission_id"] == accepted["submission_id"]
        assert terminal["payload"]["result"]["execution"]["status"] == "succeeded"
        job_client = JobSubmissionClient(provider_config.execution.ray_dashboard_url)
        logs = job_client.get_job_logs(accepted["submission_id"])
        assert {"PHASE", "LOG", "METRICS", "COMPLETED"}.issubset(
            {event["event_type"] for event in events}
        ), logs
        bundle_uri = terminal["payload"]["result_reference"]["uri"]
        assert bundle_uri
        host_bundle_uri = Path(os.environ["BROKER_RAY_STORAGE_DIR"]) / Path(
            bundle_uri
        ).relative_to("/tmp/ray_results")
        manifest = load_bundle(str(host_bundle_uri))
        assert manifest["status"] == "succeeded"
        assert manifest["artifacts"]
        bundle_uris.append(bundle_uri)
        if profile == "single_worker":
            info = job_client.get_job_info(accepted["submission_id"])
            evidence = payload + json.dumps(events) + logs + repr(info.metadata)
            assert _SECRET not in evidence

    for (profile, concurrency), bundle_uri in zip(
        (("single_worker", 1), ("distributed", 2)),
        bundle_uris,
        strict=True,
    ):
        operation_id = f"inference-{profile}-{uuid.uuid4().hex}"
        spec = _inference_spec(operation_id, bundle_uri, inference_input, concurrency)
        accepted = _admit(
            runtime,
            redis_client,
            provider_config,
            "batch_inference",
            operation_id,
            _request(operation_id, "batch_inference", profile, spec),
        )
        terminal, events = _wait_terminal(
            redis_client, provider_config, "batch_inference", operation_id
        )
        assert terminal["event_type"] == "COMPLETED", json.dumps(events, sort_keys=True)
        assert terminal["submission_id"] == accepted["submission_id"]
        receipt = terminal["payload"]["result_reference"]
        assert receipt["sink_id"] == "parquet-v1"
        assert receipt["uri"] == f"/tmp/ray_results/{operation_id}/output"
        assert receipt["result_id"]
        output_path = Path(os.environ["BROKER_RAY_STORAGE_DIR"], operation_id, "output")
        assert output_path.exists()
        output = pq.read_table(output_path)
        assert output.num_rows > 0
        assert {"prediction", "score"}.issubset(output.column_names)
        assert {"PHASE", "PROGRESS", "COMPLETED"}.issubset(
            {event["event_type"] for event in events}
        )


def test_representative_training_and_inference_failures(
    runtime: RedisBrokerRuntime,
    redis_client: redis.Redis,
    provider_config: RedisBrokerConfig,
    inference_input: str,
) -> None:
    training_id = f"training-failure-{uuid.uuid4().hex}"
    accepted = _admit(
        runtime,
        redis_client,
        provider_config,
        "training",
        training_id,
        _request(
            training_id,
            "training",
            "single_worker",
            _training_spec(training_id, 1, path="/provider-data/missing.csv"),
        ),
    )
    terminal, _ = _wait_terminal(redis_client, provider_config, "training", training_id)
    assert accepted["event_type"] == "ACCEPTED"
    assert terminal["event_type"] == "FAILED"
    assert terminal["payload"]["error_code"] == "EXECUTION_FAILED"

    inference_id = f"inference-failure-{uuid.uuid4().hex}"
    _admit(
        runtime,
        redis_client,
        provider_config,
        "batch_inference",
        inference_id,
        _request(
            inference_id,
            "batch_inference",
            "single_worker",
            _inference_spec(
                inference_id,
                "/tmp/ray_results/missing-bundle",
                inference_input,
                1,
            ),
        ),
    )
    terminal, _ = _wait_terminal(
        redis_client, provider_config, "batch_inference", inference_id
    )
    assert terminal["event_type"] == "FAILED"


def test_queued_and_running_cancel_boundaries(
    runtime: RedisBrokerRuntime,
    redis_client: redis.Redis,
    provider_config: RedisBrokerConfig,
) -> None:
    channel = provider_config.channels.training
    queued_id = f"queued-cancel-{uuid.uuid4().hex}"
    redis_client.set(channel.cancel_key(queued_id), "1")
    queued = _admit(
        runtime,
        redis_client,
        provider_config,
        "training",
        queued_id,
        _request(
            queued_id,
            "training",
            "single_worker",
            _training_spec(queued_id, 1),
        ),
    )
    assert queued["event_type"] == "CANCELLED"
    assert queued["submission_id"] is None

    running_id = f"running-cancel-{uuid.uuid4().hex}"
    accepted = _admit(
        runtime,
        redis_client,
        provider_config,
        "training",
        running_id,
        _request(
            running_id,
            "training",
            "single_worker",
            _training_spec(running_id, 1, rounds=100000),
        ),
    )
    assert accepted["event_type"] == "ACCEPTED"
    redis_client.set(channel.cancel_key(running_id), "1")
    terminal, events = _wait_terminal(
        redis_client,
        provider_config,
        "training",
        running_id,
        timeout=240,
    )
    assert terminal["event_type"] == "CANCELLED", events
    status = JobSubmissionClient(
        provider_config.execution.ray_dashboard_url
    ).get_job_status(accepted["submission_id"])
    assert status == JobStatus.STOPPED
