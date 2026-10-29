"""Pending recovery uses Redis pagination and the existing Ray identity map."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest
from tributo.ray_jobs import RayJobSubmission

from conftest import FakeRedis
from tributo_broker_redis.config import RedisBrokerConfig
from tributo_broker_redis.consumer import RedisTaskConsumer
from tributo_broker_redis.operations import PreparedOperation
from tributo_broker_redis.runtime import RedisBrokerRuntime


def _delivery(index: int) -> tuple[str, dict[str, str]]:
    operation_id = f"operation-{index}"
    return (
        f"{index}-0",
        {
            "operation_id": operation_id,
            "payload": json.dumps(
                {
                    "protocol_profile": "tributo-generic-v1",
                    "protocol_version": "1.0",
                    "operation_id": operation_id,
                    "operation_type": "training",
                    "execution_profile": "single_worker",
                    "run_id": operation_id,
                    "attempt_id": "attempt-1",
                    "request_digest": "d" * 64,
                    "spec": {},
                }
            ),
        },
    )


def _runtime(
    config: RedisBrokerConfig,
    redis: FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[RedisBrokerRuntime, list[float], list[dict[str, Any]]]:
    clock = [0.0]
    submissions: list[dict[str, Any]] = []
    monkeypatch.setattr(
        "tributo_broker_redis.runtime.time",
        SimpleNamespace(monotonic=lambda: clock[0]),
    )

    def submit(_entrypoint: str, **kwargs: Any) -> RayJobSubmission:
        submissions.append(kwargs)
        return RayJobSubmission(
            run_id=kwargs["run_id"],
            attempt_id=kwargs["attempt_id"],
            submission_id=f"submission-{len(submissions)}",
            request_digest=kwargs["request_digest"],
        )

    runtime = RedisBrokerRuntime(
        config,
        redis_client=redis,
        submitter=submit,
        # Recovery behavior is independent of workload-specific configuration.
        operation_preparer=lambda _request: PreparedOperation(
            operation_payload={}, credential_ref=None
        ),
        job_lister=lambda _url: [],
    )
    monkeypatch.setattr(runtime._cancel_watcher, "start", lambda: None)
    return runtime, clock, submissions


def test_runtime_advances_all_pending_pages_and_restarts_scan(
    config: RedisBrokerConfig, fake_redis: FakeRedis, monkeypatch: pytest.MonkeyPatch
) -> None:
    cursors: list[str] = []
    pages = {
        "0-0": ["2-0", [_delivery(1)]],
        "2-0": ["3-0", [_delivery(2)]],
        "3-0": ["0-0", [_delivery(3)]],
    }

    def claim(stream: str, *_args: Any, start_id: str, **_kwargs: Any) -> list[Any]:
        if stream != config.channels.training.task_stream_key:
            return ["0-0", []]
        cursors.append(start_id)
        if len(fake_redis.acked) == 3:
            return ["0-0", []]
        return pages[start_id]

    monkeypatch.setattr(fake_redis, "xautoclaim", claim, raising=False)
    runtime, clock, submissions = _runtime(config, fake_redis, monkeypatch)
    runtime.start()
    assert runtime.run_once(timeout_ms=0)
    for instant in (1.0, 2.0):
        clock[0] = instant
        assert runtime.run_once(timeout_ms=0)
    clock[0] = 3.0
    assert not runtime.run_once(timeout_ms=0)

    assert cursors == ["0-0", "2-0", "3-0", "0-0"]
    assert len(submissions) == 3
    assert [entry[2] for entry in fake_redis.acked] == ["1-0", "2-0", "3-0"]
    runtime.close()


def test_pending_delivery_is_discovered_after_reaching_idle_threshold(
    config: RedisBrokerConfig, fake_redis: FakeRedis, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = config.model_copy(
        update={
            "transport": config.transport.model_copy(update={"claim_idle_ms": 2000})
        }
    )
    runtime, clock, submissions = _runtime(config, fake_redis, monkeypatch)
    training_scans: list[float] = []

    def claim(
        stream: str, *_args: Any, min_idle_time: int, **_kwargs: Any
    ) -> list[Any]:
        if stream != config.channels.training.task_stream_key:
            return ["0-0", []]
        training_scans.append(clock[0])
        eligible = clock[0] * 1000 >= min_idle_time and not fake_redis.acked
        return ["0-0", [_delivery(1)] if eligible else []]

    monkeypatch.setattr(fake_redis, "xautoclaim", claim, raising=False)
    runtime.start()
    for instant in (0.0, 0.5, 1.0, 1.5):
        clock[0] = instant
        assert not runtime.run_once(timeout_ms=0)
    clock[0] = 2.0
    assert runtime.run_once(timeout_ms=0)

    assert training_scans == [0.0, 1.0, 2.0]
    assert len(submissions) == len(fake_redis.acked) == 1
    runtime.close()


def test_claimed_page_is_drained_before_claiming_again(
    config: RedisBrokerConfig, fake_redis: FakeRedis, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[int] = []

    def claim(*_args: Any, **_kwargs: Any) -> list[Any]:
        calls.append(1)
        return ["0-0", [_delivery(1), _delivery(2)]]

    monkeypatch.setattr(fake_redis, "xautoclaim", claim, raising=False)
    consumer = RedisTaskConsumer(
        fake_redis,
        config.transport.model_copy(update={"claim_idle_ms": 0}),
        config.channels.training,
        "training",
    )
    assert consumer.recover_pending() == 2
    for _ in range(3):
        assert consumer.recover_pending() == 0
    first = consumer.poll(0)
    assert first is not None and first.delivery_token == "1-0"
    assert consumer.recover_pending() == 0
    second = consumer.poll(0)
    assert second is not None and second.delivery_token == "2-0"
    assert calls == [1]


def test_reclaimed_unacked_admission_reuses_active_ray_submission(
    config: RedisBrokerConfig, fake_redis: FakeRedis, monkeypatch: pytest.MonkeyPatch
) -> None:
    def claim(stream: str, *_args: Any, **_kwargs: Any) -> list[Any]:
        eligible = (
            stream == config.channels.training.task_stream_key and not fake_redis.acked
        )
        return ["0-0", [_delivery(1)] if eligible else []]

    original_xadd = fake_redis.xadd
    attempts: list[int] = []

    def flaky_xadd(*args: Any, **kwargs: Any) -> str:
        attempts.append(1)
        if len(attempts) <= 2:
            raise ConnectionError("synthetic event publication failure")
        return original_xadd(*args, **kwargs)

    monkeypatch.setattr(fake_redis, "xautoclaim", claim, raising=False)
    monkeypatch.setattr(fake_redis, "xadd", flaky_xadd)
    runtime, clock, submissions = _runtime(config, fake_redis, monkeypatch)
    runtime.start()
    for instant in (0.0, 1.0):
        clock[0] = instant
        assert runtime.run_once(timeout_ms=0)
        assert fake_redis.acked == []
        assert len(submissions) == 1
    clock[0] = 2.0
    assert runtime.run_once(timeout_ms=0)

    assert len(submissions) == len(fake_redis.acked) == 1
    assert len(attempts) == 3
    event = json.loads(next(iter(fake_redis.events.values()))[-1]["payload"])
    assert event["payload"]["recovered"] is True
    runtime.close()


def test_pending_recovery_retries_transient_redis_error_without_hot_loop(
    config: RedisBrokerConfig,
    fake_redis: FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    calls: list[str] = []

    def claim(stream: str, *_args: Any, **_kwargs: Any) -> list[Any]:
        calls.append(stream)
        if len(calls) <= 2:
            raise ConnectionError("synthetic claim failure")
        return ["0-0", []]

    monkeypatch.setattr(fake_redis, "xautoclaim", claim, raising=False)
    runtime, clock, _submissions = _runtime(config, fake_redis, monkeypatch)
    runtime.start()
    for _ in range(5):
        assert not runtime.run_once(timeout_ms=0)
    assert len(calls) == 2
    clock[0] = 1.0
    assert not runtime.run_once(timeout_ms=0)
    assert len(calls) == 4
    assert "synthetic claim failure" not in caplog.text
    assert "error_type=ConnectionError" in caplog.text
    runtime.close()
