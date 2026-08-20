"""Atomic terminal guard and restart-safe active job supervision tests."""

from __future__ import annotations

import fnmatch
import json
from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from redis import exceptions as redis_exceptions
from tributo.integrations.broker import Message, TaskDisposition

from tributo_broker_redis.active_jobs import (
    ActiveJobRecord,
    ActiveJobStore,
    TerminalCandidateStore,
)
from tributo_broker_redis.config import RedisBrokerConfig
from tributo_broker_redis.protocol import (
    MAX_EVENT_TIMESTAMP,
    MAX_JOB_ID_LENGTH,
    MIN_TERMINAL_EVENT_BYTES,
    event_payload,
    validate_terminal_event,
)
from tributo_broker_redis.reporter import RedisEventReporter
from tributo_broker_redis.runtime import RedisBrokerRuntime
from tributo_broker_redis.supervisor import ActiveJobSupervisor
from tributo_broker_redis.terminal_guard import (
    TERMINAL_EVENT_TYPES,
    TerminalGuard,
    assert_single_key_lua,
)


class StatefulRedis:
    """Small Redis test double implementing the two provider Lua scripts."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.streams: dict[str, list[tuple[str, dict[str, str]]]] = {}
        self.publish_failures = 0
        self.active_set_failures = 0
        self.candidate_set_failures = 0
        self.eval_key_counts: list[int] = []
        self.expired: list[tuple[str, int]] = []
        self.scan_keys_override: list[str] | None = None

    def set(self, key: str, value: str, *, ex: int, nx: bool = False) -> bool:
        del ex
        if ":active:" in key and self.active_set_failures:
            self.active_set_failures -= 1
            raise ConnectionError("active record unavailable")
        if ":terminal-candidate:" in key and self.candidate_set_failures:
            self.candidate_set_failures -= 1
            raise ConnectionError("candidate unavailable")
        if nx and key in self.values:
            return False
        self.values[key] = value
        return True

    def get(self, key: str) -> str | None:
        return self.values.get(key)

    def delete(self, key: str) -> int:
        return int(self.values.pop(key, None) is not None)

    def exists(self, key: str) -> int:
        return int(key in self.values)

    def scan_iter(self, *, match: str, count: int) -> Iterator[str]:
        del count
        if self.scan_keys_override is not None:
            yield from self.scan_keys_override
            return
        yield from sorted(key for key in self.values if fnmatch.fnmatch(key, match))

    def expire(self, key: str, seconds: int) -> bool:
        self.expired.append((key, seconds))
        return key in self.values

    def xrevrange(
        self, key: str, *, max: str, min: str, count: int
    ) -> list[tuple[str, dict[str, str]]]:
        del max, min
        return list(reversed(self.streams.get(key, [])))[:count]

    def eval(self, script: str, key_count: int, *args: str) -> Any:
        self.eval_key_counts.append(key_count)
        if "XADD" in script:
            return self._publish(*args)
        if "STAGE_TERMINAL_CANDIDATE" in script:
            key, encoded, _ttl, job_id, _max_timestamp, _max_duration = args
            if self.candidate_set_failures:
                self.candidate_set_failures -= 1
                raise ConnectionError("candidate unavailable")
            raw = self.values.get(key)
            if raw is not None:
                try:
                    existing = json.loads(raw)
                except json.JSONDecodeError:
                    existing = None
                if isinstance(existing, dict):
                    try:
                        validate_terminal_event(existing, job_id)
                    except ValueError:
                        pass
                    else:
                        return raw
            self.values[key] = encoded
            return encoded
        if "local incoming" in script:
            key, encoded, _ttl = args
            if self.active_set_failures:
                self.active_set_failures -= 1
                raise ConnectionError("active record unavailable")
            incoming = json.loads(encoded)
            raw = self.values.get(key)
            if raw is not None:
                existing = json.loads(raw)
                phase = existing.get("current_phase")
                if phase not in (None, "", "QUEUED"):
                    incoming["current_phase"] = phase
            self.values[key] = json.dumps(incoming, separators=(",", ":"))
            return incoming["current_phase"]
        if "record.current_phase" in script:
            key, phase, _ttl = args
            raw = self.values.get(key)
            if raw is None:
                record = {"placeholder": True, "current_phase": phase}
            else:
                record = json.loads(raw)
            record["current_phase"] = phase
            self.values[key] = json.dumps(record, separators=(",", ":"))
            return 1
        raise AssertionError("unknown Lua script")

    def _publish(
        self,
        stream_key: str,
        job_id: str,
        encoded: str,
        event_type: str,
        phase: str,
        max_length: str,
    ) -> list[str]:
        if self.publish_failures:
            self.publish_failures -= 1
            raise ConnectionError("XADD unavailable")
        existing = [
            json.loads(fields["payload"])
            for _, fields in self.streams.get(stream_key, [])
            if fields["job_id"] == job_id and job_id
        ]
        terminal_seen = any(
            event.get("event_type") in TERMINAL_EVENT_TYPES for event in existing
        )
        if terminal_seen:
            return [
                (
                    "terminal_exists"
                    if event_type in TERMINAL_EVENT_TYPES
                    else "rejected_after_terminal"
                ),
                "",
            ]
        if event_type == "PHASE" and any(
            event.get("event_type") == "PHASE" and event.get("phase") == phase
            for event in existing
        ):
            return ["duplicate_phase", ""]
        event_id = f"{len(self.streams.get(stream_key, [])) + 1}-0"
        events = self.streams.setdefault(stream_key, [])
        events.append((event_id, {"job_id": job_id, "payload": encoded}))
        overflow = len(events) - int(max_length)
        if overflow > 0:
            del events[:overflow]
        return ["published", event_id]

    def events(self, config: RedisBrokerConfig, job_id: str) -> list[dict[str, Any]]:
        return [
            json.loads(fields["payload"])
            for _, fields in self.streams.get(config.event_stream_key(job_id), [])
        ]


def _record(
    config: RedisBrokerConfig,
    *,
    job_id: str = "job-1",
    deadline_at: float | None = None,
) -> ActiveJobRecord:
    return ActiveJobRecord(
        job_id=job_id,
        run_id=job_id,
        attempt_id="attempt-1",
        submission_id=f"submission-{job_id}",
        execution_id=f"ray-{job_id}",
        submitted_at=100.0,
        deadline_at=deadline_at,
        current_phase="TRAINING",
        request_metadata={"model_id": "model-1"},
        candidate_ref=config.terminal_candidate_key(job_id),
    )


def _legacy_message(job_id: str = "job-1") -> Message:
    return Message(
        job_id,
        {
            "raw": json.dumps(
                {
                    "protocol_version": "2.0",
                    "training_config": {"data": {"type": "csv"}},
                }
            )
        },
        delivery_id="7-0",
    )


def _terminal_payload(event_type: str) -> dict[str, Any]:
    common: dict[str, Any] = {"duration_seconds": 1.23456}
    if event_type == "COMPLETED":
        return {
            **common,
            "result_summary": {"status": "success"},
            "training_result": {"algorithm_key": "xgboost"},
            "artifact_manifest": {"model_id": "model-1"},
        }
    if event_type == "FAILED":
        return {
            **common,
            "phase": "TRAINING",
            "error_code": "TRAINING_FAILED",
            "error_message": "controlled failure",
        }
    if event_type == "CANCELLED":
        return {
            **common,
            "phase": "TRAINING",
            "has_best_model": False,
        }
    raise AssertionError(f"unsupported terminal event type: {event_type}")


def _encoded_terminal(event_type: str, job_id: str = "job-1") -> str:
    payload = _terminal_payload(event_type)
    payload["duration_seconds"] = 1.235
    return json.dumps(
        event_payload(
            job_id=job_id,
            event_type=event_type,
            payload={"timestamp": 1, **payload},
        ),
        separators=(",", ":"),
    )


def test_lua_guard_uses_one_key_and_deduplicates_phase() -> None:
    redis = StatefulRedis()
    config = RedisBrokerConfig()
    reporter = RedisEventReporter(redis, config, "job-1")

    reporter.report_phase("job-1", "QUEUED")
    reporter.report_phase("job-1", "QUEUED")

    assert [event["phase"] for event in redis.events(config, "job-1")] == ["QUEUED"]
    assert redis.eval_key_counts == [1, 1, 1, 1]
    assert_single_key_lua()


def test_lua_guard_checks_decoded_payload_is_a_table() -> None:
    from tributo_broker_redis import terminal_guard

    assert "type(existing) == 'table'" in terminal_guard._PUBLISH_EVENT_LUA


@pytest.mark.parametrize(
    ("first", "second"),
    [("COMPLETED", "FAILED"), ("CANCELLED", "COMPLETED")],
)
def test_worker_watchdog_and_cancel_completion_races_emit_one_terminal(
    first: str, second: str
) -> None:
    redis = StatefulRedis()
    config = RedisBrokerConfig()
    reporter_a = RedisEventReporter(redis, config, "job-1")
    reporter_b = RedisEventReporter(redis, config, "job-1")

    assert reporter_a._publish("job-1", first, _terminal_payload(first))
    assert reporter_b._publish("job-1", second, _terminal_payload(second))
    assert reporter_b._publish("job-1", "LOG", {"message": "too late"}) is False

    events = redis.events(config, "job-1")
    assert [event["event_type"] for event in events] == [first]


def test_candidate_survives_xadd_failure_and_supervisor_replays_after_restart() -> None:
    redis = StatefulRedis()
    config = RedisBrokerConfig(max_publish_retries=0, supervisor_interval_seconds=0)
    ActiveJobStore(redis, config).save(_record(config))
    redis.publish_failures = 1
    reporter = RedisEventReporter(redis, config, "job-1")

    assert (
        reporter.report_completed_payload("job-1", _terminal_payload("COMPLETED"))
        is False
    )
    assert redis.get(config.terminal_candidate_key("job-1")) is not None
    ray = MagicMock()
    ray.get_job_status.return_value = "SUCCEEDED"

    # A new supervisor instance proves recovery does not rely on memory.
    ActiveJobSupervisor(
        redis,
        config,
        ray_client_factory=lambda _url: ray,
    ).maintain()

    assert [event["event_type"] for event in redis.events(config, "job-1")] == [
        "COMPLETED"
    ]
    assert ActiveJobStore(redis, config).load("job-1") is None
    assert redis.get(config.terminal_candidate_key("job-1")) is None


def test_first_terminal_candidate_wins_without_unconditional_overwrite() -> None:
    redis = StatefulRedis()
    config = RedisBrokerConfig()
    candidates = TerminalCandidateStore(redis, config)
    completed = _encoded_terminal("COMPLETED")
    failed = _encoded_terminal("FAILED")

    assert candidates.save("job-1", completed) == completed
    assert candidates.save("job-1", failed) == completed
    assert candidates.load("job-1") == completed


@pytest.mark.parametrize(
    "poison",
    [
        {"protocol_version": "2.0", "event_type": "FAILED", "job_id": "job-1"},
        {
            "protocol_version": "2.0",
            "event_type": "CANCELLED",
            "job_id": "job-1",
            "timestamp": 1,
            "duration_seconds": "1.0",
            "phase": "TRAINING",
            "has_best_model": False,
        },
        {
            "protocol_version": "1.0",
            "event_type": "COMPLETED",
            "job_id": "job-1",
            "timestamp": 1,
            **_terminal_payload("COMPLETED"),
        },
    ],
)
def test_invalid_existing_candidate_is_atomically_replaced(
    poison: dict[str, Any],
) -> None:
    redis = StatefulRedis()
    config = RedisBrokerConfig()
    candidates = TerminalCandidateStore(redis, config)
    redis.values[config.terminal_candidate_key("job-1")] = json.dumps(poison)
    failed = _encoded_terminal("FAILED")

    assert candidates.save("job-1", failed) == failed
    assert candidates.load("job-1") == failed


def test_incomplete_new_candidate_fails_closed_before_redis() -> None:
    redis = StatefulRedis()
    config = RedisBrokerConfig()
    incomplete = json.dumps(
        event_payload(job_id="job-1", event_type="FAILED", payload={"timestamp": 1})
    )

    with pytest.raises(ValueError, match="duration_seconds"):
        TerminalCandidateStore(redis, config).save("job-1", incomplete)

    assert config.terminal_candidate_key("job-1") not in redis.values


def test_candidate_rejects_timestamp_above_shared_wire_limit() -> None:
    redis = StatefulRedis()
    config = RedisBrokerConfig()
    event = json.loads(_encoded_terminal("FAILED"))
    event["timestamp"] = MAX_EVENT_TIMESTAMP + 1

    with pytest.raises(ValueError, match="timestamp"):
        TerminalCandidateStore(redis, config).save("job-1", json.dumps(event))


def test_candidate_duration_is_normalized_before_supervisor_replay() -> None:
    redis = StatefulRedis()
    config = RedisBrokerConfig(supervisor_interval_seconds=0)
    ActiveJobStore(redis, config).save(_record(config))
    event = json.loads(_encoded_terminal("COMPLETED"))
    event["duration_seconds"] = 1.23456
    staged = TerminalCandidateStore(redis, config).save(
        "job-1", json.dumps(event, indent=2)
    )

    assert json.loads(staged)["duration_seconds"] == 1.235
    ray = MagicMock()
    ActiveJobSupervisor(redis, config, ray_client_factory=lambda _url: ray).maintain()

    replayed = redis.events(config, "job-1")[-1]
    assert replayed["event_type"] == "COMPLETED"
    assert replayed["duration_seconds"] == 1.235
    assert set(replayed) >= {
        "protocol_version",
        "event_type",
        "job_id",
        "timestamp",
        "duration_seconds",
        "result_summary",
        "training_result",
        "artifact_manifest",
    }
    ray.get_job_status.assert_not_called()


def test_candidate_storage_outage_eventually_becomes_controlled_failed() -> None:
    redis = StatefulRedis()
    config = RedisBrokerConfig(max_publish_retries=0, supervisor_interval_seconds=0)
    ActiveJobStore(redis, config).save(_record(config))
    redis.candidate_set_failures = 1

    assert (
        RedisEventReporter(redis, config, "job-1").report_completed_payload(
            "job-1", _terminal_payload("COMPLETED")
        )
        is False
    )
    ray = MagicMock()
    ray.get_job_status.return_value = "FAILED"
    ActiveJobSupervisor(redis, config, ray_client_factory=lambda _url: ray).maintain()

    terminal = redis.events(config, "job-1")[-1]
    assert terminal["event_type"] == "FAILED"
    assert terminal["error_code"] == "TRAINING_FAILED"


def test_timeout_and_worker_failed_race_keeps_timeout_terminal() -> None:
    redis = StatefulRedis()
    config = RedisBrokerConfig(supervisor_interval_seconds=0)
    ActiveJobStore(redis, config).save(_record(config, deadline_at=101.0))
    ray = MagicMock()
    ActiveJobSupervisor(
        redis,
        config,
        ray_client_factory=lambda _url: ray,
        wall_clock=lambda: 102.0,
    ).maintain()

    reporter = RedisEventReporter(redis, config, "job-1")
    assert reporter.report_failed_with_code("job-1", "worker failed")
    terminals = [
        event
        for event in redis.events(config, "job-1")
        if event["event_type"] in TERMINAL_EVENT_TYPES
    ]
    assert len(terminals) == 1
    assert terminals[0]["error_code"] == "TRAINING_TIMEOUT"
    ray.stop_job.assert_called_once_with("ray-job-1")


@pytest.mark.parametrize(
    ("status", "expected_terminal"),
    [
        ("SUCCEEDED", "FAILED"),
        ("FAILED", "FAILED"),
        ("STOPPED", "CANCELLED"),
        ("PENDING", None),
        ("RUNNING", None),
    ],
)
def test_stop_false_reconciles_ray_status_before_terminal_decision(
    status: str, expected_terminal: str | None
) -> None:
    redis = StatefulRedis()
    config = RedisBrokerConfig(supervisor_interval_seconds=0)
    ActiveJobStore(redis, config).save(_record(config))
    redis.values[config.cancel_key("job-1")] = "1"
    ray = MagicMock()
    ray.stop_job.return_value = False
    ray.get_job_status.return_value = status

    ActiveJobSupervisor(redis, config, ray_client_factory=lambda _url: ray).maintain()

    terminals = [
        event["event_type"]
        for event in redis.events(config, "job-1")
        if event["event_type"] in TERMINAL_EVENT_TYPES
    ]
    assert terminals == ([] if expected_terminal is None else [expected_terminal])
    assert (ActiveJobStore(redis, config).load("job-1") is not None) is (
        expected_terminal is None
    )


def test_stop_false_rechecks_candidate_created_during_stop_race() -> None:
    redis = StatefulRedis()
    config = RedisBrokerConfig(supervisor_interval_seconds=0)
    ActiveJobStore(redis, config).save(_record(config))
    redis.values[config.cancel_key("job-1")] = "1"
    candidate = _encoded_terminal("COMPLETED")
    ray = MagicMock()

    def stop_job(_execution_id: str) -> bool:
        TerminalCandidateStore(redis, config).save("job-1", candidate)
        return False

    ray.stop_job.side_effect = stop_job
    ActiveJobSupervisor(redis, config, ray_client_factory=lambda _url: ray).maintain()

    assert redis.events(config, "job-1")[-1]["event_type"] == "COMPLETED"
    ray.get_job_status.assert_not_called()


def test_timeout_stop_false_and_stopped_status_emits_timeout_failed() -> None:
    redis = StatefulRedis()
    config = RedisBrokerConfig(supervisor_interval_seconds=0)
    ActiveJobStore(redis, config).save(_record(config, deadline_at=101.0))
    ray = MagicMock()
    ray.stop_job.return_value = False
    ray.get_job_status.return_value = "STOPPED"

    ActiveJobSupervisor(
        redis,
        config,
        ray_client_factory=lambda _url: ray,
        wall_clock=lambda: 102.0,
    ).maintain()

    terminal = redis.events(config, "job-1")[-1]
    assert terminal["event_type"] == "FAILED"
    assert terminal["error_code"] == "TRAINING_TIMEOUT"


@pytest.mark.parametrize(
    "error",
    [
        ConnectionError("dashboard unavailable"),
        RuntimeError("Request failed with status code 503: unavailable"),
    ],
)
def test_ray_status_outage_retains_active_record_for_retry(
    error: BaseException,
) -> None:
    redis = StatefulRedis()
    config = RedisBrokerConfig(supervisor_interval_seconds=0)
    ActiveJobStore(redis, config).save(_record(config))
    ray = MagicMock()
    ray.get_job_status.side_effect = error
    supervisor = ActiveJobSupervisor(redis, config, ray_client_factory=lambda _url: ray)

    with pytest.raises(type(error)):
        supervisor.maintain()

    assert ActiveJobStore(redis, config).load("job-1") is not None
    assert redis.events(config, "job-1") == []


@pytest.mark.parametrize(
    "error_type",
    [
        redis_exceptions.ConnectionError,
        redis_exceptions.TimeoutError,
        redis_exceptions.ReadOnlyError,
        redis_exceptions.MasterDownError,
        redis_exceptions.ClusterDownError,
        redis_exceptions.TryAgainError,
        redis_exceptions.ClusterError,
        redis_exceptions.SlotNotCoveredError,
    ],
)
def test_redis_topology_errors_are_global_transients(
    error_type: type[BaseException],
) -> None:
    assert ActiveJobSupervisor._is_global_transient(error_type("redis unavailable"))


def test_generic_redis_response_error_is_not_global_transient() -> None:
    assert not ActiveJobSupervisor._is_global_transient(
        redis_exceptions.ResponseError("WRONGTYPE isolated bad record")
    )


@pytest.mark.parametrize(
    "error",
    [
        redis_exceptions.AskError("1 127.0.0.1:6379"),
        redis_exceptions.MovedError("1 127.0.0.1:6379"),
    ],
)
def test_cluster_redirect_errors_are_global_transients(error: BaseException) -> None:
    assert ActiveJobSupervisor._is_global_transient(error)


def test_cross_slot_error_is_not_global_transient() -> None:
    assert not ActiveJobSupervisor._is_global_transient(
        redis_exceptions.CrossSlotTransactionError("keys map to different slots")
    )


@pytest.mark.parametrize(
    ("ray_result", "candidate_type"),
    [
        ("SUCCEEDED", "COMPLETED"),
        ("FAILED", "FAILED"),
        ("STOPPED", "CANCELLED"),
        (RuntimeError("Ray job does not exist"), "COMPLETED"),
        (RuntimeError("Ray request returned 404"), "CANCELLED"),
    ],
)
def test_candidate_created_during_status_query_wins_terminal_race(
    ray_result: str | BaseException,
    candidate_type: str,
) -> None:
    redis = StatefulRedis()
    config = RedisBrokerConfig(supervisor_interval_seconds=0)
    ActiveJobStore(redis, config).save(_record(config))
    ray = MagicMock()
    candidate = _encoded_terminal(candidate_type)

    def status_side_effect(_execution_id: str) -> str:
        TerminalCandidateStore(redis, config).save("job-1", candidate)
        if isinstance(ray_result, BaseException):
            raise ray_result
        return ray_result

    ray.get_job_status.side_effect = status_side_effect

    ActiveJobSupervisor(redis, config, ray_client_factory=lambda _url: ray).maintain()

    terminals = [
        event
        for event in redis.events(config, "job-1")
        if event["event_type"] in TERMINAL_EVENT_TYPES
    ]
    assert [event["event_type"] for event in terminals] == [candidate_type]
    assert ActiveJobStore(redis, config).load("job-1") is None


def test_bad_active_record_isolated_from_healthy_job() -> None:
    redis = StatefulRedis()
    config = RedisBrokerConfig(supervisor_interval_seconds=0)
    bad_key = config.active_job_key("bad-job")
    redis.values[bad_key] = "not-json"
    ActiveJobStore(redis, config).save(_record(config, job_id="healthy-job"))
    ray = MagicMock()
    ray.get_job_status.return_value = "FAILED"

    ActiveJobSupervisor(redis, config, ray_client_factory=lambda _url: ray).maintain()

    assert redis.events(config, "healthy-job")[-1]["event_type"] == "FAILED"
    assert bad_key not in redis.values


def test_bad_candidate_isolated_and_converted_to_controlled_terminal() -> None:
    redis = StatefulRedis()
    config = RedisBrokerConfig(supervisor_interval_seconds=0)
    ActiveJobStore(redis, config).save(_record(config, job_id="bad-candidate"))
    ActiveJobStore(redis, config).save(_record(config, job_id="healthy-job"))
    redis.values[config.terminal_candidate_key("bad-candidate")] = "not-json"
    ray = MagicMock()
    ray.get_job_status.return_value = "FAILED"

    ActiveJobSupervisor(redis, config, ray_client_factory=lambda _url: ray).maintain()

    assert redis.events(config, "bad-candidate")[-1]["event_type"] == "FAILED"
    assert redis.events(config, "healthy-job")[-1]["event_type"] == "FAILED"


def test_unknown_and_not_found_jobs_do_not_starve_later_healthy_job() -> None:
    redis = StatefulRedis()
    config = RedisBrokerConfig(supervisor_interval_seconds=0)
    for job_id in ("a-unknown", "b-not-found", "c-healthy"):
        ActiveJobStore(redis, config).save(_record(config, job_id=job_id))
    ray = MagicMock()
    ray.get_job_status.side_effect = [
        "MYSTERY",
        RuntimeError("Ray job not found"),
        "FAILED",
    ]

    ActiveJobSupervisor(redis, config, ray_client_factory=lambda _url: ray).maintain()

    for job_id in ("a-unknown", "b-not-found", "c-healthy"):
        assert redis.events(config, job_id)[-1]["event_type"] == "FAILED"


def test_pending_job_refreshes_active_record_ttl() -> None:
    redis = StatefulRedis()
    config = RedisBrokerConfig(supervisor_interval_seconds=0)
    ActiveJobStore(redis, config).save(_record(config))
    ray = MagicMock()
    ray.get_job_status.return_value = "RUNNING"

    ActiveJobSupervisor(redis, config, ray_client_factory=lambda _url: ray).maintain()

    assert redis.expired == [
        (config.active_job_key("job-1"), config.active_job_ttl_seconds)
    ]


def test_scan_budget_counts_stale_examined_keys() -> None:
    redis = StatefulRedis()
    config = RedisBrokerConfig(
        supervisor_interval_seconds=0,
        supervisor_scan_count=2,
    )
    ActiveJobStore(redis, config).save(_record(config, job_id="healthy-job"))
    redis.scan_keys_override = [
        config.active_job_key("stale-a"),
        config.active_job_key("stale-b"),
        config.active_job_key("healthy-job"),
    ]
    ray = MagicMock()
    ray.get_job_status.return_value = "FAILED"
    supervisor = ActiveJobSupervisor(redis, config, ray_client_factory=lambda _url: ray)

    supervisor.maintain()

    ray.get_job_status.assert_not_called()
    assert ActiveJobStore(redis, config).load("healthy-job") is not None


def test_runtime_maintenance_delegates_one_supervisor_tick() -> None:
    runtime = RedisBrokerRuntime.__new__(RedisBrokerRuntime)
    runtime._supervisor = MagicMock()

    runtime.maintain()

    runtime._supervisor.maintain.assert_called_once_with()


def test_runtime_maintenance_redacts_dashboard_failure_before_runner_logging() -> None:
    runtime = RedisBrokerRuntime.__new__(RedisBrokerRuntime)
    runtime._supervisor = MagicMock()
    runtime._supervisor.maintain.side_effect = ConnectionError(
        "Authorization: Bearer dashboard-secret"
    )

    with pytest.raises(RuntimeError) as failure:
        runtime.maintain()

    assert "dashboard-secret" not in str(failure.value)
    assert "[REDACTED]" in str(failure.value)


def test_submit_success_record_failure_retries_and_reconciles_on_redelivery() -> None:
    redis = StatefulRedis()
    redis.active_set_failures = 1
    config = RedisBrokerConfig(allow_legacy_training_config=True)
    runtime = RedisBrokerRuntime.__new__(RedisBrokerRuntime)
    runtime.config = config
    runtime._redis = redis
    runtime._consumer = MagicMock()
    submission = MagicMock(
        run_id="job-1",
        attempt_id="attempt-1",
        job_id="ray-job-1",
        submission_id="submission-1",
    )

    with patch(
        "tributo_broker_redis.runtime.submit_training_job_with_identity",
        return_value=submission,
    ) as submit:
        first = runtime.handle(_legacy_message())
        second = runtime.handle(_legacy_message())

    assert first.disposition is TaskDisposition.RETRY
    assert second.disposition is TaskDisposition.ACK
    assert submit.call_count == 2
    assert [event["event_type"] for event in redis.events(config, "job-1")] == ["PHASE"]
    assert ActiveJobStore(redis, config).load("job-1") is not None


def test_new_task_is_not_submitted_until_queued_phase_is_durable() -> None:
    redis = StatefulRedis()
    redis.publish_failures = 1
    config = RedisBrokerConfig(allow_legacy_training_config=True, max_publish_retries=0)
    runtime = RedisBrokerRuntime.__new__(RedisBrokerRuntime)
    runtime.config = config
    runtime._redis = redis
    runtime._consumer = MagicMock()

    with patch(
        "tributo_broker_redis.runtime.submit_training_job_with_identity"
    ) as submit:
        outcome = runtime.handle(_legacy_message())

    assert outcome.disposition is TaskDisposition.RETRY
    submit.assert_not_called()
    assert ActiveJobStore(redis, config).load("job-1") is None


def test_ack_redelivery_with_active_record_does_not_queue_or_submit_again() -> None:
    redis = StatefulRedis()
    config = RedisBrokerConfig(allow_legacy_training_config=True)
    runtime = RedisBrokerRuntime.__new__(RedisBrokerRuntime)
    runtime.config = config
    runtime._redis = redis
    runtime._consumer = MagicMock()
    submission = MagicMock(
        run_id="job-1",
        attempt_id="attempt-1",
        job_id="ray-job-1",
        submission_id="submission-1",
    )

    with patch(
        "tributo_broker_redis.runtime.submit_training_job_with_identity",
        return_value=submission,
    ) as submit:
        assert runtime.handle(_legacy_message()).disposition is TaskDisposition.ACK
        assert runtime.handle(_legacy_message()).disposition is TaskDisposition.ACK

    assert submit.call_count == 1
    assert [event["event_type"] for event in redis.events(config, "job-1")] == ["PHASE"]


def test_active_cancel_is_left_for_supervisor_to_stop_existing_ray_job() -> None:
    redis = StatefulRedis()
    config = RedisBrokerConfig(
        allow_legacy_training_config=True, supervisor_interval_seconds=0
    )
    ActiveJobStore(redis, config).save(_record(config))
    redis.values[config.cancel_key("job-1")] = "1"
    runtime = RedisBrokerRuntime.__new__(RedisBrokerRuntime)
    runtime.config = config
    runtime._redis = redis
    runtime._consumer = MagicMock()

    with patch(
        "tributo_broker_redis.runtime.submit_training_job_with_identity"
    ) as submit:
        assert runtime.handle(_legacy_message()).disposition is TaskDisposition.ACK
    assert redis.events(config, "job-1") == []
    submit.assert_not_called()

    ray = MagicMock()
    ActiveJobSupervisor(redis, config, ray_client_factory=lambda _url: ray).maintain()
    assert redis.events(config, "job-1")[-1]["event_type"] == "CANCELLED"
    ray.stop_job.assert_called_once_with("ray-job-1")


def test_oversized_completed_becomes_compact_controlled_failed() -> None:
    redis = StatefulRedis()
    config = RedisBrokerConfig(max_event_bytes=MIN_TERMINAL_EVENT_BYTES)
    job_id = "j" * MAX_JOB_ID_LENGTH
    reporter = RedisEventReporter(redis, config, job_id)

    assert reporter.report_completed_payload(job_id, {"blob": "x" * 2000})

    stream_fields = redis.streams[config.event_stream_key(job_id)][-1][1]
    event = json.loads(stream_fields["payload"])
    assert event["event_type"] == "FAILED"
    assert event["error_code"] == "PAYLOAD_TOO_LARGE"
    assert event["phase"] == "EVALUATING"
    assert isinstance(event["duration_seconds"], float)
    assert set(event) == {
        "protocol_version",
        "event_type",
        "job_id",
        "timestamp",
        "phase",
        "error_code",
        "error_message",
        "duration_seconds",
    }
    assert len(stream_fields["payload"].encode("utf-8")) <= MIN_TERMINAL_EVENT_BYTES
    assert event["duration_seconds"] == round(event["duration_seconds"], 3)


def test_successful_phase_updates_durable_active_record_best_effort() -> None:
    redis = StatefulRedis()
    config = RedisBrokerConfig()
    ActiveJobStore(redis, config).save(_record(config))

    RedisEventReporter(redis, config, "job-1").report_phase("job-1", "EVALUATING")

    record = ActiveJobStore(redis, config).load("job-1")
    assert record is not None
    assert record.current_phase == "EVALUATING"


def test_phase_published_before_active_registration_is_merged_without_regression() -> (
    None
):
    redis = StatefulRedis()
    config = RedisBrokerConfig()

    RedisEventReporter(redis, config, "job-1").report_phase("job-1", "LOADING_DATA")
    ActiveJobStore(redis, config).save(_record(config))

    record = ActiveJobStore(redis, config).load("job-1")
    assert record is not None
    assert record.current_phase == "LOADING_DATA"


def test_terminal_lookup_is_scoped_to_job_on_shared_invalid_stream() -> None:
    redis = StatefulRedis()
    guard = TerminalGuard(redis, max_stream_length=100)
    redis.streams["shared"] = [
        (
            "1-0",
            {
                "job_id": "job-a",
                "payload": json.dumps({"job_id": "job-a", "event_type": "FAILED"}),
            },
        )
    ]

    assert guard.terminal_event("shared", "job-a") is not None
    assert guard.terminal_event("shared", "job-b") is None
