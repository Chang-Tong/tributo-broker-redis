from __future__ import annotations

import base64
import json
import sys
from types import SimpleNamespace
from typing import Any, cast

import pytest
from conftest import FakeRedis, inference_request, training_config
from tributo.ray_jobs import RayJobSubmission

import tributo_broker_redis.execution_driver as execution_driver
from tributo_broker_redis.cancellation import (
    ActiveSubmission,
    ActiveSubmissionMap,
    CancelWatcher,
)
from tributo_broker_redis.config import OperationType, RedisBrokerConfig
from tributo_broker_redis.execution_driver import (
    CredentialUnavailable,
    _load_driver_input,
    _resolve_credential_reference,
)
from tributo_broker_redis.operations import PreparedOperation
from tributo_broker_redis.protocol import DriverInput, GenericRequest
from tributo_broker_redis.reporter import RedisEventReporter, redact
from tributo_broker_redis.runtime import RedisBrokerRuntime


def _request(
    operation_id: str,
    operation_type: str,
    profile: str,
    spec: dict[str, Any],
) -> str:
    return json.dumps(
        {
            "protocol_profile": "tributo-generic-v1",
            "protocol_version": "1.0",
            "operation_id": operation_id,
            "operation_type": operation_type,
            "execution_profile": profile,
            "run_id": f"run-{operation_id}",
            "attempt_id": "attempt-1",
            "request_digest": "d" * 64,
            "spec": spec,
        },
        separators=(",", ":"),
    )


class SubmissionRecorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def __call__(self, entrypoint: str, **kwargs: Any) -> RayJobSubmission:
        self.calls.append((entrypoint, kwargs))
        return RayJobSubmission(
            run_id=kwargs["run_id"],
            attempt_id=kwargs["attempt_id"],
            submission_id=f"submission-{len(self.calls)}",
            ray_job_id="ray-job-1",
            request_digest=kwargs["request_digest"],
        )


def test_runtime_accepts_thin_protocol_and_driver_hooks(
    config: RedisBrokerConfig,
    fake_redis: FakeRedis,
) -> None:
    operation_id = "knova-training-1"
    fake_redis.messages["tasks:training"] = [
        (
            "1-0",
            {
                "operation_id": operation_id,
                "payload": '{"protocol_version":"2.0","job_id":"knova-training-1"}',
            },
        )
    ]
    parsed_payloads: list[str] = []

    def parse_knova(
        raw_payload: str,
        *,
        outer_operation_id: str,
        expected_operation_type: OperationType,
    ) -> GenericRequest:
        parsed_payloads.append(raw_payload)
        return GenericRequest(
            protocol_profile="tributo-generic-v1",
            protocol_version="1.0",
            operation_id=outer_operation_id,
            operation_type=expected_operation_type,
            execution_profile="distributed",
            run_id=outer_operation_id,
            request_digest="d" * 64,
            spec={"knova_request": {"job_id": outer_operation_id}},
        )

    def prepare_knova(request: GenericRequest) -> PreparedOperation:
        return PreparedOperation(
            operation_payload=dict(request.spec),
            credential_ref=None,
        )

    submitter = SubmissionRecorder()
    runtime = RedisBrokerRuntime(
        config,
        submitter=submitter,
        redis_client=fake_redis,
        request_parser=parse_knova,
        operation_preparer=prepare_knova,
        driver_entrypoint="python -m tributo_knova.execution_driver",
    )

    assert runtime.run_once(timeout_ms=0) is True
    assert parsed_payloads == [
        '{"protocol_version":"2.0","job_id":"knova-training-1"}'
    ]
    assert submitter.calls[0][0] == "python -m tributo_knova.execution_driver"
    assert fake_redis.acked == [("tasks:training", "group:training", "1-0")]


@pytest.mark.parametrize(
    ("operation_type", "stream", "spec"),
    [
        (
            "training",
            "tasks:training",
            {"algorithm": "xgboost", "config": training_config(1)},
        ),
        (
            "batch_inference",
            "tasks:inference",
            {"profile": "bundle-backed", "request": inference_request(1)},
        ),
    ],
)
def test_runtime_consumes_each_channel_admits_one_driver_and_acks(
    config: RedisBrokerConfig,
    fake_redis: FakeRedis,
    operation_type: str,
    stream: str,
    spec: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation_id = f"operation-{operation_type}"
    secret_value = "opaque-runtime-secret-value"
    monkeypatch.setenv("PREPROVISIONED_RUNTIME_CREDENTIAL", secret_value)
    spec = json.loads(json.dumps(spec))
    spec["credential_ref"] = "env:PREPROVISIONED_RUNTIME_CREDENTIAL"
    fake_redis.messages[stream] = [
        (
            "1-0",
            {
                "operation_id": operation_id,
                "payload": _request(
                    operation_id, operation_type, "single_worker", spec
                ),
            },
        )
    ]
    submitter = SubmissionRecorder()
    runtime = RedisBrokerRuntime(
        config,
        submitter=submitter,
        redis_client=fake_redis,
    )

    assert runtime.run_once(timeout_ms=0) is True
    assert fake_redis.acked[-1][0] == stream
    assert len(submitter.calls) == 1
    entrypoint, kwargs = submitter.calls[0]
    assert entrypoint == "python -m tributo_broker_redis.execution_driver"
    driver_input = json.loads(
        base64.urlsafe_b64decode(
            kwargs["env_vars"]["TRIBUTO_REDIS_DRIVER_INPUT_B64"]
        ).decode()
    )
    assert driver_input["operation_type"] == operation_type
    assert driver_input["credential_ref"] == "env:PREPROVISIONED_RUNTIME_CREDENTIAL"
    assert driver_input["run_id"] == f"run-{operation_id}"
    assert kwargs["run_id"] == driver_input["run_id"]
    assert kwargs["operation_namespace"] == f"redis-{operation_type.replace('_', '-')}"
    assert "submission_id" not in driver_input
    assert secret_value not in json.dumps(kwargs, default=str)
    active = runtime.active_submissions.get(operation_id)
    assert active is not None
    event_stream = config.channels.for_operation(
        cast(OperationType, operation_type)
    ).event_stream_key(operation_id)
    accepted = json.loads(fake_redis.events[event_stream][-1]["payload"])
    assert accepted["event_type"] == "ACCEPTED"
    assert accepted["submission_id"] == active.submission.submission_id
    runtime.close()


def test_driver_input_requires_matching_run_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = DriverInput(
        operation_id="operation-1",
        operation_type="training",
        execution_profile="single_worker",
        run_id="run-1",
        attempt_id="attempt-1",
        operation_payload={"training_config": training_config(1)},
        redis_url="redis://redis:6379/0",
        event_stream_prefix="events:training",
        outer_identity_field="operation_id",
        max_event_bytes=1024,
        max_stream_length=100,
    )
    encoded = base64.urlsafe_b64encode(value.model_dump_json().encode()).decode()
    monkeypatch.setenv("TRIBUTO_REDIS_DRIVER_INPUT_B64", encoded)
    monkeypatch.setenv("TRIBUTO_SUBMISSION_ID", "submission-1")
    monkeypatch.setenv("TRIBUTO_ATTEMPT_ID", value.attempt_id)
    monkeypatch.setenv("TRIBUTO_RUN_ID", value.run_id)
    assert _load_driver_input() == (value, "submission-1")

    monkeypatch.setenv("TRIBUTO_RUN_ID", "different-run")
    with pytest.raises(ValueError, match="run identity mismatch"):
        _load_driver_input()


@pytest.mark.parametrize("serialization_fails", [False, True])
def test_terminal_serialization_or_publication_failure_is_not_execution_failure(
    monkeypatch: pytest.MonkeyPatch,
    serialization_fails: bool,
) -> None:
    value = DriverInput(
        operation_id="operation-1",
        operation_type="training",
        execution_profile="single_worker",
        run_id="run-1",
        attempt_id="attempt-1",
        operation_payload={"training_config": training_config(1)},
        redis_url="redis://redis:6379/0",
        event_stream_prefix="events:training",
        outer_identity_field="operation_id",
        max_event_bytes=1024,
        max_stream_length=100,
    )

    class Reporter:
        def __init__(self) -> None:
            self.failures: list[tuple[str, str, str]] = []

        def phase(self, _phase: str) -> None:
            return None

        def log(self, _message: str) -> None:
            return None

        def publish(
            self,
            event_type: str,
            _payload: dict[str, object],
            *,
            phase: str,
        ) -> None:
            if event_type == "COMPLETED" and not serialization_fails:
                raise ValueError("event cannot be encoded")

        def failed(self, code: str, error_type: str, phase: str) -> None:
            self.failures.append((code, error_type, phase))

    def serialize_result(**_kwargs: Any) -> dict[str, object]:
        if serialization_fails:
            raise TypeError("result cannot be serialized")
        return {"bundle_status": "succeeded"}

    result = SimpleNamespace(
        metrics={"accuracy": 1.0},
        bundle_uri="/tmp/bundle",
        execution_id="execution-1",
        model_dump=serialize_result,
    )
    redis_client = SimpleNamespace(close=lambda: None)
    reporter = Reporter()
    monkeypatch.setattr(
        execution_driver,
        "_load_driver_input",
        lambda: (value, "submission-1"),
    )
    monkeypatch.setattr(
        execution_driver, "create_redis_client", lambda _url: redis_client
    )
    monkeypatch.setattr(execution_driver, "_reporter", lambda *_args: reporter)
    monkeypatch.setitem(
        sys.modules, "ray", SimpleNamespace(init=lambda **_kwargs: None)
    )
    monkeypatch.setitem(
        sys.modules,
        "tributo.training.xgboost_trainer",
        SimpleNamespace(run_training_result_with_config=lambda _config: result),
    )

    assert execution_driver.main() == 1
    assert reporter.failures == [
        (
            "TERMINAL_EVENT_PUBLICATION_FAILED",
            "TerminalEventPublicationError",
            "PUBLISHING",
        )
    ]


def test_mapping_failure_event_preserves_parsed_execution_identity(
    config: RedisBrokerConfig,
    fake_redis: FakeRedis,
) -> None:
    operation_id = "unsupported-distributed-training"
    request = json.loads(
        _request(
            operation_id,
            "training",
            "distributed",
            {"algorithm": "lightgbm", "config": training_config(2)},
        )
    )
    request["run_id"] = "run-explicit"
    request["attempt_id"] = "attempt-2"
    fake_redis.messages[config.channels.training.task_stream_key] = [
        (
            "1-0",
            {
                "operation_id": operation_id,
                "payload": json.dumps(request),
            },
        )
    ]
    submitter = SubmissionRecorder()
    runtime = RedisBrokerRuntime(
        config,
        submitter=submitter,
        redis_client=fake_redis,
    )

    assert runtime.run_once(timeout_ms=0) is True

    assert submitter.calls == []
    event = json.loads(
        fake_redis.events[config.channels.training.event_stream_key(operation_id)][-1][
            "payload"
        ]
    )
    assert event["event_type"] == "FAILED"
    assert event["execution_profile"] == "distributed"
    assert event["run_id"] == "run-explicit"
    assert event["attempt_id"] == "attempt-2"
    assert event["payload"]["error_code"] == "UNSUPPORTED_ALGORITHM"
    runtime.close()


def test_training_nonterminal_event_failure_does_not_change_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = DriverInput(
        operation_id="operation-training",
        operation_type="training",
        execution_profile="single_worker",
        run_id="run-training",
        attempt_id="attempt-1",
        operation_payload={"training_config": training_config(1)},
        redis_url="redis://redis:6379/0",
        event_stream_prefix="events:training",
        max_event_bytes=1024,
        max_stream_length=100,
    )
    published: list[str] = []

    class Reporter:
        def publish(
            self,
            event_type: str,
            _payload: dict[str, object],
            *,
            phase: str,
        ) -> None:
            published.append(event_type)
            if event_type == "METRICS":
                raise ConnectionError("metrics publication unavailable")

    result = SimpleNamespace(
        metrics={"accuracy": 1.0},
        bundle_uri="/tmp/bundle",
        execution_id="execution-1",
        model_dump=lambda **_kwargs: {"bundle_status": "succeeded"},
    )
    monkeypatch.setitem(
        sys.modules,
        "tributo.training.xgboost_trainer",
        SimpleNamespace(run_training_result_with_config=lambda _config: result),
    )

    assert execution_driver._run_training(value, cast(Any, Reporter())) == 0
    assert "METRICS" in published
    assert published[-1] == "COMPLETED"


def test_nonterminal_event_retries_once_after_a_transient_failure() -> None:
    attempts = 0
    published: list[str] = []

    class Reporter:
        def publish(
            self,
            event_type: str,
            _payload: dict[str, object],
            *,
            phase: str,
        ) -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise ConnectionError("stale Redis connection")
            published.append(event_type)

    execution_driver._publish_nonterminal(
        cast(Any, Reporter()),
        "METRICS",
        {"metrics": {"accuracy": 1.0}},
        phase="EXECUTING",
    )

    assert attempts == 2
    assert published == ["METRICS"]


def test_training_metrics_use_the_json_serialized_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = DriverInput(
        operation_id="operation-training",
        operation_type="training",
        execution_profile="single_worker",
        run_id="run-training",
        attempt_id="attempt-1",
        operation_payload={"training_config": training_config(1)},
        redis_url="redis://redis:6379/0",
        event_stream_prefix="events:training",
        max_event_bytes=1024,
        max_stream_length=100,
    )
    published: list[tuple[str, dict[str, object]]] = []

    class Reporter:
        def publish(
            self,
            event_type: str,
            payload: dict[str, object],
            *,
            phase: str,
        ) -> None:
            json.dumps(payload, allow_nan=False)
            published.append((event_type, payload))

    result = SimpleNamespace(
        metrics={"raw": object()},
        bundle_uri="/tmp/bundle",
        execution_id="execution-1",
        model_dump=lambda **_kwargs: {
            "metrics": {"raw": "serialized"},
            "bundle_status": "succeeded",
        },
    )
    monkeypatch.setitem(
        sys.modules,
        "tributo.training.xgboost_trainer",
        SimpleNamespace(run_training_result_with_config=lambda _config: result),
    )

    assert execution_driver._run_training(value, cast(Any, Reporter())) == 0
    metrics = next(payload for event, payload in published if event == "METRICS")
    assert metrics["metrics"] == {"raw": "serialized"}


def test_inference_nonterminal_event_failure_does_not_change_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = DriverInput(
        operation_id="operation-inference",
        operation_type="batch_inference",
        execution_profile="single_worker",
        run_id="run-inference",
        attempt_id="attempt-1",
        operation_payload={"inference_request": inference_request(1)},
        redis_url="redis://redis:6379/0",
        event_stream_prefix="events:inference",
        max_event_bytes=1024,
        max_stream_length=100,
    )
    published: list[str] = []

    class Reporter:
        def publish(
            self,
            event_type: str,
            _payload: dict[str, object],
            *,
            phase: str,
        ) -> None:
            published.append(event_type)
            if event_type == "PROGRESS":
                raise ConnectionError("progress publication unavailable")

    class Plan:
        def model_copy(self, **_kwargs: Any) -> Plan:
            return self

        def model_dump(self, **_kwargs: Any) -> dict[str, object]:
            return {}

    class RequestContract:
        @classmethod
        def model_validate(cls, _value: object) -> object:
            return object()

    class ResolvedContract:
        @classmethod
        def model_validate(cls, _value: object) -> Plan:
            return Plan()

    result = SimpleNamespace(
        status="succeeded",
        input_rows=2,
        output_rows=2,
        sink_receipt=SimpleNamespace(
            model_dump=lambda **_kwargs: {"sink_id": "parquet-v1"}
        ),
        model_dump=lambda **_kwargs: {"status": "succeeded"},
    )
    monkeypatch.setitem(
        sys.modules,
        "tributo.inference.api",
        SimpleNamespace(
            resolve_inference=lambda _request: Plan(),
            run_resolved_inference=lambda _plan: result,
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "tributo.inference.contracts",
        SimpleNamespace(
            InferenceRequest=RequestContract,
            ResolvedInference=ResolvedContract,
        ),
    )

    assert (
        execution_driver._run_batch_inference(
            value, cast(Any, Reporter()), "submission-inference"
        )
        == 0
    )
    assert "PROGRESS" in published
    assert published[-1] == "COMPLETED"


def test_queued_cancel_is_fail_closed_acked_and_never_submitted(
    config: RedisBrokerConfig, fake_redis: FakeRedis
) -> None:
    operation_id = "queued-cancel"
    channel = config.channels.training
    fake_redis.cancelled.add(channel.cancel_key(operation_id))
    fake_redis.messages[channel.task_stream_key] = [
        (
            "2-0",
            {
                "operation_id": operation_id,
                "payload": _request(
                    operation_id,
                    "training",
                    "single_worker",
                    {"algorithm": "xgboost", "config": training_config(1)},
                ),
            },
        )
    ]
    submitter = SubmissionRecorder()
    runtime = RedisBrokerRuntime(config, submitter=submitter, redis_client=fake_redis)

    assert runtime.run_once(timeout_ms=0) is True
    assert submitter.calls == []
    assert fake_redis.acked[-1][2] == "2-0"
    event = json.loads(
        fake_redis.events[channel.event_stream_key(operation_id)][-1]["payload"]
    )
    assert event["event_type"] == "CANCELLED"
    assert event["submission_id"] is None
    runtime.close()


def test_running_cancel_stops_submission_before_cancelled_event(
    config: RedisBrokerConfig, fake_redis: FakeRedis
) -> None:
    submission = RayJobSubmission(
        run_id="operation-1",
        attempt_id="attempt-1",
        submission_id="submission-1",
        ray_job_id="ray-job-1",
    )
    item = ActiveSubmission(
        operation_id="operation-1",
        operation_type="training",
        execution_profile="single_worker",
        run_id="run-1",
        channel=config.channels.training,
        submission=submission,
    )
    active = ActiveSubmissionMap()
    active.put(item)
    fake_redis.cancelled.add(config.channels.training.cancel_key(item.operation_id))
    statuses = iter(["RUNNING", "STOPPED"])
    stopped: list[str] = []

    def stop(submission_id: str, **_kwargs: Any) -> bool:
        stopped.append(submission_id)
        return True

    watcher = CancelWatcher(
        fake_redis,
        active,
        dashboard_url="http://ray:8265",
        interval_seconds=1,
        max_event_bytes=1024 * 1024,
        max_stream_length=100,
        status_getter=lambda *_args, **_kwargs: next(statuses),
        stopper=stop,
    )

    watcher.check_once()
    assert stopped == [submission.submission_id]
    assert not fake_redis.events
    watcher.check_once()
    event = json.loads(
        fake_redis.events[config.channels.training.event_stream_key(item.operation_id)][
            -1
        ]["payload"]
    )
    assert event["event_type"] == "CANCELLED"
    assert event["submission_id"] == submission.submission_id
    assert active.get(item.operation_id) is None


def test_event_redaction_and_credential_reference_boundary(
    monkeypatch: pytest.MonkeyPatch, fake_redis: FakeRedis
) -> None:
    monkeypatch.setenv("PREPROVISIONED_SECRET", "do-not-publish")
    _resolve_credential_reference("env:PREPROVISIONED_SECRET")
    with pytest.raises(CredentialUnavailable):
        _resolve_credential_reference("env:MISSING_SECRET")

    reporter = RedisEventReporter(
        fake_redis,
        event_stream_prefix="events",
        operation_id="operation-1",
        operation_type="training",
        execution_profile="single_worker",
        run_id="run-1",
        attempt_id="attempt-1",
        submission_id="submission-1",
        outer_identity_field="request_id",
    )
    reporter.publish(
        "LOG",
        {
            "credential_ref": "env:PREPROVISIONED_SECRET",
            "password": "do-not-publish",
        },
    )
    event_fields = fake_redis.events["events:operation-1"][-1]
    encoded = event_fields["payload"]
    assert event_fields["request_id"] == "operation-1"
    assert "operation_id" not in event_fields
    assert "do-not-publish" not in encoded
    assert "env:PREPROVISIONED_SECRET" in encoded
    assert redact({"api_token": "x"}) == {"api_token": "[REDACTED]"}


def test_late_cancel_does_not_stop_a_terminal_operation(
    config: RedisBrokerConfig, fake_redis: FakeRedis
) -> None:
    submission = RayJobSubmission(
        run_id="operation-terminal",
        attempt_id="attempt-1",
        submission_id="submission-terminal",
    )
    item = ActiveSubmission(
        operation_id="operation-terminal",
        operation_type="training",
        execution_profile="single_worker",
        run_id="run-terminal",
        channel=config.channels.training,
        submission=submission,
    )
    active = ActiveSubmissionMap()
    active.put(item)
    reporter = RedisEventReporter(
        fake_redis,
        event_stream_prefix=item.channel.event_stream_prefix,
        operation_id=item.operation_id,
        operation_type=item.operation_type,
        execution_profile=item.execution_profile,
        run_id=item.run_id,
        attempt_id=item.submission.attempt_id,
        submission_id=item.submission.submission_id,
    )
    reporter.publish("COMPLETED", {"result_reference": "/bundle"})
    fake_redis.cancelled.add(item.channel.cancel_key(item.operation_id))
    stopped: list[str] = []

    def stop(submission_id: str, **_kwargs: Any) -> bool:
        stopped.append(submission_id)
        return True

    watcher = CancelWatcher(
        fake_redis,
        active,
        dashboard_url="http://ray:8265",
        interval_seconds=1,
        max_event_bytes=1024 * 1024,
        max_stream_length=100,
        status_getter=lambda *_args, **_kwargs: "RUNNING",
        stopper=stop,
    )

    watcher.check_once()

    assert stopped == []
    assert active.get(item.operation_id) is None


def test_external_stopped_submission_is_removed_without_cancelled_event(
    config: RedisBrokerConfig, fake_redis: FakeRedis
) -> None:
    submission = RayJobSubmission(
        run_id="run-external-stop",
        attempt_id="attempt-1",
        submission_id="submission-external-stop",
    )
    item = ActiveSubmission(
        operation_id="operation-external-stop",
        operation_type="training",
        execution_profile="single_worker",
        run_id=submission.run_id,
        channel=config.channels.training,
        submission=submission,
    )
    active = ActiveSubmissionMap()
    active.put(item)

    watcher = CancelWatcher(
        fake_redis,
        active,
        dashboard_url="http://ray:8265",
        interval_seconds=1,
        max_event_bytes=1024 * 1024,
        max_stream_length=100,
        status_getter=lambda *_args, **_kwargs: "STOPPED",
        stopper=lambda *_args, **_kwargs: pytest.fail("stop must not be requested"),
    )

    watcher.check_once()

    assert active.get(item.operation_id) is None
    assert item.channel.event_stream_key(item.operation_id) not in fake_redis.events


def test_old_attempt_terminal_event_does_not_hide_active_submission(
    config: RedisBrokerConfig,
    fake_redis: FakeRedis,
) -> None:
    current = RayJobSubmission(
        run_id="run-current",
        attempt_id="attempt-2",
        submission_id="submission-current",
    )
    item = ActiveSubmission(
        operation_id="operation-retry",
        operation_type="training",
        execution_profile="single_worker",
        run_id="run-current",
        channel=config.channels.training,
        submission=current,
    )
    active = ActiveSubmissionMap()
    active.put(item)
    old_reporter = RedisEventReporter(
        fake_redis,
        event_stream_prefix=item.channel.event_stream_prefix,
        operation_id=item.operation_id,
        operation_type=item.operation_type,
        execution_profile=item.execution_profile,
        run_id="run-old",
        attempt_id="attempt-1",
        submission_id="submission-old",
    )
    old_reporter.publish("COMPLETED", {"result_reference": "/old-bundle"})
    fake_redis.cancelled.add(item.channel.cancel_key(item.operation_id))
    stopped: list[str] = []

    def stop(submission_id: str, **_kwargs: Any) -> bool:
        stopped.append(submission_id)
        return True

    watcher = CancelWatcher(
        fake_redis,
        active,
        dashboard_url="http://ray:8265",
        interval_seconds=1,
        max_event_bytes=1024 * 1024,
        max_stream_length=100,
        status_getter=lambda *_args, **_kwargs: "RUNNING",
        stopper=stop,
    )

    watcher.check_once()

    assert stopped == [current.submission_id]
    assert active.get(item.operation_id) is item
