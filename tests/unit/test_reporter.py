"""Unit tests for best-effort Redis event publishing and ACK-independent failure."""

from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock

import pytest
from tributo.integrations.broker import JobResult

from tributo_broker_redis.config import RedisBrokerConfig
from tributo_broker_redis.reporter import RedisEventReporter
from tributo_broker_redis.run_training import _result_from_summary


def test_reporter_uses_knova_two_field_envelope() -> None:
    client = MagicMock()
    reporter = RedisEventReporter(client, RedisBrokerConfig(), "job-1")
    reporter.report_phase("job-1", "QUEUED")
    stream, values = client.xadd.call_args.args[:2]
    assert stream == "knova:training:events:job-1"
    assert set(values) == {"job_id", "payload"}
    assert '"event_type":"PHASE"' in values["payload"]
    assert json.loads(values["payload"])["message"] == "Training job queued"


def test_reporter_maps_multiclass_logloss_to_loss() -> None:
    client = MagicMock()
    reporter = RedisEventReporter(client, RedisBrokerConfig(), "job-1")
    reporter.report_metrics("job-1", {"train-mlogloss": 0.25}, 0.5)
    event = json.loads(client.xadd.call_args.args[1]["payload"])
    assert event["metrics"] == [{"metric_name": "loss", "train": 0.25}]


def test_reporter_retries_and_does_not_raise_when_redis_is_down() -> None:
    client = MagicMock()
    client.xadd.side_effect = ConnectionError("redis down")
    reporter = RedisEventReporter(
        client,
        RedisBrokerConfig(max_publish_retries=2, publish_retry_delay=0),
        "job-1",
        sleep=lambda _delay: None,
    )
    reporter.report_failed_with_code("job-1", "bad", "INVALID_PAYLOAD")
    assert client.xadd.call_count == 3


def test_terminal_event_can_retry_after_failed_publish() -> None:
    client = MagicMock()
    client.xadd.side_effect = [ConnectionError("down"), "1-0"]
    reporter = RedisEventReporter(
        client,
        RedisBrokerConfig(max_publish_retries=0),
        "job-1",
    )
    reporter.report_completed("job-1", JobResult("job-1", "success"))
    reporter.report_completed("job-1", JobResult("job-1", "success"))
    assert client.xadd.call_count == 2


def test_invalid_job_id_uses_dedicated_stream_without_sentinel_identity() -> None:
    client = MagicMock()
    config = RedisBrokerConfig()
    reporter = RedisEventReporter(
        client,
        config,
        None,
        stream_key=config.invalid_event_stream_key,
    )
    reporter.report_failed_with_code(
        None,
        "missing job id",
        "INVALID_JOB_ID",
        delivery_id="7-0",
    )
    stream, values = client.xadd.call_args.args[:2]
    assert stream == config.invalid_event_stream_key
    assert values["job_id"] == ""
    assert json.loads(values["payload"])["job_id"] is None
    assert json.loads(values["payload"])["delivery_id"] == "7-0"


def test_reporter_warning_is_rate_limited(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = MagicMock()
    client.xadd.side_effect = ConnectionError("redis down")
    reporter = RedisEventReporter(
        client,
        RedisBrokerConfig(
            max_publish_retries=2,
            publish_retry_delay=0,
            failure_log_interval=300,
        ),
        "job-1",
        sleep=lambda _delay: None,
    )
    with caplog.at_level(logging.WARNING, logger="tributo_broker_redis.reporter"):
        reporter.report_failed_with_code("job-1", "bad", "FAILED")
    warning_records = [
        record
        for record in caplog.records
        if "Failed to publish broker event" in record.getMessage()
    ]
    assert len(warning_records) == 1


def test_reporter_drops_events_over_configured_size_limit() -> None:
    client = MagicMock()
    reporter = RedisEventReporter(
        client,
        RedisBrokerConfig(max_event_bytes=32),
        "job-1",
    )
    assert reporter._publish("job-1", "LOG", {"message": "x" * 100}) is False
    client.xadd.assert_not_called()


def test_reporter_publishes_log_metrics_and_completion_fields() -> None:
    client = MagicMock()
    reporter = RedisEventReporter(client, RedisBrokerConfig(), "job-1")
    reporter.report_log("job-1", "started", "INFO")
    reporter.report_metrics("job-1", {"loss": 0.5}, 0.5)
    reporter.report_completed(
        "job-1",
        JobResult(
            "job-1",
            "success",
            run_id="job-1",
            attempt_id="attempt-1",
            execution_id="ray-job-1",
            submission_id="submission-1",
            bundle_id="bundle-1",
            bundle_uri="/models/bundle-1",
            manifest_uri="/models/bundle-1/manifest.json",
            artifact_refs=[{"name": "onnx-model", "format": "onnx"}],
        ),
    )
    events = [
        json.loads(call.args[1]["payload"]) for call in client.xadd.call_args_list
    ]
    assert [event["event_type"] for event in events] == [
        "LOG",
        "METRICS",
        "COMPLETED",
    ]
    assert events[0]["level"] == "info"
    assert events[1]["metrics"] == [{"metric_name": "loss", "eval": 0.5}]
    assert events[1]["current_round"] == 0
    completed = events[-1]
    assert completed["bundle_id"] == "bundle-1"
    assert completed["bundle_uri"] == "/models/bundle-1"
    assert completed["artifact_refs"] == [{"name": "onnx-model", "format": "onnx"}]


def test_reporter_normalizes_log_level_to_lowercase() -> None:
    client = MagicMock()
    reporter = RedisEventReporter(client, RedisBrokerConfig(), "job-1")

    reporter.report_log("job-1", "warning", "WaRnInG")

    event = json.loads(client.xadd.call_args.args[1]["payload"])
    assert event["event_type"] == "LOG"
    assert event["level"] == "warning"


def test_reporter_maps_warn_alias_to_warning() -> None:
    client = MagicMock()
    reporter = RedisEventReporter(client, RedisBrokerConfig(), "job-1")

    reporter.report_log("job-1", "warning", "warn")

    event = json.loads(client.xadd.call_args.args[1]["payload"])
    assert event["level"] == "warning"


@pytest.mark.parametrize(
    "level",
    ["debug", "info", "warning", "error", "success"],
)
def test_reporter_accepts_wire_log_levels(level: str) -> None:
    client = MagicMock()
    reporter = RedisEventReporter(client, RedisBrokerConfig(), "job-1")

    reporter.report_log("job-1", "message", level.upper())

    event = json.loads(client.xadd.call_args.args[1]["payload"])
    assert event["level"] == level


def test_reporter_rejects_invalid_log_level_without_publishing() -> None:
    client = MagicMock()
    reporter = RedisEventReporter(client, RedisBrokerConfig(), "job-1")

    with pytest.raises(ValueError, match="Unsupported broker log level"):
        reporter.report_log("job-1", "message", "critical")

    client.xadd.assert_not_called()


def test_reporter_maps_train_and_validation_metrics_to_process_metrics() -> None:
    client = MagicMock()
    reporter = RedisEventReporter(client, RedisBrokerConfig(), "job-1")

    reporter.report_metrics(
        "job-1",
        {"round": 25.0, "train-logloss": 0.4, "val-logloss": 0.5},
        0.25,
    )

    stream, fields = client.xadd.call_args.args[:2]
    assert stream == "knova:training:events:job-1"
    assert set(fields) == {"job_id", "payload"}
    event = json.loads(fields["payload"])
    assert event["current_round"] == 25
    assert event["total_rounds"] == 100
    assert event["metrics"] == [{"metric_name": "loss", "train": 0.4, "eval": 0.5}]


def test_reporter_converts_xgboost_error_rate_to_accuracy_for_both_scopes() -> None:
    client = MagicMock()
    reporter = RedisEventReporter(client, RedisBrokerConfig(), "job-1")

    reporter.report_metrics("job-1", {"train-error": 0.25, "val-error": 0.4}, 0.5)

    event = json.loads(client.xadd.call_args.args[1]["payload"])
    assert event["metrics"] == [{"metric_name": "accuracy", "train": 0.75, "eval": 0.6}]


@pytest.mark.parametrize("value", [-0.1, 1.1])
def test_reporter_rejects_out_of_range_xgboost_error_rate(value: float) -> None:
    client = MagicMock()
    reporter = RedisEventReporter(client, RedisBrokerConfig(), "job-1")

    with pytest.raises(ValueError, match="error rate must be in"):
        reporter.report_metrics("job-1", {"train-error": value}, 0.5)

    client.xadd.assert_not_called()


def test_reporter_redacts_log_messages_before_publish() -> None:
    client = MagicMock()
    reporter = RedisEventReporter(client, RedisBrokerConfig(), "job-1")

    reporter.report_log(
        "job-1", "failed redis://user:secret@redis password=hunter2", "error"
    )

    event = json.loads(client.xadd.call_args.args[1]["payload"])
    assert "secret" not in event["message"]
    assert "hunter2" not in event["message"]


def test_reporter_rejects_non_finite_metrics_without_publishing() -> None:
    client = MagicMock()
    reporter = RedisEventReporter(client, RedisBrokerConfig(), "job-1")

    with pytest.raises(ValueError, match="must be finite"):
        reporter.report_metrics("job-1", {"train-loss": float("nan")}, 0.5)

    client.xadd.assert_not_called()


def test_reporter_publishes_cancelled_event() -> None:
    client = MagicMock()
    reporter = RedisEventReporter(client, RedisBrokerConfig(), "job-1")
    reporter.report_cancelled("job-1", "TRAINING")
    event = json.loads(client.xadd.call_args.args[1]["payload"])
    assert event["event_type"] == "CANCELLED"
    assert event["phase"] == "TRAINING"


def test_training_summary_bridges_bundle_and_metric_fields() -> None:
    result = _result_from_summary(
        "job-1",
        {
            "metrics": {"accuracy": 0.9, "accuracy_history": [0.7, 0.9]},
            "bundle_id": "bundle-1",
            "canonical_uri": "/models/bundle-1",
            "manifest_uri": "/models/bundle-1/manifest.json",
            "artifacts": [{"name": "onnx-model", "format": "onnx"}],
        },
        run_id="job-1",
        attempt_id="attempt-1",
        execution_id="ray-job-1",
        submission_id="submission-1",
    )
    assert result.bundle_id == "bundle-1"
    assert result.bundle_uri == "/models/bundle-1"
    assert result.manifest_uri == "/models/bundle-1/manifest.json"
    assert result.metrics == {"accuracy": 0.9}
    assert result.artifacts == ["onnx-model"]
    assert result.artifact_refs == [{"name": "onnx-model", "format": "onnx"}]
    assert result.submission_id == "submission-1"
