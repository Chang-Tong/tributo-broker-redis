"""Unit tests for Redis Ray worker completion identity."""

from __future__ import annotations

import json
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
from ray.train.v2._internal.exceptions import UserExceptionWithTraceback
from ray.train.v2.api.exceptions import ControllerError, WorkerGroupError
from tributo.exceptions import ModelExportError
from tributo.training.execution_context import TrainingCancelledError

from tributo_broker_redis.config import RedisBrokerConfig
from tributo_broker_redis.run_training import (
    _is_training_cancelled_error,
    _result_from_summary,
    _worker_job_identity,
    main,
)


def test_worker_identity_falls_back_to_submission_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("RAY_JOB_ID", raising=False)
    monkeypatch.setenv("TRIBUTO_SUBMISSION_ID", "tributo-train-1")

    assert _worker_job_identity() == (
        "tributo-train-1",
        "tributo-train-1",
    )


def test_completion_result_preserves_both_ray_identities() -> None:
    result = _result_from_summary(
        "job-1",
        {"metrics": {"accuracy": 0.9}},
        run_id="job-1",
        attempt_id="attempt-1",
        execution_id="ray-job-1",
        submission_id="tributo-train-1",
    )

    assert result.execution_id == "ray-job-1"
    assert result.submission_id == "tributo-train-1"


def _set_worker_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRIBUTO_RUN_ID", "job-1")
    monkeypatch.setenv(
        "TRIBUTO_BROKER_REQUEST_JSON",
        json.dumps(
            {
                "job_id": "job-1",
                "model_id": "model-1",
                "version_id": "version-1",
                "tenant_id": "tenant-1",
                "datasource": {
                    "type": "LOCAL",
                    "properties": {"path": "/tmp/train.csv"},
                },
                "features": [{"feature_id": "feature-1", "result_column": "x"}],
                "target": {
                    "result_column": "label",
                    "task_type": "BINARY_CLASSIFICATION",
                },
                "storage_context": {"type": "local", "prefix": "/tmp/models/"},
            }
        ),
    )
    monkeypatch.setenv("TRIBUTO_TRAINING_CONFIG_JSON", "{}")
    monkeypatch.setenv(
        "TRIBUTO_BROKER_CONFIG_JSON",
        RedisBrokerConfig().model_dump_json(),
    )


def test_training_cancelled_error_publishes_only_cancelled_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_worker_environment(monkeypatch)
    redis_client = MagicMock()
    reporter = MagicMock()
    with (
        patch(
            "tributo_broker_redis.run_training.create_redis_client",
            return_value=redis_client,
        ),
        patch(
            "tributo_broker_redis.run_training.RedisEventReporter",
            return_value=reporter,
        ),
        patch(
            "tributo.training.xgboost_trainer.run_training_with_config",
            side_effect=TrainingCancelledError("cancelled in worker"),
        ),
    ):
        assert main() == 0

    assert reporter.report_cancelled.call_args.args == ("job-1", "TRAINING")
    assert reporter.report_cancelled.call_args.kwargs["duration_seconds"] >= 0
    reporter.report_failed_with_code.assert_not_called()
    reporter.report_completed.assert_not_called()
    redis_client.close.assert_called_once_with()


def test_training_cancelled_error_is_recognized_through_wrapper() -> None:
    try:
        try:
            raise TrainingCancelledError("cancelled in worker")
        except TrainingCancelledError as exc:
            raise RuntimeError("Ray training failed") from exc
    except RuntimeError as wrapped:
        assert _is_training_cancelled_error(wrapped) is True


def test_training_cancelled_error_is_recognized_in_ray_worker_group_error() -> None:
    worker_error = UserExceptionWithTraceback(
        TrainingCancelledError("cancelled in Ray worker"),
        "worker traceback",
    )
    wrapped = WorkerGroupError("rank 0 failed", {0: worker_error})

    assert _is_training_cancelled_error(wrapped) is True


def test_training_cancelled_error_is_recognized_in_ray_controller_error() -> None:
    wrapped = ControllerError(TrainingCancelledError("controller cancelled"))

    assert _is_training_cancelled_error(wrapped) is True


def test_training_cancelled_error_recursion_handles_containers_and_cycles() -> None:
    recursive: list[object] = []
    recursive.append(recursive)
    recursive.append({"nested": (TrainingCancelledError("cancelled"),)})

    assert _is_training_cancelled_error(
        WorkerGroupError("failed", cast(Any, {0: recursive}))
    )


def test_training_cancelled_error_respects_suppressed_context() -> None:
    try:
        try:
            raise TrainingCancelledError("hidden cancellation")
        except TrainingCancelledError:
            raise RuntimeError("replacement failure") from None
    except RuntimeError as wrapped:
        assert wrapped.__context__ is not None
        assert wrapped.__suppress_context__ is True
        assert _is_training_cancelled_error(wrapped) is False


def test_worker_uses_core_realtime_events_and_only_publishes_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_worker_environment(monkeypatch)
    reporter = MagicMock()
    summary = {
        "feature_columns": ["x"],
        "row_counts": {"train": 8, "val": 1, "test": 1},
        "evaluation": {"eval_auc": 0.8, "eval_test_rows": 1},
        "artifact_refs": [
            {
                "kind": "onnx",
                "uri": "/tmp/models/model.onnx",
                "sha256": "abc123",
                "size_bytes": 1,
            }
        ],
    }
    with (
        patch("tributo_broker_redis.run_training.create_redis_client", MagicMock()),
        patch(
            "tributo_broker_redis.run_training.RedisEventReporter",
            return_value=reporter,
        ),
        patch(
            "tributo.training.xgboost_trainer.run_training_with_config",
            return_value=summary,
        ) as train,
    ):
        assert main() == 0

    train.assert_called_once_with({})
    reporter.report_phase.assert_not_called()
    reporter.report_metrics.assert_not_called()
    reporter.report_log.assert_not_called()
    reporter.report_completed.assert_not_called()
    payload = reporter.report_completed_payload.call_args.args[1]
    assert payload["phase"] == "COMPLETED"
    assert payload["result_summary"]["sample_rows"]["total"] == 10


def test_legacy_cancelled_summary_remains_cancelled_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_worker_environment(monkeypatch)
    reporter = MagicMock()
    with (
        patch("tributo_broker_redis.run_training.create_redis_client", MagicMock()),
        patch(
            "tributo_broker_redis.run_training.RedisEventReporter",
            return_value=reporter,
        ),
        patch(
            "tributo.training.xgboost_trainer.run_training_with_config",
            return_value={"metrics": {"_tributo_cancelled": True}},
        ) as train,
    ):
        assert main() == 0

    assert train.call_args.kwargs == {}
    assert reporter.report_cancelled.call_args.args == ("job-1", "TRAINING")
    assert reporter.report_cancelled.call_args.kwargs["duration_seconds"] >= 0
    reporter.report_failed_with_code.assert_not_called()
    reporter.report_completed.assert_not_called()


def test_worker_raises_when_completed_terminal_cannot_be_published(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_worker_environment(monkeypatch)
    reporter = MagicMock()
    reporter.report_completed_payload.return_value = False
    summary = {
        "evaluation": {"eval_auc": 0.8},
        "artifact_refs": [
            {
                "kind": "onnx",
                "uri": "/tmp/models/model.onnx",
                "sha256": "abc123",
                "size_bytes": 1,
            }
        ],
    }
    with (
        patch("tributo_broker_redis.run_training.create_redis_client", MagicMock()),
        patch(
            "tributo_broker_redis.run_training.RedisEventReporter",
            return_value=reporter,
        ),
        patch(
            "tributo.training.xgboost_trainer.run_training_with_config",
            return_value=summary,
        ),
        pytest.raises(RuntimeError, match="COMPLETED terminal event"),
    ):
        main()

    reporter.report_failed_with_code.assert_not_called()


def test_worker_raises_when_failed_terminal_cannot_be_published(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_worker_environment(monkeypatch)
    reporter = MagicMock()
    reporter.report_failed_with_code.return_value = False
    with (
        patch("tributo_broker_redis.run_training.create_redis_client", MagicMock()),
        patch(
            "tributo_broker_redis.run_training.RedisEventReporter",
            return_value=reporter,
        ),
        patch(
            "tributo.training.xgboost_trainer.run_training_with_config",
            side_effect=ValueError("bad password=hunter2"),
        ),
        pytest.raises(RuntimeError, match="FAILED terminal event"),
    ):
        main()

    assert reporter.report_failed_with_code.call_count == 1


@pytest.mark.parametrize(
    ("error", "expected_code", "expected_phase", "expected_message"),
    [
        (
            ValueError("invalid password=hunter2"),
            "INVALID_PAYLOAD",
            "TRAINING",
            "invalid password=[REDACTED]",
        ),
        (
            ModelExportError("onnx export failed"),
            "MODEL_EXPORT_FAILED",
            "EVALUATING",
            "onnx export failed",
        ),
        (
            RuntimeError("Ray wrapper"),
            "MODEL_EXPORT_FAILED",
            "EVALUATING",
            "nested export failed",
        ),
    ],
)
def test_main_preserves_real_exception_for_terminal_mapping(
    monkeypatch: pytest.MonkeyPatch,
    error: BaseException,
    expected_code: str,
    expected_phase: str,
    expected_message: str,
) -> None:
    _set_worker_environment(monkeypatch)
    if str(error) == "Ray wrapper":
        error.__cause__ = ModelExportError("nested export failed")
    redis_client = MagicMock()
    redis_client.eval.side_effect = lambda script, _keys, *args: (
        args[1] if "STAGE_TERMINAL_CANDIDATE" in script else ["published", "1-0"]
    )
    with (
        patch(
            "tributo_broker_redis.run_training.create_redis_client",
            return_value=redis_client,
        ),
        patch(
            "tributo.training.xgboost_trainer.run_training_with_config",
            side_effect=error,
        ),
    ):
        assert main() == 1

    event = json.loads(redis_client.eval.call_args.args[4])
    assert event["error_code"] == expected_code
    assert event["phase"] == expected_phase
    assert event["error_message"] == expected_message
    assert "hunter2" not in json.dumps(event)


def test_worker_raises_when_cancelled_terminal_cannot_be_published(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_worker_environment(monkeypatch)
    reporter = MagicMock()
    reporter.report_cancelled.return_value = False
    with (
        patch("tributo_broker_redis.run_training.create_redis_client", MagicMock()),
        patch(
            "tributo_broker_redis.run_training.RedisEventReporter",
            return_value=reporter,
        ),
        patch(
            "tributo.training.xgboost_trainer.run_training_with_config",
            side_effect=TrainingCancelledError("cancelled"),
        ),
        pytest.raises(RuntimeError, match="CANCELLED terminal event"),
    ):
        main()

    reporter.report_failed_with_code.assert_not_called()


def test_explicit_legacy_worker_preserves_legacy_completion_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_worker_environment(monkeypatch)
    monkeypatch.setenv(
        "TRIBUTO_BROKER_REQUEST_JSON",
        json.dumps({"job_id": "job-1", "training_config": {"data": {}}}),
    )
    reporter = MagicMock()
    reporter.report_legacy_completed.return_value = True
    with (
        patch("tributo_broker_redis.run_training.create_redis_client", MagicMock()),
        patch(
            "tributo_broker_redis.run_training.RedisEventReporter",
            return_value=reporter,
        ),
        patch(
            "tributo.training.xgboost_trainer.run_training_with_config",
            return_value={"metrics": {"accuracy": 0.9}},
        ),
    ):
        assert main() == 0

    reporter.report_legacy_completed.assert_called_once()
    reporter.report_completed_payload.assert_not_called()
