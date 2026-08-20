"""Unit tests for Redis Ray worker completion identity."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from ray.train.v2._internal.exceptions import UserExceptionWithTraceback
from ray.train.v2.api.exceptions import ControllerError, WorkerGroupError
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
    monkeypatch.setenv("TRIBUTO_BROKER_REQUEST_JSON", json.dumps({"job_id": "job-1"}))
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

    reporter.report_cancelled.assert_called_once_with("job-1", "TRAINING")
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

    assert _is_training_cancelled_error(WorkerGroupError("failed", {0: recursive}))


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
    reporter.report_cancelled.assert_called_once_with("job-1", "TRAINING")
    reporter.report_failed_with_code.assert_not_called()
    reporter.report_completed.assert_not_called()
