"""Ray Job entrypoint used by the Redis provider."""

from __future__ import annotations

import json
import logging
import os
import sys
from collections.abc import Mapping, Sequence, Set
from typing import Any

from tributo.integrations.broker import JobResult
from tributo.training.execution_context import TrainingCancelledError

from tributo_broker_redis.config import RedisBrokerConfig
from tributo_broker_redis.redis_client import create_redis_client
from tributo_broker_redis.reporter import RedisEventReporter

logger = logging.getLogger(__name__)


def _read_json_env(name: str) -> dict[str, Any]:
    raw = os.environ.get(name)
    if not raw:
        raise ValueError(f"Missing required environment variable {name}")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError(f"Environment variable {name} must contain a JSON object")
    return value


def _emit_history(
    reporter: RedisEventReporter,
    job_id: str,
    summary: Mapping[str, Any],
) -> None:
    metrics = summary.get("metrics")
    metric_values = metrics if isinstance(metrics, Mapping) else summary
    histories: dict[str, list[Any]] = {}
    for key, value in metric_values.items():
        if key.endswith("_history") and isinstance(value, list):
            histories[key.removesuffix("_history")] = value
    if not histories:
        return
    total_rounds = max(len(values) for values in histories.values())
    for index in range(total_rounds):
        metrics = {
            key: values[index]
            for key, values in histories.items()
            if index < len(values) and isinstance(values[index], (int, float))
        }
        if metrics:
            reporter.report_metrics(
                job_id,
                {key: float(value) for key, value in metrics.items()},
                (index + 1) / total_rounds,
            )


def _result_from_summary(
    job_id: str,
    summary: Mapping[str, Any],
    *,
    run_id: str | None,
    attempt_id: str | None,
    execution_id: str | None,
    submission_id: str | None,
) -> JobResult:
    """Convert the trainer summary into a broker-neutral completion result."""
    raw_metrics = summary.get("metrics")
    metrics = raw_metrics if isinstance(raw_metrics, Mapping) else {}
    artifact_refs = [
        dict(value)
        for value in summary.get("artifacts", [])
        if isinstance(value, Mapping)
    ]
    artifacts = [
        str(value.get("name"))
        for value in artifact_refs
        if isinstance(value.get("name"), str)
    ]
    return JobResult(
        job_id=job_id,
        status="success",
        run_id=run_id,
        attempt_id=attempt_id,
        execution_id=execution_id,
        submission_id=submission_id,
        metrics={
            key: float(value)
            for key, value in metrics.items()
            if not key.startswith("_tributo_")
            and isinstance(value, (int, float))
            and not isinstance(value, bool)
        },
        artifacts=artifacts,
        artifact_refs=artifact_refs,
        bundle_id=(
            str(summary["bundle_id"]) if summary.get("bundle_id") is not None else None
        ),
        bundle_uri=(
            str(summary["canonical_uri"])
            if summary.get("canonical_uri") is not None
            else None
        ),
        manifest_uri=(
            str(summary["manifest_uri"])
            if summary.get("manifest_uri") is not None
            else None
        ),
    )


def _worker_job_identity() -> tuple[str | None, str | None]:
    """Return the Ray execution lookup token and deterministic submission ID."""
    submission_id = os.environ.get("TRIBUTO_SUBMISSION_ID")
    return os.environ.get("RAY_JOB_ID") or submission_id, submission_id


def _is_training_cancelled_error(error: BaseException) -> bool:
    """Recognize Core cancellation through Ray Train's nested error containers."""
    pending: list[object] = [error]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, TrainingCancelledError):
            return True

        if isinstance(current, Mapping):
            pending.extend(current.values())
            continue
        if isinstance(current, (Sequence, Set)) and not isinstance(
            current, (str, bytes, bytearray)
        ):
            pending.extend(current)
            continue

        if isinstance(current, BaseException):
            if current.__cause__ is not None:
                pending.append(current.__cause__)
            if not current.__suppress_context__ and current.__context__ is not None:
                pending.append(current.__context__)

        # Ray Train v2 exposes nested failures on these public/private wrapper
        # attributes rather than through Python exception chaining.
        for attribute in (
            "cause",
            "worker_failures",
            "controller_failure",
            "health_check_failure",
            "exceptions",
            "exception",
            "_base_exc",
        ):
            try:
                candidate = getattr(current, attribute)
            except (AttributeError, RuntimeError):
                continue
            if candidate is not current:
                pending.append(candidate)
    return False


def main() -> int:
    request_data = _read_json_env("TRIBUTO_BROKER_REQUEST_JSON")
    training_config = _read_json_env("TRIBUTO_TRAINING_CONFIG_JSON")
    broker_config = RedisBrokerConfig.from_mapping(
        _read_json_env("TRIBUTO_BROKER_CONFIG_JSON")
    )
    raw_job_id = os.environ.get("TRIBUTO_RUN_ID") or request_data.get("job_id")
    if not isinstance(raw_job_id, str) or not raw_job_id:
        raise ValueError("Training job request is missing a non-empty job_id")
    job_id = raw_job_id
    redis_client = create_redis_client(broker_config)
    reporter = RedisEventReporter(redis_client, broker_config, job_id)
    try:
        reporter.report_phase(job_id, "TRAINING")
        reporter.report_log(job_id, "Training started", "INFO")
        from tributo.training.xgboost_trainer import run_training_with_config

        summary = run_training_with_config(
            {
                **training_config,
                "_tributo_execution_context": json.loads(
                    os.environ.get("TRIBUTO_EXECUTION_CONTEXT", "{}")
                ),
            }
        )
        if not isinstance(summary, dict):
            summary = {"result": summary}
        raw_metrics = summary.get("metrics")
        if isinstance(raw_metrics, Mapping) and raw_metrics.get("_tributo_cancelled"):
            reporter.report_cancelled(job_id, "TRAINING")
            return 0
        _emit_history(reporter, job_id, summary)
        execution_id, submission_id = _worker_job_identity()
        result = _result_from_summary(
            job_id,
            summary,
            run_id=os.environ.get("TRIBUTO_RUN_ID"),
            attempt_id=os.environ.get("TRIBUTO_ATTEMPT_ID"),
            execution_id=execution_id,
            submission_id=submission_id,
        )
        reporter.report_completed(job_id, result)
        return 0
    except Exception as exc:
        if _is_training_cancelled_error(exc):
            logger.info("Redis broker training job cancelled: job_id=%s", job_id)
            reporter.report_cancelled(job_id, "TRAINING")
            return 0
        logger.exception("Redis broker training job failed: job_id=%s", job_id)
        reporter.report_failed_with_code(job_id, str(exc), type(exc).__name__)
        return 1
    finally:
        close = getattr(redis_client, "close", None)
        if callable(close):
            close()


if __name__ == "__main__":
    sys.exit(main())
