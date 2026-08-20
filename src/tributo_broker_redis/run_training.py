"""Ray Job entrypoint used by the Redis provider."""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from collections.abc import Mapping, Sequence, Set
from typing import Any

from tributo.integrations.broker import JobResult
from tributo.training.execution_context import TrainingCancelledError

from tributo_broker_redis.completion import (
    build_completed_payload,
    failure_phase,
    redact_sensitive,
)
from tributo_broker_redis.config import RedisBrokerConfig
from tributo_broker_redis.protocol import TrainingJobRequest
from tributo_broker_redis.redis_client import create_redis_client
from tributo_broker_redis.reporter import RedisEventReporter

logger = logging.getLogger(__name__)


class TerminalPublishError(RuntimeError):
    """A terminal result could not be durably published to Redis."""


def _read_json_env(name: str) -> dict[str, Any]:
    raw = os.environ.get(name)
    if not raw:
        raise ValueError(f"Missing required environment variable {name}")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError(f"Environment variable {name} must contain a JSON object")
    return value


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
    started_at = time.monotonic()
    request_data = _read_json_env("TRIBUTO_BROKER_REQUEST_JSON")
    training_config = _read_json_env("TRIBUTO_TRAINING_CONFIG_JSON")
    broker_config = RedisBrokerConfig.from_mapping(
        _read_json_env("TRIBUTO_BROKER_CONFIG_JSON")
    )
    raw_job_id = os.environ.get("TRIBUTO_RUN_ID") or request_data.get("job_id")
    if not isinstance(raw_job_id, str) or not raw_job_id:
        raise ValueError("Training job request is missing a non-empty job_id")
    job_id = raw_job_id
    request_data["job_id"] = job_id
    request = TrainingJobRequest.model_validate(request_data)
    redis_client = create_redis_client(broker_config)
    reporter = RedisEventReporter(redis_client, broker_config, job_id)
    try:
        from tributo.training.xgboost_trainer import run_training_with_config

        summary = run_training_with_config(training_config)
        if not isinstance(summary, dict):
            summary = {"result": summary}
        raw_metrics = summary.get("metrics")
        if isinstance(raw_metrics, Mapping) and raw_metrics.get("_tributo_cancelled"):
            published = reporter.report_cancelled(
                job_id,
                "TRAINING",
                duration_seconds=time.monotonic() - started_at,
            )
            if not published:
                raise TerminalPublishError("CANCELLED terminal event was not published")
            return 0
        if request.training_config is not None:
            execution_id, submission_id = _worker_job_identity()
            published = reporter.report_legacy_completed(
                job_id,
                _result_from_summary(
                    job_id,
                    summary,
                    run_id=os.environ.get("TRIBUTO_RUN_ID"),
                    attempt_id=os.environ.get("TRIBUTO_ATTEMPT_ID"),
                    execution_id=execution_id,
                    submission_id=submission_id,
                ),
            )
            if not published:
                raise TerminalPublishError("COMPLETED terminal event was not published")
            return 0
        published = reporter.report_completed_payload(
            job_id,
            build_completed_payload(
                request,
                summary,
                duration_seconds=time.monotonic() - started_at,
            ),
        )
        if not published:
            raise TerminalPublishError("COMPLETED terminal event was not published")
        return 0
    except TerminalPublishError:
        # A second terminal would create contradictory state. Let Ray record
        # this execution as FAILED; durable replay belongs to the supervisor.
        raise
    except Exception as exc:
        if _is_training_cancelled_error(exc):
            logger.info("Redis broker training job cancelled: job_id=%s", job_id)
            published = reporter.report_cancelled(
                job_id,
                "TRAINING",
                duration_seconds=time.monotonic() - started_at,
            )
            if not published:
                raise TerminalPublishError(
                    "CANCELLED terminal event was not published"
                ) from exc
            return 0
        logger.error(
            "Redis broker training job failed: job_id=%s error=%s",
            job_id,
            redact_sensitive(str(exc)),
        )
        published = reporter.report_failed_with_code(
            job_id,
            exc,
            phase=failure_phase(exc),
            duration_seconds=time.monotonic() - started_at,
        )
        if not published:
            raise TerminalPublishError(
                "FAILED terminal event was not published"
            ) from exc
        return 1
    finally:
        close = getattr(redis_client, "close", None)
        if callable(close):
            close()


if __name__ == "__main__":
    sys.exit(main())
