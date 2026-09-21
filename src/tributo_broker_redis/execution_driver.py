"""Single Ray Job driver for provider-owned training and batch inference."""

from __future__ import annotations

import base64
import logging
import os
import re
import sys
from pathlib import Path

from tributo_broker_redis.protocol import DriverInput
from tributo_broker_redis.redis_client import create_redis_client
from tributo_broker_redis.reporter import RedisEventReporter

_DRIVER_ENV = "TRIBUTO_REDIS_DRIVER_INPUT_B64"
_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
logger = logging.getLogger(__name__)


class CredentialUnavailable(Exception):
    pass


class _TerminalEventPublicationError(Exception):
    """A completed execution whose terminal notification could not be emitted."""


def _load_driver_input() -> tuple[DriverInput, str]:
    raw = os.environ.get(_DRIVER_ENV)
    if raw is None:
        raise ValueError(f"{_DRIVER_ENV} is required")
    try:
        decoded = base64.urlsafe_b64decode(raw.encode("ascii")).decode("utf-8")
    except Exception:
        raise ValueError("driver input is not valid base64 UTF-8") from None
    value = DriverInput.model_validate_json(decoded)
    submission_id = os.environ.get("TRIBUTO_SUBMISSION_ID")
    if not submission_id:
        raise ValueError("driver submission identity is required")
    if os.environ.get("TRIBUTO_ATTEMPT_ID") != value.attempt_id:
        raise ValueError("driver attempt identity mismatch")
    if os.environ.get("TRIBUTO_RUN_ID") != value.run_id:
        raise ValueError("driver run identity mismatch")
    return value, submission_id


def _resolve_credential_reference(reference: str | None) -> None:
    """Verify a pre-provisioned env or mount reference without transporting it."""
    if reference is None:
        return
    if reference.startswith("env:"):
        name = reference.removeprefix("env:")
        if not _ENV_NAME.fullmatch(name) or not os.environ.get(name):
            raise CredentialUnavailable("environment credential is unavailable")
        return
    if reference.startswith("mount:"):
        path = Path(reference.removeprefix("mount:"))
        if not path.is_absolute() or not path.is_file():
            raise CredentialUnavailable("mounted credential is unavailable")
        try:
            with path.open("rb") as stream:
                stream.read(1)
        except OSError as exc:
            raise CredentialUnavailable("mounted credential is unreadable") from exc
        return
    raise CredentialUnavailable("unsupported credential reference")


def _reporter(
    value: DriverInput,
    redis_client: object,
    submission_id: str,
) -> RedisEventReporter:
    return RedisEventReporter(
        redis_client,
        event_stream_prefix=value.event_stream_prefix,
        operation_id=value.operation_id,
        operation_type=value.operation_type,
        execution_profile=value.execution_profile,
        run_id=value.run_id,
        attempt_id=value.attempt_id,
        submission_id=submission_id,
        ray_job_id=os.environ.get("RAY_JOB_ID") or value.ray_job_id,
        outer_identity_field=value.outer_identity_field,
        max_event_bytes=value.max_event_bytes,
        max_stream_length=value.max_stream_length,
    )


def _publish_completed(
    reporter: RedisEventReporter,
    payload: dict[str, object],
) -> None:
    try:
        reporter.publish("COMPLETED", payload, phase="COMPLETED")
    except Exception as exc:
        raise _TerminalEventPublicationError from exc


def _publish_nonterminal(
    reporter: RedisEventReporter,
    event_type: str,
    payload: dict[str, object],
    *,
    phase: str,
) -> None:
    """Publish one informational event without changing the workload result."""
    for attempt in range(2):
        try:
            reporter.publish(event_type, payload, phase=phase)
            return
        except Exception:
            if attempt == 0:
                logger.warning(
                    "Best-effort nonterminal event publication failed; "
                    "retrying once: event_type=%s",
                    event_type,
                    exc_info=True,
                )
            else:
                logger.warning(
                    "Best-effort nonterminal event publication dropped: event_type=%s",
                    event_type,
                    exc_info=True,
                )


def _run_training(value: DriverInput, reporter: RedisEventReporter) -> int:
    from tributo.training.xgboost_trainer import run_training_result_with_config

    _publish_nonterminal(reporter, "PHASE", {"phase": "PREPARING"}, phase="PREPARING")
    _publish_nonterminal(
        reporter,
        "LOG",
        {"level": "INFO", "message": "Training driver started"},
        phase="PREPARING",
    )
    _publish_nonterminal(reporter, "PHASE", {"phase": "EXECUTING"}, phase="EXECUTING")
    result = run_training_result_with_config(
        dict(value.operation_payload["training_config"])
    )
    try:
        serialized_result = result.model_dump(mode="json")
    except Exception as exc:
        raise _TerminalEventPublicationError from exc
    _publish_nonterminal(
        reporter,
        "METRICS",
        {"progress": 1.0, "metrics": serialized_result.get("metrics", {})},
        phase="EXECUTING",
    )
    _publish_nonterminal(
        reporter,
        "PHASE",
        {"phase": "MATERIALIZING"},
        phase="MATERIALIZING",
    )
    _publish_completed(
        reporter,
        {
            "result": serialized_result,
            "result_reference": {
                "kind": "bundle",
                "uri": result.bundle_uri,
                "execution_id": result.execution_id,
            },
        },
    )
    return 0


def _run_batch_inference(
    value: DriverInput,
    reporter: RedisEventReporter,
    submission_id: str,
) -> int:
    from tributo.inference.api import resolve_inference, run_resolved_inference
    from tributo.inference.contracts import InferenceRequest, ResolvedInference

    request = InferenceRequest.model_validate(
        value.operation_payload["inference_request"]
    )
    _publish_nonterminal(reporter, "PHASE", {"phase": "PREPARING"}, phase="PREPARING")
    _publish_nonterminal(
        reporter,
        "LOG",
        {"level": "INFO", "message": "Batch inference driver started"},
        phase="PREPARING",
    )
    _publish_nonterminal(reporter, "PROGRESS", {"progress": 0.0}, phase="PREPARING")
    plan = resolve_inference(request).model_copy(
        update={
            "run_id": value.run_id,
            "attempt_id": value.attempt_id,
            "submission_id": submission_id,
        }
    )
    plan = ResolvedInference.model_validate(plan.model_dump(mode="python"))
    _publish_nonterminal(reporter, "PHASE", {"phase": "EXECUTING"}, phase="EXECUTING")
    result = run_resolved_inference(plan)
    if result.status != "succeeded":
        code = result.failure.code if result.failure is not None else "INFERENCE_FAILED"
        reporter.failed(code, "InferenceResult", "EXECUTING")
        return 1
    _publish_nonterminal(
        reporter,
        "PROGRESS",
        {
            "progress": 1.0,
            "input_rows": result.input_rows,
            "output_rows": result.output_rows,
        },
        phase="MATERIALIZING",
    )
    try:
        serialized_result = result.model_dump(mode="json")
    except Exception as exc:
        raise _TerminalEventPublicationError from exc
    _publish_completed(
        reporter,
        {
            "result": serialized_result,
            "result_reference": serialized_result.get("sink_receipt"),
        },
    )
    return 0


def main() -> int:
    value, submission_id = _load_driver_input()
    redis_client = create_redis_client(value.redis_connection)
    reporter = _reporter(value, redis_client, submission_id)
    try:
        _resolve_credential_reference(value.credential_ref)
        import ray

        ray.init(address="auto", ignore_reinit_error=True)
        if value.operation_type == "training":
            return _run_training(value, reporter)
        return _run_batch_inference(value, reporter, submission_id)
    except CredentialUnavailable:
        reporter.failed("CREDENTIAL_UNAVAILABLE", "CredentialUnavailable", "PREPARING")
        return 1
    except _TerminalEventPublicationError:
        reporter.failed(
            "TERMINAL_EVENT_PUBLICATION_FAILED",
            "TerminalEventPublicationError",
            "PUBLISHING",
        )
        return 1
    except Exception as exc:
        reporter.failed("EXECUTION_FAILED", type(exc).__name__, "EXECUTING")
        return 1
    finally:
        close = getattr(redis_client, "close", None)
        if callable(close):
            close()


if __name__ == "__main__":
    sys.exit(main())


__all__ = ["CredentialUnavailable", "main"]
