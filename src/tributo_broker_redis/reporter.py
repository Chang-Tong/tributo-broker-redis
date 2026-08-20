"""Guarded Redis Stream lifecycle reporter."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from typing import Any

from tributo.integrations.broker import EventReporter, JobResult

from tributo_broker_redis.active_jobs import ActiveJobStore, TerminalCandidateStore
from tributo_broker_redis.completion import (
    build_cancelled_payload,
    build_failed_payload,
    finite_float,
    redact_sensitive,
)
from tributo_broker_redis.config import RedisBrokerConfig
from tributo_broker_redis.protocol import (
    ProcessMetric,
    event_payload,
    quantize_duration_seconds,
    validate_job_id,
)
from tributo_broker_redis.terminal_guard import (
    TERMINAL_EVENT_TYPES,
    PublishDecision,
    TerminalGuard,
)

logger = logging.getLogger(__name__)

_LOG_LEVELS = frozenset({"debug", "info", "warning", "error", "success"})
_METRIC_NAMES = {
    "logloss": "loss",
    "mlogloss": "loss",
    "error": "accuracy",
    "merror": "accuracy",
    "roc_auc": "auc",
    "aucpr": "average_precision",
}
_ERROR_RATE_METRICS = frozenset({"error", "merror"})
_PHASE_MESSAGES = {
    "QUEUED": "Training job queued",
    "LOADING_DATA": "Loading training data",
    "FEATURE_ENGINEERING": "Preparing model features",
    "DATA_SPLITTING": "Splitting training, validation, and test data",
    "TRAINING": "Training model",
    "EVALUATING": "Evaluating and exporting model",
}


class RedisEventReporter(EventReporter):
    """Publish KnoVa-compatible events with bounded retries.

    The public methods follow the Core :class:`EventReporter` contract. The
    provider-specific ``report_failed_with_code`` method preserves KnoVa's
    error-code field for invalid task envelopes. Non-terminal worker events
    remain fail-open; terminal events are durably staged before guarded XADD.
    """

    def __init__(
        self,
        redis_client: Any,
        config: RedisBrokerConfig,
        job_id: str | None = None,
        *,
        stream_key: str | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._redis = redis_client
        self._config = config
        self._job_id = job_id
        self._stream_key = stream_key
        self._sleep = sleep
        self._last_failure_log_at: float | None = None
        self._started_at = time.monotonic()

    @property
    def job_id(self) -> str | None:
        """Return the default job identity bound to this reporter."""
        return self._job_id

    def _publish(
        self,
        job_id: str | None,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> bool:
        effective_job_id = job_id if job_id is not None else self._job_id
        if effective_job_id is not None:
            try:
                effective_job_id = validate_job_id(effective_job_id)
            except ValueError:
                logger.warning(
                    "Skipping broker event with invalid job_id: event_type=%s",
                    event_type,
                )
                return False
        if event_type in TERMINAL_EVENT_TYPES and payload is not None:
            payload = dict(payload)
            if "duration_seconds" in payload:
                try:
                    payload["duration_seconds"] = quantize_duration_seconds(
                        payload["duration_seconds"]
                    )
                except ValueError:
                    logger.warning(
                        "Skipping terminal event with invalid duration_seconds: "
                        "job_id=%s event_type=%s",
                        effective_job_id,
                        event_type,
                    )
                    return False
        event = event_payload(
            job_id=effective_job_id,
            event_type=event_type,
            payload={"timestamp": int(time.time() * 1000), **(payload or {})},
        )
        try:
            encoded = json.dumps(event, separators=(",", ":"), allow_nan=False)
        except (TypeError, ValueError) as exc:
            logger.warning(
                "Skipping non-JSON broker event: job_id=%s event_type=%s error=%s",
                effective_job_id,
                event_type,
                type(exc).__name__,
            )
            return False
        if len(encoded.encode("utf-8")) > self._config.max_event_bytes:
            if event_type == "COMPLETED":
                return self._publish(
                    effective_job_id,
                    "FAILED",
                    build_failed_payload(
                        "COMPLETED event exceeded the configured size limit",
                        phase="EVALUATING",
                        duration_seconds=time.monotonic() - self._started_at,
                        error_code="PAYLOAD_TOO_LARGE",
                    ),
                )
            logger.warning(
                "Skipping oversized broker event: job_id=%s event_type=%s limit=%d",
                effective_job_id,
                event_type,
                self._config.max_event_bytes,
            )
            return False
        stream_key = self._stream_key or (
            self._config.event_stream_key(effective_job_id)
            if effective_job_id is not None
            else self._config.invalid_event_stream_key
        )
        terminal = event_type in TERMINAL_EVENT_TYPES
        guard = TerminalGuard(
            self._redis,
            max_stream_length=self._config.max_stream_length,
        )
        candidates = TerminalCandidateStore(self._redis, self._config)
        candidate_saved = False
        for attempt in range(self._config.max_publish_retries + 1):
            try:
                if terminal and effective_job_id is not None and not candidate_saved:
                    encoded = candidates.save(effective_job_id, encoded)
                    staged_event = json.loads(encoded)
                    if not isinstance(staged_event, dict):
                        raise ValueError("terminal candidate must be a JSON object")
                    event_type = str(staged_event.get("event_type", ""))
                    payload = {
                        key: value
                        for key, value in staged_event.items()
                        if key not in {"protocol_version", "event_type", "job_id"}
                    }
                    candidate_saved = True
                result = guard.publish(
                    stream_key,
                    job_id=effective_job_id,
                    encoded_event=encoded,
                    event_type=event_type,
                    phase=(payload or {}).get("phase"),
                )
                if result.decision is PublishDecision.REJECTED_AFTER_TERMINAL:
                    return False
                if result.accepted:
                    if terminal and effective_job_id is not None:
                        try:
                            candidates.delete(effective_job_id)
                        except Exception as cleanup_exc:
                            logger.warning(
                                "Terminal candidate cleanup failed: job_id=%s "
                                "error=%s detail=%s",
                                effective_job_id,
                                type(cleanup_exc).__name__,
                                redact_sensitive(str(cleanup_exc)),
                            )
                    if event_type == "PHASE" and effective_job_id is not None:
                        try:
                            ActiveJobStore(self._redis, self._config).update_phase(
                                effective_job_id,
                                str((payload or {}).get("phase", "")),
                            )
                        except Exception as phase_exc:
                            logger.debug(
                                "Active phase update failed: job_id=%s "
                                "error=%s detail=%s",
                                effective_job_id,
                                type(phase_exc).__name__,
                                redact_sensitive(str(phase_exc)),
                            )
                    return True
            except Exception as exc:
                now = time.monotonic()
                should_log = (
                    self._last_failure_log_at is None
                    or now - self._last_failure_log_at
                    >= self._config.failure_log_interval
                )
                if should_log:
                    self._last_failure_log_at = now
                    logger.warning(
                        "Failed to publish broker event: job_id=%s event_type=%s "
                        "attempt=%d/%d error=%s detail=%s",
                        effective_job_id,
                        event_type,
                        attempt + 1,
                        self._config.max_publish_retries + 1,
                        type(exc).__name__,
                        redact_sensitive(str(exc)),
                    )
                else:
                    logger.debug(
                        "Broker event publish retry suppressed from warning log: "
                        "job_id=%s event_type=%s attempt=%d/%d",
                        effective_job_id,
                        event_type,
                        attempt + 1,
                        self._config.max_publish_retries + 1,
                    )
                if attempt < self._config.max_publish_retries:
                    self._sleep(self._config.publish_retry_delay)
        return False

    def report_phase(self, job_id: str, phase: str) -> None:
        self.report_phase_durable(job_id, phase)

    def report_phase_durable(self, job_id: str, phase: str) -> bool:
        """Publish a phase and expose durability to Driver-side control flow."""
        return self._publish(
            job_id,
            "PHASE",
            {"phase": phase, "message": _PHASE_MESSAGES.get(phase, phase)},
        )

    def report_log(self, job_id: str, message: str, level: str = "INFO") -> None:
        normalized_level = level.strip().lower()
        if normalized_level == "warn":
            normalized_level = "warning"
        if normalized_level not in _LOG_LEVELS:
            supported = ", ".join(sorted(_LOG_LEVELS))
            raise ValueError(
                f"Unsupported broker log level {level!r}; expected one of: {supported}"
            )
        self._publish(
            job_id,
            "LOG",
            {"message": redact_sensitive(message), "level": normalized_level},
        )

    def report_metrics(
        self,
        job_id: str,
        metrics: dict[str, float],
        progress: float,
    ) -> None:
        progress = finite_float(progress, "progress")
        if not 0 <= progress <= 1:
            raise ValueError("progress must be in [0, 1]")
        current_round = int(metrics.get("round", 0))
        total_rounds = (
            max(current_round, int(round(current_round / progress)))
            if current_round and progress
            else current_round
        )
        values: dict[str, dict[str, Any]] = {}
        for raw_name, raw_value in metrics.items():
            if raw_name == "round":
                continue
            value = finite_float(raw_value, f"metrics.{raw_name}")
            scope, separator, metric_name = raw_name.partition("-")
            if not separator:
                metric_name = raw_name
                scope = "eval"
            if metric_name in _ERROR_RATE_METRICS:
                if not 0 <= value <= 1:
                    raise ValueError(f"metrics.{raw_name} error rate must be in [0, 1]")
                value = 1.0 - value
            metric_name = _METRIC_NAMES.get(metric_name, metric_name)
            entry = values.setdefault(metric_name, {"metric_name": metric_name})
            entry["train" if scope == "train" else "eval"] = value
        process_metrics = [
            ProcessMetric.model_validate(value).model_dump(exclude_none=True)
            for value in values.values()
        ]
        self._publish(
            job_id,
            "METRICS",
            {
                "phase": "TRAINING",
                "current_round": current_round,
                "total_rounds": total_rounds,
                "progress_percent": round(progress * 100, 1),
                "metrics": process_metrics,
            },
        )

    def report_completed(self, job_id: str, result: JobResult) -> None:
        self.report_legacy_completed(job_id, result)

    def report_legacy_completed(self, job_id: str, result: JobResult) -> bool:
        """Publish and expose durability for the opt-in legacy worker path."""
        duration_seconds = time.monotonic() - self._started_at
        return self._publish(
            job_id,
            "COMPLETED",
            {
                "phase": "COMPLETED",
                "duration_seconds": duration_seconds,
                "result_summary": {
                    "status": result.status,
                    "metrics": result.metrics,
                },
                "training_result": {
                    "run_id": result.run_id,
                    "attempt_id": result.attempt_id,
                    "execution_id": result.execution_id,
                    "submission_id": result.submission_id,
                },
                "artifact_manifest": {
                    "bundle_id": result.bundle_id,
                    "bundle_uri": result.bundle_uri,
                    "manifest_uri": result.manifest_uri,
                    "artifacts": result.artifacts,
                    "artifact_refs": result.artifact_refs,
                },
                "status": result.status,
                "run_id": result.run_id,
                "attempt_id": result.attempt_id,
                "execution_id": result.execution_id,
                "submission_id": result.submission_id,
                "bundle_id": result.bundle_id,
                "bundle_uri": result.bundle_uri,
                "manifest_uri": result.manifest_uri,
                "metrics": result.metrics,
                "artifacts": result.artifacts,
                "artifact_refs": result.artifact_refs,
            },
        )

    def report_completed_payload(self, job_id: str, payload: dict[str, Any]) -> bool:
        """Publish a completion already built from Core's neutral summary."""
        return self._publish(job_id, "COMPLETED", payload)

    def report_failed(self, job_id: str, error: str) -> None:
        self.report_failed_with_code(job_id, error)

    def report_failed_with_code(
        self,
        job_id: str | None,
        error: BaseException | str,
        error_code: str | None = None,
        *,
        delivery_id: str | None = None,
        phase: str = "TRAINING",
        duration_seconds: float | None = None,
    ) -> bool:
        payload = build_failed_payload(
            error,
            phase=phase,
            duration_seconds=(
                time.monotonic() - self._started_at
                if duration_seconds is None
                else duration_seconds
            ),
            error_code=error_code,
        )
        if delivery_id is not None:
            payload["delivery_id"] = delivery_id
        return self._publish(
            job_id,
            "FAILED",
            payload,
        )

    def report_cancelled(
        self,
        job_id: str,
        phase: str = "TRAINING",
        *,
        duration_seconds: float | None = None,
        has_best_model: bool = False,
    ) -> bool:
        return self._publish(
            job_id,
            "CANCELLED",
            build_cancelled_payload(
                phase=phase,
                duration_seconds=(
                    time.monotonic() - self._started_at
                    if duration_seconds is None
                    else duration_seconds
                ),
                has_best_model=has_best_model,
            ),
        )
