"""Best-effort Redis Stream lifecycle reporter."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from typing import Any

from tributo.integrations.broker import EventReporter, JobResult

from tributo_broker_redis.config import RedisBrokerConfig
from tributo_broker_redis.protocol import event_payload

logger = logging.getLogger(__name__)

_LOG_LEVELS = frozenset({"debug", "info", "warning", "error", "success"})


class RedisEventReporter(EventReporter):
    """Publish KnoVa-compatible events with bounded, fail-open retries.

    The public methods follow the Core :class:`EventReporter` contract. The
    provider-specific ``report_failed_with_code`` method preserves KnoVa's
    error-code field for invalid task envelopes.
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
        self._terminal_sent = False
        self._last_failure_log_at: float | None = None

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
        if event_type in {"COMPLETED", "FAILED", "CANCELLED"} and self._terminal_sent:
            logger.warning(
                "Refusing duplicate terminal event: job_id=%s event_type=%s",
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
        for attempt in range(self._config.max_publish_retries + 1):
            try:
                fields = {"payload": encoded}
                if effective_job_id is not None:
                    fields["job_id"] = effective_job_id
                self._redis.xadd(
                    stream_key,
                    fields,
                    maxlen=self._config.max_stream_length,
                )
                if event_type in {"COMPLETED", "FAILED", "CANCELLED"}:
                    self._terminal_sent = True
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
                        "attempt=%d/%d error=%s",
                        effective_job_id,
                        event_type,
                        attempt + 1,
                        self._config.max_publish_retries + 1,
                        type(exc).__name__,
                        exc_info=True,
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
        self._publish(job_id, "PHASE", {"phase": phase})

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
            {"message": message, "level": normalized_level},
        )

    def report_metrics(
        self,
        job_id: str,
        metrics: dict[str, float],
        progress: float,
    ) -> None:
        self._publish(
            job_id,
            "METRICS",
            {
                "progress": progress,
                "progress_percent": round(progress * 100, 1),
                "metrics": metrics,
            },
        )

    def report_completed(self, job_id: str, result: JobResult) -> None:
        self._publish(
            job_id,
            "COMPLETED",
            {
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

    def report_failed(self, job_id: str, error: str) -> None:
        self.report_failed_with_code(job_id, error)

    def report_failed_with_code(
        self,
        job_id: str | None,
        error: str,
        error_code: str = "UNKNOWN",
        *,
        delivery_id: str | None = None,
    ) -> bool:
        payload: dict[str, Any] = {
            "error_message": error,
            "error_code": error_code,
        }
        if delivery_id is not None:
            payload["delivery_id"] = delivery_id
        return self._publish(
            job_id,
            "FAILED",
            payload,
        )

    def report_cancelled(self, job_id: str, phase: str = "TRAINING") -> None:
        self._publish(job_id, "CANCELLED", {"phase": phase})
