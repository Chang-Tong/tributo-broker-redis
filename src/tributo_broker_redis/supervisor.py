"""Redis-backed reconciliation for accepted Ray training jobs."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable, Iterator
from typing import Any

from ray.job_submission import JobSubmissionClient
from redis import exceptions as redis_exceptions

from tributo_broker_redis.active_jobs import (
    ActiveJobRecord,
    ActiveJobStore,
    TerminalCandidateStore,
)
from tributo_broker_redis.config import RedisBrokerConfig
from tributo_broker_redis.reporter import RedisEventReporter
from tributo_broker_redis.terminal_guard import TERMINAL_EVENT_TYPES, TerminalGuard

logger = logging.getLogger(__name__)
_RAY_NOT_FOUND = "__RAY_JOB_NOT_FOUND__"


def _redis_transient_exception_types() -> tuple[type[BaseException], ...]:
    result: list[type[BaseException]] = []
    for name in (
        "ConnectionError",
        "TimeoutError",
        "ReadOnlyError",
        "MasterDownError",
        "ClusterDownError",
        "TryAgainError",
        "ClusterError",
        "SlotNotCoveredError",
        "AskError",
        "MovedError",
    ):
        value = getattr(redis_exceptions, name, None)
        if isinstance(value, type) and issubclass(value, BaseException):
            result.append(value)
    return tuple(result)


_REDIS_GLOBAL_TRANSIENT_ERRORS = _redis_transient_exception_types()


def _create_ray_client(dashboard_url: str) -> JobSubmissionClient:
    return JobSubmissionClient(dashboard_url)


class ActiveJobSupervisor:
    """Perform one bounded, restart-safe active job reconciliation tick."""

    def __init__(
        self,
        redis_client: Any,
        config: RedisBrokerConfig,
        *,
        ray_client_factory: Callable[[str], Any] = _create_ray_client,
        monotonic: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        self._redis = redis_client
        self._config = config
        self._active = ActiveJobStore(redis_client, config)
        self._candidates = TerminalCandidateStore(redis_client, config)
        self._guard = TerminalGuard(
            redis_client, max_stream_length=config.max_stream_length
        )
        self._ray_client_factory = ray_client_factory
        self._ray_client: Any | None = None
        self._monotonic = monotonic
        self._wall_clock = wall_clock
        self._next_tick = 0.0
        self._scan_keys: Iterator[str] | None = None

    def maintain(self) -> None:
        now = self._monotonic()
        if now < self._next_tick:
            return
        self._next_tick = now + self._config.supervisor_interval_seconds
        for key in self._next_keys(self._config.supervisor_scan_count):
            try:
                record = self._active.load_key(key)
            except Exception as exc:
                if self._is_global_transient(exc):
                    raise
                try:
                    self._handle_bad_active(key, exc)
                except Exception as recovery_exc:
                    if self._is_global_transient(recovery_exc):
                        raise
                    self._log_isolation_failure(
                        self._job_id_from_active_key(key), recovery_exc
                    )
                continue
            if record is None:
                continue
            try:
                self._reconcile(record)
            except Exception as exc:
                if self._is_global_transient(exc):
                    raise
                try:
                    self._handle_job_error(record, exc)
                except Exception as recovery_exc:
                    if self._is_global_transient(recovery_exc):
                        raise
                    self._log_isolation_failure(record.job_id, recovery_exc)

    def _next_keys(self, limit: int) -> Iterator[str]:
        examined = 0
        while examined < limit:
            if self._scan_keys is None:
                self._scan_keys = iter(self._active.scan_keys())
            try:
                key = next(self._scan_keys)
            except StopIteration:
                self._scan_keys = None
                break
            examined += 1
            yield key

    @staticmethod
    def _is_global_transient(error: BaseException) -> bool:
        if isinstance(
            error,
            (ConnectionError, TimeoutError, OSError, *_REDIS_GLOBAL_TRANSIENT_ERRORS),
        ):
            return True
        detail = str(error).lower()
        return isinstance(error, RuntimeError) and (
            "temporarily unavailable" in detail
            or "dashboard unavailable" in detail
            or any(f"status code {code}" in detail for code in range(500, 600))
        )

    def _job_id_from_active_key(self, key: str) -> str | None:
        prefix = f"{self._config.active_job_key_prefix}:{{"
        if key.startswith(prefix) and key.endswith("}"):
            return key[len(prefix) : -1] or None
        return None

    def _handle_bad_active(self, key: str, error: BaseException) -> None:
        job_id = self._job_id_from_active_key(key)
        logger.warning(
            "Discarding invalid active job record: job_id=%s error=%s detail=%s",
            job_id,
            type(error).__name__,
            self._safe_detail(error),
        )
        if job_id is None:
            self._redis.delete(key)
            return
        reporter = RedisEventReporter(self._redis, self._config, job_id)
        reporter.report_failed_with_code(
            job_id,
            "Active job record is invalid",
            "TRAINING_FAILED",
            phase="QUEUED",
            duration_seconds=0.0,
        )
        if not self._cleanup_if_terminal(job_id):
            self._redis.delete(key)

    def _handle_job_error(self, record: ActiveJobRecord, error: BaseException) -> None:
        logger.warning(
            "Active job reconciliation failed in isolation: job_id=%s "
            "error=%s detail=%s",
            record.job_id,
            type(error).__name__,
            self._safe_detail(error),
        )
        try:
            if self._prefer_candidate(record):
                return
        except (ValueError, TypeError, json.JSONDecodeError):
            # A corrupt candidate is job-local. Reporter staging atomically
            # replaces invalid candidate data with the controlled failure.
            pass
        self._publish_failed(
            record,
            code="TRAINING_FAILED",
            message="Active job reconciliation failed",
        )
        self._cleanup_if_terminal(record.job_id)

    def _log_isolation_failure(self, job_id: str | None, error: BaseException) -> None:
        logger.warning(
            "Job-isolated recovery also failed: job_id=%s error=%s detail=%s",
            job_id,
            type(error).__name__,
            self._safe_detail(error),
        )

    @staticmethod
    def _safe_detail(error: BaseException) -> str:
        from tributo_broker_redis.completion import redact_sensitive

        return redact_sensitive(str(error))

    def _client(self) -> Any:
        if self._ray_client is None:
            self._ray_client = self._ray_client_factory(self._config.ray_dashboard_url)
        return self._ray_client

    def _terminal(self, job_id: str) -> dict[str, Any] | None:
        return self._guard.terminal_event(self._config.event_stream_key(job_id), job_id)

    def _cleanup_if_terminal(self, job_id: str) -> bool:
        if self._terminal(job_id) is None:
            return False
        self._active.delete(job_id)
        self._candidates.delete(job_id)
        return True

    def _replay_candidate(self, record: ActiveJobRecord) -> bool:
        encoded = self._candidates.load(record.job_id)
        if encoded is None:
            return False
        try:
            event = json.loads(encoded)
        except json.JSONDecodeError as exc:
            raise ValueError("terminal candidate is invalid JSON") from exc
        if not isinstance(event, dict):
            raise ValueError("terminal candidate must be an object")
        if event.get("job_id") != record.job_id:
            raise ValueError("terminal candidate job_id does not match active record")
        event_type = event.get("event_type")
        if event_type not in TERMINAL_EVENT_TYPES:
            raise ValueError("terminal candidate is not a terminal event")
        result = self._guard.publish(
            self._config.event_stream_key(record.job_id),
            job_id=record.job_id,
            encoded_event=encoded,
            event_type=str(event_type),
            phase=str(event.get("phase", "")),
        )
        return result.accepted

    def _prefer_candidate(self, record: ActiveJobRecord) -> bool:
        """Replay a worker terminal before synthesizing a supervisor terminal."""
        if self._cleanup_if_terminal(record.job_id):
            return True
        if self._candidates.load(record.job_id) is None:
            return False
        if self._replay_candidate(record):
            self._cleanup_if_terminal(record.job_id)
        return True

    def _publish_failed(
        self, record: ActiveJobRecord, *, code: str, message: str
    ) -> None:
        RedisEventReporter(
            self._redis, self._config, record.job_id
        ).report_failed_with_code(
            record.job_id,
            message,
            code,
            phase=record.current_phase,
            duration_seconds=max(0.0, self._wall_clock() - record.submitted_at),
        )

    def _publish_cancelled(self, record: ActiveJobRecord) -> None:
        RedisEventReporter(self._redis, self._config, record.job_id).report_cancelled(
            record.job_id,
            record.current_phase,
            duration_seconds=max(0.0, self._wall_clock() - record.submitted_at),
        )

    def _status(self, record: ActiveJobRecord) -> str | None:
        try:
            raw_status = self._client().get_job_status(record.execution_id)
        except Exception as exc:
            detail = str(exc).lower()
            if any(
                marker in detail for marker in ("not found", "does not exist", "404")
            ):
                return _RAY_NOT_FOUND
            raise
        return str(getattr(raw_status, "value", raw_status)).upper()

    def _apply_status(
        self,
        record: ActiveJobRecord,
        status: str,
        *,
        stopped_terminal: str | None = None,
    ) -> None:
        if self._prefer_candidate(record):
            return
        if status in {"PENDING", "RUNNING"}:
            self._active.refresh(record.job_id)
            return
        if status == "STOPPED" and stopped_terminal == "CANCELLED":
            self._publish_cancelled(record)
        elif status == "STOPPED" and stopped_terminal == "TRAINING_TIMEOUT":
            self._publish_failed(
                record,
                code="TRAINING_TIMEOUT",
                message="Training exceeded max_training_time_seconds",
            )
        elif status == _RAY_NOT_FOUND:
            self._publish_failed(
                record,
                code="TRAINING_FAILED",
                message="Ray job no longer exists",
            )
        elif status in {"SUCCEEDED", "FAILED", "STOPPED"}:
            self._publish_failed(
                record,
                code="TRAINING_FAILED",
                message=f"Ray job {status.lower()} without a durable terminal event",
            )
        else:
            self._publish_failed(
                record,
                code="TRAINING_FAILED",
                message="Ray returned an unknown job status",
            )
        self._cleanup_if_terminal(record.job_id)

    def _stop_and_reconcile(
        self, record: ActiveJobRecord, *, stopped_terminal: str
    ) -> None:
        stopped = bool(self._client().stop_job(record.execution_id))
        if stopped:
            if self._prefer_candidate(record):
                return
            if stopped_terminal == "CANCELLED":
                self._publish_cancelled(record)
            else:
                self._publish_failed(
                    record,
                    code="TRAINING_TIMEOUT",
                    message="Training exceeded max_training_time_seconds",
                )
            self._cleanup_if_terminal(record.job_id)
            return

        if self._prefer_candidate(record):
            return
        status = self._status(record)
        if status is not None:
            self._apply_status(
                record,
                status,
                stopped_terminal=stopped_terminal,
            )

    def _reconcile(self, record: ActiveJobRecord) -> None:
        if self._prefer_candidate(record):
            return

        cancelled = bool(self._redis.exists(self._config.cancel_key(record.job_id)))
        if cancelled:
            self._stop_and_reconcile(record, stopped_terminal="CANCELLED")
            return

        if record.deadline_at is not None and self._wall_clock() >= record.deadline_at:
            self._stop_and_reconcile(record, stopped_terminal="TRAINING_TIMEOUT")
            return

        status = self._status(record)
        if status is not None:
            self._apply_status(record, status)
