"""Provider-owned active submission tracking and Ray Jobs cancellation."""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from tributo.ray_jobs import RayJobSubmission, get_ray_job_status, stop_ray_job

from tributo_broker_redis.config import ChannelConfig, ExecutionProfile, OperationType
from tributo_broker_redis.reporter import RedisEventReporter

logger = logging.getLogger(__name__)
_TERMINAL_EVENT_TYPES = frozenset({"COMPLETED", "FAILED", "CANCELLED"})
_TERMINAL_LOOKBACK = 16


@dataclass(frozen=True)
class ActiveSubmission:
    operation_id: str
    operation_type: OperationType
    execution_profile: ExecutionProfile
    run_id: str
    channel: ChannelConfig
    submission: RayJobSubmission
    received_at_ms: int | None = None
    deadline_at_ms: int | None = None


class ActiveSubmissionMap:
    """Intentionally in-memory v0.1 operation-to-submission mapping."""

    def __init__(self) -> None:
        self._items: dict[str, ActiveSubmission] = {}
        self._lock = threading.Lock()

    def put(self, value: ActiveSubmission) -> None:
        with self._lock:
            self._items[value.operation_id] = value

    def remove(self, operation_id: str) -> ActiveSubmission | None:
        with self._lock:
            return self._items.pop(operation_id, None)

    def snapshot(self) -> tuple[ActiveSubmission, ...]:
        with self._lock:
            return tuple(self._items.values())

    def get(self, operation_id: str) -> ActiveSubmission | None:
        with self._lock:
            return self._items.get(operation_id)


def terminal_event_published(
    redis_client: Any,
    item: ActiveSubmission,
) -> bool:
    """Find a recent terminal event even if a recovery event followed it."""
    xrevrange = getattr(redis_client, "xrevrange", None)
    if not callable(xrevrange):
        return False
    stream = item.channel.event_stream_key(item.operation_id)
    entries = xrevrange(stream, count=_TERMINAL_LOOKBACK)
    for _entry_id, fields in entries:
        raw = fields.get("payload", fields.get(b"payload"))
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        if not isinstance(raw, str):
            continue
        try:
            event = json.loads(raw)
            event_type = event.get("event_type")
            submission_id = event.get("submission_id")
        except (json.JSONDecodeError, AttributeError):
            continue
        if (
            event_type in _TERMINAL_EVENT_TYPES
            and submission_id == item.submission.submission_id
        ):
            return True
        # KnoVa v2 uses an immutable job_id/execution_id and has no submission_id.
        # Keep the generic protocol's strict attempt matching above intact.
        if (
            event_type in _TERMINAL_EVENT_TYPES
            and submission_id is None
            and event.get("protocol_version") == "2.0"
            and item.channel.outer_identity_field in {"job_id", "execution_id"}
            and event.get(item.channel.outer_identity_field) == item.operation_id
        ):
            return True
    return False


class CancelWatcher:
    """Observe cancel keys and publish CANCELLED only after Ray reports STOPPED."""

    def __init__(
        self,
        redis_client: Any,
        active: ActiveSubmissionMap,
        *,
        dashboard_url: str,
        interval_seconds: float,
        max_event_bytes: int,
        max_stream_length: int,
        status_getter: Callable[..., str] = get_ray_job_status,
        stopper: Callable[..., bool] = stop_ray_job,
        reporter_factory: Callable[..., RedisEventReporter] = RedisEventReporter,
    ) -> None:
        self._redis = redis_client
        self._active = active
        self._dashboard_url = dashboard_url
        self._interval = interval_seconds
        self._max_event_bytes = max_event_bytes
        self._max_stream_length = max_stream_length
        self._status_getter = status_getter
        self._stopper = stopper
        self._reporter_factory = reporter_factory
        self._stop_requested: set[str] = set()
        self._timeout_requested: set[str] = set()
        self._closed = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run,
            name="tributo-redis-cancel-watcher",
            daemon=True,
        )
        self._thread.start()

    def _run(self) -> None:
        while not self._closed.wait(self._interval):
            self.check_once()

    def check_once(self) -> None:
        for item in self._active.snapshot():
            try:
                if self._terminal_event_published(item):
                    logger.info(
                        "Terminal event observed: operation_id=%s submission_id=%s",
                        item.operation_id,
                        item.submission.submission_id,
                    )
                    self._active.remove(item.operation_id)
                    self._stop_requested.discard(item.operation_id)
                    self._timeout_requested.discard(item.operation_id)
                    continue
                status = self._status_getter(
                    item.submission.submission_id,
                    dashboard_url=self._dashboard_url,
                )
                cancel_key = item.channel.cancel_key(item.operation_id)
                cancelled = bool(self._redis.exists(cancel_key))
                timed_out = (
                    item.deadline_at_ms is not None
                    and time.time() * 1000 >= item.deadline_at_ms
                )
                # Ray metadata plus the persistent cancel marker also recover
                # intent after a Consumer restarts between stop and STOPPED.
                timeout_stop = item.operation_id in self._timeout_requested or (
                    timed_out
                    and not cancelled
                    and item.operation_id not in self._stop_requested
                )
                if status in {"SUCCEEDED", "FAILED"}:
                    code = (
                        "EXECUTION_TIMEOUT"
                        if timeout_stop
                        else "RAY_JOB_FAILED"
                        if status == "FAILED"
                        else "TERMINAL_EVENT_MISSING"
                    )
                    self._report_failed(item, code)
                    self._active.remove(item.operation_id)
                    self._stop_requested.discard(item.operation_id)
                    self._timeout_requested.discard(item.operation_id)
                    continue
                if status == "STOPPED":
                    if timeout_stop:
                        self._report_failed(item, "EXECUTION_TIMEOUT")
                    elif cancelled or item.operation_id in self._stop_requested:
                        self._report_cancelled(item)
                    else:
                        self._report_failed(item, "RAY_JOB_STOPPED")
                    self._active.remove(item.operation_id)
                    self._stop_requested.discard(item.operation_id)
                    self._timeout_requested.discard(item.operation_id)
                    continue
                if cancelled or timed_out:
                    self._stopper(
                        item.submission.submission_id,
                        dashboard_url=self._dashboard_url,
                    )
                    if cancelled:
                        self._stop_requested.add(item.operation_id)
                    else:
                        self._timeout_requested.add(item.operation_id)
            except Exception:
                logger.warning(
                    "Ray lifecycle watcher check failed; will retry: "
                    "operation_id=%s submission_id=%s",
                    item.operation_id,
                    item.submission.submission_id,
                    exc_info=True,
                )

    def _terminal_event_published(self, item: ActiveSubmission) -> bool:
        return terminal_event_published(self._redis, item)

    def _report_cancelled(self, item: ActiveSubmission) -> None:
        reporter = self._reporter(item)
        reporter.publish(
            "CANCELLED",
            {"reason": "cancel key observed after admission"},
            phase="CANCELLED",
        )

    def _report_failed(self, item: ActiveSubmission, code: str) -> None:
        reporter = self._reporter(item)
        logger.warning(
            "Ray job ended without a terminal event; publishing fallback: "
            "operation_id=%s submission_id=%s ray_job_id=%s code=%s stream=%s",
            item.operation_id,
            item.submission.submission_id,
            item.submission.ray_job_id or "<unknown>",
            code,
            reporter.stream_key,
        )
        reporter.publish(
            "FAILED",
            {
                "error_code": code,
                "sanitized_message": "Ray job ended before publishing a terminal event",
                "retryable": False,
            },
            phase="FAILED",
        )
        logger.info(
            "Fallback FAILED event published: operation_id=%s submission_id=%s "
            "code=%s stream=%s",
            item.operation_id,
            item.submission.submission_id,
            code,
            reporter.stream_key,
        )

    def _reporter(self, item: ActiveSubmission) -> RedisEventReporter:
        reporter = self._reporter_factory(
            self._redis,
            event_stream_prefix=item.channel.event_stream_prefix,
            operation_id=item.operation_id,
            operation_type=item.operation_type,
            execution_profile=item.execution_profile,
            run_id=item.run_id,
            attempt_id=item.submission.attempt_id,
            submission_id=item.submission.submission_id,
            ray_job_id=item.submission.ray_job_id,
            outer_identity_field=item.channel.outer_identity_field,
            max_event_bytes=self._max_event_bytes,
            max_stream_length=self._max_stream_length,
            received_at_ms=item.received_at_ms,
        )
        return reporter

    def close(self) -> None:
        self._closed.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self._interval * 2))


__all__ = [
    "ActiveSubmission",
    "ActiveSubmissionMap",
    "CancelWatcher",
    "terminal_event_published",
]
