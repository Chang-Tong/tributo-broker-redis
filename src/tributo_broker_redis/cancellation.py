"""Provider-owned active submission tracking and Ray Jobs cancellation."""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from tributo.ray_jobs import RayJobSubmission, get_ray_job_status, stop_ray_job

from tributo_broker_redis.config import ChannelConfig, ExecutionProfile, OperationType
from tributo_broker_redis.reporter import RedisEventReporter

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ActiveSubmission:
    operation_id: str
    operation_type: OperationType
    execution_profile: ExecutionProfile
    run_id: str
    channel: ChannelConfig
    submission: RayJobSubmission


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
                    self._active.remove(item.operation_id)
                    self._stop_requested.discard(item.operation_id)
                    continue
                status = self._status_getter(
                    item.submission.submission_id,
                    dashboard_url=self._dashboard_url,
                )
                if status in {"SUCCEEDED", "FAILED"}:
                    code = (
                        "RAY_JOB_FAILED"
                        if status == "FAILED"
                        else "TERMINAL_EVENT_MISSING"
                    )
                    self._report_failed(item, code)
                    self._active.remove(item.operation_id)
                    self._stop_requested.discard(item.operation_id)
                    continue
                if status == "STOPPED":
                    if item.operation_id in self._stop_requested:
                        self._report_cancelled(item)
                    else:
                        self._report_failed(item, "RAY_JOB_STOPPED")
                    self._active.remove(item.operation_id)
                    self._stop_requested.discard(item.operation_id)
                    continue
                cancel_key = item.channel.cancel_key(item.operation_id)
                if bool(self._redis.exists(cancel_key)):
                    self._stopper(
                        item.submission.submission_id,
                        dashboard_url=self._dashboard_url,
                    )
                    self._stop_requested.add(item.operation_id)
            except Exception:
                logger.warning(
                    "Cancellation watcher check failed: operation_id=%s",
                    item.operation_id,
                    exc_info=True,
                )

    def _terminal_event_published(self, item: ActiveSubmission) -> bool:
        xrevrange = getattr(self._redis, "xrevrange", None)
        if not callable(xrevrange):
            return False
        stream = item.channel.event_stream_key(item.operation_id)
        entries = xrevrange(stream, count=1)
        if not entries:
            return False
        raw = entries[0][1].get("payload")
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        if not isinstance(raw, str):
            return False
        try:
            event = json.loads(raw)
            event_type = event.get("event_type")
            submission_id = event.get("submission_id")
        except (json.JSONDecodeError, AttributeError):
            return False
        return (
            event_type
            in {
                "COMPLETED",
                "FAILED",
                "CANCELLED",
            }
            and submission_id == item.submission.submission_id
        )

    def _report_cancelled(self, item: ActiveSubmission) -> None:
        reporter = self._reporter(item)
        reporter.publish(
            "CANCELLED",
            {"reason": "cancel key observed after admission"},
            phase="CANCELLED",
        )

    def _report_failed(self, item: ActiveSubmission, code: str) -> None:
        reporter = self._reporter(item)
        reporter.publish(
            "FAILED",
            {
                "error_code": code,
                "sanitized_message": "Ray job ended before publishing a terminal event",
                "retryable": False,
            },
            phase="FAILED",
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
        )
        return reporter

    def close(self) -> None:
        self._closed.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self._interval * 2))


__all__ = ["ActiveSubmission", "ActiveSubmissionMap", "CancelWatcher"]
