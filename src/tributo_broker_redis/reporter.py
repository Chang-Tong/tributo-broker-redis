"""Best-effort generic event publication owned by the provider."""

from __future__ import annotations

import json
import re
import time
import uuid
from typing import Any

from tributo_broker_redis.config import ExecutionProfile, OperationType
from tributo_broker_redis.protocol import PROTOCOL_PROFILE, PROTOCOL_VERSION

_SENSITIVE_KEY = re.compile(
    r"(?:password|passwd|secret|token|access[_-]?key|private[_-]?key)", re.IGNORECASE
)


def redact(value: Any) -> Any:
    """Recursively remove values carried by credential-like keys."""
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if key == "credential_ref":
                result[str(key)] = str(item)
            elif _SENSITIVE_KEY.search(str(key)):
                result[str(key)] = "[REDACTED]"
            else:
                result[str(key)] = redact(item)
        return result
    if isinstance(value, (list, tuple)):
        return [redact(item) for item in value]
    return value


class RedisEventReporter:
    """Publish bounded events for one public operation identity."""

    def __init__(
        self,
        redis_client: Any,
        *,
        event_stream_prefix: str,
        operation_id: str,
        operation_type: OperationType,
        execution_profile: ExecutionProfile,
        run_id: str,
        attempt_id: str,
        submission_id: str | None = None,
        ray_job_id: str | None = None,
        outer_identity_field: str = "operation_id",
        max_event_bytes: int = 1024 * 1024,
        max_stream_length: int = 1000,
        received_at_ms: int | None = None,
        deadline_at_ms: int | None = None,
        cancel_key: str | None = None,
    ) -> None:
        self._redis = redis_client
        self._stream = f"{event_stream_prefix}:{operation_id}"
        self._outer_identity_field = outer_identity_field
        self._identity = {
            "operation_id": operation_id,
            "operation_type": operation_type,
            "execution_profile": execution_profile,
            "run_id": run_id,
            "attempt_id": attempt_id,
            "submission_id": submission_id,
            "ray_job_id": ray_job_id,
        }
        self._max_event_bytes = max_event_bytes
        self._max_stream_length = max_stream_length
        self._received_at_ms = (
            int(received_at_ms) if received_at_ms is not None else None
        )
        self._deadline_at_ms = deadline_at_ms
        self._cancel_key = cancel_key

    @property
    def stream_key(self) -> str:
        return self._stream

    def publish(
        self,
        event_type: str,
        payload: dict[str, Any] | None = None,
        *,
        phase: str | None = None,
    ) -> dict[str, Any]:
        event = {
            "protocol_profile": PROTOCOL_PROFILE,
            "protocol_version": PROTOCOL_VERSION,
            "event_id": uuid.uuid4().hex,
            "timestamp_ms": int(time.time() * 1000),
            **self._identity,
            "event_type": event_type,
            "phase": phase,
            "payload": redact(payload or {}),
        }
        encoded = json.dumps(
            event,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        )
        if len(encoded.encode("utf-8")) > self._max_event_bytes:
            raise ValueError("event exceeds configured size limit")
        self._redis.xadd(
            self._stream,
            {
                self._outer_identity_field: self._identity["operation_id"],
                "payload": encoded,
            },
            maxlen=self._max_stream_length,
            approximate=True,
        )
        return event

    def phase(self, phase: str) -> dict[str, Any]:
        return self.publish("PHASE", {"phase": phase}, phase=phase)

    def log(self, message: str, level: str = "INFO") -> dict[str, Any]:
        return self.publish(
            "LOG",
            {"level": level, "message": message[:4096]},
        )

    def failed(
        self,
        code: str,
        error_type: str,
        phase: str,
        *,
        sanitized_message: str = "operation failed",
    ) -> dict[str, Any]:
        """Publish a caller-approved summary; never pass raw external exceptions."""
        return self.publish(
            "FAILED",
            {
                "error_code": code,
                "error_type": error_type,
                "sanitized_message": sanitized_message,
                "retryable": False,
            },
            phase=phase,
        )


__all__ = ["RedisEventReporter", "redact"]
