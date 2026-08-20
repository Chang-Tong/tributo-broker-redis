"""Redis Streams task consumer and pending-message recovery."""

from __future__ import annotations

import logging
from typing import Any

from redis import exceptions as redis_exceptions
from redis.exceptions import ResponseError
from tributo.integrations.broker import Message, TaskConsumer

from tributo_broker_redis.config import RedisBrokerConfig
from tributo_broker_redis.protocol import validate_job_id
from tributo_broker_redis.redis_client import create_redis_client

logger = logging.getLogger(__name__)

CORE_DEFAULT_POLL_TIMEOUT_MS = 5000


def _decode(value: Any) -> Any:
    return value.decode() if isinstance(value, bytes) else value


class RedisTaskConsumer(TaskConsumer):
    """Read task envelopes using a Redis consumer group."""

    def __init__(self, redis_client: Any, config: RedisBrokerConfig) -> None:
        self._redis = redis_client
        self._config = config
        self._pending_messages: list[Message] = []
        self._delivery_attempts: dict[str, int] = {}
        self._claim_cursor = "0-0"
        self._ensure_group()

    def _ensure_group(self) -> None:
        try:
            self._redis.xgroup_create(
                name=self._config.task_stream_key,
                groupname=self._config.consumer_group,
                id=self._config.group_start_id,
                mkstream=True,
            )
        except ResponseError as exc:
            busy_group_error = getattr(redis_exceptions, "BusyGroupError", ())
            if busy_group_error and isinstance(exc, busy_group_error):
                return
            # redis-py 6-7 expose BUSYGROUP as a generic ResponseError rather
            # than a dedicated exception. Match only the Redis error code,
            # never arbitrary ResponseError text.
            error_code, _, _ = str(exc).partition(" ")
            if error_code.upper() == "BUSYGROUP":
                return
            raise

    def _message_from_fields(self, delivery_id: Any, values: dict[Any, Any]) -> Message:
        decoded = {str(_decode(key)): _decode(value) for key, value in values.items()}
        raw_job_id = decoded.get("job_id")
        job_id = raw_job_id if isinstance(raw_job_id, str) and raw_job_id else None
        job_id_error: str | None = None
        if job_id is not None:
            try:
                job_id = validate_job_id(job_id)
            except ValueError as exc:
                job_id = None
                job_id_error = str(exc)
        payload = decoded.get("payload")
        delivery = str(_decode(delivery_id))
        payload_error: str | None = None
        for field_name, field_value in decoded.items():
            field_size = len(str(field_name).encode("utf-8")) + len(
                str(field_value).encode("utf-8")
            )
            if field_size > self._config.max_payload_bytes:
                payload_error = (
                    f"Redis task field {field_name!r} exceeds the configured "
                    f"size limit of {self._config.max_payload_bytes} bytes"
                )
                break
        if payload_error is None and isinstance(payload, str):
            payload_size = len(payload.encode("utf-8"))
            if payload_size > self._config.max_payload_bytes:
                payload_error = (
                    f"payload exceeds the configured size limit of "
                    f"{self._config.max_payload_bytes} bytes"
                )
        attempt = self._delivery_attempts.get(delivery, 0) + 1
        self._delivery_attempts[delivery] = attempt
        return Message(
            job_id=job_id,
            payload={"raw": payload},
            metadata={
                "stream": self._config.task_stream_key,
                "payload_error": payload_error,
                "job_id_error": job_id_error,
            },
            delivery_id=delivery,
            delivery_attempt=attempt,
        )

    def poll(self, timeout_ms: int = CORE_DEFAULT_POLL_TIMEOUT_MS) -> Message | None:
        if self._pending_messages:
            return self._pending_messages.pop(0)
        # Core's BrokerRunner passes its own default explicitly. Keep the
        # shared default named so a Core API change is easy to audit.
        block_ms = (
            self._config.block_ms
            if timeout_ms == CORE_DEFAULT_POLL_TIMEOUT_MS
            else timeout_ms
        )
        result = self._redis.xreadgroup(
            groupname=self._config.consumer_group,
            consumername=self._config.consumer_name,
            streams={self._config.task_stream_key: ">"},
            count=1,
            block=block_ms,
        )
        if not result:
            return None
        _stream, messages = result[0]
        if not messages:
            return None
        delivery_id, values = messages[0]
        return self._message_from_fields(delivery_id, values)

    def ack(self, message: Message) -> None:
        if not message.delivery_id:
            raise ValueError("Redis message requires delivery_id for ACK")
        self._redis.xack(
            self._config.task_stream_key,
            self._config.consumer_group,
            message.delivery_id,
        )

    def retry(self, message: Message, error: str | None = None) -> None:
        error_text = str(error) if error is not None else None
        logger.warning(
            "Leaving Redis task pending for recovery: job_id=%s delivery_id=%s "
            "error=%s",
            message.job_id,
            message.delivery_id,
            error_text,
        )

    def recover_pending(self) -> int:
        """Claim idle messages from other consumers for redelivery."""
        if not hasattr(self._redis, "xautoclaim"):
            return 0
        result = self._redis.xautoclaim(
            self._config.task_stream_key,
            self._config.consumer_group,
            self._config.consumer_name,
            min_idle_time=self._config.claim_idle_ms,
            start_id=getattr(self, "_claim_cursor", "0-0"),
            count=self._config.claim_count,
        )
        if not isinstance(result, (list, tuple)) or len(result) < 2:
            return 0
        self._claim_cursor = str(_decode(result[0]))
        claimed = result[1]
        if not isinstance(claimed, (list, tuple)):
            return 0
        for delivery_id, values in claimed:
            self._pending_messages.append(
                self._message_from_fields(delivery_id, values)
            )
        return len(claimed)

    def close(self) -> None:
        close = getattr(self._redis, "close", None)
        if callable(close):
            close()


def create_consumer(config: RedisBrokerConfig) -> RedisTaskConsumer:
    return RedisTaskConsumer(create_redis_client(config), config)
