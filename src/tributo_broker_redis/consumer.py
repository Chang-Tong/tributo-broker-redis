"""Redis Streams delivery adapter for one operation channel."""

from __future__ import annotations

import logging
from typing import Any

from redis.exceptions import ResponseError, TimeoutError
from tributo.integrations.broker import BrokerError, Message, TaskConsumer

from tributo_broker_redis.config import (
    ChannelConfig,
    OperationType,
    RedisTransportConfig,
)

logger = logging.getLogger(__name__)
_MAX_OPERATION_ID_LENGTH = 256


def _decode(value: Any) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


class RedisTaskConsumer(TaskConsumer):
    """Read one operation's tasks through a Redis consumer group."""

    def __init__(
        self,
        redis_client: Any,
        transport: RedisTransportConfig,
        channel: ChannelConfig,
        operation_type: OperationType,
    ) -> None:
        self._redis = redis_client
        self.transport = transport
        self.channel = channel
        self.operation_type = operation_type
        self._pending_messages: list[Message] = []
        self._claim_cursor = "0-0"
        self._ensure_group()

    def _ensure_group(self) -> None:
        try:
            self._redis.xgroup_create(
                name=self.channel.task_stream_key,
                groupname=self.channel.consumer_group,
                id=self.channel.group_start_id,
                mkstream=True,
            )
        except ResponseError as exc:
            code, _, _ = str(exc).partition(" ")
            if code.upper() != "BUSYGROUP":
                raise

    def _message(self, delivery_id: Any, fields: dict[Any, Any]) -> Message:
        decoded = {_decode(key): _decode(value) for key, value in fields.items()}
        expected_fields = {self.channel.outer_identity_field, "payload"}
        envelope_error = (
            "task envelope contains unsupported or missing fields"
            if set(decoded) != expected_fields
            else ""
        )
        raw = decoded.get("payload", "")
        outer_operation_id = decoded.get(self.channel.outer_identity_field, "")
        if len(outer_operation_id) > _MAX_OPERATION_ID_LENGTH:
            outer_operation_id = ""
        payload_error = ""
        if len(raw.encode("utf-8")) > self.transport.max_payload_bytes:
            payload_error = "payload exceeds configured size limit"
        return Message(
            payload={"raw": raw},
            delivery_token=_decode(delivery_id),
            metadata={
                "stream": self.channel.task_stream_key,
                "operation_type": self.operation_type,
                "outer_operation_id": outer_operation_id,
                "outer_identity_field": self.channel.outer_identity_field,
                "envelope_error": envelope_error,
                "payload_error": payload_error,
            },
        )

    def poll(self, timeout_ms: int = 5000) -> Message | None:
        if self._pending_messages:
            return self._pending_messages.pop(0)
        block_ms = timeout_ms if timeout_ms > 0 else None
        try:
            response = self._redis.xreadgroup(
                groupname=self.channel.consumer_group,
                consumername=self.channel.consumer_name,
                streams={self.channel.task_stream_key: ">"},
                count=1,
                block=block_ms,
            )
        except TimeoutError:
            # redis-py 8.x applies a finite socket timeout by default. A
            # blocking XREADGROUP can therefore time out at the client just
            # as Redis completes an empty poll. Treat that race exactly like
            # an empty response so an idle consumer remains available.
            return None
        if not response or not response[0][1]:
            return None
        delivery_id, fields = response[0][1][0]
        return self._message(delivery_id, fields)

    def ack(self, message: Message) -> None:
        self._redis.xack(
            self.channel.task_stream_key,
            self.channel.consumer_group,
            message.delivery_token,
        )

    def retry(self, message: Message, error: BrokerError | None = None) -> None:
        logger.warning(
            "Leaving Redis delivery pending: stream=%s delivery=%s code=%s",
            self.channel.task_stream_key,
            message.delivery_token,
            error.code if error else "UNKNOWN",
        )

    def reject(self, message: Message, error: BrokerError | None = None) -> None:
        logger.warning(
            "No DLQ configured; acknowledging rejected delivery: stream=%s "
            "delivery=%s code=%s",
            self.channel.task_stream_key,
            message.delivery_token,
            error.code if error else "UNKNOWN",
        )
        self.ack(message)

    def recover_pending(self) -> int:
        """Best-effort XAUTOCLAIM; v0.1 makes no recovery guarantee."""
        xautoclaim = getattr(self._redis, "xautoclaim", None)
        if not callable(xautoclaim):
            return 0
        result = xautoclaim(
            self.channel.task_stream_key,
            self.channel.consumer_group,
            self.channel.consumer_name,
            min_idle_time=self.transport.claim_idle_ms,
            start_id=self._claim_cursor,
            count=self.transport.claim_count,
        )
        if not isinstance(result, (list, tuple)) or len(result) < 2:
            return 0
        self._claim_cursor = _decode(result[0])
        claimed = result[1]
        if not isinstance(claimed, (list, tuple)):
            return 0
        self._pending_messages.extend(
            self._message(delivery_id, fields) for delivery_id, fields in claimed
        )
        return len(claimed)

    def close(self) -> None:
        return None


__all__ = ["RedisTaskConsumer"]
