"""Redis Streams delivery adapter for one operation channel."""

from __future__ import annotations

import logging
import time
from typing import Any

from redis import Redis
from redis.client import NEVER_DECODE
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
    return (
        value.decode("utf-8", errors="surrogateescape")
        if isinstance(value, bytes)
        else str(value)
    )


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
        logger.info(
            "Redis consumer ready: operation_type=%s stream=%s group=%s consumer=%s",
            self.operation_type,
            self.channel.task_stream_key,
            self.channel.consumer_group,
            self.channel.consumer_name,
        )

    def _message(self, delivery_id: Any, fields: dict[Any, Any]) -> Message:
        decoded = {_decode(key): _decode(value) for key, value in fields.items()}
        invalid_encoding = any(
            any(0xD800 <= ord(char) <= 0xDFFF for char in text)
            for pair in decoded.items()
            for text in pair
        )
        expected_fields = {self.channel.outer_identity_field, "payload"}
        envelope_error = (
            "task envelope contains unsupported or missing fields"
            if set(decoded) != expected_fields
            else ""
        )
        raw = decoded.get("payload", "")
        outer_operation_id = decoded.get(self.channel.outer_identity_field, "")
        if len(outer_operation_id) > _MAX_OPERATION_ID_LENGTH or any(
            0xD800 <= ord(char) <= 0xDFFF for char in outer_operation_id
        ):
            outer_operation_id = ""
        payload_error = ""
        if (
            len(raw.encode("utf-8", errors="surrogateescape"))
            > self.transport.max_payload_bytes
        ):
            payload_error = "payload exceeds configured size limit"
        message = Message(
            payload={"raw": raw},
            delivery_token=_decode(delivery_id),
            metadata={
                "stream": self.channel.task_stream_key,
                "operation_type": self.operation_type,
                "outer_operation_id": outer_operation_id,
                "outer_identity_field": self.channel.outer_identity_field,
                "envelope_error": envelope_error,
                "payload_error": payload_error,
                "encoding_error": "payload is not valid UTF-8"
                if invalid_encoding
                else "",
                "received_at_ms": str(int(time.time() * 1000)),
            },
        )
        logger.debug(
            "Redis delivery received: operation_type=%s stream=%s delivery=%s "
            "operation_id=%s payload_bytes=%d envelope_error=%s payload_error=%s",
            self.operation_type,
            self.channel.task_stream_key,
            message.delivery_token,
            outer_operation_id or "<missing>",
            len(raw.encode("utf-8", errors="surrogateescape")),
            bool(envelope_error),
            bool(payload_error),
        )
        return message

    def poll(self, timeout_ms: int = 5000) -> Message | None:
        if self._pending_messages:
            return self._pending_messages.pop(0)
        block_ms = timeout_ms if timeout_ms > 0 else None
        try:
            if isinstance(self._redis, Redis):
                # Decode each delivery ourselves even when an injected client
                # uses strict Unicode decoding. redis-py's normal parser keeps
                # the stream envelope and pending-delivery bookkeeping intact.
                args = [
                    "GROUP",
                    self.channel.consumer_group,
                    self.channel.consumer_name,
                    "COUNT",
                    1,
                ]
                if block_ms is not None:
                    args.extend(["BLOCK", block_ms])
                args.extend(["STREAMS", self.channel.task_stream_key, ">"])
                response = self._redis.execute_command(
                    "XREADGROUP", *args, **{NEVER_DECODE: True}
                )
            else:
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
            "Leaving Redis delivery pending: operation_type=%s operation_id=%s "
            "stream=%s delivery=%s code=%s",
            self.operation_type,
            message.metadata.get("outer_operation_id") or "<missing>",
            self.channel.task_stream_key,
            message.delivery_token,
            error.code if error else "UNKNOWN",
        )

    def reject(self, message: Message, error: BrokerError | None = None) -> None:
        logger.warning(
            "No DLQ configured; acknowledging rejected delivery: "
            "operation_type=%s operation_id=%s stream=%s delivery=%s code=%s",
            self.operation_type,
            message.metadata.get("outer_operation_id") or "<missing>",
            self.channel.task_stream_key,
            message.delivery_token,
            error.code if error else "UNKNOWN",
        )
        self.ack(message)

    def recover_pending(self) -> int:
        """Claim one pending page after the previously claimed page is consumed."""
        if self._pending_messages:
            return 0
        xautoclaim = getattr(self._redis, "xautoclaim", None)
        if not callable(xautoclaim):
            return 0
        if isinstance(self._redis, Redis):
            result = self._redis.execute_command(
                "XAUTOCLAIM",
                self.channel.task_stream_key,
                self.channel.consumer_group,
                self.channel.consumer_name,
                self.transport.claim_idle_ms,
                self._claim_cursor,
                "COUNT",
                self.transport.claim_count,
                **{NEVER_DECODE: True},
            )
        else:
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
        if claimed:
            logger.info(
                "Recovered pending Redis deliveries: operation_type=%s "
                "stream=%s count=%d",
                self.operation_type,
                self.channel.task_stream_key,
                len(claimed),
            )
        return len(claimed)

    def close(self) -> None:
        return None


__all__ = ["RedisTaskConsumer"]
