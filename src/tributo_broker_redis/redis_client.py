"""Lazy standalone or Sentinel Redis client construction."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import redis
from redis.sentinel import Sentinel

from tributo_broker_redis.config import (
    RedisBrokerConfig,
    RedisConnectionConfig,
    RedisTransportConfig,
)


class RedisCredentialUnavailable(RuntimeError):
    """A configured Redis credential reference cannot be resolved."""


def _resolve_secret(reference: str | None) -> str | None:
    if reference is None:
        return None
    if reference.startswith("env:"):
        value = os.environ.get(reference.removeprefix("env:"))
        if value:
            return value
        raise RedisCredentialUnavailable(
            "Redis credential environment variable is unavailable"
        )
    if reference.startswith("mount:"):
        path = Path(reference.removeprefix("mount:"))
        try:
            value = path.read_text(encoding="utf-8").rstrip("\r\n")
        except OSError as exc:
            raise RedisCredentialUnavailable(
                "mounted Redis credential is unavailable"
            ) from exc
        if value:
            return value
        raise RedisCredentialUnavailable("mounted Redis credential is empty")
    raise RedisCredentialUnavailable("unsupported Redis credential reference")


def _sentinel_endpoint(node: str) -> tuple[str, int]:
    parsed = urlsplit(f"redis://{node}")
    if parsed.hostname is None or parsed.port is None:
        raise ValueError("invalid Redis Sentinel node")
    return parsed.hostname, parsed.port


def create_redis_client(
    config: RedisBrokerConfig | RedisConnectionConfig | RedisTransportConfig | str,
) -> Any:
    """Create a decoded redis-py client from credential references."""
    connection: RedisConnectionConfig
    if isinstance(config, RedisBrokerConfig):
        connection = config.transport
    elif isinstance(config, RedisConnectionConfig):
        connection = config
    else:
        connection = RedisConnectionConfig(mode="standalone", url=config)

    password = _resolve_secret(connection.password_ref)
    if connection.mode == "standalone":
        if connection.url is None:
            raise ValueError("standalone Redis URL is unavailable")
        kwargs: dict[str, Any] = {"decode_responses": True}
        if password is not None:
            kwargs["password"] = password
        return redis.Redis.from_url(connection.url, **kwargs)

    sentinel_password = _resolve_secret(connection.sentinel_password_ref)
    sentinel_kwargs: dict[str, Any] = {"decode_responses": True}
    if sentinel_password is not None:
        sentinel_kwargs["password"] = sentinel_password
    connection_kwargs: dict[str, Any] = {
        "db": connection.database,
        "decode_responses": True,
    }
    if password is not None:
        connection_kwargs["password"] = password
    sentinel = Sentinel(
        [_sentinel_endpoint(node) for node in connection.sentinel_nodes],
        sentinel_kwargs=sentinel_kwargs,
        **connection_kwargs,
    )
    if connection.sentinel_master is None:
        raise ValueError("Redis Sentinel master is unavailable")
    return sentinel.master_for(connection.sentinel_master)


__all__ = ["RedisCredentialUnavailable", "create_redis_client"]
