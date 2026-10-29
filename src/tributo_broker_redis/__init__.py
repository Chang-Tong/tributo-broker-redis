"""Redis Streams provider; contract resources can load without the runtime stack."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tributo_broker_redis.plugin import RedisBrokerPlugin

__all__ = ["RedisBrokerPlugin"]


def __getattr__(name: str) -> Any:
    if name == "RedisBrokerPlugin":
        from tributo_broker_redis.plugin import RedisBrokerPlugin

        return RedisBrokerPlugin
    raise AttributeError(name)
