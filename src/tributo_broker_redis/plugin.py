"""Tributo Broker API v1 plugin for Redis Streams."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from tributo.integrations.broker import BROKER_API_VERSION, BrokerPlugin

from tributo_broker_redis.config import normalize_config
from tributo_broker_redis.redis_client import create_redis_client
from tributo_broker_redis.runtime import (
    RedisBrokerRuntime,
    validate_execution_environment,
)


class RedisBrokerPlugin(BrokerPlugin):
    """Generic healthy-path Alpha provider; discovery has no side effects."""

    api_version = BROKER_API_VERSION
    broker_id = "tributo-redis"
    stability = "alpha"
    capabilities = frozenset(
        {
            "transport.redis.streams",
            "transport.redis.consumer-group",
            "transport.redis.sentinel",
            "transport.redis.credential-reference",
            "protocol.tributo-generic-v1",
            "protocol.aimodel-v2",
            "operation.training.single_worker",
            "operation.training.distributed",
            "operation.batch_inference.single_worker",
            "operation.batch_inference.distributed",
            "event.basic",
        }
    )

    def validate_config(
        self,
        config: Mapping[str, Any],
        *,
        check_connectivity: bool = False,
    ) -> None:
        parsed = normalize_config(config)
        validate_execution_environment(parsed)
        if check_connectivity:
            client = create_redis_client(parsed)
            try:
                client.ping()
            finally:
                client.close()

    def create_runtime(self, config: Mapping[str, Any]) -> RedisBrokerRuntime:
        parsed = normalize_config(config)
        if parsed.protocol.profile == "aimodel-v2":
            from tributo_broker_redis.profiles.aimodel_v2.broker import create_runtime

            return create_runtime(parsed)
        return RedisBrokerRuntime(parsed)


__all__ = ["RedisBrokerPlugin"]
