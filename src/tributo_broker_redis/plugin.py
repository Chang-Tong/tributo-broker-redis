"""Tributo BrokerPlugin implementation for Redis Streams."""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Mapping
from typing import Any

from tributo.integrations.broker import (
    BROKER_API_VERSION,
    BrokerPlugin,
    BrokerRuntime,
    CancellationChecker,
    CancellationSpec,
    EventReporter,
    EventReporterSpec,
)

from tributo_broker_redis.cancellation import build_cancellation_checker
from tributo_broker_redis.config import RedisBrokerConfig
from tributo_broker_redis.redis_client import create_redis_client
from tributo_broker_redis.reporter import RedisEventReporter
from tributo_broker_redis.runtime import create_runtime

logger = logging.getLogger(__name__)


class RedisBrokerPlugin(BrokerPlugin):
    """KnoVa Redis Streams provider; all Redis work is lazy and explicit."""

    api_version: int = BROKER_API_VERSION
    broker_id: str = "knova-redis"
    capabilities: frozenset[str] = frozenset(
        {
            "task-consumer",
            "consumer-group",
            "pending-recovery",
            "event-reporter",
            "cancellation-checker",
            "standalone",
            "sentinel",
            "cluster",
        }
    )

    def validate_config(
        self,
        config: Mapping[str, Any],
        *,
        check_connectivity: bool = False,
    ) -> None:
        parsed = RedisBrokerConfig.from_mapping(dict(config))
        if check_connectivity:
            from tributo_broker_redis.redis_client import create_redis_client

            client = create_redis_client(parsed)
            try:
                client.ping()
            finally:
                close = getattr(client, "close", None)
                if callable(close):
                    close()

    def create_runtime(self, config: Mapping[str, Any]) -> BrokerRuntime:
        return create_runtime(config)

    def create_cancellation_checker(
        self,
        spec: CancellationSpec,
    ) -> CancellationChecker:
        config = self._config_from_spec(spec)
        return build_cancellation_checker(spec, config)

    def create_event_reporter(self, spec: EventReporterSpec) -> EventReporter:
        config = self._config_from_spec(spec)
        return RedisEventReporter(create_redis_client(config), config, spec.job_id)

    @staticmethod
    def _config_from_spec(
        spec: CancellationSpec | EventReporterSpec,
    ) -> RedisBrokerConfig:
        options = spec.options
        config_env = options.get("config_env")
        if isinstance(config_env, str):
            raw = os.environ.get(config_env)
            if raw:
                value = json.loads(raw)
                if isinstance(value, dict):
                    return RedisBrokerConfig.from_mapping(value)
        config = options.get("config")
        if isinstance(config, dict):
            return RedisBrokerConfig.from_mapping(config)
        raise ValueError(
            "knova-redis execution spec requires config_env or a safe config"
        )
