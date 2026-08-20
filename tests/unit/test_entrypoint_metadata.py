"""Wheel metadata contract for Tributo Broker discovery."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from tributo.integrations.broker import EventReporterSpec

from tributo_broker_redis.plugin import RedisBrokerPlugin
from tributo_broker_redis.reporter import RedisEventReporter


def test_tributo_broker_entrypoint_is_declared() -> None:
    pyproject = Path(__file__).parents[2] / "pyproject.toml"
    with pyproject.open("rb") as stream:
        project = tomllib.load(stream)["project"]
    entrypoints = project["entry-points"]["tributo.brokers"]
    assert entrypoints["knova-redis"] == (
        "tributo_broker_redis.plugin:RedisBrokerPlugin"
    )


def test_plugin_rebuilds_worker_event_reporter_from_env_reference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REDIS_PASSWORD", "actual-secret")
    monkeypatch.setenv(
        "WORKER_BROKER_CONFIG",
        json.dumps({"host": "worker-redis", "password_env": "REDIS_PASSWORD"}),
    )
    spec = EventReporterSpec(
        broker_id="knova-redis",
        job_id="job-1",
        options={"config_env": "WORKER_BROKER_CONFIG"},
    )
    client = MagicMock()

    with patch(
        "tributo_broker_redis.plugin.create_redis_client", return_value=client
    ) as create:
        reporter = RedisBrokerPlugin().create_event_reporter(spec)

    assert isinstance(reporter, RedisEventReporter)
    assert reporter.job_id == "job-1"
    assert reporter._redis is client
    assert create.call_args.args[0].host == "worker-redis"
    assert "actual-secret" not in spec.as_dict()["options"]["config_env"]
