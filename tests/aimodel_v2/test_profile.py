"""The public provider owns v2 routing without the deployment package."""

from __future__ import annotations

import copy
import json
import os
import runpy
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from tributo_broker_redis import cli
from tributo_broker_redis.config import RedisBrokerConfig
from tributo_broker_redis.plugin import RedisBrokerPlugin
from tributo_broker_redis.profiles.aimodel_v2 import broker


def _config(tmp_path: Path) -> dict:
    core = tmp_path / "core" / "src" / "tributo"
    core.mkdir(parents=True)
    return {
        "transport": {"url": "redis://127.0.0.1:6379/0"},
        "protocol": {"profile": "aimodel-v2", "protocol_version": "2.0"},
        "channels": {
            kind: {
                "task_stream_key": f"test:{kind}:tasks",
                "event_stream_prefix": f"test:{kind}:events",
                "cancel_key_prefix": f"test:{kind}:cancel",
                "consumer_group": f"test:{kind}",
                "outer_identity_field": identity,
            }
            for kind, identity in (
                ("training", "job_id"),
                ("batch_inference", "execution_id"),
            )
        },
        "operations": {
            kind: {"execution_profiles": ["distributed"]}
            for kind in ("training", "batch_inference")
        },
        "execution": {
            "project_root": str(core.parent.parent),
            "runtime_pip_packages": ["tributo-broker-redis[aimodel-v2]==0.2.0"],
        },
    }


@pytest.mark.parametrize(
    "profile,version", [("aimodel-v2", "1.0"), ("tributo-generic-v1", "2.0")]
)
def test_profile_version_mismatch_is_rejected(tmp_path, profile, version):
    raw = _config(tmp_path)
    raw["protocol"] = {"profile": profile, "protocol_version": version}
    with pytest.raises(ValidationError, match="requires protocol_version"):
        RedisBrokerConfig.model_validate(raw)


@pytest.mark.parametrize("kind", ["training", "batch_inference"])
def test_v2_requires_correct_wire_identity_and_supported_execution(tmp_path, kind):
    raw = _config(tmp_path)
    invalid = copy.deepcopy(raw)
    invalid["channels"][kind]["outer_identity_field"] = "operation_id"
    with pytest.raises(ValidationError, match="outer_identity_field"):
        RedisBrokerConfig.model_validate(invalid)
    raw["operations"][kind]["execution_profiles"] = ["single_worker"]
    with pytest.raises(ValidationError, match="only distributed"):
        RedisBrokerConfig.model_validate(raw)


@pytest.mark.parametrize("entry", ["plugin", "factory"])
def test_public_plugin_selects_owned_v2_parser_driver_and_reporter(tmp_path, entry):
    from tributo_broker_redis.runtime import create_runtime

    raw = _config(tmp_path)
    with patch.object(broker, "RedisBrokerRuntime") as runtime:
        result = (
            RedisBrokerPlugin().create_runtime(raw)
            if entry == "plugin"
            else create_runtime(raw)
        )
    assert result is runtime.return_value
    kwargs = runtime.call_args.kwargs
    assert kwargs["request_parser"] is broker.parse_broker_request
    assert kwargs["operation_preparer"] is broker.prepare_broker_operation
    assert kwargs["driver_entrypoint"] == broker.DRIVER_ENTRYPOINT
    assert kwargs["reporter_factory"].__module__.startswith("tributo_broker_redis.")
    assert (
        "python -m tributo_knova.execution_driver"
        in kwargs["recovery_driver_entrypoints"]
    )


def test_consume_cli_uses_profile_selection(tmp_path, monkeypatch):
    raw = _config(tmp_path)
    path = tmp_path / "config.json"
    path.write_text(json.dumps(raw))
    calls = []
    runtime = SimpleNamespace(
        start=lambda: calls.append("start"),
        run_once=lambda: calls.append("once"),
        close=lambda: calls.append("close"),
    )
    monkeypatch.setattr(cli.signal, "signal", lambda *_args: None)
    with patch.object(broker, "create_runtime", return_value=runtime):
        assert cli.main(["consume", "--config", str(path), "--once"]) == 0
    assert calls == ["start", "once", "close"]


@pytest.mark.parametrize(
    "entrypoint",
    [broker.DRIVER_ENTRYPOINT, "python -m tributo_knova.execution_driver"],
)
def test_profile_recovers_jobs_admitted_by_either_driver(
    tmp_path, monkeypatch, entrypoint
):
    from tributo_broker_redis.runtime import RedisBrokerRuntime

    fixtures = runpy.run_path(Path(__file__).with_name("test_broker.py"))
    client = fixtures["_FakeRedis"]("{}")
    raw = _config(tmp_path)
    job = SimpleNamespace(
        status="RUNNING",
        submission_id="previous-submission",
        job_id="previous-ray-job",
        metadata={
            "tributo.operation_id": "job-1",
            "tributo.operation_type": "training",
            "tributo.execution_profile": "distributed",
            "tributo.run_id": "job-1",
            "tributo.attempt_id": "attempt-1",
            "tributo.driver_entrypoint": entrypoint,
            "tributo.task_stream": raw["channels"]["training"]["task_stream_key"],
            "tributo.request_digest": "a" * 64,
        },
    )

    def factory(parsed, **kwargs):
        return RedisBrokerRuntime(
            parsed, redis_client=client, job_lister=lambda _url: [job], **kwargs
        )

    monkeypatch.setattr(broker, "RedisBrokerRuntime", factory)
    runtime = RedisBrokerPlugin().create_runtime(raw)
    monkeypatch.setattr(runtime._cancel_watcher, "start", lambda: None)
    try:
        runtime.start()
        active = runtime.active_submissions.get("job-1")
        assert active is not None
        assert active.submission.submission_id == "previous-submission"
    finally:
        runtime.close()


@pytest.mark.parametrize("kind", ["training", "inference"])
def test_profile_can_parse_and_load_execution_modules_with_knova_blocked(
    tmp_path, kind
):
    request = runpy.run_path(Path(__file__).with_name(f"test_{kind}.py"))["_request"]()
    request_path = tmp_path / "request.json"
    request_path.write_text(request.model_dump_json())
    code = """
import importlib.abc, json, pathlib, sys
class BlockKnova(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'tributo_knova' or fullname.startswith('tributo_knova.'):
            raise AssertionError('Broker must not import the deployment package')
sys.meta_path.insert(0, BlockKnova())
from tributo_broker_redis.profiles.aimodel_v2 import broker, execution_driver
from tributo_broker_redis.profiles.aimodel_v2 import training, inference
payload = pathlib.Path(sys.argv[1]).read_text()
data = json.loads(payload)
kind = 'training' if 'job_id' in data else 'batch_inference'
identity = data.get('job_id', data.get('execution_id'))
request = broker.parse_broker_request(
    payload, outer_operation_id=identity, expected_operation_type=kind
)
assert broker.prepare_broker_operation(request).operation_payload['knova_request']
assert not any(name.startswith('tributo_knova') for name in sys.modules)
"""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [str(Path(__file__).resolve().parents[2] / "src"), env.get("PYTHONPATH", "")]
    )
    result = subprocess.run(
        [sys.executable, "-c", code, str(request_path)],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
