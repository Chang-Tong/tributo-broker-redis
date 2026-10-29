"""Real Redis wire/driver lifecycle; Ray execution is deliberately substituted."""

from __future__ import annotations

import base64
import json
import os
import runpy
import shutil
import socket
import subprocess
import time
from pathlib import Path

import pytest
import redis
from tributo.ray_jobs import RayJobSubmission

from tributo_broker_redis.contracts.aimodel_v2 import load_resource, validate_trace
from tributo_broker_redis.contracts.aimodel_v2.suite import materialize
from tributo_broker_redis.plugin import RedisBrokerPlugin
from tributo_broker_redis.profiles.aimodel_v2 import broker, execution_driver
from tributo_broker_redis.profiles.aimodel_v2.reporter import ExecutionControlError
from tributo_broker_redis.protocol import DriverInput
from tributo_broker_redis.runtime import RedisBrokerRuntime

pytestmark = pytest.mark.integration
_TESTS = Path(__file__).resolve().parents[1] / "aimodel_v2"


@pytest.fixture
def redis_url(tmp_path):
    executable = os.environ.get("TRIBUTO_TEST_REDIS_SERVER") or shutil.which(
        "redis-server"
    )
    if not executable:
        if os.environ.get("TRIBUTO_CONTRACT_REQUIRE_REDIS") == "1":
            pytest.fail("Redis is required by the protocol CI gate")
        pytest.skip("set TRIBUTO_TEST_REDIS_SERVER to an installed redis-server")
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    process = subprocess.Popen(
        [
            executable,
            "--bind",
            "127.0.0.1",
            "--port",
            str(port),
            "--save",
            "",
            "--appendonly",
            "no",
            "--dir",
            str(tmp_path),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"redis://127.0.0.1:{port}/0"
    client = redis.Redis.from_url(url)
    try:
        deadline = time.monotonic() + 5
        while True:
            try:
                client.ping()
                break
            except redis.ConnectionError:
                if process.poll() is not None or time.monotonic() >= deadline:
                    pytest.fail("isolated Redis did not start")
                time.sleep(0.02)
        yield url
    finally:
        client.close()
        process.terminate()
        process.wait(timeout=5)


@pytest.mark.parametrize("kind", ["training", "inference"])
@pytest.mark.parametrize("terminal", ["COMPLETED", "FAILED", "CANCELLED"])
def test_public_provider_wire_and_driver_terminal(
    tmp_path, monkeypatch, redis_url, kind, terminal
):
    config = runpy.run_path(_TESTS / "test_profile.py")["_config"](tmp_path)
    config["transport"]["url"] = redis_url
    request = runpy.run_path(_TESTS / f"test_{kind}.py")["_request"]()
    operation = "training" if kind == "training" else "batch_inference"
    identity_field = "job_id" if kind == "training" else "execution_id"
    identity = request.execution_id
    channel = config["channels"][operation]
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    client.xadd(
        channel["task_stream_key"],
        {identity_field: identity, "payload": request.model_dump_json()},
    )
    submissions = []

    def submit(entrypoint, **kwargs):
        assert entrypoint == broker.DRIVER_ENTRYPOINT
        submissions.append(kwargs)
        return RayJobSubmission(
            run_id=kwargs["run_id"],
            attempt_id=kwargs["attempt_id"],
            submission_id="isolated-admission",
            request_digest=kwargs["request_digest"],
        )

    def runtime_factory(parsed, **kwargs):
        return RedisBrokerRuntime(
            parsed,
            redis_client=client,
            submitter=submit,
            job_lister=lambda _url: [],
            **kwargs,
        )

    monkeypatch.setattr(broker, "RedisBrokerRuntime", runtime_factory)
    runtime = RedisBrokerPlugin().create_runtime(config)
    try:
        assert runtime.run_once(timeout_ms=0)
        assert len(submissions) == 1
        # Preserve the existing admission-ACK semantics during this ownership move.
        assert (
            client.xpending(channel["task_stream_key"], channel["consumer_group"])[
                "pending"
            ]
            == 0
        )
        encoded = submissions[0]["env_vars"]["TRIBUTO_REDIS_DRIVER_INPUT_B64"]
        value = DriverInput.model_validate_json(
            base64.urlsafe_b64decode(encoded).decode()
        )
        monkeypatch.setattr(
            execution_driver,
            "_load_driver_input",
            lambda: (value, "isolated-admission"),
        )
        monkeypatch.setattr(execution_driver, "_initialize_ray", lambda: "isolated-ray")
        monkeypatch.setattr(
            execution_driver,
            "create_redis_client",
            lambda _connection: redis.Redis.from_url(redis_url, decode_responses=True),
        )

        def dispatch(_value, _request, reporter):
            reporter.phase("EXECUTING")
            if terminal == "CANCELLED":
                raise ExecutionControlError(
                    "CANCELLED", "TRAINING" if kind == "training" else "PREDICT"
                )
            if terminal == "FAILED":
                raise execution_driver.ExecutionNotImplemented("isolated failure")
            payload = (
                load_resource("fixtures/training-completed.json")
                if kind == "training"
                else {"result_rows": 2, "processed_rows": 2, "total_rows": 2}
            )
            reporter.publish("COMPLETED", payload, phase="COMPLETED")

        monkeypatch.setattr(execution_driver, "_dispatch", dispatch)
        assert execution_driver.main() == (0 if terminal == "COMPLETED" else 1)
        stream = f"{channel['event_stream_prefix']}:{identity}"
        entries = client.xrange(stream)
        assert all(set(fields) == {identity_field, "payload"} for _, fields in entries)
        events = [json.loads(fields["payload"]) for _, fields in entries]
        assert all(
            event[identity_field] == identity and event["protocol_version"] == "2.0"
            for event in events
        )
        assert all(isinstance(event["timestamp"], int) for event in events)
        terminals = [
            event
            for event in events
            if event["event_type"] in {"COMPLETED", "FAILED", "CANCELLED"}
        ]
        assert [event["event_type"] for event in terminals] == [terminal]
        assert (
            validate_trace(
                [
                    {"stream_key": stream, "entry_id": entry_id, "fields": fields}
                    for entry_id, fields in entries
                ],
                kind,
                producer=True,
            )["terminal"]
            == terminal
        )
        # A second process/reporting instance cannot append another terminal.
        reporter = runtime._reporter(
            operation_id=identity,
            operation_type=operation,
            execution_profile="distributed",
            run_id=identity,
            attempt_id="attempt-1",
        )
        reporter.publish("FAILED", {"error_code": "UNKNOWN"})
        assert client.xlen(stream) == len(entries)
    finally:
        runtime.close()


@pytest.mark.parametrize(
    "case",
    [case for case in load_resource("cases.json") if not case["accepted"]],
    ids=lambda case: case["id"],
)
def test_invalid_contract_request_is_failed_and_acked_without_ray(
    tmp_path, monkeypatch, redis_url, case
):
    config = runpy.run_path(_TESTS / "test_profile.py")["_config"](tmp_path)
    config["transport"]["url"] = redis_url
    kind = case["kind"].removesuffix("-request")
    payload = materialize(case)
    operation = "training" if kind == "training" else "batch_inference"
    field = "job_id" if kind == "training" else "execution_id"
    identity = payload[field]
    channel = config["channels"][operation]
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    client.xadd(
        channel["task_stream_key"], {field: identity, "payload": json.dumps(payload)}
    )

    def forbidden_submit(*_args, **_kwargs):
        pytest.fail("invalid protocol request reached Ray submission")

    def runtime_factory(parsed, **kwargs):
        return RedisBrokerRuntime(
            parsed,
            redis_client=client,
            submitter=forbidden_submit,
            job_lister=lambda _url: [],
            **kwargs,
        )

    monkeypatch.setattr(broker, "RedisBrokerRuntime", runtime_factory)
    runtime = RedisBrokerPlugin().create_runtime(config)
    try:
        assert runtime.run_once(timeout_ms=0)
        assert (
            client.xpending(channel["task_stream_key"], channel["consumer_group"])[
                "pending"
            ]
            == 0
        )
        stream = f"{channel['event_stream_prefix']}:{identity}"
        records = [
            {"stream_key": stream, "entry_id": entry_id, "fields": fields}
            for entry_id, fields in client.xrange(stream)
        ]
        assert validate_trace(records, kind, producer=True)["terminal"] == "FAILED"
        event = json.loads(records[-1]["fields"]["payload"])
        expected = (
            "UNSUPPORTED_PROTOCOL_VERSION"
            if "version" in case["id"]
            else "INVALID_PAYLOAD"
        )
        assert event["error_code"] == expected
    finally:
        runtime.close()


@pytest.mark.parametrize("kind", ["training", "inference"])
@pytest.mark.parametrize(
    "fault",
    [
        "extra_field",
        "flattened",
        "invalid_json",
        "duplicate_key",
        "nan",
        "infinity",
        "overflow",
        "identity",
    ],
)
def test_malformed_redis_wire_never_reaches_tributo(
    tmp_path, monkeypatch, redis_url, kind, fault
):
    config = runpy.run_path(_TESTS / "test_profile.py")["_config"](tmp_path)
    config["transport"]["url"] = redis_url
    payload = load_resource(f"fixtures/{kind}-request.json")
    operation = "training" if kind == "training" else "batch_inference"
    field = "job_id" if kind == "training" else "execution_id"
    identity = payload[field]
    channel = config["channels"][operation]
    raw = json.dumps(payload)
    fields = {field: identity, "payload": raw}
    if fault == "extra_field":
        fields["extra"] = "forbidden"
    elif fault == "flattened":
        fields = {key: json.dumps(value) for key, value in payload.items()}
        fields[field] = identity
    elif fault == "invalid_json":
        fields["payload"] = "{broken"
    elif fault == "duplicate_key":
        fields["payload"] = raw[:-1] + ',"protocol_version":"2.0"}'
    elif fault in {"nan", "infinity", "overflow"}:
        constant = {"nan": "NaN", "infinity": "Infinity", "overflow": "1e999"}[fault]
        fields["payload"] = raw[:-1] + ',"unknown_field":' + constant + "}"
    else:
        changed = {**payload, field: "other"}
        fields["payload"] = json.dumps(changed)
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    client.xadd(channel["task_stream_key"], fields)

    def runtime_factory(parsed, **kwargs):
        def forbidden_submit(*_args, **_kwargs):
            pytest.fail("malformed wire reached Tributo/Ray")

        return RedisBrokerRuntime(
            parsed,
            redis_client=client,
            submitter=forbidden_submit,
            job_lister=lambda _url: [],
            **kwargs,
        )

    monkeypatch.setattr(broker, "RedisBrokerRuntime", runtime_factory)
    runtime = RedisBrokerPlugin().create_runtime(config)
    try:
        assert runtime.run_once(timeout_ms=0)
        stream = f"{channel['event_stream_prefix']}:{identity}"
        records = [
            {"stream_key": stream, "entry_id": entry_id, "fields": values}
            for entry_id, values in client.xrange(stream)
        ]
        assert validate_trace(records, kind, producer=True)["terminal"] == "FAILED"
        assert (
            json.loads(records[-1]["fields"]["payload"])["error_code"]
            == "INVALID_PAYLOAD"
        )
        assert (
            client.xpending(channel["task_stream_key"], channel["consumer_group"])[
                "pending"
            ]
            == 0
        )
    finally:
        runtime.close()


@pytest.mark.parametrize(
    "fault",
    [
        "none",
        "unknown_fields_allowed",
        "request_extra_field",
        "request_invalid_json",
        "request_identity",
        "feedback_missing_timestamp",
        "feedback_missing_result",
        "feedback_wrong_model",
        "feedback_invalid_error_code",
        "feedback_wrong_identity",
        "feedback_invalid_json",
        "feedback_duplicate_terminal",
        "feedback_round_zero",
        "feedback_incomplete_inference",
    ],
)
def test_ci_process_rejects_protocol_drift_captured_from_real_redis(
    protocol_candidate, redis_url, fault
):
    """Fault injection proves the CI executable fails on captured wire facts."""
    import sys
    import xml.etree.ElementTree as ET

    root = protocol_candidate
    manifest = json.loads((root / "candidate.json").read_text())
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    try:
        for item in manifest["requests"]:
            kind = item["operation"]
            field = "job_id" if kind == "training" else "execution_id"
            path = root / item["file"]
            payload = json.loads(path.read_text())
            if fault == "unknown_fields_allowed":
                payload["future_optional_field"] = {"nested": "opaque"}
                path.write_text(json.dumps(payload))
            fields = {field: item["outer_identity"], "payload": json.dumps(payload)}
            if kind == "training":
                if fault == "request_extra_field":
                    fields["extra"] = "forbidden"
                elif fault == "request_invalid_json":
                    fields["payload"] = "{broken"
                elif fault == "request_identity":
                    fields[field] = "wrong"
            stream = f"isolated:{kind}:tasks"
            client.xadd(stream, fields)
            records = [
                {"stream_key": stream, "entry_id": entry_id, "fields": values}
                for entry_id, values in client.xrange(stream)
            ]
            (root / item["wire_file"]).write_text(json.dumps(records))
        for item in manifest["traces"]:
            path = root / item["file"]
            source = json.loads(path.read_text())
            kind = "training" if "job_id" in source[0]["fields"] else "inference"
            field = "job_id" if kind == "training" else "execution_id"
            stream = f"isolated:{kind}:events:{item['identity']}"
            for index, record in enumerate(source):
                event = json.loads(record["fields"]["payload"])
                if kind == "training" and index == len(source) - 1:
                    if fault == "feedback_missing_timestamp":
                        event.pop("timestamp")
                    elif fault == "feedback_missing_result":
                        event.pop("training_result")
                    elif fault == "feedback_wrong_model":
                        event["artifact_manifest"]["model_id"] = "other-model"
                    elif fault == "feedback_invalid_error_code":
                        event = load_resource("fixtures/training-failed.json")
                        event["error_code"] = "INVALID_TYPO"
                if fault == "feedback_round_zero" and event["event_type"] == "METRICS":
                    event["current_round"] = 0
                if (
                    fault == "feedback_incomplete_inference"
                    and kind == "inference"
                    and event["event_type"] == "COMPLETED"
                ):
                    event["processed_rows"] = 0
                fields = {field: item["identity"], "payload": json.dumps(event)}
                if kind == "training" and index == len(source) - 1:
                    if fault == "feedback_wrong_identity":
                        fields[field] = "other"
                    elif fault == "feedback_invalid_json":
                        fields["payload"] = "{broken"
                client.xadd(stream, fields)
            if fault == "feedback_duplicate_terminal" and kind == "training":
                client.xadd(stream, source[-1]["fields"])
            records = [
                {"stream_key": stream, "entry_id": entry_id, "fields": values}
                for entry_id, values in client.xrange(stream)
            ]
            path.write_text(json.dumps(records))
        report, junit = root / "ci-result.json", root / "ci-result.xml"
        process = subprocess.run(
            [
                sys.executable,
                "-m",
                "tributo_broker_redis.contracts.aimodel_v2.cli",
                "candidate",
                "--directory",
                str(root),
                "--level",
                "backend",
                "--report",
                str(report),
                "--junit",
                str(junit),
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        should_pass = fault in {"none", "unknown_fields_allowed"}
        capture_dir = os.environ.get("TRIBUTO_CONTRACT_CAPTURE_DIR")
        if capture_dir:
            destination = Path(capture_dir) / fault
            destination.mkdir(parents=True, exist_ok=True)
            for captured in root.iterdir():
                if captured.is_file() and captured.suffix in {".json", ".xml"}:
                    shutil.copy2(captured, destination / captured.name)
            (destination / "expectation.json").write_text(
                json.dumps(
                    {
                        "fault": fault,
                        "expected_exit": 0 if should_pass else 1,
                        "actual_exit": process.returncode,
                        "capture_source": "isolated-real-redis",
                    }
                )
            )
        assert process.returncode == (0 if should_pass else 1), (
            process.stdout + process.stderr
        )
        if not should_pass:
            failure = json.loads(report.read_text())
            assert failure["passed"] is False and failure["code"]
            assert failure["path"].startswith("$")
        assert (
            ET.parse(junit).getroot().find("testcase/failure") is None
        ) == should_pass
    finally:
        client.close()


@pytest.mark.parametrize("kind", ["training", "inference"])
@pytest.mark.parametrize("corrupt_feedback", [False, True])
def test_current_tributo_result_mapping_is_checked_on_real_redis(
    monkeypatch, redis_url, kind, corrupt_feedback
):
    """Real current-Core result classes and real adapter/serializer, no ML job."""
    from tributo.algorithms import AlgorithmExecutionResult, AlgorithmRunResult
    from tributo.data import IngestionPlanReceipt
    from tributo.inference.contracts import InferenceResult, ResultSinkReceipt

    from tributo_broker_redis.contracts.aimodel_v2 import (
        ContractViolation,
        validate_feedback,
    )
    from tributo_broker_redis.profiles.aimodel_v2 import inference, training
    from tributo_broker_redis.profiles.aimodel_v2.reporter import (
        KnovaRedisEventReporter,
    )

    helpers = runpy.run_path(_TESTS / f"test_{kind}.py")
    request = helpers["_request"]()
    client = redis.Redis.from_url(redis_url, decode_responses=True)

    class CallbackReporter(KnovaRedisEventReporter):
        def publish(self, event_type, payload, *, phase=None):
            if corrupt_feedback and event_type == "COMPLETED":
                # Simulate a future adapter regression after receiving Core data.
                if kind == "training":
                    payload["training_result"].pop("task_type")
                else:
                    payload["processed_rows"] = payload["total_rows"] + 1
            return super().publish(event_type, payload, phase=phase)

    reporter = CallbackReporter(
        client,
        event_stream_prefix=f"core:{kind}:events",
        operation_id=request.execution_id,
        operation_type="training" if kind == "training" else "batch_inference",
        execution_profile="distributed",
        run_id=request.execution_id,
        attempt_id="contract-attempt",
        outer_identity_field="job_id" if kind == "training" else "execution_id",
    )
    try:
        if kind == "training":
            helpers["_published_bundle"].__wrapped__(monkeypatch)
            dispatcher = helpers["_Dispatcher"]()
            original = dispatcher.result.execution
            dispatcher.result = AlgorithmRunResult(
                run_id=request.execution_id,
                plan_id="a" * 64,
                execution=AlgorithmExecutionResult(
                    status="succeeded",
                    metrics={
                        **original.metrics,
                        "evaluation": {
                            key: value
                            for key, value in original.metrics["evaluation"].items()
                            if key != "invalid"
                        },
                    },
                    outputs=original.outputs,
                ),
                actual_versions={},
                input_provenance={},
            )
            monkeypatch.setattr(training, "run_xgboost_training", dispatcher)
            training.execute_training(request, reporter)
        else:
            receipt = IngestionPlanReceipt(
                request_digest="1" * 64,
                engine_id="tributo.ray_data",
                engine_version="ci",
                provider_id="tributo.parquet",
                connector_id="parquet",
                binding_id="tributo.ray.parquet",
                scan_kind="file",
                logical_plan_version=1,
                logical_plan_digest="2" * 64,
                source_ref="c" * 64,
                dataset_ref="3" * 64,
                transform_ir_version=1,
                transform_digest="4" * 64,
                binding_distribution="tributo",
                binding_distribution_version="1.0.0",
                reader_api="ray.data.read_parquet",
                transport_id="ray-data",
            )
            result = InferenceResult(
                run_id=request.execution_id,
                attempt_id="attempt",
                submission_id="submission",
                plan_digest="a" * 64,
                bundle_id="bundle",
                manifest_sha256="b" * 64,
                role="inference",
                flavor_id="onnx-runtime-v1",
                source_ref_id="c" * 64,
                ingestion_receipt=receipt,
                sink_receipt=ResultSinkReceipt(
                    sink_id="clickhouse",
                    result_id="d" * 64,
                    uri="clickhouse://isolated/result",
                    rows_written=7,
                ),
                input_rows=7,
                output_rows=7,
                status="succeeded",
            )
            monkeypatch.setattr(inference, "run_inference", lambda *_a, **_k: result)
            monkeypatch.setattr(inference, "_prepare_model", lambda core, *_a: core)
            monkeypatch.setattr(inference, "_measure_input_rows", lambda *_a: 7)
            inference.execute_inference(request, reporter)
        stream = f"core:{kind}:events:{request.execution_id}"
        records = [
            {"stream_key": stream, "entry_id": entry_id, "fields": fields}
            for entry_id, fields in client.xrange(stream)
        ]
        if corrupt_feedback:
            with pytest.raises(ContractViolation):
                validate_feedback(request.model_dump(mode="json"), records, kind)
        else:
            assert (
                validate_feedback(request.model_dump(mode="json"), records, kind)[
                    "terminal"
                ]
                == "COMPLETED"
            )
    finally:
        client.close()
