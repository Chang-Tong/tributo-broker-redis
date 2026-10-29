"""Public CI entry points, producer evidence and fail-closed runtime evidence."""

from __future__ import annotations

import json

import pytest

from tributo_broker_redis.contracts.aimodel_v2 import ContractViolation, load_resource
from tributo_broker_redis.contracts.aimodel_v2.cli import check_candidate, main


@pytest.fixture
def candidate(protocol_candidate):
    return protocol_candidate


def test_backend_candidate_checks_real_producer_files(candidate):
    result = check_candidate(candidate, level="backend")
    assert result["request_count"] == 2 and not result["live_ray_probed"]


def test_runtime_cannot_pass_with_golden_json_and_no_real_ray(candidate):
    with pytest.raises(ContractViolation, match="LIVE_RAY_REQUIRED"):
        check_candidate(candidate, level="runtime")
    assert main(["candidate", "--directory", str(candidate), "--level", "runtime"]) == 1


def test_runtime_probe_checks_job_and_business_identity(candidate, monkeypatch):
    from tributo_broker_redis.contracts.aimodel_v2 import cli

    calls = []

    def probe(url, submission):
        calls.append(submission)
        return {
            "status": "SUCCEEDED",
            "entrypoint": (
                "python -m tributo_broker_redis.profiles.aimodel_v2.execution_driver"
            ),
            "metadata": {
                "tributo.operation_id": "train-contract-1"
                if submission == "job-train"
                else "infer-contract-1",
                "tributo.protocol_profile": "aimodel-v2",
            },
        }

    monkeypatch.setattr(cli, "_ray_job", probe)
    assert check_candidate(
        candidate, level="runtime", ray_url="http://isolated-ray:8265"
    )["live_ray_probed"]
    assert calls == ["job-train", "job-infer"]
    monkeypatch.setattr(cli, "_ray_job", lambda *_a: {"status": "RUNNING"})
    with pytest.raises(ContractViolation, match="RAY_BUSINESS_STATE"):
        check_candidate(candidate, level="runtime", ray_url="http://isolated-ray:8265")


@pytest.mark.parametrize(
    "mutation",
    [
        "outer_identity",
        "missing_projection",
        "missing_operation",
        "outside_file",
        "wrong_manifest_type",
        "wrong_list_type",
    ],
)
def test_backend_evidence_rejects_incomplete_or_unsafe_inputs(candidate, mutation):
    path = candidate / "candidate.json"
    value = json.loads(path.read_text())
    if mutation == "outer_identity":
        value["requests"][0]["outer_identity"] = "other"
    elif mutation == "missing_projection":
        value.pop("projections")
    elif mutation == "missing_operation":
        value["requests"].pop()
    elif mutation == "outside_file":
        value["requests"][0]["file"] = "../not-owned.json"
    elif mutation == "wrong_manifest_type":
        value = []
    elif mutation == "wrong_list_type":
        value["traces"] = None
    path.write_text(json.dumps(value))
    with pytest.raises(ContractViolation):
        check_candidate(candidate, level="backend")


def test_exported_bundle_and_junit_are_available_without_execution_dependencies(
    tmp_path,
):
    bundle = tmp_path / "bundle"
    junit = tmp_path / "result.xml"
    report = tmp_path / "report.json"
    assert main(["export", "--output", str(bundle)]) == 0
    assert (bundle / "schemas/common.schema.json").is_file()
    assert main(["self-test", "--junit", str(junit), "--report", str(report)]) == 0
    assert junit.read_text().startswith("<?xml")
    assert not json.loads(report.read_text())["full_protocol_compatible"]
    assert main(["export", "--output", str(bundle)]) == 1


def test_full_protocol_gate_is_red_until_gaps_are_implemented(capsys):
    assert main(["coverage", "--require-full"]) == 1
    assert not json.loads(capsys.readouterr().out)["full_protocol_compatible"]


def test_spec_drift_fails_without_echoing_document(tmp_path):
    (tmp_path / "protocol-conventions.md").write_text("do not print this source")
    assert main(["verify-spec", "--root", str(tmp_path)]) == 1


def test_projection_must_belong_to_submitted_requests(candidate):
    path = candidate / "training-request.json"
    value = json.loads(path.read_text())
    value["model_id"] = "unrelated-model"
    path.write_text(json.dumps(value))
    wire = candidate / "training-wire.json"
    records = json.loads(wire.read_text())
    records[0]["fields"]["payload"] = json.dumps(value)
    wire.write_text(json.dumps(records))
    with pytest.raises(ContractViolation, match="PROJECTION_REQUEST_LINK"):
        check_candidate(candidate, level="backend")


def test_replayed_old_entry_cannot_hide_written_row_count(candidate, monkeypatch):
    from tributo_broker_redis.contracts.aimodel_v2 import cli

    def probe(_url, submission):
        return {
            "status": "SUCCEEDED",
            "entrypoint": (
                "python -m tributo_broker_redis.profiles.aimodel_v2.execution_driver"
            ),
            "metadata": {
                "tributo.operation_id": "train-contract-1"
                if submission == "job-train"
                else "infer-contract-1",
                "tributo.protocol_profile": "aimodel-v2",
            },
        }

    monkeypatch.setattr(cli, "_ray_job", probe)
    path = candidate / "inference-trace.json"
    records = json.loads(path.read_text())
    records.append(records[0])
    path.write_text(json.dumps(records))
    (candidate / "inference-rows.json").write_text("[]")
    with pytest.raises(ContractViolation, match="WRITTEN_ROW_COUNT"):
        check_candidate(candidate, level="runtime", ray_url="http://unit-test:8265")


def test_malformed_runtime_metadata_is_a_contract_failure(candidate, monkeypatch):
    from tributo_broker_redis.contracts.aimodel_v2 import cli

    monkeypatch.setattr(
        cli, "_ray_job", lambda *_a: {"status": "SUCCEEDED", "metadata": ["bad"]}
    )
    with pytest.raises(ContractViolation, match="RAY_BROKER_IDENTITY"):
        check_candidate(candidate, level="runtime", ray_url="http://unit-test:8265")


def test_coverage_references_locked_docs_and_real_vector_ids():
    matrix = load_resource("coverage.json")
    lock = load_resource("spec-lock.json")
    docs = {d["path"] for d in lock["documents"]}
    cases = {c["id"] for c in load_resource("cases.json")}
    assert {r["document"] for r in matrix["requirements"]} == docs
    assert all(set(r["case_ids"]) <= cases for r in matrix["requirements"])
    assert {r["execution_status"] for r in matrix["requirements"]} >= {
        "supported",
        "partial",
        "unsupported",
        "reserved",
        "extension",
    }
    assert not matrix["full_protocol_compatible"]


@pytest.mark.parametrize(
    "fault,code",
    [
        ("request_extra_wire_field", "REQUEST_WIRE_FIELDS"),
        ("request_flattened", "REQUEST_WIRE_FIELDS"),
        ("request_invalid_json", "INVALID_JSON"),
        ("request_wrong_identity", "REQUEST_WIRE_MISMATCH"),
        ("missing_wire", "CANDIDATE_FILE"),
        ("missing_feedback", "ALL_REQUEST_TRACES_REQUIRED"),
        ("event_missing_timestamp", "EVENT_TIMESTAMP"),
        ("event_invalid_error_code", "SCHEMA_ENUM"),
        ("event_wrong_model", "FEEDBACK_MANIFEST_IDENTITY"),
        ("event_wrong_feature", "FEEDBACK_FEATURE_IDENTITY"),
        ("event_missing_result", "SCHEMA_REQUIRED"),
        ("projection_differs_from_feedback", "PROJECTION_FEEDBACK_MISMATCH"),
    ],
)
def test_component_gate_fails_on_request_or_feedback_drift(
    candidate, fault, code, capsys
):
    manifest_path = candidate / "candidate.json"
    manifest = json.loads(manifest_path.read_text())
    if fault.startswith("request_"):
        path = candidate / "training-wire.json"
        records = json.loads(path.read_text())
        fields = records[0]["fields"]
        if fault == "request_extra_wire_field":
            fields["extra"] = "forbidden"
        elif fault == "request_flattened":
            records[0]["fields"] = json.loads(fields["payload"])
        elif fault == "request_invalid_json":
            fields["payload"] = "{broken"
        else:
            fields["job_id"] = "other"
        path.write_text(json.dumps(records))
    elif fault == "missing_wire":
        manifest["requests"][0].pop("wire_file")
    elif fault == "missing_feedback":
        manifest["traces"].pop()
    elif fault == "projection_differs_from_feedback":
        path = candidate / "training-completed.json"
        event = json.loads(path.read_text())
        event["duration_seconds"] += 1
        path.write_text(json.dumps(event))
    else:
        path = candidate / "training-trace.json"
        records = json.loads(path.read_text())
        event = json.loads(records[-1]["fields"]["payload"])
        if fault == "event_missing_timestamp":
            event.pop("timestamp")
        elif fault == "event_invalid_error_code":
            event = load_resource("fixtures/training-failed.json")
            event["error_code"] = "TYPO_NOT_PROTOCOL"
        elif fault == "event_wrong_model":
            event["artifact_manifest"]["model_id"] = "other-model"
        elif fault == "event_wrong_feature":
            event["training_result"]["model_features"][0]["feature_id"] = "other"
        else:
            event.pop("training_result")
        records[-1]["fields"]["payload"] = json.dumps(event)
        path.write_text(json.dumps(records))
    manifest_path.write_text(json.dumps(manifest))
    report, junit = candidate / "failure.json", candidate / "failure.xml"
    assert (
        main(
            [
                "candidate",
                "--directory",
                str(candidate),
                "--level",
                "backend",
                "--report",
                str(report),
                "--junit",
                str(junit),
            ]
        )
        == 1
    )
    failure = json.loads(report.read_text())
    assert failure["code"] == code
    assert failure["path"].startswith("$")
    if fault == "event_missing_result":
        assert failure["path"].endswith(".training_result")
    assert "<failure" in junit.read_text()
    assert json.loads(capsys.readouterr().out)["passed"] is False


def test_trace_cli_is_strict_for_producer_and_explicit_for_consumer(tmp_path):
    event = load_resource("fixtures/training-failed.json")
    event["error_code"] = "FUTURE_ERROR"
    path = tmp_path / "trace.json"
    path.write_text(
        json.dumps(
            [
                {
                    "stream_key": "training:events:" + event["job_id"],
                    "entry_id": "1-0",
                    "fields": {"job_id": event["job_id"], "payload": json.dumps(event)},
                }
            ]
        )
    )
    args = ["trace", "--operation", "training", "--input", str(path)]
    assert main(args) == 1
    assert main([*args, "--consumer"]) == 0
