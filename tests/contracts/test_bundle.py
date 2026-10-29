"""Portable protocol oracles: no Broker executor, Redis, Core or Ray required."""

from __future__ import annotations

import copy
import json
import subprocess
import sys

import pytest

from tributo_broker_redis.contracts.aimodel_v2 import (
    ContractViolation,
    load_resource,
    validate_payload,
    validate_projection,
    validate_result_rows,
    validate_trace,
)
from tributo_broker_redis.contracts.aimodel_v2.suite import materialize

CASES = load_resource("cases.json")


@pytest.mark.parametrize("case", CASES, ids=lambda c: c["id"])
def test_protocol_request_vector(case):
    if case["accepted"]:
        validate_payload(case["kind"], materialize(case))
    else:
        with pytest.raises(ContractViolation):
            validate_payload(case["kind"], materialize(case))


@pytest.mark.parametrize("operation", ["training", "inference"])
def test_golden_trace_and_replayed_delivery(operation):
    records = load_resource(f"fixtures/{operation}-trace.json")
    result = validate_trace([*records, copy.deepcopy(records[-1])], operation)
    assert result["terminal"] == "COMPLETED"
    assert result["duplicate_deliveries"] == 1


@pytest.mark.parametrize("operation", ["training", "inference"])
@pytest.mark.parametrize("terminal", ["failed", "cancelled"])
def test_non_success_terminals_are_valid_protocol_messages(operation, terminal):
    validate_payload(
        operation + "-event", load_resource(f"fixtures/{operation}-{terminal}.json")
    )


def _mutate_event(records, index, **fields):
    value = json.loads(records[index]["fields"]["payload"])
    value.update(fields)
    records[index]["fields"]["payload"] = json.dumps(value)


@pytest.mark.parametrize(
    "mutation",
    [
        "extra_wire_field",
        "wrong_identity",
        "wrong_stream",
        "flattened",
        "seconds_string",
        "after_terminal",
        "replay_changed",
        "decreasing_rows",
        "missing_terminal",
        "uppercase_level",
        "duplicate_json_key",
        "wrong_identity_key",
        "out_of_order",
    ],
)
def test_invalid_wire_or_lifecycle_is_rejected(mutation):
    records = load_resource("fixtures/inference-trace.json")
    if mutation == "extra_wire_field":
        records[0]["fields"]["timestamp"] = "1000"
    elif mutation == "wrong_identity":
        records[0]["fields"]["execution_id"] = "wrong"
    elif mutation == "wrong_stream":
        records[0]["stream_key"] += ":wrong"
    elif mutation == "flattened":
        records[0]["fields"] = json.loads(records[0]["fields"]["payload"])
    elif mutation == "seconds_string":
        _mutate_event(records, 0, timestamp="1781161200")
    elif mutation == "after_terminal":
        tail = copy.deepcopy(records[-2])
        tail["entry_id"] = "99-0"
        records.append(tail)
    elif mutation == "replay_changed":
        tail = copy.deepcopy(records[-1])
        records.append(tail)
        _mutate_event(records, -1, duration_seconds=999)
    elif mutation == "decreasing_rows":
        _mutate_event(records, 3, processed_rows=0)
    elif mutation == "missing_terminal":
        records.pop()
    elif mutation == "uppercase_level":
        _mutate_event(records, 2, level="ERROR")
    elif mutation == "duplicate_json_key":
        records[0]["fields"]["payload"] = records[0]["fields"]["payload"].replace(
            '"timestamp":', '"timestamp":0,"timestamp":'
        )
    elif mutation == "wrong_identity_key":
        _mutate_event(records, 0, job_id="wrong")
    elif mutation == "out_of_order":
        records[1]["entry_id"] = "0-1"
    with pytest.raises(ContractViolation):
        validate_trace(records, "inference")


def test_unknown_event_is_ignored_and_error_log_is_not_failure():
    records = load_resource("fixtures/inference-trace.json")
    unknown = copy.deepcopy(records[0])
    unknown["entry_id"] = "1-1"
    _mutate_event([unknown], 0, event_type="FUTURE_HEARTBEAT")
    records.insert(1, unknown)
    result = validate_trace(records, "inference")
    assert result["terminal"] == "COMPLETED"
    assert result["unknown_events"] == 1


def test_unknown_failure_code_maps_to_unknown_for_consumer_but_not_producer():
    event = load_resource("fixtures/inference-failed.json")
    event["error_code"] = "FUTURE_ERROR"
    with pytest.raises(ContractViolation):
        validate_payload("inference-event", event)
    row = {
        "stream_key": "test:" + event["execution_id"],
        "entry_id": "1-0",
        "fields": {"execution_id": event["execution_id"], "payload": json.dumps(event)},
    }
    assert validate_trace([row], "inference")["unknown_error_codes"] == 1


def test_open_trace_cannot_pass_a_completed_ci_gate():
    records = load_resource("fixtures/inference-trace.json")[:-1]
    assert (
        validate_trace(records, "inference", require_terminal=False)["terminal"] is None
    )
    with pytest.raises(ContractViolation):
        validate_trace(records, "inference")


def test_training_metrics_start_at_first_round():
    records = load_resource("fixtures/training-trace.json")
    _mutate_event(records, 2, current_round=2, progress_percent=66.7)
    with pytest.raises(ContractViolation, match="MISSING_FIRST_ROUND"):
        validate_trace(records, "training")


def test_projection_is_exact_not_just_a_matching_model_id():
    completed = load_resource("fixtures/training-completed.json")
    request = load_resource("fixtures/inference-request.json")
    validate_projection(completed, request)
    request["model"]["model_artifacts"]["model_weights"]["alternatives"][0]["files"][0][
        "metadata"
    ]["backend_trimmed"] = True
    with pytest.raises(ContractViolation, match="MANIFEST_PROJECTION"):
        validate_projection(completed, request)


@pytest.mark.parametrize(
    "mutation",
    [
        "binary_inversion",
        "wrong_entity_type",
        "regression_columns",
        "missing_feature",
        "wrong_identity",
        "duplicate_entity",
        "nonfinite",
        "filtered_out",
    ],
)
def test_result_table_semantics(mutation):
    request = load_resource("fixtures/inference-request.json")
    rows = load_resource("fixtures/inference-rows.json")
    validate_result_rows(request, rows)
    if mutation == "binary_inversion":
        rows[0]["pred_label"] = "否"
    elif mutation == "wrong_entity_type":
        rows[0]["entity_id"] = 42
    elif mutation == "regression_columns":
        rows[0]["pred_value"] = 1
    elif mutation == "missing_feature":
        rows[0]["pred_extra"] = '{"inference_feature_values":[]}'
    elif mutation == "wrong_identity":
        rows[0]["tenant_id"] = "other"
    elif mutation == "duplicate_entity":
        rows.append(copy.deepcopy(rows[0]))
    elif mutation == "nonfinite":
        rows[0]["pred_probability"] = float("nan")
    elif mutation == "filtered_out":
        request["output"]["result_filter"] = {"mode": "EXCLUDE", "labels": ["是"]}
    with pytest.raises(ContractViolation):
        validate_result_rows(request, rows)


def test_zero_output_and_positive_zero_are_valid():
    request = load_resource("fixtures/inference-request.json")
    validate_result_rows(request, [])
    rows = load_resource("fixtures/inference-rows.json")
    request["model"]["positive_label_value"] = "否"
    rows[0]["pred_label"] = "否"
    validate_result_rows(request, rows)


def test_contract_checker_never_imports_execution_stack():
    code = """import importlib.abc,sys
class Deny(importlib.abc.MetaPathFinder):
 def find_spec(self,fullname,path=None,target=None):
  if fullname.split('.')[0] in {'tributo','ray','redis','tributo_knova'}:
   raise AssertionError(fullname)
sys.meta_path.insert(0,Deny())
from tributo_broker_redis.contracts.aimodel_v2.suite import run_vectors
assert all(r['passed'] for r in run_vectors())
"""
    run = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert run.returncode == 0, run.stderr


def test_schema_failure_does_not_echo_a_credential_value():
    value = load_resource("fixtures/inference-request.json")
    value["model"]["storage"]["prefix"] = "SECRET-MARKER"
    with pytest.raises(ContractViolation) as caught:
        validate_payload("inference-request", value)
    assert "SECRET-MARKER" not in str(caught.value)


@pytest.mark.parametrize("field,value", [("stream_key", []), ("event_type", [])])
def test_malformed_trace_envelope_has_safe_contract_error(field, value):
    records = load_resource("fixtures/inference-trace.json")
    if field == "stream_key":
        records[0][field] = value
    else:
        event = json.loads(records[0]["fields"]["payload"])
        event[field] = value
        records[0]["fields"]["payload"] = json.dumps(event)
    with pytest.raises(ContractViolation):
        validate_trace(records, "inference")


@pytest.mark.parametrize("values", [[1], {"feature_name": "wrong"}, None])
def test_malformed_result_features_have_safe_contract_error(values):
    rows = load_resource("fixtures/inference-rows.json")
    rows[0]["pred_extra"] = json.dumps({"inference_feature_values": values})
    with pytest.raises(ContractViolation, match="RESULT_FEATURE_ALIGNMENT"):
        validate_result_rows(load_resource("fixtures/inference-request.json"), rows)


@pytest.mark.parametrize(
    "section,key,value,code",
    [
        ("artifact_manifest", "model_id", "wrong", "FEEDBACK_MANIFEST_IDENTITY"),
        ("artifact_manifest", "tenant_id", "wrong", "FEEDBACK_MANIFEST_IDENTITY"),
        ("artifact_manifest", "version_id", "wrong", "FEEDBACK_MANIFEST_IDENTITY"),
        ("artifact_manifest", "algorithm_key", "other", "FEEDBACK_TRAINING_MODEL"),
        ("training_result", "algorithm_key", "other", "FEEDBACK_TRAINING_MODEL"),
        ("training_result", "task_type", "REGRESSION", "FEEDBACK_TRAINING_MODEL"),
        ("storage", "bucket", "/other", "FEEDBACK_STORAGE_SCOPE"),
        ("storage", "prefix", "other/", "FEEDBACK_STORAGE_SCOPE"),
    ],
)
def test_feedback_is_linked_to_request_semantics(section, key, value, code):
    from tributo_broker_redis.contracts.aimodel_v2 import validate_feedback

    request = load_resource("fixtures/training-request.json")
    records = load_resource("fixtures/training-trace.json")
    event = json.loads(records[-1]["fields"]["payload"])
    target = (
        event[section]
        if section != "storage"
        else event["artifact_manifest"]["storage"]
    )
    target[key] = value
    records[-1]["fields"]["payload"] = json.dumps(event)
    with pytest.raises(ContractViolation, match=code):
        validate_feedback(request, records, "training")


def test_training_storage_may_use_canonical_alias_and_child_prefix():
    from tributo_broker_redis.contracts.aimodel_v2 import validate_feedback

    request = load_resource("fixtures/training-request.json")
    request["storage_context"]["type"] = "nfs"
    records = load_resource("fixtures/training-trace.json")
    event = json.loads(records[-1]["fields"]["payload"])
    event["artifact_manifest"]["storage"]["prefix"] += "bundle/"
    records[-1]["fields"]["payload"] = json.dumps(event)
    assert validate_feedback(request, records, "training")["terminal"] == "COMPLETED"
