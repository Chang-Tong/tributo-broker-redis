"""Engine-side conformance: compare Broker admission/output to protocol oracles."""

from __future__ import annotations

import json
from typing import Any

import pytest

from tributo_broker_redis.contracts.aimodel_v2 import (
    load_resource,
    validate_payload,
    validate_trace,
)
from tributo_broker_redis.contracts.aimodel_v2.suite import materialize
from tributo_broker_redis.profiles.aimodel_v2.protocol import (
    KnovaProtocolFailure,
    parse_request,
)
from tributo_broker_redis.profiles.aimodel_v2.reporter import KnovaRedisEventReporter


@pytest.mark.parametrize("case", load_resource("cases.json"), ids=lambda c: c["id"])
def test_broker_admission_matches_protocol_vector(case):
    payload = materialize(case)
    operation = "training" if case["kind"] == "training-request" else "batch_inference"
    identity = payload["job_id" if operation == "training" else "execution_id"]
    if case["accepted"]:
        parsed = parse_request(
            json.dumps(payload),
            outer_operation_id=identity,
            expected_operation_type=operation,
        )
        assert parsed.execution_id == identity
    else:
        with pytest.raises(KnovaProtocolFailure) as caught:
            parse_request(
                json.dumps(payload),
                outer_operation_id=identity,
                expected_operation_type=operation,
            )
        assert caught.value.code in {"UNSUPPORTED_PROTOCOL_VERSION", "INVALID_PAYLOAD"}


class Recorder:
    def __init__(self):
        self.records = []

    def xadd(self, stream: str, fields: dict[str, str], **kwargs: Any):
        entry_id = f"{len(self.records) + 1}-0"
        self.records.append(
            {"stream_key": stream, "entry_id": entry_id, "fields": fields}
        )
        return entry_id


@pytest.mark.parametrize("operation", ["training", "inference"])
@pytest.mark.parametrize("terminal", ["COMPLETED", "FAILED", "CANCELLED"])
def test_actual_broker_reporter_matches_independent_schema_and_trace(
    operation, terminal
):
    wire = load_resource(f"fixtures/{operation}-trace.json")
    identity_field = "job_id" if operation == "training" else "execution_id"
    identity = wire[0]["fields"][identity_field]
    redis = Recorder()
    reporter = KnovaRedisEventReporter(
        redis,
        event_stream_prefix=f"contract:{operation}",
        operation_id=identity,
        operation_type="training" if operation == "training" else "batch_inference",
        execution_profile="distributed",
        run_id=identity,
        attempt_id="attempt",
        outer_identity_field=identity_field,
    )
    for record in wire[:2] if terminal == "CANCELLED" else wire[:-1]:
        event = json.loads(record["fields"]["payload"])
        reporter.publish(event["event_type"], event, phase=event.get("phase"))
    event = (
        json.loads(wire[-1]["fields"]["payload"])
        if terminal == "COMPLETED"
        else load_resource(f"fixtures/{operation}-{terminal.lower()}.json")
    )
    if terminal == "FAILED":
        event["sanitized_message"] = event["error_message"]
    reporter.publish(terminal, event, phase=event["phase"])
    count = len(redis.records)
    reporter.publish("LOG", {"message": "late write"})
    assert len(redis.records) == count
    assert validate_trace(redis.records, operation)["terminal"] == terminal
    for record in redis.records:
        emitted = json.loads(record["fields"]["payload"])
        validate_payload(operation + "-event", emitted)
        assert (
            not {"protocol_profile", "operation_id", "operation_type", "submission_id"}
            & emitted.keys()
        )
