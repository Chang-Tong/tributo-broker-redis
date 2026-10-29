"""Keep native model checks independent of ONNX telemetry network callbacks."""

import importlib.util
import json

import pytest

from tributo_broker_redis.contracts.aimodel_v2 import load_resource


@pytest.fixture(scope="session", autouse=True)
def disable_onnx_telemetry() -> None:
    # ONNX Runtime 1.28 on macOS can abort in its telemetry HTTP worker during
    # interpreter shutdown. This does not disable model execution or assertions.
    if importlib.util.find_spec("onnxruntime") is None:
        return
    import onnxruntime

    onnxruntime.disable_telemetry_events()


@pytest.fixture
def protocol_candidate(tmp_path):
    for name in (
        "training-request",
        "inference-request",
        "training-completed",
        "training-trace",
        "inference-trace",
        "inference-rows",
    ):
        (tmp_path / (name + ".json")).write_text(
            json.dumps(load_resource("fixtures/" + name + ".json"))
        )
    value = {
        "contract_revision": load_resource("spec-lock.json")["contract_revision"],
        "producer": {"component": "backend", "revision": "fixture"},
        "requests": [
            {
                "operation": "training",
                "file": "training-request.json",
                "wire_file": "training-wire.json",
                "outer_identity": "train-contract-1",
            },
            {
                "operation": "inference",
                "file": "inference-request.json",
                "wire_file": "inference-wire.json",
                "outer_identity": "infer-contract-1",
            },
        ],
        "projections": [
            {
                "completed": "training-completed.json",
                "inference": "inference-request.json",
            }
        ],
        "traces": [
            {
                "identity": "train-contract-1",
                "file": "training-trace.json",
                "submission_id": "job-train",
            },
            {
                "identity": "infer-contract-1",
                "file": "inference-trace.json",
                "submission_id": "job-infer",
            },
        ],
        "result_sets": [
            {"identity": "infer-contract-1", "file": "inference-rows.json"}
        ],
    }
    for item in value["requests"]:
        operation = item["operation"]
        field = "job_id" if operation == "training" else "execution_id"
        (tmp_path / item["wire_file"]).write_text(
            json.dumps(
                [
                    {
                        "stream_key": operation + ":tasks",
                        "entry_id": "1-0",
                        "fields": {
                            field: item["outer_identity"],
                            "payload": (tmp_path / item["file"]).read_text(),
                        },
                    }
                ]
            )
        )
    (tmp_path / "candidate.json").write_text(json.dumps(value))
    return tmp_path
