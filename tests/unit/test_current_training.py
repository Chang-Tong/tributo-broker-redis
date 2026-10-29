"""Current Core contract and deliberate retirement of legacy trainer config."""

from __future__ import annotations

import importlib.util
import json
from typing import Any

import pytest
from tributo.algorithms import AlgorithmExecutionResult, AlgorithmRunResult

from conftest import training_config
from tributo_broker_redis.operations import MappingFailure, prepare_operation
from tributo_broker_redis.protocol import GenericRequest
from tributo_broker_redis.training import GenericTrainingConfig, serialize_result


def request(config: dict[str, Any]) -> GenericRequest:
    return GenericRequest(
        protocol_profile="tributo-generic-v1",
        protocol_version="1.0",
        operation_id="train",
        operation_type="training",
        execution_profile="single_worker",
        spec={"algorithm": "xgboost", "config": config},
    )


def test_current_core_does_not_restore_removed_trainer() -> None:
    assert importlib.util.find_spec("tributo.training.xgboost_trainer") is None


def test_legacy_trainer_config_is_explicitly_rejected() -> None:
    with pytest.raises(MappingFailure) as caught:
        prepare_operation(
            request(
                {
                    "data": {"type": "csv", "path": "/old.csv"},
                    "ray": {"num_workers": 1},
                    "training": {"val_size": 0.0},
                    "output": {"bundle_uri": "/old-bundle"},
                }
            )
        )
    assert caught.value.code == "INVALID_TRAINING_REQUEST"


@pytest.mark.parametrize("engine", ["ray", "tributo.ray_data"])
def test_ingestion_uses_current_canonical_engine(engine: str) -> None:
    config = training_config(1)
    config["ingestion"]["engine"] = engine
    result = GenericTrainingConfig.model_validate(config)
    assert result.ingestion.engine == "tributo.ray_data"
    assert result.resources.as_core().num_cpus == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"train_ratio": 0.8, "test_ratio": 0.3},
        {"val_size": 0.2},
        {"early_stopping_rounds": 3},
    ],
)
def test_invalid_or_retired_split_parameters_fail_admission(
    changes: dict[str, Any],
) -> None:
    config = training_config(1)
    config["training"].update(changes)
    with pytest.raises(MappingFailure) as caught:
        prepare_operation(request(config))
    assert caught.value.code == "INVALID_TRAINING_REQUEST"


def test_timeout_is_transported_to_the_shared_lifecycle_watchdog() -> None:
    config = training_config(1)
    config["training"]["max_training_time_seconds"] = 30
    prepared = prepare_operation(request(config))
    assert prepared.timeout_seconds == 30


def test_public_result_serializes_without_legacy_pydantic_methods() -> None:
    result = AlgorithmRunResult(
        run_id="run",
        plan_id="a" * 64,
        execution=AlgorithmExecutionResult(
            status="succeeded",
            metrics={"accuracy": 0.9},
            outputs={"bundle_uri": "/bundle"},
        ),
        actual_versions={"ray": "2.55.1"},
        input_provenance={"dataset_ref": "fixture"},
        worker_metadata={"workers": [{"rank": 0}]},
    )
    value = serialize_result(result)
    assert (
        json.loads(json.dumps(value))["execution"]["outputs"]["bundle_uri"] == "/bundle"
    )
    assert value["worker_metadata"]["workers"] == [{"rank": 0}]
