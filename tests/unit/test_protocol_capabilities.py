"""Canonical v2 capability matrix and fail-fast submission gates."""

from __future__ import annotations

import copy
import json
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from tributo.integrations.broker import Message, TaskDisposition

from tributo_broker_redis.capabilities import (
    UnsupportedCapability,
    validate_supported_capabilities,
)
from tributo_broker_redis.config import RedisBrokerConfig
from tributo_broker_redis.protocol import TrainingJobRequest
from tributo_broker_redis.runtime import RedisBrokerRuntime


def _canonical() -> dict[str, Any]:
    return {
        "protocol_version": "2.0",
        "job_id": "job-1",
        "model_id": "model-1",
        "version_id": "version-1",
        "tenant_id": "tenant-1",
        "algorithm": {"category": "CLASSIFICATION", "algorithm_key": "xgboost"},
        "datasource": {
            "type": "LOCAL",
            "properties": {"path": "/data/train.csv", "format": "csv"},
        },
        "data_query": {"mode": "DIRECT_QUERY"},
        "features": [{"feature_id": "f1", "result_column": "x"}],
        "target": {
            "result_column": "label",
            "task_type": "BINARY_CLASSIFICATION",
        },
        "feature_engineering": {
            "default_missing_value_strategy": "NONE",
            "default_outlier_strategy": "NONE",
            "default_scaling_method": "NONE",
            "default_encoding_method": "NONE",
        },
        "data_split": {
            "strategy": "RANDOM",
            "train_ratio": 0.8,
            "validation_ratio": 0.1,
            "test_ratio": 0.1,
        },
        "storage_context": {"type": "local", "prefix": "/tmp/models/"},
    }


@pytest.mark.parametrize(
    ("mutation", "path"),
    [
        (lambda value: value["datasource"].update(type="DORIS"), "datasource.type"),
        (
            lambda value: value.update(
                tables=[
                    {
                        "table_alias": "t",
                        "database_name": "db",
                        "table_name": "samples",
                    }
                ]
            ),
            "tables",
        ),
        (
            lambda value: value.update(
                relations=[
                    {
                        "left_alias": "a",
                        "right_alias": "b",
                        "on": [{"left_column": "id", "right_column": "id"}],
                    }
                ]
            ),
            "relations",
        ),
        (
            lambda value: value["data_query"].update(
                mode="TIMESERIES_PIVOT",
                timeseries_pivot={"time_column": "ts", "time_values": ["2026"]},
            ),
            "data_query.mode",
        ),
        (
            lambda value: value["features"][0].update(
                treatment={"scaling_method": "STANDARD"}
            ),
            "features.0.treatment.scaling_method",
        ),
        (
            lambda value: value.update(data_sampling={"sample_ratio": 50}),
            "data_sampling.sample_ratio",
        ),
        (
            lambda value: value["data_split"].update(
                cross_validation={"enabled": True, "k_folds": 5}
            ),
            "data_split.cross_validation.enabled",
        ),
        (
            lambda value: value.update(
                tuning={"mode": "AUTO", "auto_config": {"budget": 2}}
            ),
            "tuning.mode",
        ),
        (
            lambda value: value.update(
                evaluation={"artifacts": {"correlation_matrix": True}}
            ),
            "evaluation.artifacts.correlation_matrix",
        ),
    ],
)
def test_valid_but_unimplemented_capability_reports_exact_path(
    mutation: Callable[[dict[str, Any]], Any], path: str
) -> None:
    value = copy.deepcopy(_canonical())
    mutation(value)
    request = TrainingJobRequest.model_validate(value)

    with pytest.raises(UnsupportedCapability, match=path.replace(".", r"\.")):
        validate_supported_capabilities(request)


def test_supported_semantics_map_to_core_without_loss() -> None:
    value = _canonical()
    value["data_query"]["entity_key"] = {
        "origin": {"table_alias": "t", "column_name": "id", "column_type": "String"},
        "result_column": "entity_id",
    }
    value["data_sampling"] = {
        "class_balance": {
            "strategy": "CUSTOM_AMOUNT",
            "class_amounts": {"0": 10, "1": 12},
        }
    }
    value["data_split"]["stratify"] = True
    value["evaluation"] = {
        "enabled": True,
        "artifacts": {
            "roc_curve": True,
            "threshold_analysis": True,
            "confusion_matrix": True,
            "feature_importance": False,
        },
    }
    request = TrainingJobRequest.model_validate(value)

    validate_supported_capabilities(request)
    config = request.resolve_training_config()

    assert config["data"]["entity_key_column"] == "entity_id"
    assert config["data"]["class_balance_strategy"] == "CUSTOM_AMOUNT"
    assert config["data"]["class_amounts"] == {"0": 10, "1": 12}
    assert config["training"]["split_strategy"] == "RANDOM"
    assert config["training"]["stratify"] is True
    assert config["evaluation"] == {
        "enabled": True,
        "roc_curve": True,
        "threshold_analysis": True,
        "confusion_matrix": True,
        "feature_importance": False,
    }


def test_safe_feature_engineering_defaults_are_noop_and_supported() -> None:
    request = TrainingJobRequest.model_validate(_canonical())

    validate_supported_capabilities(request)

    assert request.feature_engineering.model_dump() == {
        "default_missing_value_strategy": "NONE",
        "default_outlier_strategy": "NONE",
        "default_scaling_method": "NONE",
        "default_encoding_method": "NONE",
    }


def test_omitted_feature_engineering_fails_closed_instead_of_downgrading_auto() -> None:
    value = _canonical()
    del value["feature_engineering"]

    with pytest.raises(
        UnsupportedCapability,
        match=r"feature_engineering\.default_missing_value_strategy",
    ):
        validate_supported_capabilities(TrainingJobRequest.model_validate(value))


def test_resource_max_epochs_maps_rounds_and_conflict_is_rejected() -> None:
    value = _canonical()
    value["resource_limits"] = {"max_epochs": 37}
    request = TrainingJobRequest.model_validate(value)
    validate_supported_capabilities(request)
    assert request.resolve_training_config()["training"]["num_rounds"] == 37

    value["algorithm"]["hyper_params"] = {"num_rounds": 12}
    request = TrainingJobRequest.model_validate(value)
    validate_supported_capabilities(request)
    assert request.resolve_training_config()["training"]["num_rounds"] == 12

    value["algorithm"]["hyper_params"] = {"num_rounds": 38}
    request = TrainingJobRequest.model_validate(value)
    with pytest.raises(UnsupportedCapability, match=r"resource_limits\.max_epochs"):
        validate_supported_capabilities(request)


def test_default_max_epochs_is_an_upper_bound_not_a_thousand_round_default() -> None:
    value = _canonical()
    request = TrainingJobRequest.model_validate(value)
    validate_supported_capabilities(request)
    assert request.resolve_training_config()["training"]["num_rounds"] == 100

    value["algorithm"]["hyper_params"] = {"n_estimators": 100}
    request = TrainingJobRequest.model_validate(value)
    validate_supported_capabilities(request)
    assert request.resolve_training_config()["training"]["num_rounds"] == 100


def test_conflicting_round_aliases_fail_closed() -> None:
    value = _canonical()
    value["algorithm"]["hyper_params"] = {"num_rounds": 50, "n_estimators": 60}
    with pytest.raises(
        UnsupportedCapability, match=r"algorithm\.hyper_params\.num_rounds"
    ):
        validate_supported_capabilities(TrainingJobRequest.model_validate(value))


@pytest.mark.parametrize(
    ("task_type", "category", "metric"),
    [
        ("BINARY_CLASSIFICATION", "CLASSIFICATION", "rmse"),
        ("MULTICLASS_CLASSIFICATION", "CLASSIFICATION", "average_precision"),
        ("REGRESSION", "REGRESSION", "auc"),
        ("REGRESSION", "REGRESSION", "accuracy"),
    ],
)
def test_evaluation_metrics_must_be_task_correct_and_actually_produced(
    task_type: str, category: str, metric: str
) -> None:
    value = _canonical()
    value["algorithm"]["category"] = category
    value["target"]["task_type"] = task_type
    value["evaluation"] = {"primary_metric": metric}
    if task_type == "MULTICLASS_CLASSIFICATION":
        value["algorithm"]["hyper_params"] = {"num_class": 3}

    with pytest.raises(UnsupportedCapability, match=r"evaluation\.primary_metric"):
        validate_supported_capabilities(TrainingJobRequest.model_validate(value))


def test_supported_additional_metrics_are_validated_but_not_silently_ignored() -> None:
    value = _canonical()
    value["evaluation"] = {
        "primary_metric": "auc",
        "additional_metrics": ["f1", "precision", "recall", "average_precision"],
    }
    validate_supported_capabilities(TrainingJobRequest.model_validate(value))

    value["evaluation"]["additional_metrics"] = ["logloss"]
    with pytest.raises(
        UnsupportedCapability, match=r"evaluation\.additional_metrics\.0"
    ):
        validate_supported_capabilities(TrainingJobRequest.model_validate(value))


@pytest.mark.parametrize(
    ("mutation", "path"),
    [
        (
            lambda value: value["target"].update(label_mapping={"no": 0, "yes": 1}),
            "target.label_mapping",
        ),
        (
            lambda value: value["target"].update(positive_label_value="yes"),
            "target.positive_label_value",
        ),
        (
            lambda value: value["target"].update(pivot_time_value="2026-01"),
            "target.pivot_time_value",
        ),
        (
            lambda value: value["datasource"]["properties"].update(ignored=True),
            "datasource.properties.ignored",
        ),
    ],
)
def test_accepted_but_unexecuted_semantics_fail_closed(
    mutation: Callable[[dict[str, Any]], Any], path: str
) -> None:
    value = _canonical()
    mutation(value)
    with pytest.raises(UnsupportedCapability, match=path.replace(".", r"\.")):
        validate_supported_capabilities(TrainingJobRequest.model_validate(value))


@pytest.mark.parametrize(
    ("datasource_update", "property_update", "path"),
    [
        ({"password": "inline"}, {}, "datasource.password"),
        ({"credential_ref": "vault:item"}, {}, "datasource.credential_ref"),
        (
            {"connection_string": "postgres://user:secret@db/name"},
            {},
            "datasource.connection_string",
        ),
        ({}, {"access_key_id": "AKIA..."}, "datasource.properties.access_key_id"),
        (
            {},
            {"secret_access_key": "inline"},
            "datasource.properties.secret_access_key",
        ),
        (
            {},
            {"endpoint": "https://user:secret@minio.internal"},
            "datasource.properties.endpoint",
        ),
    ],
)
def test_canonical_datasource_credentials_fail_closed_before_worker_env(
    datasource_update: dict[str, Any],
    property_update: dict[str, Any],
    path: str,
) -> None:
    value = _canonical()
    value["datasource"]["type"] = "S3" if property_update else "LOCAL"
    value["datasource"].update(datasource_update)
    value["datasource"]["properties"].update(property_update)
    if property_update:
        value["datasource"]["properties"].update(
            uri="s3://bucket/train.parquet", format="parquet"
        )
        value["datasource"]["properties"].pop("path", None)

    with pytest.raises(UnsupportedCapability, match=path.replace(".", r"\.")):
        validate_supported_capabilities(TrainingJobRequest.model_validate(value))


@pytest.mark.parametrize(
    ("task_type", "category", "hyper_params", "path"),
    [
        (
            "BINARY_CLASSIFICATION",
            "CLASSIFICATION",
            {"objective": "reg:squarederror"},
            "algorithm.hyper_params.objective",
        ),
        (
            "BINARY_CLASSIFICATION",
            "CLASSIFICATION",
            {"objective": "binary:hinge"},
            "algorithm.hyper_params.objective",
        ),
        (
            "REGRESSION",
            "REGRESSION",
            {"objective": "binary:logistic"},
            "algorithm.hyper_params.objective",
        ),
        (
            "BINARY_CLASSIFICATION",
            "CLASSIFICATION",
            {"num_class": 3},
            "algorithm.hyper_params.num_class",
        ),
        (
            "MULTICLASS_CLASSIFICATION",
            "CLASSIFICATION",
            {"num_class": 3, "eval_metric": "logloss"},
            "algorithm.hyper_params.eval_metric",
        ),
        (
            "BINARY_CLASSIFICATION",
            "CLASSIFICATION",
            {"num_rounds": "10"},
            "algorithm.hyper_params.num_rounds",
        ),
    ],
)
def test_hyperparameter_control_fields_are_task_correct_and_strict(
    task_type: str,
    category: str,
    hyper_params: dict[str, Any],
    path: str,
) -> None:
    value = _canonical()
    value["algorithm"].update(category=category, hyper_params=hyper_params)
    value["target"]["task_type"] = task_type
    if task_type == "REGRESSION":
        value["evaluation"] = {"primary_metric": "rmse"}
    elif task_type == "MULTICLASS_CLASSIFICATION":
        value["evaluation"] = {"primary_metric": "f1_macro"}

    with pytest.raises(UnsupportedCapability, match=path.replace(".", r"\.")):
        validate_supported_capabilities(TrainingJobRequest.model_validate(value))


def test_supported_multiclass_objective_and_metric_are_preserved() -> None:
    value = _canonical()
    value["target"]["task_type"] = "MULTICLASS_CLASSIFICATION"
    value["algorithm"]["hyper_params"] = {
        "objective": "multi:softprob",
        "num_class": 3,
        "eval_metric": "mlogloss",
    }
    value["evaluation"] = {"primary_metric": "f1"}
    request = TrainingJobRequest.model_validate(value)

    validate_supported_capabilities(request)
    config = request.resolve_training_config()
    assert config["model"]["objective"] == "multi:softprob"
    assert config["model"]["num_class"] == 3
    assert config["model"]["eval_metric"] == "mlogloss"


def test_multiclass_softmax_rejects_auc_evaluation_request() -> None:
    value = _canonical()
    value["target"]["task_type"] = "MULTICLASS_CLASSIFICATION"
    value["algorithm"]["hyper_params"] = {
        "objective": "multi:softmax",
        "num_class": 3,
        "eval_metric": "merror",
    }
    value["evaluation"] = {"primary_metric": "auc"}
    with pytest.raises(UnsupportedCapability, match=r"evaluation\.primary_metric"):
        validate_supported_capabilities(TrainingJobRequest.model_validate(value))


@pytest.mark.parametrize(
    ("hyper_params", "path"),
    [
        ({"unknown_knob": 1}, "algorithm.hyper_params.unknown_knob"),
        ({"max_depth": "deep"}, "algorithm.hyper_params.max_depth"),
        (
            {"eta": 0.1, "learning_rate": 0.2},
            "algorithm.hyper_params.eta",
        ),
        ({"subsample": 1.5}, "algorithm.hyper_params.subsample"),
    ],
)
def test_model_hyperparameters_use_a_strict_allowlist(
    hyper_params: dict[str, Any], path: str
) -> None:
    value = _canonical()
    value["algorithm"]["hyper_params"] = hyper_params
    with pytest.raises(UnsupportedCapability, match=path.replace(".", r"\.")):
        validate_supported_capabilities(TrainingJobRequest.model_validate(value))


@pytest.mark.parametrize(
    "field",
    [
        "objective",
        "eval_metric",
        "num_rounds",
        "n_estimators",
        "num_workers",
        "use_gpu",
        "max_failures",
        "seed",
        "early_stopping_rounds",
        "eta",
        "learning_rate",
        "max_depth",
        "subsample",
        "tree_method",
    ],
)
def test_explicit_null_hyperparameter_never_falls_back_or_stringifies(
    field: str,
) -> None:
    value = _canonical()
    value["algorithm"]["hyper_params"] = {field: None}
    if field == "num_rounds":
        value["algorithm"]["hyper_params"]["n_estimators"] = 12

    with pytest.raises(
        UnsupportedCapability,
        match=rf"algorithm\.hyper_params\.{field}",
    ):
        validate_supported_capabilities(TrainingJobRequest.model_validate(value))


def test_opaque_extensions_are_accepted_as_driver_metadata() -> None:
    value = _canonical()
    value["extensions"] = {"future": {"enabled": True, "labels": ["safe"]}}
    request = TrainingJobRequest.model_validate(value)
    validate_supported_capabilities(request)


@pytest.mark.parametrize("column_type", ["string", "category", "object"])
def test_non_numeric_feature_types_require_unimplemented_encoding(
    column_type: str,
) -> None:
    value = _canonical()
    value["features"][0]["column_type"] = column_type
    with pytest.raises(UnsupportedCapability, match=r"features\.0\.column_type"):
        validate_supported_capabilities(TrainingJobRequest.model_validate(value))


@pytest.mark.parametrize(
    "column_type",
    [
        "int",
        "int8",
        "int16",
        "int32",
        "int64",
        "integer",
        "bigint",
        "float",
        "float32",
        "float64",
        "double",
        "decimal",
        "numeric",
        "boolean",
        "bool",
    ],
)
def test_numeric_and_boolean_feature_type_synonyms_are_supported(
    column_type: str,
) -> None:
    value = _canonical()
    value["features"][0]["column_type"] = column_type
    validate_supported_capabilities(TrainingJobRequest.model_validate(value))


@pytest.mark.parametrize(
    "column_type",
    [
        "DECIMAL(18, 4)",
        "decimal ( 9 , 0 )",
        "Decimal32(2)",
        "decimal64 ( 8 )",
        "Decimal128(20)",
        "Decimal256(40)",
        "Nullable(Int64)",
        " nullable ( Boolean ) ",
        "Nullable(Decimal(20, 6))",
        "Nullable(Decimal128(12))",
    ],
)
def test_native_numeric_qualifiers_and_nullable_features_are_supported(
    column_type: str,
) -> None:
    value = _canonical()
    value["features"][0]["column_type"] = column_type
    validate_supported_capabilities(TrainingJobRequest.model_validate(value))


@pytest.mark.parametrize(
    "column_type",
    [
        "Nullable(String)",
        "Array(Int64)",
        "Nullable(Array(Float64))",
        "Decimal(4, 5)",
        "Decimal64(19)",
        "Nullable()",
    ],
)
def test_non_numeric_or_malformed_native_feature_types_are_rejected(
    column_type: str,
) -> None:
    value = _canonical()
    value["features"][0]["column_type"] = column_type
    with pytest.raises(UnsupportedCapability, match=r"features\.0\.column_type"):
        validate_supported_capabilities(TrainingJobRequest.model_validate(value))


@pytest.mark.parametrize(
    ("task_type", "category", "column_type", "supported"),
    [
        ("BINARY_CLASSIFICATION", "CLASSIFICATION", "bool", True),
        ("BINARY_CLASSIFICATION", "CLASSIFICATION", "int64", True),
        ("BINARY_CLASSIFICATION", "CLASSIFICATION", "string", False),
        ("MULTICLASS_CLASSIFICATION", "CLASSIFICATION", "integer", True),
        ("MULTICLASS_CLASSIFICATION", "CLASSIFICATION", "category", False),
        ("REGRESSION", "REGRESSION", "float64", True),
        ("REGRESSION", "REGRESSION", "bool", False),
        ("REGRESSION", "REGRESSION", "datetime", False),
    ],
)
def test_target_column_type_is_task_correct(
    task_type: str,
    category: str,
    column_type: str,
    supported: bool,
) -> None:
    value = _canonical()
    value["algorithm"]["category"] = category
    value["target"].update(task_type=task_type, column_type=column_type)
    if task_type == "MULTICLASS_CLASSIFICATION":
        value["algorithm"]["hyper_params"] = {"num_class": 3}
        value["evaluation"] = {"primary_metric": "f1"}
    elif task_type == "REGRESSION":
        value["evaluation"] = {"primary_metric": "rmse"}

    request = TrainingJobRequest.model_validate(value)
    if supported:
        validate_supported_capabilities(request)
    else:
        with pytest.raises(UnsupportedCapability, match=r"target\.column_type"):
            validate_supported_capabilities(request)


@pytest.mark.parametrize(
    ("task_type", "category", "column_type"),
    [
        ("BINARY_CLASSIFICATION", "CLASSIFICATION", "Nullable(Boolean)"),
        ("BINARY_CLASSIFICATION", "CLASSIFICATION", "Decimal32(0)"),
        ("REGRESSION", "REGRESSION", "Nullable(Float64)"),
        ("REGRESSION", "REGRESSION", "Decimal(20, 8)"),
    ],
)
def test_target_uses_the_same_native_numeric_type_parser(
    task_type: str, category: str, column_type: str
) -> None:
    value = _canonical()
    value["algorithm"]["category"] = category
    value["target"].update(task_type=task_type, column_type=column_type)
    if task_type == "REGRESSION":
        value["evaluation"] = {"primary_metric": "rmse"}
    validate_supported_capabilities(TrainingJobRequest.model_validate(value))


def test_invalid_file_format_fails_before_ray_submission() -> None:
    value = _canonical()
    value["datasource"]["properties"]["format"] = "jsonl"
    with pytest.raises(UnsupportedCapability, match=r"datasource\.properties\.format"):
        validate_supported_capabilities(TrainingJobRequest.model_validate(value))


def test_supervised_training_timeout_is_supported() -> None:
    value = _canonical()
    value["resource_limits"] = {"max_training_time_seconds": 300}

    validate_supported_capabilities(TrainingJobRequest.model_validate(value))


@pytest.mark.parametrize(
    ("sampling", "path"),
    [
        (
            {"class_balance": {"strategy": "CUSTOM_AMOUNT", "class_amounts": {"0": 2}}},
            "data_sampling.class_balance",
        ),
        (None, "data_split.stratify"),
    ],
)
def test_regression_rejects_classification_only_sampling_semantics(
    sampling: dict[str, Any] | None, path: str
) -> None:
    value = _canonical()
    value["algorithm"]["category"] = "REGRESSION"
    value["target"]["task_type"] = "REGRESSION"
    value["evaluation"] = {"primary_metric": "rmse"}
    if sampling is not None:
        value["data_sampling"] = sampling
    else:
        value["data_split"]["stratify"] = True

    with pytest.raises(UnsupportedCapability, match=path.replace(".", r"\.")):
        validate_supported_capabilities(TrainingJobRequest.model_validate(value))


def test_runtime_capability_gate_fails_before_ray_submission() -> None:
    value = _canonical()
    value["datasource"]["type"] = "JDBC"
    runtime = RedisBrokerRuntime.__new__(RedisBrokerRuntime)
    runtime.config = RedisBrokerConfig()
    runtime._redis = MagicMock()
    runtime._redis.eval.side_effect = lambda script, _keys, *args: (
        args[1] if "STAGE_TERMINAL_CANDIDATE" in script else ["published", "1-0"]
    )
    runtime._consumer = MagicMock()

    with patch(
        "tributo_broker_redis.runtime.submit_training_job_with_identity"
    ) as submit:
        outcome = runtime.handle(
            Message("job-1", {"raw": json.dumps(value)}, delivery_id="1-0")
        )

    assert outcome.disposition == TaskDisposition.ACK
    submit.assert_not_called()
    event = json.loads(runtime._redis.eval.call_args.args[4])
    assert event["error_code"] == "INVALID_PAYLOAD"
    assert event["phase"] == "QUEUED"
    assert "datasource.type" in event["error_message"]
