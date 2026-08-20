"""Canonical datasource mapping into Tributo Core training configs."""

from __future__ import annotations

from typing import Any

import pytest
from tributo.training.xgboost_trainer import XGBoostTrainingConfig

from tributo_broker_redis.protocol import TrainingJobRequest


def _request(
    datasource: dict[str, Any], *, sql: str = "SELECT x, label FROM samples"
) -> TrainingJobRequest:
    payload: dict[str, Any] = {
        "job_id": "job-1",
        "model_id": "model-1",
        "version_id": "version-1",
        "tenant_id": "tenant-1",
        "datasource": datasource,
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
        "storage_context": {"type": "local", "prefix": "/tmp/models/"},
    }
    if str(datasource.get("type", "")).upper() in {"CLICKHOUSE", "HIVE"}:
        payload["data_query"] = {"query": {"sql": sql, "params": {"tenant": "acme"}}}
    return TrainingJobRequest.model_validate(payload)


def test_hive_canonical_request_maps_all_core_fields_without_loss() -> None:
    request = _request(
        {
            "type": "HIVE",
            "host": "hive.internal",
            "port": 10000,
            "database_name": "warehouse",
            "username": "reader",
            "properties": {
                "batch_size": "512",
                "shard_mode": "hash",
                "hash_column": "subscriber_id",
                "hash_shards": "8",
                "parallelism": "4",
            },
        }
    )

    config = request.resolve_training_config()

    assert config["data"] == {
        "type": "hive",
        "hive_host": "hive.internal",
        "hive_port": 10000,
        "hive_database": "warehouse",
        "hive_user": "reader",
        "hive_password": "",
        "hive_sql": "SELECT x, label FROM samples",
        "hive_sql_params": {"tenant": "acme"},
        "hive_batch_size": 512,
        "hive_shard_mode": "hash",
        "hive_hash_column": "subscriber_id",
        "hive_hash_shards": 8,
        "hive_parallelism": 4,
        "label_col": "label",
        "feature_columns": ["x"],
        "feature_id_map": {"x": "f1"},
    }
    validated = XGBoostTrainingConfig.model_validate(config).model_dump()
    for field, value in config["data"].items():
        assert validated["data"][field] == value


def test_hive_defaults_hash_shards_to_64_and_omits_unspecified_options() -> None:
    config = _request(
        {
            "type": "HIVE",
            "host": "hive.internal",
            "port": 10000,
            "database_name": "warehouse",
        }
    ).resolve_training_config()

    assert config["data"]["hive_hash_shards"] == 64
    assert "hive_batch_size" not in config["data"]
    assert "hive_parallelism" not in config["data"]


def test_hive_uses_hiveserver2_port_when_request_omits_port() -> None:
    request = _request(
        {
            "type": "HIVE",
            "host": "hive.internal",
            "database_name": "warehouse",
        }
    )

    assert "port" not in request.datasource.model_fields_set
    assert request.resolve_training_config()["data"]["hive_port"] == 10000


def test_hive_preserves_explicit_protocol_default_port() -> None:
    request = _request(
        {
            "type": "HIVE",
            "host": "hive.internal",
            "port": 9000,
            "database_name": "warehouse",
        }
    )

    assert "port" in request.datasource.model_fields_set
    assert request.resolve_training_config()["data"]["hive_port"] == 9000


def test_clickhouse_maps_sort_key_parallelism_and_integer_port() -> None:
    request = _request(
        {
            "type": "CLICKHOUSE",
            "host": "clickhouse.internal",
            "port": 8123,
            "database_name": "analytics",
            "properties": {"sort_key": "event_id", "parallelism": "3"},
        }
    )

    config = request.resolve_training_config()

    assert config["data"]["ch_port"] == 8123
    assert config["data"]["ch_sort_key"] == "event_id"
    assert config["data"]["ch_parallelism"] == 3
    validated = XGBoostTrainingConfig.model_validate(config).model_dump()
    assert validated["data"]["ch_sort_key"] == "event_id"
    assert validated["data"]["ch_parallelism"] == 3


@pytest.mark.parametrize(
    "sort_key",
    ["event.id", "event-id", "event id", "event_id; DROP TABLE samples", 42],
)
def test_clickhouse_rejects_non_identifier_sort_key_with_exact_path(
    sort_key: object,
) -> None:
    request = _request(
        {
            "type": "CLICKHOUSE",
            "host": "clickhouse.internal",
            "port": 8123,
            "database_name": "analytics",
            "properties": {"sort_key": sort_key},
        }
    )

    with pytest.raises(ValueError, match=r"datasource\.properties\.sort_key"):
        request.resolve_training_config()


@pytest.mark.parametrize(
    "hash_column",
    ["subscriber.id", "subscriber-id", "subscriber id", "id) OR 1=1", 42],
)
def test_hive_rejects_non_identifier_hash_column_with_exact_path(
    hash_column: object,
) -> None:
    request = _request(
        {
            "type": "HIVE",
            "host": "hive.internal",
            "database_name": "warehouse",
            "properties": {"hash_column": hash_column},
        }
    )

    with pytest.raises(ValueError, match=r"datasource\.properties\.hash_column"):
        request.resolve_training_config()


@pytest.mark.parametrize("shard_mode", ["random", "HASH", "hash; DROP", 42])
def test_hive_rejects_invalid_shard_mode_with_exact_path(
    shard_mode: object,
) -> None:
    request = _request(
        {
            "type": "HIVE",
            "host": "hive.internal",
            "database_name": "warehouse",
            "properties": {"shard_mode": shard_mode},
        }
    )

    with pytest.raises(ValueError, match=r"datasource\.properties\.shard_mode"):
        request.resolve_training_config()


@pytest.mark.parametrize("shard_mode", ["auto", "hash", "offset"])
def test_hive_accepts_supported_shard_modes(shard_mode: str) -> None:
    config = _request(
        {
            "type": "HIVE",
            "host": "hive.internal",
            "database_name": "warehouse",
            "properties": {"shard_mode": shard_mode},
        }
    ).resolve_training_config()

    assert config["data"]["hive_shard_mode"] == shard_mode


@pytest.mark.parametrize("port", [0, 65536])
def test_database_port_range_error_keeps_datasource_path(port: int) -> None:
    request = _request(
        {
            "type": "CLICKHOUSE",
            "host": "clickhouse.internal",
            "port": port,
            "database_name": "analytics",
        }
    )
    with pytest.raises(ValueError, match=r"datasource\.port"):
        request.resolve_training_config()


@pytest.mark.parametrize(
    ("datasource", "sql", "path"),
    [
        (
            {"type": "HIVE", "port": 10000, "database_name": "warehouse"},
            "SELECT x, label FROM samples",
            "datasource.host",
        ),
        (
            {"type": "HIVE", "host": "hive", "port": 10000},
            "SELECT x, label FROM samples",
            "datasource.database_name",
        ),
        (
            {
                "type": "HIVE",
                "host": "hive",
                "port": 10000,
                "database_name": "warehouse",
            },
            "",
            "data_query.query.sql",
        ),
    ],
)
def test_hive_missing_required_fields_have_canonical_paths(
    datasource: dict[str, Any],
    sql: str,
    path: str,
) -> None:
    with pytest.raises(ValueError, match=path):
        _request(datasource, sql=sql).resolve_training_config()


@pytest.mark.parametrize(
    ("datasource_type", "property_name", "value"),
    [
        ("CLICKHOUSE", "parallelism", "many"),
        ("HIVE", "batch_size", "large"),
        ("HIVE", "hash_shards", 0),
        ("HIVE", "parallelism", 0),
    ],
)
def test_invalid_integer_properties_report_exact_field_path(
    datasource_type: str,
    property_name: str,
    value: object,
) -> None:
    datasource = {
        "type": datasource_type,
        "host": "database.internal",
        "port": 10000 if datasource_type == "HIVE" else 8123,
        "database_name": "analytics",
        "properties": {property_name: value},
    }
    with pytest.raises(
        ValueError,
        match=rf"datasource\.properties\.{property_name}",
    ):
        _request(datasource).resolve_training_config()


@pytest.mark.parametrize(
    ("datasource", "expected"),
    [
        (
            {
                "type": "S3",
                "properties": {
                    "uri": "s3://bucket/train.parquet",
                    "format": "parquet",
                    "region": "us-east-1",
                },
            },
            {
                "type": "s3",
                "uri": "s3://bucket/train.parquet",
                "format": "parquet",
                "s3": {"region": "us-east-1"},
            },
        ),
        (
            {
                "type": "LOCAL",
                "properties": {"path": "/data/train.csv", "format": "csv"},
            },
            {"type": "csv", "path": "/data/train.csv", "format": "csv"},
        ),
    ],
)
def test_s3_and_local_mapping_regression(
    datasource: dict[str, Any], expected: dict[str, Any]
) -> None:
    data = _request(datasource).resolve_training_config()["data"]
    for key, value in expected.items():
        assert data[key] == value
