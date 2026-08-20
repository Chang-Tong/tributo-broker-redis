"""Unit tests for Redis provider protocol/config boundaries."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import ValidationError
from redis.cluster import ClusterNode

from tributo_broker_redis.config import RedisBrokerConfig
from tributo_broker_redis.protocol import (
    MAX_JOB_ID_LENGTH,
    MAX_TERMINAL_DURATION_SECONDS,
    MIN_TERMINAL_EVENT_BYTES,
    TrainingJobRequest,
    check_protocol_version,
    event_payload,
    is_training_task,
    quantize_duration_seconds,
)
from tributo_broker_redis.redis_client import create_redis_client


def test_event_size_limit_cannot_disable_emergency_terminal() -> None:
    with pytest.raises(
        ValueError, match=f"greater than or equal to {MIN_TERMINAL_EVENT_BYTES}"
    ):
        RedisBrokerConfig(max_event_bytes=MIN_TERMINAL_EVENT_BYTES - 1)


def test_job_id_limit_is_shared_by_protocol_and_terminal_budget() -> None:
    maximum = "j" * MAX_JOB_ID_LENGTH
    TrainingJobRequest(job_id=maximum, training_config={"data": {}})
    with pytest.raises(ValidationError, match="job_id"):
        TrainingJobRequest(job_id=f"{maximum}j", training_config={"data": {}})
    with pytest.raises(ValidationError, match="job_id"):
        TrainingJobRequest(job_id="unsafe/job", training_config={"data": {}})

    failed = event_payload(
        job_id=maximum,
        event_type="FAILED",
        payload={
            "timestamp": 99_999_999_999_999,
            "phase": "EVALUATING",
            "error_code": "PAYLOAD_TOO_LARGE",
            "error_message": "COMPLETED event exceeded the configured size limit",
            "duration_seconds": quantize_duration_seconds(
                MAX_TERMINAL_DURATION_SECONDS
            ),
        },
    )
    cancelled = event_payload(
        job_id=maximum,
        event_type="CANCELLED",
        payload={
            "timestamp": 99_999_999_999_999,
            "phase": "EVALUATING",
            "duration_seconds": quantize_duration_seconds(
                MAX_TERMINAL_DURATION_SECONDS
            ),
            "has_best_model": False,
        },
    )
    encoded_sizes = [
        len(json.dumps(event, separators=(",", ":")).encode("utf-8"))
        for event in (failed, cancelled)
    ]
    assert max(encoded_sizes) == MIN_TERMINAL_EVENT_BYTES
    assert RedisBrokerConfig(max_event_bytes=MIN_TERMINAL_EVENT_BYTES)


def test_protocol_version_and_training_scope() -> None:
    assert check_protocol_version({"protocol_version": "2.3"}) is None
    assert check_protocol_version({"protocol_version": "1.9"})
    assert is_training_task({}) is True
    assert is_training_task({"task_type": "INFERENCE"}) is False
    assert is_training_task({"job_type": "TRAINING", "task_type": "INFERENCE"}) is False
    with pytest.raises(ValidationError, match="job_type/task_type conflict"):
        TrainingJobRequest.model_validate(
            {
                "job_id": "job-1",
                "job_type": "TRAINING",
                "task_type": "INFERENCE",
                "training_config": {"data": {}},
            }
        )


def test_training_request_rejects_legacy_config_by_default() -> None:
    request = TrainingJobRequest(job_id="job-1", training_config={"data": {}})
    with pytest.raises(ValueError, match="legacy and disabled"):
        request.resolve_training_config()
    assert request.resolve_training_config(allow_legacy_training_config=True) == {
        "data": {}
    }
    with pytest.raises(ValueError, match="non-empty"):
        TrainingJobRequest(job_id="job-1", training_config={}).resolve_training_config(
            allow_legacy_training_config=True
        )


def test_protocol_models_forbid_unknown_fields() -> None:
    with pytest.raises(ValidationError, match="extra_forbidden"):
        TrainingJobRequest.model_validate({"job_id": "job-1", "surprise": True})
    with pytest.raises(ValidationError, match="extra_forbidden"):
        TrainingJobRequest.model_validate(
            {
                "job_id": "job-1",
                "algorithm": {"algorithm_key": "xgboost", "surprise": 1},
            }
        )


@pytest.mark.parametrize("field", ["model_id", "version_id", "tenant_id"])
def test_canonical_protocol_requires_non_empty_ownership_identity(field: str) -> None:
    payload = {
        "job_id": "job-1",
        "model_id": "model-1",
        "version_id": "version-1",
        "tenant_id": "tenant-1",
        "features": [{"feature_id": "f1", "result_column": "x"}],
    }
    payload[field] = ""
    with pytest.raises(ValidationError, match=field):
        TrainingJobRequest.model_validate(payload)


def test_canonical_protocol_requires_feature_ids() -> None:
    with pytest.raises(ValidationError, match=r"features\.0\.feature_id"):
        TrainingJobRequest.model_validate(
            {
                "job_id": "job-1",
                "model_id": "model-1",
                "version_id": "version-1",
                "tenant_id": "tenant-1",
                "features": [{"result_column": "x"}],
            }
        )


@pytest.mark.parametrize(
    ("path", "payload"),
    [
        ("algorithm.is_deep_learning", {"algorithm": {"is_deep_learning": "false"}}),
        ("evaluation.enabled", {"evaluation": {"enabled": "false"}}),
        ("resource_limits.max_epochs", {"resource_limits": {"max_epochs": "10"}}),
    ],
)
def test_protocol_models_are_strict_and_never_coerce_wire_scalars(
    path: str, payload: dict[str, object]
) -> None:
    with pytest.raises(ValidationError) as captured:
        TrainingJobRequest.model_validate({"job_id": "job-1", **payload})

    assert ".".join(str(part) for part in captured.value.errors()[0]["loc"]) == path


def test_legacy_config_cannot_override_runtime_identity_or_broker_fields() -> None:
    request = TrainingJobRequest(
        job_id="job-1",
        training_config={"execution_context": {"job_id": "other"}},
    )
    with pytest.raises(ValueError, match="cannot override.*execution_context"):
        request.resolve_training_config(allow_legacy_training_config=True)

    secret_request = TrainingJobRequest(
        job_id="job-1",
        training_config={"data": {"s3": {"secret_access_key": "inline"}}},
    )
    with pytest.raises(
        ValueError, match=r"inline secret field data\.s3\.secret_access_key"
    ):
        secret_request.resolve_training_config(allow_legacy_training_config=True)


@pytest.mark.parametrize("container", [list, tuple])
def test_legacy_secret_walk_rejects_nested_sequences_and_camel_case(
    container: type[list[object]] | type[tuple[object, ...]],
) -> None:
    nested = container([{"accessToken": "inline"}, {"privateKey": "inline"}])
    request = TrainingJobRequest(
        job_id="job-1",
        training_config={"data": {"items": nested}},
    )

    with pytest.raises(
        ValueError, match=r"inline secret field data\.items\.0\.accessToken"
    ):
        request.resolve_training_config(allow_legacy_training_config=True)


@pytest.mark.parametrize(
    "secret_value",
    [
        "-----BEGIN PRIVATE KEY-----\nopaque\n-----END PRIVATE KEY-----",
        "redis://user:password@redis.internal:6379/0",
        "Authorization: Bearer opaque-token",
        "Bearer opaque-token",
        "AKIAIOSFODNN7EXAMPLE",
    ],
)
def test_legacy_secret_walk_rejects_secret_values_under_innocent_keys(
    secret_value: str,
) -> None:
    request = TrainingJobRequest(
        job_id="job-1",
        training_config={"data": {"metadata": [{"value": secret_value}]}},
    )

    with pytest.raises(
        ValueError, match=r"inline secret value data\.metadata\.0\.value"
    ):
        request.resolve_training_config(allow_legacy_training_config=True)


@pytest.mark.parametrize("auth", ["NONE", "NOSASL", "none", "nosasl"])
@pytest.mark.parametrize("field", ["auth", "hive_auth"])
def test_legacy_hive_passwordless_auth_is_executed(auth: str, field: str) -> None:
    request = TrainingJobRequest(
        job_id="job-1",
        training_config={
            "data": {
                "type": "hive",
                "hive_sql": "SELECT 1",
                field: auth,
            }
        },
    )

    config = request.resolve_training_config(allow_legacy_training_config=True)

    assert config["data"][field] == auth.upper()


@pytest.mark.parametrize("auth", ["LDAP", "CUSTOM", "KERBEROS", 7])
@pytest.mark.parametrize("field", ["auth", "hive_auth"])
def test_legacy_hive_rejects_unexecutable_auth_with_exact_path(
    auth: object, field: str
) -> None:
    request = TrainingJobRequest(
        job_id="job-1",
        training_config={
            "data": {
                "type": "hive",
                "hive_sql": "SELECT 1",
                field: auth,
            }
        },
    )

    with pytest.raises(ValueError, match=rf"training_config\.data\.{field}"):
        request.resolve_training_config(allow_legacy_training_config=True)


def test_canonical_clickhouse_request_maps_to_xgboost_bundle_config() -> None:
    request = TrainingJobRequest.model_validate(
        {
            "protocol_version": "2.0",
            "job_id": "train-job-1",
            "model_id": "model-1",
            "version_id": "version-1",
            "tenant_id": "tenant-1",
            "algorithm": {
                "algorithm_key": "xgboost",
                "hyper_params": {
                    "max_depth": 6,
                    "learning_rate": 0.08,
                    "n_estimators": 100,
                    "num_workers": 1,
                },
            },
            "datasource": {
                "type": "CLICKHOUSE",
                "host": "analytics.internal",
                "port": 9000,
                "database_name": "analytics",
                "username": "reader",
            },
            "data_query": {
                "query": {
                    "sql": "SELECT x1, x2, label FROM samples WHERE ts > {p0:String}",
                    "params": {"p0": "2026-01-01"},
                }
            },
            "features": [
                {"feature_id": "f1", "result_column": "x1"},
                {"feature_id": "f2", "result_column": "x2"},
            ],
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
                "train_ratio": 0.7,
                "validation_ratio": 0.1,
                "test_ratio": 0.2,
                "random_seed": 7,
            },
            "resource_limits": {"early_stopping_patience": 9},
            "storage_context": {
                "type": "s3",
                "bucket": "knova-models",
                "prefix": "tenant/model/version/",
            },
        }
    )

    config = request.resolve_training_config()

    assert config["data"]["type"] == "clickhouse"
    assert config["data"]["feature_columns"] == ["x1", "x2"]
    assert config["data"]["feature_id_map"] == {"x1": "f1", "x2": "f2"}
    assert config["model"]["eta"] == 0.08
    assert "learning_rate" not in config["model"]
    assert config["training"] == {
        "num_rounds": 100,
        "val_size": 0.1,
        "test_size": 0.2,
        "seed": 7,
        "early_stopping_rounds": 9,
        "split_strategy": "RANDOM",
        "stratify": False,
    }
    assert config["ray"]["storage_path"] == (
        "s3://knova-models/tenant/model/version/_ray"
    )
    assert config["output"] == {
        "onnx_path": "s3://knova-models/tenant/model/version/model.onnx",
        "metrics_path": "s3://knova-models/tenant/model/version/metrics.json",
        "onnx_opset": 12,
        "onnx_optional": False,
    }


def test_canonical_request_requires_fields_needed_before_ray_submission() -> None:
    with pytest.raises(ValueError, match="datasource.host"):
        TrainingJobRequest.model_validate(
            {
                "job_id": "job-1",
                "model_id": "model-1",
                "version_id": "version-1",
                "tenant_id": "tenant-1",
                "features": [{"feature_id": "f1", "result_column": "x1"}],
                "target": {"result_column": "label"},
                "feature_engineering": {
                    "default_missing_value_strategy": "NONE",
                    "default_outlier_strategy": "NONE",
                    "default_scaling_method": "NONE",
                    "default_encoding_method": "NONE",
                },
                "storage_context": {
                    "type": "local",
                    "prefix": "/tmp/bundles/",
                },
            }
        ).resolve_training_config()


def test_redis_config_supports_modes_and_rejects_missing_topology() -> None:
    first = RedisBrokerConfig()
    second = RedisBrokerConfig()
    assert first.mode == "standalone"
    assert first.consumer_name != second.consumer_name
    assert first.group_start_id == "$"
    assert (
        RedisBrokerConfig(
            mode="sentinel", sentinel_hosts=[("redis-sentinel", 26379)]
        ).mode
        == "sentinel"
    )
    with pytest.raises(ValidationError, match="cluster_startup_nodes"):
        RedisBrokerConfig(mode="cluster")
    with pytest.raises(ValidationError, match="requires db=0"):
        RedisBrokerConfig(
            mode="cluster",
            db=1,
            cluster_startup_nodes=[("redis-cluster", 6379)],
        )


def test_provider_limits_and_worker_specific_secret_reference() -> None:
    config = RedisBrokerConfig(
        max_payload_bytes=128,
        max_event_bytes=512,
        claim_count=25,
        worker_password_env="WORKER_REDIS_PASSWORD",
        extra_py_modules=["/provider/tributo_broker_redis"],
    )
    assert config.claim_count == 25
    assert config.worker_password_env == "WORKER_REDIS_PASSWORD"
    assert config.extra_py_modules == ["/provider/tributo_broker_redis"]


def test_redis_dependency_floor_supports_sentinel_force_master_ip() -> None:
    pyproject = Path(__file__).parents[2] / "pyproject.toml"
    with pyproject.open("rb") as stream:
        dependencies = tomllib.load(stream)["project"]["dependencies"]
    assert "redis>=6.0,<8.0" in dependencies


def test_password_is_resolved_by_env_reference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TEST_REDIS_PASSWORD", "not-in-config")
    config = RedisBrokerConfig(password_env="TEST_REDIS_PASSWORD")
    assert config.password() == "not-in-config"
    assert "not-in-config" not in config.model_dump_json()


def test_redis_url_rejects_embedded_credentials() -> None:
    with pytest.raises(ValidationError, match="must not contain credentials"):
        RedisBrokerConfig(url="redis://:embedded-secret@redis.example:6379")
    with pytest.raises(ValidationError, match="must not contain credentials"):
        RedisBrokerConfig(worker_url="redis://:embedded-secret@redis.example:6379")


def test_url_client_does_not_pass_false_ssl_to_redis_py() -> None:
    config = RedisBrokerConfig(url="redis://redis.example:6379")
    with patch("redis.Redis.from_url") as from_url:
        create_redis_client(config)
    assert from_url.call_args.kwargs == {"decode_responses": True}


def test_standalone_sentinel_and_cluster_clients_are_provider_owned() -> None:
    standalone = RedisBrokerConfig(host="redis", port=6380)
    sentinel = RedisBrokerConfig(mode="sentinel", sentinel_hosts=[("sentinel", 26379)])
    cluster = RedisBrokerConfig(
        mode="cluster",
        cluster_startup_nodes=[("node", 6379)],
        cluster_address_remap_host="127.0.0.1",
    )
    with (
        patch("redis.Redis") as redis_cls,
        patch("redis.Sentinel") as sentinel_cls,
        patch("redis.RedisCluster") as cluster_cls,
    ):
        create_redis_client(standalone)
        create_redis_client(sentinel)
        create_redis_client(cluster)
    redis_cls.assert_called_once_with(
        host="redis", port=6380, db=0, decode_responses=True
    )
    sentinel_cls.assert_called_once_with(
        [("sentinel", 26379)],
        decode_responses=True,
    )
    sentinel_cls.return_value.master_for.assert_called_once_with("mymaster", db=0)
    cluster_cls.assert_called_once_with(
        startup_nodes=[ClusterNode("node", 6379)],
        address_remap=cluster_cls.call_args.kwargs["address_remap"],
        decode_responses=True,
    )
    address_remap = cluster_cls.call_args.kwargs["address_remap"]
    assert address_remap(("10.0.0.2", 7001)) == ("127.0.0.1", 7001)


def test_sentinel_address_map_is_passed_to_provider_client() -> None:
    config = RedisBrokerConfig(
        mode="sentinel",
        sentinel_hosts=[("sentinel", 26379)],
        sentinel_address_map={"10.0.0.2:6379": ("127.0.0.1", 16381)},
    )
    with patch(
        "tributo_broker_redis.redis_client._AddressRemappingSentinel"
    ) as sentinel_cls:
        create_redis_client(config)
    sentinel_cls.assert_called_once_with(
        [("sentinel", 26379)],
        address_map={"10.0.0.2:6379": ("127.0.0.1", 16381)},
        decode_responses=True,
    )
