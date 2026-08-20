"""Map KnoVa protocol v2 training requests to Tributo Core configuration."""

from __future__ import annotations

import re
from typing import Any

from tributo_broker_redis.protocol import TrainingJobRequest

_CONTROL_HYPERPARAMS = {
    "early_stopping_rounds",
    "eta",
    "learning_rate",
    "max_failures",
    "n_estimators",
    "num_class",
    "num_rounds",
    "num_workers",
    "objective",
    "seed",
    "use_gpu",
}
_SIMPLE_SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_HIVE_SHARD_MODES = frozenset({"auto", "hash", "offset"})
_SECRET_VALUE_PATTERNS = (
    re.compile(r"-----BEGIN(?: [A-Z0-9]+)* PRIVATE KEY-----", re.IGNORECASE),
    re.compile(r"[a-z][a-z0-9+.-]*://[^/@\s]+@", re.IGNORECASE),
    re.compile(r"\bauthorization\s*[:=]\s*(?:bearer|basic)\s+\S+", re.IGNORECASE),
    re.compile(r"\bbearer\s+[A-Za-z0-9._~+/=-]+", re.IGNORECASE),
    re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
)


def _required(value: Any, name: str) -> Any:
    if value is None or value == "" or value == []:
        raise ValueError(f"{name} is required for canonical training requests")
    return value


def _integer(
    value: Any,
    name: str,
    *,
    minimum: int = 1,
    maximum: int | None = None,
    allow_minus_one: bool = False,
) -> int:
    """Parse a protocol integer while retaining its canonical field path."""
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if isinstance(value, float) and not value.is_integer():
        raise ValueError(f"{name} must be an integer")
    if isinstance(value, str) and value.strip() != str(parsed):
        raise ValueError(f"{name} must be an integer")
    if allow_minus_one and parsed == -1:
        return parsed
    if parsed < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    if maximum is not None and parsed > maximum:
        raise ValueError(f"{name} must be at most {maximum}")
    return parsed


def _port(value: Any) -> int:
    return _integer(value, "datasource.port", maximum=65535)


def _sql_identifier(value: Any, name: str) -> str:
    """Validate unquoted, single-part SQL identifiers at the protocol boundary."""
    if not isinstance(value, str) or not _SIMPLE_SQL_IDENTIFIER.fullmatch(value):
        raise ValueError(f"{name} must be a simple SQL identifier")
    return value


def _hive_shard_mode(value: Any) -> str:
    name = "datasource.properties.shard_mode"
    if not isinstance(value, str) or value not in _HIVE_SHARD_MODES:
        allowed = ", ".join(sorted(_HIVE_SHARD_MODES))
        raise ValueError(f"{name} must be one of: {allowed}")
    return value


def _optional_integer(
    properties: dict[str, Any],
    key: str,
    *,
    minimum: int = 1,
    allow_minus_one: bool = False,
) -> int | None:
    value = properties.get(key)
    if value is None:
        return None
    return _integer(
        value,
        f"datasource.properties.{key}",
        minimum=minimum,
        allow_minus_one=allow_minus_one,
    )


def _data_config(request: TrainingJobRequest) -> dict[str, Any]:
    datasource = request.datasource
    data_type = datasource.type.upper()
    if data_type == "S3":
        properties = datasource.properties
        uri = _required(properties.get("uri"), "datasource.properties.uri")
        s3 = {
            key: properties[key]
            for key in (
                "region",
                "access_key_id",
                "secret_access_key",
                "endpoint",
            )
            if properties.get(key)
        }
        data: dict[str, Any] = {
            "type": "s3",
            "uri": uri,
            "format": properties.get("format", "parquet"),
            "s3": s3,
        }
    elif data_type == "CLICKHOUSE":
        properties = datasource.properties
        query = request.data_query.query if request.data_query else None
        data = {
            "type": "clickhouse",
            "ch_host": _required(datasource.host, "datasource.host"),
            "ch_port": _port(datasource.port),
            "ch_database": _required(
                datasource.database_name, "datasource.database_name"
            ),
            "ch_user": datasource.username or "default",
            "ch_password": datasource.password or "",
            "ch_sql": _required(query.sql if query else None, "data_query.query.sql"),
            "ch_sql_params": query.params if query else {},
        }
        sort_key = properties.get("sort_key")
        if sort_key is not None:
            data["ch_sort_key"] = _sql_identifier(
                sort_key,
                "datasource.properties.sort_key",
            )
        parallelism = _optional_integer(
            properties,
            "parallelism",
            allow_minus_one=True,
        )
        if parallelism is not None:
            data["ch_parallelism"] = parallelism
    elif data_type == "HIVE":
        properties = datasource.properties
        query = request.data_query.query if request.data_query else None
        data = {
            "type": "hive",
            "hive_host": _required(datasource.host, "datasource.host"),
            "hive_port": _port(
                datasource.port if "port" in datasource.model_fields_set else 10000
            ),
            "hive_database": _required(
                datasource.database_name, "datasource.database_name"
            ),
            "hive_user": datasource.username or "default",
            "hive_password": datasource.password or "",
            "hive_sql": _required(query.sql if query else None, "data_query.query.sql"),
            "hive_sql_params": query.params if query else {},
            "hive_hash_shards": _optional_integer(properties, "hash_shards") or 64,
        }
        optional_integer_fields = {
            "batch_size": "hive_batch_size",
            "parallelism": "hive_parallelism",
        }
        for property_name, core_name in optional_integer_fields.items():
            value = _optional_integer(
                properties,
                property_name,
                allow_minus_one=property_name == "parallelism",
            )
            if value is not None:
                data[core_name] = value
        shard_mode = properties.get("shard_mode")
        if shard_mode is not None:
            data["hive_shard_mode"] = _hive_shard_mode(shard_mode)
        hash_column = properties.get("hash_column")
        if hash_column is not None:
            data["hive_hash_column"] = _sql_identifier(
                hash_column,
                "datasource.properties.hash_column",
            )
    elif data_type in {"CSV", "LOCAL"}:
        properties = datasource.properties
        data = {
            "type": "csv",
            "path": _required(properties.get("path"), "datasource.properties.path"),
            "format": properties.get("format", "csv"),
        }
    else:
        raise ValueError(f"Unsupported canonical datasource type: {datasource.type!r}")

    if request.target is None:
        raise ValueError("target is required for canonical training requests")
    feature_columns = [feature.result_column for feature in request.features]
    _required(feature_columns, "features")
    data["label_col"] = request.target.result_column
    data["feature_columns"] = feature_columns
    data["feature_id_map"] = {
        feature.result_column: feature.feature_id
        for feature in request.features
        if feature.feature_id
    }
    entity_key = request.data_query.entity_key if request.data_query else None
    if entity_key is not None:
        data["entity_key_column"] = entity_key.result_column
    balance = request.data_sampling.class_balance if request.data_sampling else None
    if balance is not None and balance.strategy.upper() == "CUSTOM_AMOUNT":
        data["class_balance_strategy"] = "CUSTOM_AMOUNT"
        data["class_amounts"] = dict(balance.class_amounts or {})
    return data


def _objective(
    request: TrainingJobRequest,
    hyper_params: dict[str, Any],
) -> tuple[str, int | None]:
    if request.target is None:
        raise ValueError("target is required for canonical training requests")
    task_type = request.target.task_type.upper()
    if task_type == "BINARY_CLASSIFICATION":
        return str(hyper_params.get("objective", "binary:logistic")), None
    if task_type == "REGRESSION":
        return str(hyper_params.get("objective", "reg:squarederror")), None
    if task_type == "MULTICLASS_CLASSIFICATION":
        num_class = hyper_params.get("num_class")
        if num_class is None and request.target.label_mapping:
            num_class = len(request.target.label_mapping)
        if num_class is None:
            raise ValueError(
                "MULTICLASS_CLASSIFICATION requires num_class or label_mapping"
            )
        return str(hyper_params.get("objective", "multi:softprob")), int(num_class)
    raise ValueError(f"Unsupported canonical target task_type: {task_type!r}")


def _model_config(request: TrainingJobRequest) -> dict[str, Any]:
    hyper_params = dict(request.algorithm.hyper_params)
    objective, num_class = _objective(request, hyper_params)
    model = {
        key: value
        for key, value in hyper_params.items()
        if key not in _CONTROL_HYPERPARAMS
    }
    model["objective"] = objective
    if num_class is not None:
        model["num_class"] = num_class
    eta = hyper_params.get("eta", hyper_params.get("learning_rate"))
    if eta is not None:
        model["eta"] = eta
    return model


def _training_config(request: TrainingJobRequest) -> dict[str, Any]:
    hyper_params = request.algorithm.hyper_params
    split = request.data_split
    if split.validation_ratio + split.test_ratio >= 1.0:
        raise ValueError("validation_ratio + test_ratio must be less than 1")
    configured_rounds = hyper_params.get("num_rounds", hyper_params.get("n_estimators"))
    config: dict[str, Any] = {
        "num_rounds": (
            configured_rounds
            if configured_rounds is not None
            else min(100, request.resource_limits.max_epochs)
        ),
        "val_size": split.validation_ratio,
        "test_size": split.test_ratio,
        "seed": hyper_params.get(
            "seed", split.random_seed if split.random_seed is not None else 42
        ),
        "split_strategy": split.strategy.upper(),
        "stratify": split.stratify,
    }
    if split.order_column is not None:
        config["order_column"] = split.order_column
    early_stopping = hyper_params.get(
        "early_stopping_rounds",
        request.resource_limits.early_stopping_patience,
    )
    if early_stopping is not None:
        config["early_stopping_rounds"] = early_stopping
    return config


def _ray_config(request: TrainingJobRequest) -> dict[str, Any]:
    hyper_params = request.algorithm.hyper_params
    return {
        "num_workers": hyper_params.get("num_workers", 2),
        "use_gpu": hyper_params.get("use_gpu", False),
        "max_failures": hyper_params.get("max_failures", 0),
        "storage_path": f"{_bundle_uri(request)}/_ray",
    }


def _bundle_uri(request: TrainingJobRequest) -> str:
    """Resolve the canonical storage context to a Core-compatible URI."""
    storage = request.storage_context
    if storage is None:
        raise ValueError("storage_context is required for canonical Bundle publication")
    storage_type = storage.type.upper()
    if storage_type == "S3":
        bucket = _required(storage.bucket, "storage_context.bucket")
        prefix = _required(storage.prefix, "storage_context.prefix").rstrip("/")
        bundle_uri = f"s3://{bucket}/{prefix}"
    elif storage_type in {"FILE", "FILESYSTEM", "LOCAL"}:
        bundle_uri = _required(storage.prefix, "storage_context.prefix").rstrip("/")
    else:
        raise ValueError(
            f"Unsupported canonical storage_context type: {storage.type!r}"
        )
    return bundle_uri


def _output_config(request: TrainingJobRequest) -> dict[str, Any]:
    bundle_uri = _bundle_uri(request)
    return {
        "onnx_path": f"{bundle_uri}/model.onnx",
        "metrics_path": f"{bundle_uri}/metrics.json",
        "onnx_opset": 12,
        "onnx_optional": False,
    }


def _evaluation_config(request: TrainingJobRequest) -> dict[str, Any]:
    artifacts = request.evaluation.artifacts
    return {
        "enabled": request.evaluation.enabled,
        "roc_curve": artifacts.roc_curve,
        "threshold_analysis": artifacts.threshold_analysis,
        "confusion_matrix": artifacts.confusion_matrix,
        "feature_importance": artifacts.feature_importance,
    }


def build_training_config_from_request(
    request: TrainingJobRequest,
) -> dict[str, Any]:
    """Derive a Core XGBoost configuration from canonical KnoVa fields."""
    algorithm_key = request.algorithm.algorithm_key.lower()
    if algorithm_key != "xgboost":
        raise ValueError(
            f"Unsupported canonical algorithm: {request.algorithm.algorithm_key!r}"
        )
    return {
        "data": _data_config(request),
        "model": _model_config(request),
        "training": _training_config(request),
        "ray": _ray_config(request),
        "output": _output_config(request),
        "evaluation": _evaluation_config(request),
    }


def _validate_legacy_config(config: dict[str, Any]) -> None:
    allowed_sections = {"data", "model", "training", "ray", "output", "evaluation"}
    unknown = set(config) - allowed_sections
    if unknown:
        fields = ", ".join(sorted(map(str, unknown)))
        raise ValueError(
            f"training_config cannot override identity/broker/runtime fields: {fields}"
        )

    forbidden_control_keys = {
        "broker",
        "broker_config",
        "execution_context",
        "env_vars",
        "identity",
        "job_id",
        "password_env",
        "run_id",
        "submission_id",
    }
    sensitive_fragments = (
        "apikey",
        "authorization",
        "password",
        "privatekey",
        "secret",
        "token",
    )

    def normalize_key(value: object) -> str:
        return re.sub(r"[^a-z0-9]", "", str(value).lower())

    def walk(value: Any, path: str) -> None:
        if isinstance(value, str) and any(
            pattern.search(value) for pattern in _SECRET_VALUE_PATTERNS
        ):
            raise ValueError(
                f"training_config cannot contain inline secret value {path}"
            )
        if isinstance(value, (list, tuple)):
            for index, child in enumerate(value):
                walk(child, f"{path}.{index}" if path else str(index))
            return
        if not isinstance(value, dict):
            return
        for raw_key, child in value.items():
            key = normalize_key(raw_key)
            child_path = f"{path}.{raw_key}" if path else str(raw_key)
            if key.startswith("tributo") or key in {
                normalize_key(field) for field in forbidden_control_keys
            }:
                raise ValueError(
                    "training_config cannot override identity/broker/runtime field "
                    f"{child_path}"
                )
            if (
                any(fragment in key for fragment in sensitive_fragments)
                and child not in (None, "")
                and not key.endswith(("env", "envvar", "ref"))
            ):
                raise ValueError(
                    f"training_config cannot contain inline secret field {child_path}"
                )
            walk(child, child_path)

    walk(config, "")


def resolve_training_config(
    request: TrainingJobRequest,
    *,
    allow_legacy_training_config: bool = False,
) -> dict[str, Any]:
    """Build canonical config; legacy passthrough is opt-in and constrained."""
    if request.training_config is not None:
        if not allow_legacy_training_config:
            raise ValueError(
                "training_config is legacy and disabled; set "
                "allow_legacy_training_config=true in Provider config to allow it"
            )
        if not request.training_config:
            raise ValueError("training_config must be a non-empty object")
        config = dict(request.training_config)
        _validate_legacy_config(config)
    else:
        # Keep direct callers as safe as the Redis runtime: canonical requests
        # can never bypass capability or credential gates.
        from tributo_broker_redis.capabilities import validate_supported_capabilities

        validate_supported_capabilities(request)
        config = build_training_config_from_request(request)

    from tributo.training.xgboost_trainer import XGBoostTrainingConfig

    try:
        XGBoostTrainingConfig.model_validate(config)
    except Exception as exc:
        raise ValueError(f"Invalid Tributo XGBoost training config: {exc}") from exc
    return config
