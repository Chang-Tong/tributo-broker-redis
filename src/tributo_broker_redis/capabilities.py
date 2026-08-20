"""Fail-closed execution capability gate for canonical training requests."""

from __future__ import annotations

import math
import re
from typing import NoReturn

from tributo_broker_redis.protocol import TrainingJobRequest


class UnsupportedCapability(ValueError):
    """A valid protocol feature that this Provider cannot execute yet."""

    def __init__(self, field: str, detail: str) -> None:
        self.field = field
        self.detail = detail
        super().__init__(f"{field}: {detail}")


def _unsupported(field: str, detail: str) -> NoReturn:
    raise UnsupportedCapability(field, detail)


_DATASOURCE_PROPERTIES = {
    "S3": frozenset({"uri", "format", "region", "endpoint"}),
    "LOCAL": frozenset({"path", "format"}),
    "CLICKHOUSE": frozenset({"sort_key", "parallelism"}),
    "HIVE": frozenset(
        {"batch_size", "parallelism", "shard_mode", "hash_column", "hash_shards"}
    ),
}
_TASK_METRICS = {
    "BINARY_CLASSIFICATION": frozenset(
        {"auc", "f1", "precision", "recall", "average_precision"}
    ),
    "MULTICLASS_CLASSIFICATION": frozenset({"auc", "f1", "precision", "recall"}),
    "REGRESSION": frozenset({"rmse", "mae", "r2"}),
}
_ROUND_METRICS = {
    "BINARY_CLASSIFICATION": frozenset({"logloss", "error", "auc", "aucpr"}),
    "MULTICLASS_CLASSIFICATION": frozenset({"mlogloss", "merror"}),
    "REGRESSION": frozenset({"rmse", "mae"}),
}
_HYPERPARAMETERS = frozenset(
    {
        "objective",
        "eval_metric",
        "num_class",
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
        "min_child_weight",
        "subsample",
        "colsample_bytree",
        "gamma",
        "reg_alpha",
        "reg_lambda",
        "tree_method",
        "max_bin",
        "grow_policy",
        "booster",
        "scale_pos_weight",
    }
)
_URI_USERINFO = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*://[^/@\s]+@")
_NUMERIC_COLUMN_TYPES = frozenset(
    {
        "byte",
        "short",
        "int",
        "int8",
        "int16",
        "int32",
        "int64",
        "integer",
        "tinyint",
        "smallint",
        "bigint",
        "long",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "float",
        "float16",
        "float32",
        "float64",
        "double",
        "real",
        "decimal",
        "numeric",
        "number",
    }
)
_BOOLEAN_COLUMN_TYPES = frozenset({"bool", "boolean"})
_DECIMAL_TYPE = re.compile(
    r"decimal\s*\(\s*(?P<precision>\d+)\s*,\s*(?P<scale>\d+)\s*\)",
    re.IGNORECASE,
)
_CLICKHOUSE_DECIMAL_TYPE = re.compile(
    r"decimal\s*(?P<bits>32|64|128|256)\s*\(\s*(?P<scale>\d+)\s*\)",
    re.IGNORECASE,
)
_NULLABLE_TYPE = re.compile(
    r"nullable\s*\(\s*(?P<inner>.+)\s*\)",
    re.IGNORECASE,
)
_CLICKHOUSE_DECIMAL_PRECISION = {32: 9, 64: 18, 128: 38, 256: 76}


def _is_supported_column_type(value: str, *, allow_boolean: bool) -> bool:
    """Return whether a native type can pass through to XGBoost unchanged."""

    normalized = value.strip()
    lowered = normalized.lower()
    if lowered in _NUMERIC_COLUMN_TYPES:
        return True
    if allow_boolean and lowered in _BOOLEAN_COLUMN_TYPES:
        return True

    nullable = _NULLABLE_TYPE.fullmatch(normalized)
    if nullable is not None:
        inner = nullable.group("inner").strip()
        # ClickHouse does not permit nested Nullable types. Keeping the parser
        # equally strict also prevents malformed wrappers from being accepted.
        if _NULLABLE_TYPE.fullmatch(inner) is not None:
            return False
        return _is_supported_column_type(inner, allow_boolean=allow_boolean)

    decimal_type = _DECIMAL_TYPE.fullmatch(normalized)
    if decimal_type is not None:
        precision = int(decimal_type.group("precision"))
        scale = int(decimal_type.group("scale"))
        return 1 <= precision <= 76 and 0 <= scale <= precision

    clickhouse_decimal = _CLICKHOUSE_DECIMAL_TYPE.fullmatch(normalized)
    if clickhouse_decimal is not None:
        bits = int(clickhouse_decimal.group("bits"))
        scale = int(clickhouse_decimal.group("scale"))
        return 0 <= scale <= _CLICKHOUSE_DECIMAL_PRECISION[bits]

    return False


def _validate_control_hyperparameters(request: TrainingJobRequest) -> None:
    values = request.algorithm.hyper_params
    task_type = request.target.task_type.upper() if request.target else ""

    for field in values:
        if field not in _HYPERPARAMETERS:
            _unsupported(
                f"algorithm.hyper_params.{field}",
                "is not in the supported XGBoost parameter allowlist",
            )
        if values[field] is None:
            _unsupported(
                f"algorithm.hyper_params.{field}",
                "must not be null when explicitly provided",
            )

    objective = values.get("objective")
    allowed_objectives = {
        "BINARY_CLASSIFICATION": {"binary:logistic"},
        "MULTICLASS_CLASSIFICATION": {"multi:softprob", "multi:softmax"},
        "REGRESSION": {"reg:squarederror"},
    }[task_type]
    if objective is not None and objective not in allowed_objectives:
        _unsupported(
            "algorithm.hyper_params.objective",
            f"supported values for {task_type} are {sorted(allowed_objectives)}",
        )

    num_class = values.get("num_class")
    if task_type == "MULTICLASS_CLASSIFICATION":
        if (
            not isinstance(num_class, int)
            or isinstance(num_class, bool)
            or num_class < 2
        ):
            _unsupported(
                "algorithm.hyper_params.num_class",
                "MULTICLASS_CLASSIFICATION requires an integer >= 2",
            )
    elif num_class is not None:
        _unsupported(
            "algorithm.hyper_params.num_class",
            "is only valid for MULTICLASS_CLASSIFICATION",
        )

    metric_value = values.get("eval_metric")
    metrics = metric_value if isinstance(metric_value, list) else [metric_value]
    for index, metric in enumerate(metrics):
        if metric is None:
            continue
        suffix = f".{index}" if isinstance(metric_value, list) else ""
        if not isinstance(metric, str) or metric not in _ROUND_METRICS[task_type]:
            _unsupported(
                f"algorithm.hyper_params.eval_metric{suffix}",
                f"is not a supported process metric for {task_type}",
            )

    for field, minimum in {
        "num_rounds": 1,
        "n_estimators": 1,
        "num_workers": 1,
        "early_stopping_rounds": 1,
        "max_failures": -1,
    }.items():
        value = values.get(field)
        if value is not None and (
            not isinstance(value, int) or isinstance(value, bool) or value < minimum
        ):
            _unsupported(
                f"algorithm.hyper_params.{field}",
                f"must be an integer >= {minimum}",
            )
    seed = values.get("seed")
    if seed is not None and (not isinstance(seed, int) or isinstance(seed, bool)):
        _unsupported("algorithm.hyper_params.seed", "must be an integer")
    use_gpu = values.get("use_gpu")
    if use_gpu is not None and not isinstance(use_gpu, bool):
        _unsupported("algorithm.hyper_params.use_gpu", "must be a boolean")
    for field in ("eta", "learning_rate"):
        value = values.get(field)
        if value is not None and (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not 0 < float(value) <= 1
        ):
            _unsupported(
                f"algorithm.hyper_params.{field}",
                "must be a number in (0, 1]",
            )
    if (
        values.get("eta") is not None
        and values.get("learning_rate") is not None
        and values["eta"] != values["learning_rate"]
    ):
        _unsupported(
            "algorithm.hyper_params.eta",
            "conflicts with algorithm.hyper_params.learning_rate",
        )

    def require_number(
        field: str, *, minimum: float, maximum: float | None = None
    ) -> None:
        value = values.get(field)
        if value is None:
            return
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or float(value) < minimum
            or (maximum is not None and float(value) > maximum)
        ):
            suffix = f" and <= {maximum}" if maximum is not None else ""
            _unsupported(
                f"algorithm.hyper_params.{field}",
                f"must be a finite number >= {minimum}{suffix}",
            )

    max_depth = values.get("max_depth")
    if max_depth is not None and (
        not isinstance(max_depth, int)
        or isinstance(max_depth, bool)
        or not 0 <= max_depth <= 64
    ):
        _unsupported(
            "algorithm.hyper_params.max_depth", "must be an integer in [0, 64]"
        )
    max_bin = values.get("max_bin")
    if max_bin is not None and (
        not isinstance(max_bin, int) or isinstance(max_bin, bool) or max_bin < 2
    ):
        _unsupported("algorithm.hyper_params.max_bin", "must be an integer >= 2")
    for field in ("min_child_weight", "gamma", "reg_alpha", "reg_lambda"):
        require_number(field, minimum=0)
    for field in ("subsample", "colsample_bytree"):
        require_number(field, minimum=0.0000001, maximum=1)
    require_number("scale_pos_weight", minimum=0.0000001)
    for field, allowed in {
        "tree_method": {"auto", "exact", "approx", "hist"},
        "grow_policy": {"depthwise", "lossguide"},
        "booster": {"gbtree"},
    }.items():
        value = values.get(field)
        if value is not None and value not in allowed:
            _unsupported(
                f"algorithm.hyper_params.{field}",
                f"supported values are {sorted(allowed)}",
            )


def validate_supported_capabilities(request: TrainingJobRequest) -> None:
    """Reject every understood-but-unimplemented semantic before Ray submit."""
    algorithm = request.algorithm
    if algorithm.algorithm_key.lower() != "xgboost":
        _unsupported("algorithm.algorithm_key", "only xgboost is supported")
    if algorithm.is_deep_learning:
        _unsupported("algorithm.is_deep_learning", "deep learning is not supported")
    if algorithm.category.upper() not in {"CLASSIFICATION", "REGRESSION"}:
        _unsupported(
            "algorithm.category", "only CLASSIFICATION and REGRESSION are supported"
        )

    if request.target is None:
        _unsupported("target", "a supervised target is required")
    if request.target.task_type.upper() not in {
        "BINARY_CLASSIFICATION",
        "MULTICLASS_CLASSIFICATION",
        "REGRESSION",
    }:
        _unsupported(
            "target.task_type",
            "only binary, multiclass, and regression targets are supported",
        )
    category = algorithm.category.upper()
    if category == "CLASSIFICATION" and not request.target.task_type.upper().endswith(
        "CLASSIFICATION"
    ):
        _unsupported("target.task_type", "does not match algorithm.category")
    if category == "REGRESSION" and request.target.task_type.upper() != "REGRESSION":
        _unsupported("target.task_type", "does not match algorithm.category")
    if not _is_supported_column_type(
        request.target.column_type,
        allow_boolean=category == "CLASSIFICATION",
    ):
        _unsupported(
            "target.column_type",
            "must be numeric (or boolean for classification) in passthrough mode",
        )
    _validate_control_hyperparameters(request)

    datasource_type = request.datasource.type.upper()
    if datasource_type not in {"S3", "LOCAL", "CLICKHOUSE", "HIVE"}:
        _unsupported(
            "datasource.type",
            "supported values are S3, LOCAL, CLICKHOUSE, and HIVE",
        )
    datasource = request.datasource
    for field in ("password", "credential_ref", "connection_string"):
        if getattr(datasource, field) not in (None, ""):
            _unsupported(
                f"datasource.{field}",
                "credential resolution is not implemented; "
                "inline credentials are forbidden",
            )
    allowed_properties = _DATASOURCE_PROPERTIES[datasource_type]
    for key in datasource.properties:
        if key not in allowed_properties:
            _unsupported(
                f"datasource.properties.{key}",
                f"is not supported for {datasource_type}",
            )
    if datasource_type in {"S3", "LOCAL"}:
        location_key = "uri" if datasource_type == "S3" else "path"
        location = datasource.properties.get(location_key)
        if not isinstance(location, str) or not location:
            _unsupported(
                f"datasource.properties.{location_key}", "must be a non-empty string"
            )
        if _URI_USERINFO.match(location):
            _unsupported(
                f"datasource.properties.{location_key}",
                "URI userinfo credentials are forbidden",
            )
        data_format = datasource.properties.get(
            "format", "parquet" if datasource_type == "S3" else "csv"
        )
        if not isinstance(data_format, str) or data_format.lower() not in {
            "csv",
            "parquet",
        }:
            _unsupported(
                "datasource.properties.format", "supported values are csv and parquet"
            )
    for field in ("region", "endpoint"):
        value = datasource.properties.get(field)
        if value is not None and (not isinstance(value, str) or not value):
            _unsupported(f"datasource.properties.{field}", "must be a non-empty string")
        if (
            field == "endpoint"
            and isinstance(value, str)
            and _URI_USERINFO.match(value)
        ):
            _unsupported(
                "datasource.properties.endpoint",
                "URI userinfo credentials are forbidden",
            )
    irrelevant_fields = {
        "S3": ("host", "port", "database_name", "username"),
        "LOCAL": ("host", "port", "database_name", "username"),
    }.get(datasource_type, ())
    for field in irrelevant_fields:
        if field in datasource.model_fields_set:
            _unsupported(f"datasource.{field}", f"is not used for {datasource_type}")
    if request.tables:
        _unsupported("tables", "declarative table topology is not implemented")
    if request.relations:
        _unsupported("relations", "table relations are not implemented")

    query = request.data_query
    if query is not None:
        if query.mode.upper() != "DIRECT_QUERY":
            _unsupported("data_query.mode", "TIMESERIES_PIVOT is not implemented")
        for field in (
            "entity_sampling_query",
            "feature_query",
            "target_query",
            "timeseries_pivot",
        ):
            if getattr(query, field) is not None:
                _unsupported(f"data_query.{field}", "is not implemented")
        if query.query is not None:
            if datasource_type not in {"CLICKHOUSE", "HIVE"}:
                _unsupported("data_query.query", f"is not used for {datasource_type}")
            if query.query.resolved_sql is not None:
                _unsupported(
                    "data_query.query.resolved_sql",
                    "pre-resolved SQL is not accepted; use sql and params",
                )

    for field in type(request.feature_engineering).model_fields:
        if field not in request.feature_engineering.model_fields_set:
            _unsupported(
                f"feature_engineering.{field}",
                "must be explicitly set to NONE or PASSTHROUGH",
            )
        value = getattr(request.feature_engineering, field)
        if isinstance(value, str) and value.upper() not in {"NONE", "PASSTHROUGH"}:
            _unsupported(
                f"feature_engineering.{field}",
                "only explicit NONE/PASSTHROUGH is supported",
            )
    for index, feature in enumerate(request.features):
        if not _is_supported_column_type(feature.column_type, allow_boolean=True):
            _unsupported(
                f"features.{index}.column_type",
                "only numeric and boolean passthrough features are supported",
            )
        if feature.feature_role.upper() != "REGULAR":
            _unsupported(
                f"features.{index}.feature_role", "only REGULAR features are supported"
            )
        if feature.pivot_time_value is not None:
            _unsupported(
                f"features.{index}.pivot_time_value",
                "time-series pivot features are not implemented",
            )
        if feature.origin is not None:
            _unsupported(
                f"features.{index}.origin",
                "feature origin execution is not implemented",
            )
        if feature.treatment is not None:
            for field in feature.treatment.model_fields_set:
                value = getattr(feature.treatment, field)
                if value is not None and str(value).upper() not in {
                    "NONE",
                    "PASSTHROUGH",
                }:
                    _unsupported(
                        f"features.{index}.treatment.{field}",
                        "non-passthrough feature treatment is not implemented",
                    )

    sampling = request.data_sampling
    if sampling is not None:
        if sampling.sample_ratio is not None:
            _unsupported("data_sampling.sample_ratio", "sampling is not implemented")
        if sampling.sample_limit is not None:
            _unsupported("data_sampling.sample_limit", "sampling is not implemented")
        if sampling.random_seed is not None:
            _unsupported("data_sampling.random_seed", "sampling is not implemented")
        balance = sampling.class_balance
        if balance is not None and category == "REGRESSION":
            _unsupported(
                "data_sampling.class_balance",
                "class balancing is not valid for regression",
            )
        if balance is not None and balance.strategy.upper() not in {
            "NONE",
            "CUSTOM_AMOUNT",
        }:
            _unsupported(
                "data_sampling.class_balance.strategy",
                "only NONE and CUSTOM_AMOUNT warning validation are supported",
            )
        if balance is not None and balance.oversample_config is not None:
            _unsupported(
                "data_sampling.class_balance.oversample_config",
                "oversampling is not implemented",
            )
        if (
            balance is not None
            and balance.strategy.upper() != "CUSTOM_AMOUNT"
            and balance.class_amounts is not None
        ):
            _unsupported(
                "data_sampling.class_balance.class_amounts",
                "class amounts are only used by CUSTOM_AMOUNT",
            )

    split = request.data_split
    if split.stratify and category == "REGRESSION":
        _unsupported(
            "data_split.stratify", "stratification is not valid for regression"
        )
    if split.strategy.upper() not in {"RANDOM", "TIME_ORDERED"}:
        _unsupported(
            "data_split.strategy", "only RANDOM and TIME_ORDERED are supported"
        )
    if split.strategy.upper() == "TIME_ORDERED" and not split.order_column:
        _unsupported(
            "data_split.order_column", "is required for TIME_ORDERED splitting"
        )
    if split.cross_validation.enabled:
        _unsupported(
            "data_split.cross_validation.enabled",
            "cross-validation is not implemented",
        )
    if not split.cross_validation.enabled:
        for field in ("k_folds", "shuffle"):
            if field in split.cross_validation.model_fields_set:
                _unsupported(
                    f"data_split.cross_validation.{field}",
                    "is ignored while cross-validation is disabled",
                )

    if request.tuning.mode.upper() != "MANUAL":
        _unsupported("tuning.mode", "AUTO tuning is not implemented")
    if request.tuning.auto_config is not None:
        _unsupported("tuning.auto_config", "AUTO tuning is not implemented")
    hyper_params = request.algorithm.hyper_params
    configured_rounds = hyper_params.get("num_rounds", hyper_params.get("n_estimators"))
    num_rounds = hyper_params.get("num_rounds")
    n_estimators = hyper_params.get("n_estimators")
    if (
        num_rounds is not None
        and n_estimators is not None
        and num_rounds != n_estimators
    ):
        _unsupported(
            "algorithm.hyper_params.num_rounds",
            "conflicts with algorithm.hyper_params.n_estimators",
        )
    if (
        configured_rounds is not None
        and configured_rounds > request.resource_limits.max_epochs
    ):
        _unsupported(
            "resource_limits.max_epochs",
            "must be greater than or equal to requested training rounds",
        )

    target = request.target
    assert target is not None  # guarded above
    if target.label_mapping is not None:
        _unsupported("target.label_mapping", "label remapping is not implemented")
    if target.positive_label_value is not None:
        _unsupported(
            "target.positive_label_value",
            "positive-label remapping is not implemented",
        )
    if target.origin is not None:
        _unsupported("target.origin", "target origin execution is not implemented")
    if target.pivot_time_value is not None:
        _unsupported(
            "target.pivot_time_value", "time-series pivot targets are not implemented"
        )

    if request.evaluation.enabled:
        allowed_metrics = _TASK_METRICS[target.task_type.upper()]
        if (
            target.task_type.upper() == "MULTICLASS_CLASSIFICATION"
            and hyper_params.get("objective") == "multi:softmax"
        ):
            allowed_metrics = allowed_metrics - {"auc"}
        if request.evaluation.primary_metric.lower() not in allowed_metrics:
            _unsupported(
                "evaluation.primary_metric",
                f"is not produced for {target.task_type.upper()}",
            )
        for index, metric in enumerate(request.evaluation.additional_metrics):
            if metric.lower() not in allowed_metrics:
                _unsupported(
                    f"evaluation.additional_metrics.{index}",
                    f"is not produced for {target.task_type.upper()}",
                )
    elif (
        "primary_metric" in request.evaluation.model_fields_set
        or request.evaluation.additional_metrics
    ):
        _unsupported(
            "evaluation.primary_metric",
            "metrics cannot be requested when evaluation.enabled=false",
        )
    if request.evaluation.artifacts.correlation_matrix:
        _unsupported(
            "evaluation.artifacts.correlation_matrix",
            "correlation matrix evaluation is not implemented",
        )
