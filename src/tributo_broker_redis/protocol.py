"""Strict KnoVa training protocol v2 request and event value objects."""

from __future__ import annotations

import json
import math
import re
from typing import Any

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

PROTOCOL_VERSION = "2.0"
MAX_JOB_ID_LENGTH = 128
_JOB_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")
MAX_EVENT_TIMESTAMP = 99_999_999_999_999
MAX_TERMINAL_DURATION_SECONDS = MAX_EVENT_TIMESTAMP / 1000


def validate_job_id(value: str) -> str:
    """Validate the shared Redis, protocol, worker, and Ray run identity."""
    if (
        not value
        or len(value) > MAX_JOB_ID_LENGTH
        or _JOB_ID_PATTERN.fullmatch(value) is None
    ):
        raise ValueError(
            "job_id must be 1-128 ASCII letters, digits, '.', '_', ':', or '-' "
            "and start with a letter or digit"
        )
    return value


def quantize_duration_seconds(value: Any) -> float:
    """Return a finite non-negative terminal duration rounded to milliseconds."""
    if isinstance(value, bool):
        raise ValueError("duration_seconds must be a finite non-negative number")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "duration_seconds must be a finite non-negative number"
        ) from exc
    if (
        not math.isfinite(result)
        or result < 0
        or result > MAX_TERMINAL_DURATION_SECONDS
    ):
        raise ValueError("duration_seconds must be a finite non-negative number")
    quantized = round(result, 3)
    return 0.0 if quantized == 0 else quantized


def validate_terminal_event(value: dict[str, Any], job_id: str) -> None:
    """Validate the minimum durable wire schema shared with candidate Lua."""
    if value.get("protocol_version") != PROTOCOL_VERSION:
        raise ValueError("terminal candidate has an unsupported protocol_version")
    if value.get("job_id") != job_id:
        raise ValueError("terminal candidate job_id does not match its key")
    validate_job_id(job_id)
    timestamp = value.get("timestamp")
    if (
        isinstance(timestamp, bool)
        or not isinstance(timestamp, int)
        or timestamp < 0
        or timestamp > MAX_EVENT_TIMESTAMP
    ):
        raise ValueError(
            f"terminal candidate timestamp must be an integer in "
            f"[0, {MAX_EVENT_TIMESTAMP}]"
        )
    event_type = value.get("event_type")
    if event_type not in {"COMPLETED", "FAILED", "CANCELLED"}:
        raise ValueError("terminal candidate must contain a terminal event")
    duration = value.get("duration_seconds")
    if isinstance(duration, bool) or not isinstance(duration, (int, float)):
        raise ValueError("terminal candidate duration_seconds is invalid")
    try:
        quantize_duration_seconds(duration)
    except ValueError as exc:
        raise ValueError("terminal candidate duration_seconds is invalid") from exc
    if event_type == "FAILED":
        for field in ("phase", "error_code", "error_message"):
            if not isinstance(value.get(field), str) or not value[field]:
                raise ValueError(f"terminal candidate FAILED.{field} is required")
    elif event_type == "CANCELLED":
        if not isinstance(value.get("phase"), str) or not value["phase"]:
            raise ValueError("terminal candidate CANCELLED.phase is required")
        if not isinstance(value.get("has_best_model"), bool):
            raise ValueError(
                "terminal candidate CANCELLED.has_best_model must be boolean"
            )
    else:
        for field in ("result_summary", "training_result", "artifact_manifest"):
            if not isinstance(value.get(field), dict):
                raise ValueError(f"terminal candidate COMPLETED.{field} is required")


def _terminal_size_budget() -> int:
    """Return the exact JSON bytes needed by the largest emergency terminal."""
    common = {
        "protocol_version": PROTOCOL_VERSION,
        "job_id": "j" * MAX_JOB_ID_LENGTH,
        # Reserve the full 14-digit millisecond timestamp range.
        "timestamp": MAX_EVENT_TIMESTAMP,
        "phase": "EVALUATING",
        "duration_seconds": quantize_duration_seconds(MAX_TERMINAL_DURATION_SECONDS),
    }
    failed = {
        **common,
        "event_type": "FAILED",
        "error_code": "PAYLOAD_TOO_LARGE",
        "error_message": "COMPLETED event exceeded the configured size limit",
    }
    cancelled = {
        **common,
        "event_type": "CANCELLED",
        "has_best_model": False,
    }
    return max(
        len(json.dumps(value, separators=(",", ":")).encode("utf-8"))
        for value in (failed, cancelled)
    )


MIN_TERMINAL_EVENT_BYTES = _terminal_size_budget()


def check_protocol_version(value: dict[str, Any]) -> str | None:
    """Classify an unsupported/missing protocol major before model parsing."""
    raw = value.get("protocol_version")
    expected = PROTOCOL_VERSION.split(".", 1)[0]
    if not isinstance(raw, str) or not raw:
        return (
            f"Unsupported protocol version: {raw!r} (expected major version {expected})"
        )
    if raw.split(".", 1)[0] != expected:
        return (
            f"Unsupported protocol version: {raw!r} (expected major version {expected})"
        )
    return None


def is_training_task(value: dict[str, Any]) -> bool:
    """Return whether the envelope describes a training task."""
    accepted = {
        "TRAINING",
        "TRAIN",
        "MODEL_TRAINING",
    }
    task_fields = [
        value[field] for field in ("job_type", "task_type") if field in value
    ]
    return all(
        isinstance(task_type, str) and task_type.upper() in accepted
        for task_type in task_fields or ["TRAINING"]
    )


class ProtocolModel(BaseModel):
    """Strict base: protocol growth must be implemented intentionally."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True, strict=True)


class ColumnOrigin(ProtocolModel):
    table_alias: str = Field(min_length=1)
    column_name: str = Field(min_length=1)
    column_type: str = Field(min_length=1)


class SqlTemplate(ProtocolModel):
    sql: str = ""
    params: dict[str, Any] = Field(default_factory=dict)
    resolved_sql: str | None = None


class TableInfo(ProtocolModel):
    table_alias: str
    database_name: str
    table_name: str
    role: str = "PRIMARY"


class TableRelation(ProtocolModel):
    left_alias: str
    right_alias: str
    join_type: str = "LEFT"
    on: list[dict[str, str]]


class EntityKey(ProtocolModel):
    origin: ColumnOrigin
    result_column: str = Field(min_length=1)


class FeatureTreatment(ProtocolModel):
    treatment_type: str | None = None
    missing_value_strategy: str | None = None
    outlier_strategy: str | None = None
    scaling_method: str | None = None
    encoding_method: str | None = None


class TimeSeriesPivotConfig(ProtocolModel):
    time_column: str = ""
    time_values: list[str] = Field(default_factory=list)
    training_time_values: list[str] | None = None
    pivot_columns: list[str] = Field(default_factory=list)
    aggregation: str = "LAST"

    @model_validator(mode="after")
    def validate_windows(self) -> TimeSeriesPivotConfig:
        if (
            self.training_time_values is not None
            and self.time_values
            and len(self.training_time_values) != len(self.time_values)
        ):
            raise ValueError("training_time_values must match time_values length")
        return self


class AlgorithmConfig(ProtocolModel):
    category: str = "CLASSIFICATION"
    algorithm_key: str = "xgboost"
    is_deep_learning: bool = False
    hyper_params: dict[str, Any] = Field(default_factory=dict)


class DataSourceConfig(ProtocolModel):
    type: str = "CLICKHOUSE"  # noqa: A003
    datasource_id: str | None = Field(default=None, min_length=1)
    host: str = ""
    port: int = 9000
    database_name: str = ""
    username: str | None = None
    password: str | None = None
    credential_ref: str | None = None
    connection_string: str | None = None
    properties: dict[str, Any] = Field(default_factory=dict)


class FeatureSpec(ProtocolModel):
    display_name: str = Field(
        default="", validation_alias=AliasChoices("display_name", "name")
    )
    result_column: str = Field(
        min_length=1,
        validation_alias=AliasChoices("result_column", "physical_field"),
    )
    column_type: str = Field(
        default="float", validation_alias=AliasChoices("column_type", "data_type")
    )
    feature_role: str = "REGULAR"
    feature_id: str = ""
    origin: ColumnOrigin | None = None
    pivot_time_value: str | None = None
    treatment: FeatureTreatment | None = None


class TargetSpec(ProtocolModel):
    display_name: str = Field(
        default="", validation_alias=AliasChoices("display_name", "name")
    )
    result_column: str = Field(
        min_length=1,
        validation_alias=AliasChoices("result_column", "physical_field"),
    )
    column_type: str = Field(
        default="int", validation_alias=AliasChoices("column_type", "data_type")
    )
    task_type: str = "BINARY_CLASSIFICATION"
    label_mapping: dict[str, int] | None = None
    positive_label_value: str | None = None
    origin: ColumnOrigin | None = None
    pivot_time_value: str | None = None


class FeatureEngineeringConfig(ProtocolModel):
    default_missing_value_strategy: str = "AUTO"
    default_outlier_strategy: str = "AUTO"
    default_scaling_method: str = "AUTO"
    default_encoding_method: str = "AUTO"


class ClassBalanceConfig(ProtocolModel):
    strategy: str = "NONE"
    class_amounts: dict[str, int] | None = None
    oversample_config: dict[str, Any] | None = None

    @model_validator(mode="after")
    def validate_custom_amounts(self) -> ClassBalanceConfig:
        if self.strategy == "CUSTOM_AMOUNT" and (
            not self.class_amounts
            or any(value <= 0 for value in self.class_amounts.values())
        ):
            raise ValueError("CUSTOM_AMOUNT requires positive class_amounts")
        return self


class DataSamplingConfig(ProtocolModel):
    sample_ratio: int | None = Field(default=None, ge=1, le=99)
    sample_limit: int | None = Field(default=None, ge=1)
    class_balance: ClassBalanceConfig | None = None
    random_seed: int | None = None


class DataQueryConfig(ProtocolModel):
    mode: str = "DIRECT_QUERY"
    entity_key: EntityKey | None = None
    query: SqlTemplate | None = None
    entity_sampling_query: SqlTemplate | None = None
    feature_query: SqlTemplate | None = None
    target_query: SqlTemplate | None = None
    timeseries_pivot: TimeSeriesPivotConfig | None = None


class CrossValidationConfig(ProtocolModel):
    enabled: bool = False
    k_folds: int = Field(default=5, ge=2)
    shuffle: bool = True


class DataSplitConfig(ProtocolModel):
    strategy: str = "RANDOM"
    train_ratio: float = Field(default=0.7, gt=0.0, le=1.0)
    validation_ratio: float = Field(default=0.0, ge=0.0, lt=1.0)
    test_ratio: float = Field(default=0.3, ge=0.0, lt=1.0)
    random_seed: int | None = None
    stratify: bool = False
    order_column: str | None = None
    cross_validation: CrossValidationConfig = Field(
        default_factory=CrossValidationConfig
    )

    @model_validator(mode="after")
    def validate_ratios(self) -> DataSplitConfig:
        total = self.train_ratio + self.validation_ratio + self.test_ratio
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"data_split ratios must sum to 1.0 (got {total})")
        if self.strategy == "TIME_ORDERED" and self.stratify:
            raise ValueError("TIME_ORDERED and stratify=true are mutually exclusive")
        return self


class TuningConfig(ProtocolModel):
    mode: str = "MANUAL"
    auto_config: dict[str, Any] | None = None


class ResourceLimits(ProtocolModel):
    max_epochs: int = Field(default=1000, ge=1)
    early_stopping_patience: int | None = Field(default=20, ge=1)
    max_training_time_seconds: int | None = Field(default=None, ge=1)


class EvaluationArtifacts(ProtocolModel):
    roc_curve: bool = False
    threshold_analysis: bool = False
    confusion_matrix: bool = True
    feature_importance: bool = True
    correlation_matrix: bool = False


class EvaluationConfig(ProtocolModel):
    enabled: bool = True
    primary_metric: str = "auc"
    additional_metrics: list[str] = Field(default_factory=list)
    artifacts: EvaluationArtifacts = Field(default_factory=EvaluationArtifacts)

    @model_validator(mode="after")
    def validate_metrics(self) -> EvaluationConfig:
        normalized = [value.lower() for value in self.additional_metrics]
        if len(normalized) != len(set(normalized)):
            raise ValueError("additional_metrics must be unique")
        if self.primary_metric.lower() in normalized:
            raise ValueError("additional_metrics must not repeat primary_metric")
        return self


class StorageContext(ProtocolModel):
    type: str = "s3"  # noqa: A003
    bucket: str = ""
    prefix: str = ""

    @field_validator("prefix")
    @classmethod
    def prefix_must_end_with_slash(cls, value: str) -> str:
        if value and not value.endswith("/"):
            raise ValueError("storage_context.prefix must end with '/'")
        return value


class TrainingJobRequest(ProtocolModel):
    """Canonical v2 request. Legacy config requires an explicit provider gate."""

    protocol_version: str = PROTOCOL_VERSION
    job_type: str = "TRAINING"
    task_type: str | None = None
    job_id: str = Field(min_length=1, max_length=MAX_JOB_ID_LENGTH)
    model_id: str = ""
    version_id: str = ""
    tenant_id: str = ""
    algorithm: AlgorithmConfig = Field(default_factory=AlgorithmConfig)
    datasource: DataSourceConfig = Field(default_factory=DataSourceConfig)
    tables: list[TableInfo] = Field(default_factory=list)
    relations: list[TableRelation] = Field(default_factory=list)
    data_query: DataQueryConfig | None = None
    features: list[FeatureSpec] = Field(default_factory=list)
    target: TargetSpec | None = None
    feature_engineering: FeatureEngineeringConfig = Field(
        default_factory=FeatureEngineeringConfig
    )
    data_sampling: DataSamplingConfig | None = None
    data_split: DataSplitConfig = Field(default_factory=DataSplitConfig)
    tuning: TuningConfig = Field(default_factory=TuningConfig)
    resource_limits: ResourceLimits = Field(default_factory=ResourceLimits)
    evaluation: EvaluationConfig = Field(default_factory=EvaluationConfig)
    storage_context: StorageContext | None = None
    extensions: dict[str, Any] = Field(default_factory=dict)
    training_config: dict[str, Any] | None = None

    @field_validator("job_id")
    @classmethod
    def job_id_is_safe(cls, value: str) -> str:
        return validate_job_id(value)

    @model_validator(mode="after")
    def validate_unique_features(self) -> TrainingJobRequest:
        accepted_tasks = {"TRAINING", "TRAIN", "MODEL_TRAINING"}
        if self.job_type.upper() not in accepted_tasks or (
            self.task_type is not None and self.task_type.upper() not in accepted_tasks
        ):
            raise ValueError("job_type/task_type conflict: only training is supported")
        columns = [feature.result_column for feature in self.features]
        if len(columns) != len(set(columns)):
            raise ValueError("features[].result_column must be unique")
        feature_ids = [
            feature.feature_id for feature in self.features if feature.feature_id
        ]
        if len(feature_ids) != len(set(feature_ids)):
            raise ValueError("features[].feature_id must be unique")
        if self.training_config is None:
            for field in ("model_id", "version_id", "tenant_id"):
                if not getattr(self, field):
                    raise ValueError(f"{field} is required for canonical requests")
            for index, feature in enumerate(self.features):
                if not feature.feature_id:
                    raise ValueError(
                        f"features.{index}.feature_id is required for "
                        "canonical requests"
                    )
        try:
            json.dumps(self.extensions, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError("extensions must be finite JSON metadata") from exc
        return self

    def resolve_training_config(
        self, *, allow_legacy_training_config: bool = False
    ) -> dict[str, Any]:
        from tributo_broker_redis.training_mapping import resolve_training_config

        return resolve_training_config(
            self,
            allow_legacy_training_config=allow_legacy_training_config,
        )


class ProcessMetric(ProtocolModel):
    metric_name: str = Field(min_length=1)
    train: float | None = None
    eval: float | None = None

    @model_validator(mode="after")
    def has_value(self) -> ProcessMetric:
        if self.train is None and self.eval is None:
            raise ValueError("process metric requires train or eval")
        return self


def event_payload(
    *,
    job_id: str | None,
    event_type: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a JSON-safe event envelope for the KnoVa stream."""
    result = {
        "protocol_version": PROTOCOL_VERSION,
        "event_type": event_type,
        "job_id": job_id,
    }
    if payload:
        result.update(payload)
    return result
