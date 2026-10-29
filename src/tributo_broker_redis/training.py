"""Current Core/official Boosting contracts for generic training."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator
from tributo.algorithms import (
    AlgorithmRunResult,
    WorkerResources,
)
from tributo.algorithms.api import AlgorithmConfigurationError
from tributo.data import IngestionRequest


class _ClosedModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


class TrainingOptions(_ClosedModel):
    num_rounds: int = Field(default=100, ge=1)
    train_ratio: float = Field(default=0.7, gt=0, le=1)
    validation_ratio: float = Field(default=0.15, ge=0, lt=1)
    test_ratio: float = Field(default=0.15, ge=0, lt=1)
    seed: int = 42
    batch_rows: int = Field(default=65536, ge=1)
    early_stopping_rounds: int | None = Field(default=None, ge=1)
    max_training_time_seconds: float | None = Field(
        default=None, gt=0, allow_inf_nan=False
    )

    @model_validator(mode="after")
    def _split(self) -> TrainingOptions:
        if not math.isclose(
            self.train_ratio + self.validation_ratio + self.test_ratio, 1
        ):
            raise ValueError("training split ratios must sum to one")
        if self.early_stopping_rounds is not None and self.validation_ratio == 0:
            raise ValueError("early stopping requires a validation split")
        return self


class TrainingResources(_ClosedModel):
    num_cpus: float = Field(default=1, gt=0, allow_inf_nan=False)
    num_gpus: float = Field(default=0, ge=0, allow_inf_nan=False)
    memory_bytes: int | None = Field(default=None, ge=1)
    custom: dict[str, float] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _public_contract(self) -> TrainingResources:
        try:
            self.as_core()
        except AlgorithmConfigurationError:
            raise ValueError("invalid worker resource contract") from None
        return self

    def as_core(self) -> WorkerResources:
        return WorkerResources(**self.model_dump())


class GenericTrainingConfig(_ClosedModel):
    """Explicit inputs; removed Core trainer/config formats are not accepted."""

    ingestion: IngestionRequest
    feature_names: tuple[str, ...] = Field(min_length=1)
    label_name: str = Field(min_length=1)
    worker_count: int = Field(ge=1)
    task_type: Literal[
        "BINARY_CLASSIFICATION", "MULTICLASS_CLASSIFICATION", "REGRESSION"
    ] = "BINARY_CLASSIFICATION"
    num_class: int | None = Field(default=None, ge=2)
    model: dict[str, Any] = Field(default_factory=dict)
    training: TrainingOptions = Field(default_factory=TrainingOptions)
    resources: TrainingResources = Field(default_factory=TrainingResources)
    storage_path: str = Field(min_length=1)
    bundle_uri: str = Field(min_length=1)

    @model_validator(mode="after")
    def _contracts(self) -> GenericTrainingConfig:
        if self.ingestion.engine != "tributo.ray_data":
            raise ValueError(
                "generic XGBoost training requires the ray ingestion engine"
            )
        if any(not name.strip() or name != name.strip() for name in self.feature_names):
            raise ValueError("feature names must be nonempty trimmed names")
        if (
            len(set(self.feature_names)) != len(self.feature_names)
            or self.label_name in self.feature_names
        ):
            raise ValueError("feature names must be unique and exclude the label")
        if self.task_type == "MULTICLASS_CLASSIFICATION" and self.num_class is None:
            raise ValueError("multiclass training requires num_class")
        return self

    def algorithm_config(self) -> dict[str, Any]:
        return {
            "model": self.model,
            "training": self.training.model_dump(exclude_none=True),
            "ray": {"storage_path": self.storage_path},
            "output": {"bundle_uri": self.bundle_uri},
        }


def execute_training(
    config: GenericTrainingConfig, *, run_id: str, reporter: Any
) -> AlgorithmRunResult:
    from tributo_algorithms_boosting.training import run_xgboost_training

    return run_xgboost_training(
        ingestion_request=config.ingestion,
        feature_names=config.feature_names,
        label_name=config.label_name,
        algorithm_config=config.algorithm_config(),
        worker_count=config.worker_count,
        resources=config.resources.as_core(),
        run_id=run_id,
        task_type=config.task_type,
        num_class=config.num_class,
        reporter=reporter,
    )


def _json_value(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _json_value(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, Enum):
        return _json_value(value.value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError("unsupported value in training result")


def serialize_result(result: AlgorithmRunResult) -> dict[str, Any]:
    if not isinstance(result, AlgorithmRunResult):
        raise TypeError("training requires a public AlgorithmRunResult")
    value = json.loads(json.dumps(_json_value(result), allow_nan=False))
    if not isinstance(value, dict):
        raise TypeError("training result must serialize to an object")
    return value
