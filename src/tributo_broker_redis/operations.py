"""Provider-owned operation validation and driver-input preparation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError

from tributo_broker_redis.protocol import (
    BatchInferenceSpec,
    GenericRequest,
    TrainingSpec,
)


class MappingFailure(Exception):
    """Stable, sanitized failure from an operation mapping."""

    def __init__(self, code: str, sanitized_message: str) -> None:
        super().__init__(sanitized_message)
        self.code = code
        self.sanitized_message = sanitized_message


@dataclass(frozen=True)
class PreparedOperation:
    operation_payload: dict[str, Any]
    credential_ref: str | None
    timeout_seconds: float | None = None


def prepare_operation(request: GenericRequest) -> PreparedOperation:
    """Validate and prepare without submitting or executing any Ray Job."""
    if request.operation_type == "training":
        return _prepare_training(request)
    return _prepare_batch_inference(request)


def _prepare_training(request: GenericRequest) -> PreparedOperation:
    raw_algorithm = request.spec.get("algorithm")
    if raw_algorithm != "xgboost":
        raise MappingFailure(
            "UNSUPPORTED_ALGORITHM",
            "generic training supports only the xgboost algorithm",
        )
    try:
        spec = TrainingSpec.model_validate(request.spec)
        from tributo_broker_redis.training import GenericTrainingConfig

        config = GenericTrainingConfig.model_validate(spec.config)
    except ValidationError as exc:
        raise MappingFailure(
            "INVALID_TRAINING_REQUEST", "training request validation failed"
        ) from exc
    if request.execution_profile == "single_worker" and config.worker_count != 1:
        raise MappingFailure(
            "EXECUTION_PROFILE_MISMATCH",
            "single_worker training requires worker_count=1",
        )
    if request.execution_profile == "distributed" and config.worker_count < 2:
        raise MappingFailure(
            "EXECUTION_PROFILE_MISMATCH",
            "distributed training requires worker_count>=2",
        )
    return PreparedOperation(
        operation_payload={
            "training_config": config.model_dump(mode="json", exclude_unset=True)
        },
        credential_ref=spec.credential_ref,
        timeout_seconds=config.training.max_training_time_seconds,
    )


def _prepare_batch_inference(request: GenericRequest) -> PreparedOperation:
    raw_profile = request.spec.get("profile", "bundle-backed")
    if raw_profile != "bundle-backed":
        raise MappingFailure(
            "UNSUPPORTED_INFERENCE_PROFILE",
            "generic v1 supports only the bundle-backed inference profile",
        )
    try:
        spec = BatchInferenceSpec.model_validate(request.spec)
        from tributo.inference.contracts import (
            BundleModelReference,
            InferenceRequest,
        )

        inference_request = InferenceRequest.model_validate(spec.request)
    except ValidationError as exc:
        raise MappingFailure(
            "INVALID_INFERENCE_REQUEST", "batch inference request validation failed"
        ) from exc
    if not isinstance(inference_request.model, BundleModelReference):
        raise MappingFailure(
            "UNSUPPORTED_INFERENCE_PROFILE",
            "bundle-backed inference requires model.kind=bundle",
        )
    run_id = request.run_id or request.operation_id
    if inference_request.run_id not in {None, run_id}:
        raise MappingFailure(
            "IDENTITY_MISMATCH", "inference request run_id conflicts with operation"
        )
    if (
        request.execution_profile == "single_worker"
        and inference_request.execution.concurrency != 1
    ):
        raise MappingFailure(
            "EXECUTION_PROFILE_MISMATCH",
            "single_worker inference requires execution.concurrency=1",
        )
    if (
        request.execution_profile == "distributed"
        and inference_request.execution.concurrency < 2
    ):
        raise MappingFailure(
            "EXECUTION_PROFILE_MISMATCH",
            "distributed inference requires execution.concurrency>=2",
        )
    payload = inference_request.model_dump(mode="json")
    payload["run_id"] = run_id
    return PreparedOperation(
        operation_payload={"inference_request": payload},
        credential_ref=spec.credential_ref,
    )


__all__ = ["MappingFailure", "PreparedOperation", "prepare_operation"]
