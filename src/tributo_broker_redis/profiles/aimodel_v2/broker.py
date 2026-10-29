"""Thin KnoVa protocol adapter for the public Redis broker runtime."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from tributo_broker_redis.config import OperationType, RedisBrokerConfig
from tributo_broker_redis.operations import MappingFailure, PreparedOperation
from tributo_broker_redis.profiles.aimodel_v2.protocol import parse_request
from tributo_broker_redis.profiles.aimodel_v2.reporter import KnovaRedisEventReporter
from tributo_broker_redis.protocol import GenericRequest
from tributo_broker_redis.runtime import RedisBrokerRuntime

DRIVER_ENTRYPOINT = (
    "python -m tributo_broker_redis.profiles.aimodel_v2.execution_driver"
)


def parse_broker_request(
    raw_payload: str,
    *,
    outer_operation_id: str,
    expected_operation_type: OperationType,
) -> GenericRequest:
    """Adapt a KnoVa v2 request to the broker's existing admission contract."""
    request = parse_request(
        raw_payload,
        outer_operation_id=outer_operation_id,
        expected_operation_type=expected_operation_type,
    )
    payload = request.model_dump(mode="json", exclude_none=True)
    canonical = json.dumps(
        payload,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return GenericRequest(
        protocol_profile="tributo-generic-v1",
        protocol_version="1.0",
        operation_id=request.execution_id,
        operation_type=expected_operation_type,
        execution_profile="distributed",
        run_id=request.execution_id,
        request_digest=hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        spec={"knova_request": payload},
    )


def prepare_broker_operation(request: GenericRequest) -> PreparedOperation:
    """Preserve the validated KnoVa request as the Ray driver payload."""
    payload = request.spec.get("knova_request")
    if not isinstance(payload, dict):
        raise MappingFailure(
            "INVALID_REQUEST",
            "validated KnoVa request is missing from broker input",
        )
    return PreparedOperation(
        operation_payload={"knova_request": dict(payload)},
        credential_ref=None,
        timeout_seconds=_timeout_seconds(payload, request.operation_type),
    )


def _timeout_seconds(
    payload: dict[str, Any], operation_type: OperationType
) -> float | None:
    section = payload.get(
        "resource_limits" if operation_type == "training" else "execution", {}
    )
    key = (
        "max_training_time_seconds"
        if operation_type == "training"
        else "max_execution_time_seconds"
    )
    value = section.get(key) if isinstance(section, dict) else None
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise MappingFailure("INVALID_PAYLOAD", f"{key} must be positive")
    return float(value)


def create_runtime(config: RedisBrokerConfig) -> RedisBrokerRuntime:
    """Build a v2 runtime without importing the deployment integration."""
    return RedisBrokerRuntime(
        config,
        request_parser=parse_broker_request,
        operation_preparer=prepare_broker_operation,
        driver_entrypoint=DRIVER_ENTRYPOINT,
        recovery_driver_entrypoints=("python -m tributo_knova.execution_driver",),
        reporter_factory=KnovaRedisEventReporter,
    )


__all__ = [
    "DRIVER_ENTRYPOINT",
    "create_runtime",
    "parse_broker_request",
    "prepare_broker_operation",
]
