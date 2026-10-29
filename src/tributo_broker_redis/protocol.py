"""Closed ``tributo-generic-v1`` request and driver contracts."""

from __future__ import annotations

import json
import re
from typing import Annotated, Any, Literal
from urllib.parse import parse_qsl, urlsplit

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from tributo_broker_redis.config import (
    ExecutionProfile,
    OperationType,
    RedisConnectionConfig,
)

PROTOCOL_PROFILE = "tributo-generic-v1"
PROTOCOL_VERSION = "1.0"
CredentialReference = Annotated[
    str,
    Field(pattern=r"^(?:env:[A-Za-z_][A-Za-z0-9_]*|mount:/[^\x00\r\n]+)$"),
]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


class GenericRequest(_StrictModel):
    """Provider-owned public request common to training and inference."""

    protocol_profile: Literal["tributo-generic-v1"]
    protocol_version: Literal["1.0"]
    operation_id: str = Field(min_length=1, max_length=256)
    operation_type: OperationType
    execution_profile: ExecutionProfile
    run_id: str | None = Field(default=None, min_length=1, max_length=256)
    attempt_id: str = Field(default="attempt-1", min_length=1, max_length=128)
    request_digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    spec: dict[str, Any]

    @field_validator("operation_id", "run_id", "attempt_id")
    @classmethod
    def _trimmed_identity(cls, value: str | None) -> str | None:
        if value is not None and value != value.strip():
            raise ValueError("identity values must not have surrounding whitespace")
        return value


class TrainingSpec(_StrictModel):
    algorithm: Literal["xgboost"]
    config: dict[str, Any]
    credential_ref: CredentialReference | None = None


class BatchInferenceSpec(_StrictModel):
    profile: Literal["bundle-backed"] = "bundle-backed"
    request: dict[str, Any]
    credential_ref: CredentialReference | None = None


class DriverInput(_StrictModel):
    """Credential-free input transported to the single provider Ray driver."""

    protocol_profile: Literal["tributo-generic-v1"] = "tributo-generic-v1"
    protocol_version: Literal["1.0"] = "1.0"
    operation_id: str
    operation_type: OperationType
    execution_profile: ExecutionProfile
    run_id: str
    attempt_id: str
    ray_job_id: str | None = None
    credential_ref: CredentialReference | None = None
    operation_payload: dict[str, Any]
    redis_transport: RedisConnectionConfig | None = None
    redis_url: str | None = None
    event_stream_prefix: str
    outer_identity_field: str = Field(
        default="operation_id",
        pattern=r"^[A-Za-z_][A-Za-z0-9_.-]{0,127}$",
    )
    max_event_bytes: int
    max_stream_length: int
    received_at_ms: int | None = Field(default=None, ge=0)
    deadline_at_ms: int | None = Field(default=None, ge=0)
    cancel_key: str | None = None

    @model_validator(mode="after")
    def _one_redis_connection(self) -> DriverInput:
        if (self.redis_transport is None) == (self.redis_url is None):
            raise ValueError(
                "driver input requires exactly one Redis connection descriptor"
            )
        return self

    @property
    def redis_connection(self) -> RedisConnectionConfig | str:
        """Return the current descriptor or a legacy standalone URL."""
        if self.redis_transport is not None:
            return self.redis_transport
        if self.redis_url is None:
            raise ValueError("driver Redis connection is unavailable")
        return self.redis_url


class ProtocolFailure(Exception):
    """Stable, sanitized protocol rejection."""

    def __init__(self, code: str, sanitized_message: str) -> None:
        super().__init__(sanitized_message)
        self.code = code
        self.sanitized_message = sanitized_message


_SENSITIVE_KEY = re.compile(
    r"(?:password|passwd|secret|token|access[_-]?key|private[_-]?key|"
    r"credential|authorization|auth[_-]?(?:header|token|value)|signature)",
    re.IGNORECASE,
)


def _plaintext_credential_path(value: Any, path: str = "spec") -> str | None:
    if isinstance(value, dict):
        for key, item in value.items():
            child = f"{path}.{key}"
            if (
                child != "spec.credential_ref"
                and _SENSITIVE_KEY.search(str(key))
                and item is not None
                and item != ""
                and item is not False
            ):
                return child
            nested = _plaintext_credential_path(item, child)
            if nested is not None:
                return nested
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            nested = _plaintext_credential_path(item, f"{path}[{index}]")
            if nested is not None:
                return nested
    elif isinstance(value, str) and "://" in value:
        try:
            parsed = urlsplit(value)
        except ValueError:
            return None
        if parsed.username is not None or parsed.password is not None:
            return path
        for key, item in parse_qsl(parsed.query, keep_blank_values=True):
            if _SENSITIVE_KEY.search(key) and item:
                return f"{path}.query.{key}"
    return None


def parse_request(
    raw_payload: str,
    *,
    outer_operation_id: str,
    expected_operation_type: OperationType,
) -> GenericRequest:
    """Parse, close, and align one request with its Redis envelope."""
    try:
        value = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise ProtocolFailure("INVALID_JSON", "payload is not valid JSON") from exc
    if not isinstance(value, dict):
        raise ProtocolFailure("INVALID_REQUEST", "payload root must be an object")

    profile = value.get("protocol_profile")
    if profile != PROTOCOL_PROFILE:
        raise ProtocolFailure(
            "UNSUPPORTED_PROTOCOL_PROFILE", "unsupported protocol profile"
        )
    version = value.get("protocol_version")
    if version != PROTOCOL_VERSION:
        raise ProtocolFailure(
            "UNSUPPORTED_PROTOCOL_VERSION", "unsupported protocol version"
        )
    leaked = _plaintext_credential_path(value.get("spec"))
    if leaked is not None:
        raise ProtocolFailure(
            "PLAINTEXT_CREDENTIAL", f"plaintext credential is forbidden at {leaked}"
        )
    try:
        request = GenericRequest.model_validate(value)
    except ValidationError as exc:
        raise ProtocolFailure(
            "INVALID_REQUEST", "request schema validation failed"
        ) from exc
    if request.operation_id != outer_operation_id:
        raise ProtocolFailure(
            "IDENTITY_MISMATCH", "outer operation_id does not match payload identity"
        )
    if request.operation_type != expected_operation_type:
        raise ProtocolFailure(
            "OPERATION_CHANNEL_MISMATCH",
            "operation_type does not match the configured task channel",
        )
    return request


__all__ = [
    "BatchInferenceSpec",
    "DriverInput",
    "GenericRequest",
    "PROTOCOL_PROFILE",
    "PROTOCOL_VERSION",
    "ProtocolFailure",
    "TrainingSpec",
    "parse_request",
]
