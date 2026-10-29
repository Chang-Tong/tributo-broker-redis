"""Public configuration for the Redis Streams provider."""

from __future__ import annotations

import os
import re
import socket
import uuid
from collections.abc import Mapping
from pathlib import PurePosixPath
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit

from packaging.requirements import InvalidRequirement, Requirement
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

OperationType = Literal["training", "batch_inference"]
ExecutionProfile = Literal["single_worker", "distributed"]
RedisCredentialReference = Annotated[
    str,
    Field(pattern=r"^(?:env:[A-Za-z_][A-Za-z0-9_]*|mount:/[^\x00\r\n]+)$"),
]

_DEFAULT_REDIS_URL = "redis://127.0.0.1:6379/0"


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


def _validate_redis_url(value: str | None) -> str | None:
    if value is None:
        return None
    parsed = urlsplit(value)
    if parsed.scheme not in {"redis", "rediss"} or not parsed.hostname:
        raise ValueError("Redis URL must use redis:// or rediss:// with a host")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("Redis URL must not contain credentials")
    if parsed.query or parsed.fragment:
        raise ValueError("Redis URL must not contain query or fragment")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("Redis URL contains an invalid port") from exc
    if port is not None and not 1 <= port <= 65535:
        raise ValueError("Redis URL contains an invalid port")
    return value


def _validate_sentinel_nodes(value: tuple[str, ...]) -> tuple[str, ...]:
    if len(set(value)) != len(value):
        raise ValueError("Redis Sentinel nodes must be unique")
    for node in value:
        if not node or node != node.strip() or any(char.isspace() for char in node):
            raise ValueError("Redis Sentinel node must use host:port")
        parsed = urlsplit(f"redis://{node}")
        try:
            port = parsed.port
        except ValueError as exc:
            raise ValueError("Redis Sentinel node contains an invalid port") from exc
        if (
            not parsed.hostname
            or port is None
            or not 1 <= port <= 65535
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("Redis Sentinel node must use host:port")
    return value


class RedisConnectionConfig(_StrictModel):
    """Credential-free standalone or Sentinel Redis connection descriptor."""

    mode: Literal["standalone", "sentinel"] = "standalone"
    url: str | None = None
    sentinel_nodes: tuple[str, ...] = ()
    sentinel_master: str | None = Field(default=None, min_length=1, max_length=256)
    database: int = Field(default=0, ge=0)
    password_ref: RedisCredentialReference | None = None
    sentinel_password_ref: RedisCredentialReference | None = None

    @model_validator(mode="before")
    @classmethod
    def _default_standalone_url(cls, value: Any) -> Any:
        if isinstance(value, Mapping):
            normalized = dict(value)
            if normalized.get("mode", "standalone") == "standalone":
                normalized.setdefault("url", _DEFAULT_REDIS_URL)
            return normalized
        return value

    @field_validator("url")
    @classmethod
    def _credential_free_url(cls, value: str | None) -> str | None:
        return _validate_redis_url(value)

    @field_validator("sentinel_nodes")
    @classmethod
    def _valid_sentinel_nodes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _validate_sentinel_nodes(value)

    @field_validator("sentinel_master")
    @classmethod
    def _trimmed_sentinel_master(cls, value: str | None) -> str | None:
        if value is not None and value != value.strip():
            raise ValueError(
                "Redis Sentinel master must not have surrounding whitespace"
            )
        return value

    @model_validator(mode="after")
    def _mode_specific_connection(self) -> RedisConnectionConfig:
        if self.mode == "standalone":
            if self.url is None:
                raise ValueError("standalone Redis requires url")
            if (
                self.sentinel_nodes
                or self.sentinel_master is not None
                or self.sentinel_password_ref is not None
                or self.database != 0
            ):
                raise ValueError(
                    "standalone Redis does not accept Sentinel connection fields"
                )
            return self
        if self.url is not None:
            raise ValueError("Redis Sentinel mode does not accept url")
        if not self.sentinel_nodes:
            raise ValueError("Redis Sentinel mode requires sentinel_nodes")
        if self.sentinel_master is None:
            raise ValueError("Redis Sentinel mode requires sentinel_master")
        return self


class RedisTransportConfig(RedisConnectionConfig):
    """Redis connection used by the consumer and Ray driver."""

    driver_url: str | None = None
    driver_sentinel_nodes: tuple[str, ...] | None = None
    block_ms: int = Field(default=1000, ge=1, le=60000)
    claim_idle_ms: int = Field(default=60000, ge=0)
    claim_count: int = Field(default=10, ge=1, le=1000)
    max_payload_bytes: int = Field(default=1024 * 1024, ge=1)
    max_driver_input_bytes: int = Field(default=256 * 1024, ge=1)
    max_event_bytes: int = Field(default=1024 * 1024, ge=1)
    max_stream_length: int = Field(default=1000, ge=1)

    @field_validator("driver_url")
    @classmethod
    def _credential_free_driver_url(cls, value: str | None) -> str | None:
        return _validate_redis_url(value)

    @field_validator("driver_sentinel_nodes")
    @classmethod
    def _valid_driver_sentinel_nodes(
        cls, value: tuple[str, ...] | None
    ) -> tuple[str, ...] | None:
        if value is None:
            return None
        if not value:
            raise ValueError("driver_sentinel_nodes must not be empty")
        return _validate_sentinel_nodes(value)

    @model_validator(mode="after")
    def _mode_specific_driver_connection(self) -> RedisTransportConfig:
        if self.mode == "standalone" and self.driver_sentinel_nodes is not None:
            raise ValueError("standalone Redis does not accept driver_sentinel_nodes")
        if self.mode == "sentinel" and self.driver_url is not None:
            raise ValueError("Redis Sentinel mode does not accept driver_url")
        return self

    @property
    def ray_driver_url(self) -> str:
        if self.mode != "standalone" or self.url is None:
            raise ValueError("Redis Sentinel transport does not have a driver URL")
        return self.driver_url or self.url

    def ray_driver_connection(self) -> RedisConnectionConfig:
        """Return the credential-free connection descriptor for the Ray driver."""
        if self.mode == "standalone":
            return RedisConnectionConfig(
                mode="standalone",
                url=self.ray_driver_url,
                password_ref=self.password_ref,
            )
        return RedisConnectionConfig(
            mode="sentinel",
            sentinel_nodes=self.driver_sentinel_nodes or self.sentinel_nodes,
            sentinel_master=self.sentinel_master,
            database=self.database,
            password_ref=self.password_ref,
            sentinel_password_ref=self.sentinel_password_ref,
        )


class ChannelConfig(_StrictModel):
    """Independent task, event, cancel, and consumer-group names."""

    task_stream_key: str = Field(min_length=1)
    event_stream_prefix: str = Field(min_length=1)
    cancel_key_prefix: str = Field(min_length=1)
    consumer_group: str = Field(min_length=1)
    consumer_name: str = Field(default_factory=lambda: _default_consumer_name())
    group_start_id: str = Field(default="0-0", min_length=1)
    outer_identity_field: str = "operation_id"

    @field_validator("outer_identity_field")
    @classmethod
    def _valid_outer_identity_field(cls, value: str) -> str:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]{0,127}", value):
            raise ValueError("outer_identity_field is not a valid Redis field name")
        if value == "payload":
            raise ValueError("outer_identity_field must not be payload")
        return value

    def event_stream_key(self, operation_id: str) -> str:
        return f"{self.event_stream_prefix}:{operation_id}"

    def cancel_key(self, operation_id: str) -> str:
        return f"{self.cancel_key_prefix}:{operation_id}"


class ChannelSet(_StrictModel):
    training: ChannelConfig
    batch_inference: ChannelConfig

    def for_operation(self, operation_type: OperationType) -> ChannelConfig:
        return getattr(self, operation_type)

    @model_validator(mode="after")
    def _require_independent_channels(self) -> ChannelSet:
        for attribute in (
            "task_stream_key",
            "event_stream_prefix",
            "cancel_key_prefix",
            "consumer_group",
        ):
            if getattr(self.training, attribute) == getattr(
                self.batch_inference, attribute
            ):
                raise ValueError(
                    f"training and batch_inference require distinct {attribute}"
                )
        return self


class ProtocolProfileConfig(_StrictModel):
    profile: Literal["tributo-generic-v1", "aimodel-v2"] = "tributo-generic-v1"
    protocol_version: Literal["1.0", "2.0"] = "1.0"

    @model_validator(mode="after")
    def _matching_version(self) -> ProtocolProfileConfig:
        expected = "2.0" if self.profile == "aimodel-v2" else "1.0"
        if self.protocol_version != expected:
            raise ValueError(f"{self.profile} requires protocol_version={expected}")
        return self


class OperationExecutionConfig(_StrictModel):
    execution_profiles: tuple[ExecutionProfile, ...] = (
        "single_worker",
        "distributed",
    )

    @field_validator("execution_profiles")
    @classmethod
    def _unique_profiles(
        cls, value: tuple[ExecutionProfile, ...]
    ) -> tuple[ExecutionProfile, ...]:
        if not value or len(set(value)) != len(value):
            raise ValueError("execution_profiles must be non-empty and unique")
        return value


class OperationSet(_StrictModel):
    training: OperationExecutionConfig = Field(default_factory=OperationExecutionConfig)
    batch_inference: OperationExecutionConfig = Field(
        default_factory=OperationExecutionConfig
    )

    def for_operation(self, operation_type: OperationType) -> OperationExecutionConfig:
        return getattr(self, operation_type)


_SENSITIVE_ENV_NAME = re.compile(
    r"(?:password|passwd|secret|token|access[_-]?key|private[_-]?key)", re.IGNORECASE
)
_RESERVED_EXECUTION_ENV_NAMES = frozenset(
    {
        "TRIBUTO_ATTEMPT_ID",
        "TRIBUTO_REDIS_DRIVER_INPUT_B64",
        "TRIBUTO_RUN_ID",
        "TRIBUTO_SUBMISSION_ID",
    }
)
_REMOTE_MODULE_SCHEMES = frozenset({"gs", "http", "https", "s3"})
_REMOTE_MODULE_SUFFIXES = frozenset({".whl", ".zip"})


def _validate_extension_module(value: str) -> str:
    normalized = value.strip()
    if not normalized or "\x00" in normalized:
        raise ValueError("execution.extra_py_modules entries must not be empty")
    if "://" not in normalized:
        if "?" in normalized or "#" in normalized:
            raise ValueError(
                "execution.extra_py_modules local paths must not contain "
                "query or fragment"
            )
        return normalized

    parsed = urlsplit(normalized)
    if parsed.scheme not in _REMOTE_MODULE_SCHEMES or not parsed.hostname:
        raise ValueError("execution.extra_py_modules contains an unsupported URI")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("execution.extra_py_modules must not contain credentials")
    if parsed.query or parsed.fragment:
        raise ValueError(
            "execution.extra_py_modules URI must not contain query or fragment"
        )
    if PurePosixPath(parsed.path).suffix.lower() not in _REMOTE_MODULE_SUFFIXES:
        raise ValueError(
            "execution.extra_py_modules remote URI must reference a wheel or zip"
        )
    return normalized


def _validate_runtime_pip_package(value: str) -> str:
    normalized = value.strip()
    if not normalized or "\x00" in normalized:
        raise ValueError("execution.runtime_pip_packages entries must not be empty")
    if normalized.startswith("-"):
        raise ValueError(
            "execution.runtime_pip_packages does not accept pip command options"
        )
    if "://" in normalized or "@" in normalized:
        raise ValueError("execution.runtime_pip_packages does not accept direct URLs")
    if normalized.lower().endswith(".whl"):
        if any(character.isspace() for character in normalized):
            raise ValueError(
                "execution.runtime_pip_packages wheel paths must not contain whitespace"
            )
        return normalized
    try:
        requirement = Requirement(normalized)
    except InvalidRequirement as exc:
        raise ValueError(
            "execution.runtime_pip_packages requires a PEP 508 requirement "
            "or wheel path"
        ) from exc
    if requirement.url is not None:
        raise ValueError("execution.runtime_pip_packages does not accept direct URLs")
    return normalized


class ExecutionConfig(_StrictModel):
    ray_dashboard_url: str = "http://127.0.0.1:8265"
    project_root: str = Field(min_length=1)
    extra_py_modules: tuple[str, ...] = ()
    runtime_pip_packages: tuple[str, ...] = ()
    env_vars: dict[str, str] = Field(default_factory=dict)
    cancel_poll_interval_seconds: float = Field(default=0.5, gt=0, le=30)
    entrypoint_num_cpus: float = Field(default=1.0, ge=0)

    @field_validator("extra_py_modules")
    @classmethod
    def _trusted_extension_modules(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(_validate_extension_module(item) for item in value)

    @field_validator("runtime_pip_packages")
    @classmethod
    def _trusted_runtime_pip_packages(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(_validate_runtime_pip_package(item) for item in value)

    @field_validator("env_vars")
    @classmethod
    def _non_sensitive_env(cls, value: dict[str, str]) -> dict[str, str]:
        reserved = sorted(_RESERVED_EXECUTION_ENV_NAMES.intersection(value))
        if reserved:
            raise ValueError(
                "execution.env_vars cannot override reserved runtime identity: "
                + ", ".join(reserved)
            )
        sensitive = sorted(name for name in value if _SENSITIVE_ENV_NAME.search(name))
        if sensitive:
            raise ValueError(
                "execution.env_vars cannot carry sensitive values; offending names: "
                + ", ".join(sensitive)
            )
        return dict(value)

    @model_validator(mode="after")
    def _require_driver_distribution(self) -> ExecutionConfig:
        if not self.extra_py_modules and not self.runtime_pip_packages:
            raise ValueError(
                "execution requires extra_py_modules or runtime_pip_packages "
                "to distribute the provider execution driver"
            )
        return self


class RedisBrokerConfig(_StrictModel):
    """Normalized provider root configuration; this is the only accepted shape."""

    broker_id: Literal["tributo-redis"] = "tributo-redis"
    api_version: Literal[1] = 1
    transport: RedisTransportConfig
    channels: ChannelSet
    protocol: ProtocolProfileConfig = Field(default_factory=ProtocolProfileConfig)
    operations: OperationSet = Field(default_factory=OperationSet)
    execution: ExecutionConfig

    @model_validator(mode="after")
    def _profile_channels(self) -> RedisBrokerConfig:
        if self.protocol.profile == "aimodel-v2":
            identities: tuple[tuple[OperationType, str], ...] = (
                ("training", "job_id"),
                ("batch_inference", "execution_id"),
            )
            for operation, identity in identities:
                channel = self.channels.for_operation(operation)
                if channel.outer_identity_field != identity:
                    raise ValueError(
                        f"aimodel-v2 {operation} requires "
                        f"outer_identity_field={identity}"
                    )
                if self.operations.for_operation(operation).execution_profiles != (
                    "distributed",
                ):
                    raise ValueError(
                        f"aimodel-v2 {operation} supports only distributed execution"
                    )
        return self

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> RedisBrokerConfig:
        return normalize_config(value)


def normalize_config(value: Mapping[str, Any]) -> RedisBrokerConfig:
    """Validate one explicit public config shape without compatibility fallbacks."""
    return RedisBrokerConfig.model_validate(dict(value))


def _default_consumer_name() -> str:
    hostname = socket.gethostname().replace(" ", "-")
    return f"tributo-{hostname}-{os.getpid()}-{uuid.uuid4().hex[:10]}"


__all__ = [
    "ChannelConfig",
    "ChannelSet",
    "ExecutionConfig",
    "ExecutionProfile",
    "OperationExecutionConfig",
    "OperationSet",
    "OperationType",
    "ProtocolProfileConfig",
    "RedisBrokerConfig",
    "RedisConnectionConfig",
    "RedisCredentialReference",
    "RedisTransportConfig",
    "normalize_config",
]
