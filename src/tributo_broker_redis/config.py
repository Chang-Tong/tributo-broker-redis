"""Provider-owned Redis configuration."""

from __future__ import annotations

import os
import socket
import uuid
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class RedisBrokerConfig(BaseModel):
    """Standalone, Sentinel, and Cluster Redis connection settings."""

    model_config = ConfigDict(extra="forbid")

    mode: Literal["standalone", "sentinel", "cluster"] = "standalone"
    url: str | None = None
    host: str = "127.0.0.1"
    port: int = Field(default=6379, ge=1, le=65535)
    username: str | None = None
    password_env: str | None = None
    db: int = Field(default=0, ge=0)
    ssl: bool = False
    worker_url: str | None = None
    sentinel_hosts: list[tuple[str, int]] = Field(default_factory=list)
    sentinel_service: str = "mymaster"
    sentinel_force_master_ip: str | None = None
    sentinel_address_map: dict[str, tuple[str, int]] = Field(default_factory=dict)
    cluster_startup_nodes: list[tuple[str, int]] = Field(default_factory=list)
    cluster_address_remap_host: str | None = None
    task_stream_key: str = "knova:training:tasks"
    event_stream_prefix: str = "knova:training:events"
    invalid_event_stream_key: str = "knova:training:events:invalid"
    cancel_key_prefix: str = "knova:training:cancel"
    consumer_group: str = "tributo"
    consumer_name: str = Field(default_factory=lambda: _default_consumer_name())
    group_start_id: str = "$"
    block_ms: int = Field(default=5000, ge=0)
    claim_idle_ms: int = Field(default=60000, ge=0)
    claim_count: int = Field(default=10, ge=1, le=1000)
    max_stream_length: int = Field(default=1000, ge=1)
    max_payload_bytes: int = Field(default=1024 * 1024, ge=1)
    max_event_bytes: int = Field(default=1024 * 1024, ge=1)
    max_publish_retries: int = Field(default=3, ge=0, le=10)
    publish_retry_delay: float = Field(default=0.2, ge=0, le=30)
    failure_log_interval: float = Field(default=5.0, ge=0, le=300)
    ray_dashboard_url: str = "http://127.0.0.1:8265"
    worker_password_env: str | None = None
    extra_py_modules: list[str] = Field(default_factory=list)
    runtime_pip_packages: list[str] = Field(default_factory=list)
    env_vars: dict[str, str] = Field(default_factory=dict)
    project_root: str | None = None
    allow_legacy_training_config: bool = False

    @field_validator("url", "worker_url")
    @classmethod
    def reject_url_credentials(cls, value: str | None) -> str | None:
        if value is not None:
            parsed = urlsplit(value)
            if parsed.username is not None or parsed.password is not None:
                raise ValueError(
                    "Redis URL must not contain credentials; use password_env "
                    "or worker_password_env"
                )
        return value

    @model_validator(mode="after")
    def validate_mode(self) -> RedisBrokerConfig:
        if self.mode == "sentinel" and not self.sentinel_hosts:
            raise ValueError("sentinel_hosts is required in sentinel mode")
        if self.mode == "cluster" and not self.cluster_startup_nodes:
            raise ValueError("cluster_startup_nodes is required in cluster mode")
        if self.mode == "cluster" and self.db != 0:
            raise ValueError("Redis Cluster mode requires db=0")
        if not self.group_start_id.strip():
            raise ValueError("group_start_id must not be empty")
        return self

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> RedisBrokerConfig:
        """Validate provider config without reading secret values into it."""
        return cls.model_validate(value)

    def password(self) -> str | None:
        """Resolve password only at client construction time."""
        return os.environ.get(self.password_env) if self.password_env else None

    def event_stream_key(self, job_id: str) -> str:
        return f"{self.event_stream_prefix}:{job_id}"

    def cancel_key(self, job_id: str) -> str:
        return f"{self.cancel_key_prefix}:{job_id}"


def _default_consumer_name() -> str:
    """Create a unique Redis consumer identity for each provider process."""
    hostname = socket.gethostname().replace(" ", "-")
    return f"tributo-{hostname}-{os.getpid()}-{uuid.uuid4().hex[:10]}"
