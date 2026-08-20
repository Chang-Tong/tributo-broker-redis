"""Redis provider runtime mapping protocol tasks to Tributo Ray Jobs."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from pydantic import ValidationError
from tributo.integrations.broker import (
    BrokerRuntime,
    CancellationSpec,
    EventReporterSpec,
    JobResult,
    Message,
    TaskDisposition,
    TaskOutcome,
)
from tributo.training.job_submitter import submit_training_job_with_identity

from tributo_broker_redis.capabilities import validate_supported_capabilities
from tributo_broker_redis.completion import redact_sensitive
from tributo_broker_redis.config import RedisBrokerConfig
from tributo_broker_redis.consumer import RedisTaskConsumer
from tributo_broker_redis.protocol import (
    TrainingJobRequest,
    check_protocol_version,
    is_training_task,
)
from tributo_broker_redis.redis_client import create_redis_client
from tributo_broker_redis.reporter import RedisEventReporter

logger = logging.getLogger(__name__)


def _validation_message(error: ValidationError) -> str:
    """Render Pydantic locations as canonical dotted field paths."""
    messages = []
    for item in error.errors(include_url=False):
        path = ".".join(str(part) for part in item["loc"])
        messages.append(f"{path}: {item['msg']}" if path else item["msg"])
    return "; ".join(messages)


class RedisBrokerRuntime(BrokerRuntime):
    """Provider runtime preserving the internal task/submit/event flow."""

    def __init__(self, config: RedisBrokerConfig) -> None:
        self.config = config
        self._redis = create_redis_client(config)
        self._consumer = RedisTaskConsumer(self._redis, config)

    @property
    def consumer(self) -> RedisTaskConsumer:
        return self._consumer

    def _report_invalid(
        self,
        message: Message,
        error: str,
        code: str,
    ) -> TaskOutcome:
        reporter = RedisEventReporter(
            self._redis,
            self.config,
            message.job_id,
            stream_key=(
                self.config.invalid_event_stream_key if not message.job_id else None
            ),
        )
        # Invalid messages must leave the queue even if FAILED publication is
        # unavailable.  The outcome is ACK independent of reporter success.
        reporter.report_failed_with_code(
            message.job_id,
            error,
            code,
            delivery_id=message.delivery_id,
            phase="QUEUED",
        )
        return TaskOutcome(
            disposition=TaskDisposition.ACK,
            error=error,
        )

    def handle(self, message: Message) -> TaskOutcome:
        if not message.job_id:
            return self._report_invalid(
                message,
                "Missing or invalid outer Redis job_id",
                "INVALID_JOB_ID",
            )
        payload_error = message.metadata.get("payload_error")
        if isinstance(payload_error, str) and payload_error:
            return self._report_invalid(message, payload_error, "PAYLOAD_TOO_LARGE")
        raw_payload = message.payload.get("raw")
        if not isinstance(raw_payload, str):
            return self._report_invalid(
                message,
                "payload must be a JSON string",
                "INVALID_PAYLOAD",
            )
        try:
            request_data = json.loads(raw_payload)
        except json.JSONDecodeError:
            return self._report_invalid(
                message,
                "Invalid JSON payload",
                "INVALID_PAYLOAD",
            )
        if not isinstance(request_data, dict):
            return self._report_invalid(
                message,
                "Payload root must be an object",
                "INVALID_PAYLOAD",
            )

        version_error = check_protocol_version(request_data)
        if version_error:
            return self._report_invalid(
                message,
                version_error,
                "UNSUPPORTED_PROTOCOL_VERSION",
            )
        if not is_training_task(request_data):
            return self._report_invalid(
                message,
                "Only training tasks are supported by broker v1",
                "UNSUPPORTED_TASK_TYPE",
            )

        # The outer Redis field is authoritative for event/cancel/idempotency
        # identity, matching the internal consumer behavior.
        request_data["job_id"] = message.job_id
        reporter = RedisEventReporter(self._redis, self.config, message.job_id)
        try:
            request = TrainingJobRequest.model_validate(request_data)
            if request.training_config is None:
                validate_supported_capabilities(request)
            training_config = request.resolve_training_config(
                allow_legacy_training_config=self.config.allow_legacy_training_config
            )
        except ValidationError as exc:
            return self._report_invalid(
                message, _validation_message(exc), "INVALID_PAYLOAD"
            )
        except Exception as exc:
            return self._report_invalid(message, str(exc), "INVALID_PAYLOAD")

        if self._is_cancelled(message.job_id):
            reporter.report_cancelled(message.job_id, "QUEUED")
            return TaskOutcome(disposition=TaskDisposition.ACK)

        # Redis delivery attempts are transport retries, not new Ray
        # execution attempts.  Reuse the same deterministic submission ID
        # after an ACK failure so a pending redelivery cannot create a second
        # Ray Job for the same business run.
        attempt_id = "attempt-1"
        cancellation = CancellationSpec(
            broker_id="knova-redis",
            job_id=message.job_id,
            options={"config_env": "TRIBUTO_BROKER_CONFIG_JSON"},
        )
        event_reporter = EventReporterSpec(
            broker_id="knova-redis",
            job_id=message.job_id,
            options={"config_env": "TRIBUTO_BROKER_CONFIG_JSON"},
        )
        execution_context = {
            "cancellation": cancellation.as_dict(),
            "event_reporter": event_reporter.as_dict(),
        }
        reporter.report_phase(message.job_id, "QUEUED")
        try:
            entrypoint = "python -m tributo_broker_redis.run_training"
            env_vars = dict(self.config.env_vars)
            worker_request_data = dict(request_data)
            # Protocol extensions are opaque Driver-side metadata. They are
            # intentionally never executed or copied into Ray worker env JSON.
            worker_request_data.pop("extensions", None)
            env_vars["TRIBUTO_BROKER_REQUEST_JSON"] = json.dumps(
                worker_request_data,
                separators=(",", ":"),
            )
            worker_config = self.config.model_dump(
                exclude={"env_vars", "worker_url", "extra_py_modules"}
            )
            if self.config.worker_url:
                worker_config["url"] = self.config.worker_url
            worker_config["password_env"] = (
                self.config.worker_password_env or self.config.password_env
            )
            worker_config.pop("worker_password_env", None)
            env_vars["TRIBUTO_BROKER_CONFIG_JSON"] = json.dumps(
                worker_config,
                separators=(",", ":"),
            )
            env_vars["TRIBUTO_TRAINING_CONFIG_JSON"] = json.dumps(
                training_config,
                separators=(",", ":"),
            )
            extra_py_modules: list[str | Path] = list(self.config.extra_py_modules)
            submission = submit_training_job_with_identity(
                entrypoint,
                dashboard_url=self.config.ray_dashboard_url,
                env_vars=env_vars,
                project_root=(
                    Path(self.config.project_root) if self.config.project_root else None
                ),
                run_id=message.job_id,
                attempt_id=attempt_id,
                extra_py_modules=extra_py_modules,
                runtime_pip_packages=self.config.runtime_pip_packages,
                execution_context=execution_context,
            )
        except Exception as exc:
            safe_error = redact_sensitive(str(exc))
            logger.warning(
                "Ray submission failed; leaving task pending: job_id=%s error=%s",
                message.job_id,
                safe_error,
            )
            return TaskOutcome(
                disposition=TaskDisposition.RETRY,
                error=safe_error,
            )

        return TaskOutcome(
            disposition=TaskDisposition.ACK,
            result=JobResult(
                job_id=message.job_id,
                status="accepted",
                run_id=submission.run_id,
                attempt_id=submission.attempt_id,
                execution_id=submission.job_id,
                submission_id=submission.submission_id,
            ),
        )

    def _is_cancelled(self, job_id: str) -> bool:
        try:
            return bool(self._redis.exists(self.config.cancel_key(job_id)))
        except Exception as exc:
            logger.warning(
                "Redis cancel check unavailable; continuing task: job_id=%s error=%s",
                job_id,
                type(exc).__name__,
            )
            return False

    def close(self) -> None:
        self._consumer.close()


def create_runtime(config: Mapping[str, Any]) -> RedisBrokerRuntime:
    return RedisBrokerRuntime(RedisBrokerConfig.from_mapping(dict(config)))
