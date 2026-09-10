"""Provider-owned dual-channel consume loop and Ray Job admission."""

from __future__ import annotations

import base64
import logging
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, cast

from tributo.integrations.broker import (
    BrokerError,
    BrokerRuntime,
    Message,
    TaskDisposition,
    TaskOutcome,
)
from tributo.ray_jobs import RayJobSubmission, submit_ray_job

from tributo_broker_redis.cancellation import (
    ActiveSubmission,
    ActiveSubmissionMap,
    CancelWatcher,
)
from tributo_broker_redis.config import (
    ExecutionProfile,
    OperationType,
    RedisBrokerConfig,
    normalize_config,
)
from tributo_broker_redis.consumer import RedisTaskConsumer
from tributo_broker_redis.operations import (
    MappingFailure,
    PreparedOperation,
    prepare_operation,
)
from tributo_broker_redis.protocol import (
    DriverInput,
    GenericRequest,
    ProtocolFailure,
    parse_request,
)
from tributo_broker_redis.redis_client import create_redis_client
from tributo_broker_redis.reporter import RedisEventReporter

logger = logging.getLogger(__name__)
_DRIVER_ENV = "TRIBUTO_REDIS_DRIVER_INPUT_B64"
_MAX_DRIVER_INPUT_BYTES = 64 * 1024
_DEFAULT_DRIVER_ENTRYPOINT = "python -m tributo_broker_redis.execution_driver"


def validate_execution_environment(
    config: RedisBrokerConfig,
) -> None:
    """Fail before Redis consumption when the Ray driver cannot be distributed."""
    root = Path(config.execution.project_root).expanduser().resolve()
    if not root.is_dir() or not (
        (root / "src" / "tributo").is_dir() or (root / "tributo").is_dir()
    ):
        raise ValueError(
            "execution.project_root must contain the Tributo package in "
            "src/tributo or tributo"
        )
    for module in config.execution.extra_py_modules:
        if "://" not in module and not Path(module).expanduser().exists():
            raise ValueError("execution.extra_py_modules contains a missing local path")


class RedisBrokerRuntime(BrokerRuntime):
    """Healthy-path runtime for training and batch inference channels."""

    def __init__(
        self,
        config: RedisBrokerConfig,
        *,
        submitter: Callable[..., RayJobSubmission] = submit_ray_job,
        redis_client: Any | None = None,
        start_cancel_watcher: bool = False,
        request_parser: Callable[..., GenericRequest] = parse_request,
        operation_preparer: Callable[[GenericRequest], PreparedOperation] = (
            prepare_operation
        ),
        driver_entrypoint: str = _DEFAULT_DRIVER_ENTRYPOINT,
        reporter_factory: Callable[..., RedisEventReporter] = RedisEventReporter,
    ) -> None:
        if not driver_entrypoint.strip():
            raise ValueError("driver_entrypoint must not be empty")
        self.config = config
        self._submitter = submitter
        self._request_parser = request_parser
        self._operation_preparer = operation_preparer
        self._driver_entrypoint = driver_entrypoint
        self._reporter_factory = reporter_factory
        validate_execution_environment(config)
        self._redis = redis_client or create_redis_client(config)
        self._consumers: dict[OperationType, RedisTaskConsumer] = {
            operation_type: RedisTaskConsumer(
                self._redis,
                config.transport,
                config.channels.for_operation(operation_type),
                operation_type,
            )
            for operation_type in ("training", "batch_inference")
        }
        self.active_submissions = ActiveSubmissionMap()
        self._cancel_watcher = CancelWatcher(
            self._redis,
            self.active_submissions,
            dashboard_url=config.execution.ray_dashboard_url,
            interval_seconds=config.execution.cancel_poll_interval_seconds,
            max_event_bytes=config.transport.max_event_bytes,
            max_stream_length=config.transport.max_stream_length,
            reporter_factory=reporter_factory,
        )
        self._next_channel = 0
        self._closed = False
        if start_cancel_watcher:
            self._cancel_watcher.start()

    @property
    def consumer(self) -> RedisTaskConsumer:
        """Return one consumer only for the minimal Core contract surface."""
        return self._consumers["training"]

    @property
    def consumers(self) -> Mapping[OperationType, RedisTaskConsumer]:
        return self._consumers

    def start(self) -> None:
        self._cancel_watcher.start()

    def _reporter(
        self,
        *,
        operation_id: str,
        operation_type: OperationType,
        execution_profile: ExecutionProfile,
        run_id: str,
        attempt_id: str,
        submission: RayJobSubmission | None = None,
    ) -> RedisEventReporter:
        channel = self.config.channels.for_operation(operation_type)
        return self._reporter_factory(
            self._redis,
            event_stream_prefix=channel.event_stream_prefix,
            operation_id=operation_id,
            operation_type=operation_type,
            execution_profile=execution_profile,
            run_id=run_id,
            attempt_id=attempt_id,
            submission_id=submission.submission_id if submission else None,
            ray_job_id=submission.ray_job_id if submission else None,
            outer_identity_field=channel.outer_identity_field,
            max_event_bytes=self.config.transport.max_event_bytes,
            max_stream_length=self.config.transport.max_stream_length,
        )

    def _invalid(
        self,
        message: Message,
        *,
        operation_type: OperationType,
        code: str,
        sanitized_message: str,
        execution_profile: ExecutionProfile = "single_worker",
        run_id: str | None = None,
        attempt_id: str = "attempt-1",
    ) -> TaskOutcome:
        operation_id = message.metadata.get("outer_operation_id") or (
            f"invalid-{message.delivery_token}"
        )
        try:
            self._reporter(
                operation_id=operation_id,
                operation_type=operation_type,
                execution_profile=execution_profile,
                run_id=run_id or operation_id,
                attempt_id=attempt_id,
            ).publish(
                "FAILED",
                {
                    "error_code": code,
                    "sanitized_message": sanitized_message,
                    "retryable": False,
                },
                phase="ADMISSION",
            )
        except Exception:
            logger.warning(
                "Could not publish invalid-request event: delivery=%s code=%s",
                message.delivery_token,
                code,
                exc_info=True,
            )
        return TaskOutcome(
            TaskDisposition.ACK,
            BrokerError(code=code, sanitized_message=sanitized_message),
        )

    def handle(self, message: Message) -> TaskOutcome:
        operation_type = cast(OperationType, message.metadata["operation_type"])
        outer_operation_id = message.metadata.get("outer_operation_id", "")
        if not outer_operation_id:
            return self._invalid(
                message,
                operation_type=operation_type,
                code="INVALID_OPERATION_ID",
                sanitized_message="outer operation_id is required",
            )
        if message.metadata.get("envelope_error"):
            return self._invalid(
                message,
                operation_type=operation_type,
                code="INVALID_ENVELOPE",
                sanitized_message=(
                    "task envelope must contain only identity and payload"
                ),
            )
        if message.metadata.get("payload_error"):
            return self._invalid(
                message,
                operation_type=operation_type,
                code="PAYLOAD_TOO_LARGE",
                sanitized_message="payload exceeds configured size limit",
            )
        raw = message.payload.get("raw") if isinstance(message.payload, dict) else None
        if not isinstance(raw, str):
            return self._invalid(
                message,
                operation_type=operation_type,
                code="INVALID_REQUEST",
                sanitized_message="payload must be a JSON string",
            )
        try:
            request = self._request_parser(
                raw,
                outer_operation_id=outer_operation_id,
                expected_operation_type=operation_type,
            )
        except ProtocolFailure as exc:
            return self._invalid(
                message,
                operation_type=operation_type,
                code=exc.code,
                sanitized_message=exc.sanitized_message,
            )
        try:
            operation_config = self.config.operations.for_operation(operation_type)
            if request.execution_profile not in operation_config.execution_profiles:
                raise MappingFailure(
                    "UNSUPPORTED_EXECUTION_PROFILE",
                    "execution profile is disabled by provider configuration",
                )
            prepared = self._operation_preparer(request)
        except MappingFailure as exc:
            return self._invalid(
                message,
                operation_type=operation_type,
                code=exc.code,
                sanitized_message=exc.sanitized_message,
                execution_profile=request.execution_profile,
                run_id=request.run_id or request.operation_id,
                attempt_id=request.attempt_id,
            )

        channel = self.config.channels.for_operation(operation_type)
        run_id = request.run_id or request.operation_id
        reporter = self._reporter(
            operation_id=request.operation_id,
            operation_type=operation_type,
            execution_profile=request.execution_profile,
            run_id=run_id,
            attempt_id=request.attempt_id,
        )
        try:
            if bool(self._redis.exists(channel.cancel_key(request.operation_id))):
                reporter.publish(
                    "CANCELLED",
                    {"reason": "cancelled before admission"},
                    phase="QUEUED",
                )
                return TaskOutcome(TaskDisposition.ACK)
        except Exception:
            return TaskOutcome(
                TaskDisposition.RETRY,
                BrokerError(
                    code="CANCEL_CHECK_UNAVAILABLE",
                    sanitized_message="queued cancellation check unavailable",
                ),
            )

        namespace = f"redis-{operation_type.replace('_', '-')}"
        driver_input = DriverInput(
            operation_id=request.operation_id,
            operation_type=operation_type,
            execution_profile=request.execution_profile,
            run_id=run_id,
            attempt_id=request.attempt_id,
            credential_ref=prepared.credential_ref,
            operation_payload=prepared.operation_payload,
            redis_url=self.config.transport.ray_driver_url,
            event_stream_prefix=channel.event_stream_prefix,
            outer_identity_field=channel.outer_identity_field,
            max_event_bytes=self.config.transport.max_event_bytes,
            max_stream_length=self.config.transport.max_stream_length,
        )
        encoded_driver_input = base64.urlsafe_b64encode(
            driver_input.model_dump_json().encode("utf-8")
        ).decode("ascii")
        if len(encoded_driver_input.encode("ascii")) > _MAX_DRIVER_INPUT_BYTES:
            return self._invalid(
                message,
                operation_type=operation_type,
                code="DRIVER_INPUT_TOO_LARGE",
                sanitized_message="validated driver input exceeds transport limit",
            )

        env_vars = dict(self.config.execution.env_vars)
        env_vars[_DRIVER_ENV] = encoded_driver_input
        try:
            submission = self._submitter(
                self._driver_entrypoint,
                operation_namespace=namespace,
                run_id=run_id,
                attempt_id=request.attempt_id,
                dashboard_url=self.config.execution.ray_dashboard_url,
                env_vars=env_vars,
                project_root=Path(self.config.execution.project_root)
                .expanduser()
                .resolve(),
                extra_py_modules=[
                    module
                    if "://" in module
                    else str(Path(module).expanduser().resolve())
                    for module in self.config.execution.extra_py_modules
                ],
                runtime_pip_packages=list(self.config.execution.runtime_pip_packages),
                metadata={
                    "tributo.operation_id": request.operation_id,
                    "tributo.operation_type": operation_type,
                    "tributo.execution_profile": request.execution_profile,
                    "tributo.protocol_profile": request.protocol_profile,
                },
                request_digest=request.request_digest,
                entrypoint_num_cpus=self.config.execution.entrypoint_num_cpus,
            )
            active = ActiveSubmission(
                operation_id=request.operation_id,
                operation_type=operation_type,
                execution_profile=request.execution_profile,
                run_id=run_id,
                channel=channel,
                submission=submission,
            )
            self.active_submissions.put(active)
            self._reporter(
                operation_id=request.operation_id,
                operation_type=operation_type,
                execution_profile=request.execution_profile,
                run_id=run_id,
                attempt_id=request.attempt_id,
                submission=submission,
            ).publish(
                "ACCEPTED",
                {
                    "submission_id": submission.submission_id,
                    "ray_job_id": submission.ray_job_id,
                },
                phase="ADMITTED",
            )
        except Exception:
            logger.warning(
                "Ray admission is not confirmed: operation_id=%s",
                request.operation_id,
                exc_info=True,
            )
            return TaskOutcome(
                TaskDisposition.RETRY,
                BrokerError(
                    code="RAY_ADMISSION_UNKNOWN",
                    sanitized_message="Ray admission could not be confirmed",
                ),
            )
        return TaskOutcome(TaskDisposition.ACK)

    def _apply_outcome(
        self, consumer: RedisTaskConsumer, message: Message, outcome: TaskOutcome
    ) -> None:
        if outcome.disposition == TaskDisposition.ACK:
            consumer.ack(message)
        elif outcome.disposition == TaskDisposition.RETRY:
            consumer.retry(message, outcome.error)
        else:
            consumer.reject(message, outcome.error)

    def run_once(self, timeout_ms: int | None = None) -> bool:
        operations: tuple[OperationType, ...] = ("training", "batch_inference")
        per_channel_timeout = (
            self.config.transport.block_ms
            if timeout_ms is None
            else max(0, timeout_ms // len(operations))
        )
        for offset in range(len(operations)):
            index = (self._next_channel + offset) % len(operations)
            operation_type = operations[index]
            consumer = self._consumers[operation_type]
            message = consumer.poll(per_channel_timeout)
            if message is None:
                continue
            outcome = self.handle(message)
            self._apply_outcome(consumer, message, outcome)
            self._next_channel = (index + 1) % len(operations)
            return True
        return False

    def run_forever(self) -> None:
        self.start()
        while not self._closed:
            self.run_once()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._cancel_watcher.close()
        close = getattr(self._redis, "close", None)
        if callable(close):
            close()


def create_runtime(config: Mapping[str, Any]) -> RedisBrokerRuntime:
    return RedisBrokerRuntime(normalize_config(config))


__all__ = [
    "RedisBrokerRuntime",
    "create_runtime",
    "validate_execution_environment",
]
