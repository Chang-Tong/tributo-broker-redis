"""Provider-owned dual-channel consume loop and Ray Job admission."""

from __future__ import annotations

import base64
import logging
import time
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Any, cast

from ray.job_submission import JobSubmissionClient
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
    terminal_event_published,
)
from tributo_broker_redis.config import (
    ExecutionProfile,
    OperationType,
    RedisBrokerConfig,
    normalize_config,
)
from tributo_broker_redis.consumer import RedisTaskConsumer
from tributo_broker_redis.driver_transport import driver_transport
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
_DEFAULT_DRIVER_ENTRYPOINT = "python -m tributo_broker_redis.execution_driver"
_RECOVERABLE_RAY_STATUSES = frozenset(
    {"PENDING", "RUNNING", "FAILED", "STOPPED", "SUCCEEDED"}
)
_PENDING_RECOVERY_INTERVAL_SECONDS = 1.0


def _metadata_milliseconds(metadata: Mapping[str, Any], key: str) -> int | None:
    value = metadata.get(key)
    try:
        return int(value) if value is not None and int(value) >= 0 else None
    except (TypeError, ValueError):
        return None


def _list_ray_jobs(dashboard_url: str) -> Iterable[Any]:
    return JobSubmissionClient(dashboard_url).list_jobs()


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
        recovery_driver_entrypoints: tuple[str, ...] = (),
        reporter_factory: Callable[..., RedisEventReporter] = RedisEventReporter,
        job_lister: Callable[[str], Iterable[Any]] = _list_ray_jobs,
    ) -> None:
        if not driver_entrypoint.strip():
            raise ValueError("driver_entrypoint must not be empty")
        self.config = config
        self._submitter = submitter
        self._request_parser = request_parser
        self._operation_preparer = operation_preparer
        self._driver_entrypoint = driver_entrypoint
        self._recovery_driver_entrypoints = {
            driver_entrypoint,
            *recovery_driver_entrypoints,
        }
        self._reporter_factory = reporter_factory
        self._job_lister = job_lister
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
        self._next_pending_recovery = 0.0
        self._closed = False
        self._started = False
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
        if self._started:
            return
        logger.info(
            "Starting Redis broker consumer: ray_dashboard=%s operations=%s",
            self.config.execution.ray_dashboard_url,
            ",".join(self._consumers),
        )
        self._recover_active_submissions()
        self._recover_pending()
        self._cancel_watcher.start()
        self._started = True
        logger.info("Redis broker consumer started")

    def _recover_pending(self) -> None:
        """Advance one native XAUTOCLAIM page per channel at a bounded cadence."""
        now = time.monotonic()
        if now < self._next_pending_recovery:
            return
        self._next_pending_recovery = now + _PENDING_RECOVERY_INTERVAL_SECONDS
        for consumer in self._consumers.values():
            try:
                consumer.recover_pending()
            except Exception as exc:
                logger.warning(
                    "Pending delivery recovery failed; will retry: "
                    "operation_type=%s error_type=%s",
                    consumer.operation_type,
                    type(exc).__name__,
                )

    def _recover_active_submissions(self) -> None:
        """Rebuild cancellation/watchdog state from credential-free Ray metadata."""
        jobs = self._job_lister(self.config.execution.ray_dashboard_url)
        for job in jobs:
            raw_status = getattr(job, "status", "")
            status = str(getattr(raw_status, "value", raw_status)).upper()
            if status not in _RECOVERABLE_RAY_STATUSES:
                continue
            metadata = getattr(job, "metadata", None)
            if not isinstance(metadata, Mapping):
                continue
            operation_id = metadata.get("tributo.operation_id")
            operation_type = metadata.get("tributo.operation_type")
            execution_profile = metadata.get("tributo.execution_profile")
            run_id = metadata.get("tributo.run_id")
            attempt_id = metadata.get("tributo.attempt_id")
            driver_entrypoint = metadata.get("tributo.driver_entrypoint")
            task_stream = metadata.get("tributo.task_stream")
            submission_id = getattr(job, "submission_id", None)
            required = (
                operation_id,
                operation_type,
                execution_profile,
                run_id,
                attempt_id,
                driver_entrypoint,
                task_stream,
                submission_id,
            )
            if (
                not all(isinstance(value, str) and value for value in required)
                or operation_type not in {"training", "batch_inference"}
                or execution_profile not in {"single_worker", "distributed"}
                or driver_entrypoint not in self._recovery_driver_entrypoints
            ):
                continue
            channel = self.config.channels.for_operation(
                cast(OperationType, operation_type)
            )
            if task_stream != channel.task_stream_key:
                continue
            ray_job_id = getattr(job, "job_id", None)
            if not isinstance(ray_job_id, str) or not ray_job_id:
                ray_job_id = None
            request_digest = metadata.get("tributo.request_digest")
            if request_digest is not None and (
                not isinstance(request_digest, str) or not request_digest
            ):
                continue
            if self.active_submissions.get(cast(str, operation_id)) is not None:
                logger.warning(
                    "Ignoring duplicate active Ray Job metadata: operation_id=%s",
                    operation_id,
                )
                continue
            self.active_submissions.put(
                ActiveSubmission(
                    operation_id=cast(str, operation_id),
                    operation_type=cast(OperationType, operation_type),
                    execution_profile=cast(ExecutionProfile, execution_profile),
                    run_id=cast(str, run_id),
                    channel=channel,
                    received_at_ms=_metadata_milliseconds(
                        metadata, "tributo.received_at_ms"
                    ),
                    deadline_at_ms=_metadata_milliseconds(
                        metadata, "tributo.deadline_at_ms"
                    ),
                    submission=RayJobSubmission(
                        run_id=cast(str, run_id),
                        attempt_id=cast(str, attempt_id),
                        submission_id=cast(str, submission_id),
                        ray_job_id=ray_job_id,
                        request_digest=request_digest,
                    ),
                )
            )
            recovered = self.active_submissions.get(cast(str, operation_id))
            if recovered is not None and terminal_event_published(
                self._redis, recovered
            ):
                self.active_submissions.remove(recovered.operation_id)
                continue
            logger.info(
                "Recovered active Ray job: operation_id=%s operation_type=%s "
                "submission_id=%s ray_job_id=%s status=%s",
                operation_id,
                operation_type,
                submission_id,
                ray_job_id or "<unknown>",
                status,
            )

    def _resume_existing_submission(
        self,
        request: GenericRequest,
        operation_type: OperationType,
    ) -> TaskOutcome | None:
        """Acknowledge an exact recovered delivery without submitting a second job."""
        existing = self.active_submissions.get(request.operation_id)
        if existing is None:
            return None
        run_id = request.run_id or request.operation_id
        if (
            existing.operation_type != operation_type
            or existing.execution_profile != request.execution_profile
            or existing.run_id != run_id
            or existing.submission.attempt_id != request.attempt_id
            or request.request_digest is None
            or existing.submission.request_digest is None
            or existing.submission.request_digest != request.request_digest
        ):
            return TaskOutcome(
                TaskDisposition.RETRY,
                BrokerError(
                    code="OPERATION_ALREADY_ACTIVE",
                    sanitized_message=(
                        "operation_id is already active with different identity"
                    ),
                ),
            )
        if terminal_event_published(self._redis, existing):
            logger.info(
                "Recovered operation is already terminal: operation_id=%s "
                "submission_id=%s ray_job_id=%s",
                request.operation_id,
                existing.submission.submission_id,
                existing.submission.ray_job_id or "<unknown>",
            )
            self.active_submissions.remove(request.operation_id)
            return TaskOutcome(TaskDisposition.ACK)
        try:
            logger.info(
                "Reconfirming recovered Ray admission: operation_id=%s "
                "submission_id=%s ray_job_id=%s",
                request.operation_id,
                existing.submission.submission_id,
                existing.submission.ray_job_id or "<unknown>",
            )
            self._reporter(
                operation_id=request.operation_id,
                operation_type=operation_type,
                execution_profile=request.execution_profile,
                run_id=run_id,
                attempt_id=request.attempt_id,
                submission=existing.submission,
                received_at_ms=existing.received_at_ms,
            ).publish(
                "ACCEPTED",
                {
                    "submission_id": existing.submission.submission_id,
                    "ray_job_id": existing.submission.ray_job_id,
                    "recovered": True,
                },
                phase="ADMITTED",
            )
        except Exception:
            logger.warning(
                "Could not confirm recovered Ray admission: operation_id=%s",
                request.operation_id,
                exc_info=True,
            )
            return TaskOutcome(
                TaskDisposition.RETRY,
                BrokerError(
                    code="RECOVERED_ADMISSION_UNCONFIRMED",
                    sanitized_message="recovered Ray admission could not be confirmed",
                ),
            )
        return TaskOutcome(TaskDisposition.ACK)

    def _reporter(
        self,
        *,
        operation_id: str,
        operation_type: OperationType,
        execution_profile: ExecutionProfile,
        run_id: str,
        attempt_id: str,
        submission: RayJobSubmission | None = None,
        received_at_ms: int | str | None = None,
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
            received_at_ms=int(received_at_ms) if received_at_ms is not None else None,
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
        logger.warning(
            "Rejecting invalid Redis task: operation_type=%s operation_id=%s "
            "delivery=%s code=%s message=%s",
            operation_type,
            operation_id,
            message.delivery_token,
            code,
            sanitized_message,
        )
        try:
            self._reporter(
                operation_id=operation_id,
                operation_type=operation_type,
                execution_profile=execution_profile,
                run_id=run_id or operation_id,
                attempt_id=attempt_id,
                received_at_ms=message.metadata.get("received_at_ms"),
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
        logger.debug(
            "Handling Redis delivery: operation_type=%s operation_id=%s delivery=%s",
            operation_type,
            outer_operation_id or "<missing>",
            message.delivery_token,
        )
        if not outer_operation_id:
            return self._invalid(
                message,
                operation_type=operation_type,
                code="INVALID_OPERATION_ID",
                sanitized_message="outer operation_id is required",
            )
        # Cancellation is an envelope-level control. It must win over malformed
        # JSON and operation schema validation, using the recoverable outer ID.
        channel = self.config.channels.for_operation(operation_type)
        try:
            if bool(self._redis.exists(channel.cancel_key(outer_operation_id))):
                existing = self.active_submissions.get(outer_operation_id)
                if existing is None:
                    self._reporter(
                        operation_id=outer_operation_id,
                        operation_type=operation_type,
                        execution_profile="single_worker",
                        run_id=outer_operation_id,
                        attempt_id="attempt-1",
                        received_at_ms=message.metadata.get("received_at_ms"),
                    ).publish(
                        "CANCELLED",
                        {"reason": "cancelled before admission"},
                        phase="QUEUED",
                    )
                # An active Ray job must first be stopped by its lifecycle watcher.
                return TaskOutcome(TaskDisposition.ACK)
        except Exception:
            return TaskOutcome(
                TaskDisposition.RETRY,
                BrokerError(
                    code="CANCEL_CHECK_UNAVAILABLE",
                    sanitized_message="queued cancellation check unavailable",
                ),
            )
        if message.metadata.get("encoding_error"):
            return self._invalid(
                message,
                operation_type=operation_type,
                code="INVALID_PAYLOAD",
                sanitized_message="payload is not valid UTF-8",
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
        recovered_outcome = self._resume_existing_submission(request, operation_type)
        if recovered_outcome is not None:
            return recovered_outcome
        reporter = self._reporter(
            operation_id=request.operation_id,
            operation_type=operation_type,
            execution_profile=request.execution_profile,
            run_id=run_id,
            attempt_id=request.attempt_id,
            received_at_ms=message.metadata.get("received_at_ms"),
        )
        try:
            if bool(self._redis.exists(channel.cancel_key(request.operation_id))):
                logger.info(
                    "Operation cancelled before Ray admission: operation_id=%s "
                    "operation_type=%s",
                    request.operation_id,
                    operation_type,
                )
                reporter.publish(
                    "CANCELLED",
                    {"reason": "cancelled before admission"},
                    phase="QUEUED",
                )
                return TaskOutcome(TaskDisposition.ACK)
        except Exception:
            logger.warning(
                "Could not check cancellation marker: operation_id=%s "
                "operation_type=%s",
                request.operation_id,
                operation_type,
                exc_info=True,
            )
            return TaskOutcome(
                TaskDisposition.RETRY,
                BrokerError(
                    code="CANCEL_CHECK_UNAVAILABLE",
                    sanitized_message="queued cancellation check unavailable",
                ),
            )

        namespace = f"redis-{operation_type.replace('_', '-')}"
        received_at_ms = int(
            message.metadata.get("received_at_ms") or time.time() * 1000
        )
        deadline_at_ms = (
            received_at_ms + int(prepared.timeout_seconds * 1000)
            if prepared.timeout_seconds is not None
            else None
        )
        driver_input = DriverInput(
            operation_id=request.operation_id,
            operation_type=operation_type,
            execution_profile=request.execution_profile,
            run_id=run_id,
            attempt_id=request.attempt_id,
            credential_ref=prepared.credential_ref,
            operation_payload=prepared.operation_payload,
            redis_transport=self.config.transport.ray_driver_connection(),
            event_stream_prefix=channel.event_stream_prefix,
            outer_identity_field=channel.outer_identity_field,
            max_event_bytes=self.config.transport.max_event_bytes,
            max_stream_length=self.config.transport.max_stream_length,
            received_at_ms=received_at_ms,
            deadline_at_ms=deadline_at_ms,
            cancel_key=channel.cancel_key(request.operation_id),
        )
        encoded_driver_input = base64.urlsafe_b64encode(
            driver_input.model_dump_json().encode("utf-8")
        ).decode("ascii")
        if (
            len(encoded_driver_input.encode("ascii"))
            > self.config.transport.max_driver_input_bytes
        ):
            logger.warning(
                "Ray driver input exceeds configured limit: operation_id=%s "
                "operation_type=%s bytes=%d limit=%d",
                request.operation_id,
                operation_type,
                len(encoded_driver_input.encode("ascii")),
                self.config.transport.max_driver_input_bytes,
            )
            return self._invalid(
                message,
                operation_type=operation_type,
                code="DRIVER_INPUT_TOO_LARGE",
                sanitized_message="validated driver input exceeds transport limit",
            )

        env_vars = dict(self.config.execution.env_vars)
        try:
            logger.info(
                "Submitting Ray job: operation_id=%s operation_type=%s "
                "run_id=%s attempt_id=%s driver_input_bytes=%d",
                request.operation_id,
                operation_type,
                run_id,
                request.attempt_id,
                len(encoded_driver_input.encode("ascii")),
            )
            modules = [
                module if "://" in module else str(Path(module).expanduser().resolve())
                for module in self.config.execution.extra_py_modules
            ]
            with driver_transport(encoded_driver_input, env_vars, modules) as (
                env_vars,
                modules,
            ):
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
                    extra_py_modules=modules,
                    runtime_pip_packages=list(
                        self.config.execution.runtime_pip_packages
                    ),
                    metadata={
                        "tributo.operation_id": request.operation_id,
                        "tributo.operation_type": operation_type,
                        "tributo.execution_profile": request.execution_profile,
                        "tributo.protocol_profile": self.config.protocol.profile,
                        "tributo.run_id": run_id,
                        "tributo.attempt_id": request.attempt_id,
                        "tributo.driver_entrypoint": self._driver_entrypoint,
                        "tributo.task_stream": channel.task_stream_key,
                        "tributo.received_at_ms": str(received_at_ms),
                        **(
                            {"tributo.deadline_at_ms": str(deadline_at_ms)}
                            if deadline_at_ms is not None
                            else {}
                        ),
                    },
                    request_digest=request.request_digest,
                    entrypoint_num_cpus=self.config.execution.entrypoint_num_cpus,
                )
            logger.info(
                "Ray job admitted: operation_id=%s operation_type=%s "
                "submission_id=%s ray_job_id=%s",
                request.operation_id,
                operation_type,
                submission.submission_id,
                submission.ray_job_id or "<unknown>",
            )
            active = ActiveSubmission(
                operation_id=request.operation_id,
                operation_type=operation_type,
                execution_profile=request.execution_profile,
                run_id=run_id,
                channel=channel,
                submission=submission,
                received_at_ms=received_at_ms,
                deadline_at_ms=deadline_at_ms,
            )
            self.active_submissions.put(active)
            self._reporter(
                operation_id=request.operation_id,
                operation_type=operation_type,
                execution_profile=request.execution_profile,
                run_id=run_id,
                attempt_id=request.attempt_id,
                submission=submission,
                received_at_ms=received_at_ms,
            ).publish(
                "ACCEPTED",
                {
                    "submission_id": submission.submission_id,
                    "ray_job_id": submission.ray_job_id,
                },
                phase="ADMITTED",
            )
            logger.info(
                "Admission event published: operation_id=%s submission_id=%s "
                "ray_job_id=%s",
                request.operation_id,
                submission.submission_id,
                submission.ray_job_id or "<unknown>",
            )
        except Exception:
            logger.warning(
                "Ray admission is not confirmed: operation_id=%s "
                "operation_type=%s delivery=%s",
                request.operation_id,
                operation_type,
                message.delivery_token,
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
        logger.debug(
            "Applying Redis delivery outcome: operation_type=%s operation_id=%s "
            "delivery=%s disposition=%s error_code=%s",
            consumer.operation_type,
            message.metadata.get("outer_operation_id") or "<missing>",
            message.delivery_token,
            outcome.disposition,
            outcome.error.code if outcome.error else "<none>",
        )
        if outcome.disposition == TaskDisposition.ACK:
            consumer.ack(message)
        elif outcome.disposition == TaskDisposition.RETRY:
            consumer.retry(message, outcome.error)
        else:
            consumer.reject(message, outcome.error)

    def run_once(self, timeout_ms: int | None = None) -> bool:
        self._recover_pending()
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
        logger.info("Stopping Redis broker consumer")
        self._cancel_watcher.close()
        close = getattr(self._redis, "close", None)
        if callable(close):
            close()
        logger.info("Redis broker consumer stopped")


def create_runtime(config: Mapping[str, Any]) -> RedisBrokerRuntime:
    parsed = normalize_config(config)
    if parsed.protocol.profile == "aimodel-v2":
        from tributo_broker_redis.profiles.aimodel_v2.broker import (
            create_runtime as create_v2_runtime,
        )

        return create_v2_runtime(parsed)
    return RedisBrokerRuntime(parsed)


__all__ = [
    "RedisBrokerRuntime",
    "create_runtime",
    "validate_execution_environment",
]
