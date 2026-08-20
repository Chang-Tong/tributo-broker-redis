"""Unit tests for Redis consumer/runtime semantics with a fake client."""

from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock, patch

import pytest
from redis.exceptions import ResponseError
from tributo.integrations.broker import Message, TaskDisposition, TaskOutcome
from tributo.integrations.broker_runner import BrokerRunner, BrokerRunnerState

from tributo_broker_redis.cancellation import RedisCancellationChecker
from tributo_broker_redis.config import RedisBrokerConfig
from tributo_broker_redis.consumer import RedisTaskConsumer
from tributo_broker_redis.runtime import RedisBrokerRuntime


def test_consumer_decodes_outer_job_id_and_acknowledges_delivery() -> None:
    client = MagicMock()
    client.xreadgroup.return_value = [
        ("tasks", [("7-0", {"job_id": "job-1", "payload": "{}"})])
    ]
    consumer = RedisTaskConsumer.__new__(RedisTaskConsumer)
    consumer._redis = client
    consumer._config = RedisBrokerConfig()
    consumer._pending_messages = []
    consumer._delivery_attempts = {}
    message = consumer.poll(10)
    assert message is not None
    assert message.job_id == "job-1"
    assert message.delivery_id == "7-0"
    consumer.ack(message)
    client.xack.assert_called_once_with("knova:training:tasks", "tributo", "7-0")


def test_consumer_uses_configured_block_timeout_for_default_poll() -> None:
    client = MagicMock()
    client.xreadgroup.return_value = []
    consumer = RedisTaskConsumer.__new__(RedisTaskConsumer)
    consumer._redis = client
    consumer._config = RedisBrokerConfig(block_ms=123)
    consumer._pending_messages = []
    consumer._delivery_attempts = {}
    assert consumer.poll() is None
    assert client.xreadgroup.call_args.kwargs["block"] == 123


def test_consumer_ignores_only_redis_busy_group_error() -> None:
    client = MagicMock()
    client.xgroup_create.side_effect = ResponseError("BUSYGROUP group exists")
    RedisTaskConsumer(client, RedisBrokerConfig())
    client.xgroup_create.assert_called_once()


def test_consumer_uses_configured_group_start_id() -> None:
    client = MagicMock()
    RedisTaskConsumer(client, RedisBrokerConfig(group_start_id="0-0"))
    assert client.xgroup_create.call_args.kwargs["id"] == "0-0"


def test_consumer_does_not_swallow_other_response_errors() -> None:
    client = MagicMock()
    client.xgroup_create.side_effect = ResponseError("WRONGTYPE not a stream")
    with pytest.raises(ResponseError, match="WRONGTYPE"):
        RedisTaskConsumer(client, RedisBrokerConfig())


def test_consumer_reclaims_pending_messages_for_next_poll() -> None:
    client = MagicMock()
    client.xautoclaim.return_value = [
        "8-0",
        [("7-0", {"job_id": "job-1", "payload": "{}"})],
        [],
    ]
    consumer = RedisTaskConsumer.__new__(RedisTaskConsumer)
    consumer._redis = client
    consumer._config = RedisBrokerConfig(claim_idle_ms=0)
    consumer._pending_messages = []
    consumer._delivery_attempts = {}
    assert consumer.recover_pending() == 1
    message = consumer.poll(0)
    assert message is not None
    assert message.delivery_attempt == 1
    assert message.job_id == "job-1"


def test_consumer_advances_xautoclaim_cursor_and_uses_claim_count() -> None:
    client = MagicMock()
    client.xautoclaim.side_effect = [
        ["10-0", [("7-0", {"job_id": "job-1", "payload": "{}"})], []],
        ["0-0", [], []],
    ]
    consumer = RedisTaskConsumer.__new__(RedisTaskConsumer)
    consumer._redis = client
    consumer._config = RedisBrokerConfig(claim_idle_ms=0, claim_count=37)
    consumer._pending_messages = []
    consumer._delivery_attempts = {}
    assert consumer.recover_pending() == 1
    assert consumer.recover_pending() == 0
    assert client.xautoclaim.call_args_list[0].kwargs["start_id"] == "0-0"
    assert client.xautoclaim.call_args_list[1].kwargs["start_id"] == "10-0"
    assert client.xautoclaim.call_args_list[0].kwargs["count"] == 37


def test_consumer_does_not_synthesize_job_id_for_invalid_envelope() -> None:
    client = MagicMock()
    client.xreadgroup.return_value = [("tasks", [("7-0", {"payload": "{}"})])]
    consumer = RedisTaskConsumer.__new__(RedisTaskConsumer)
    consumer._redis = client
    consumer._config = RedisBrokerConfig()
    consumer._pending_messages = []
    consumer._delivery_attempts = {}
    message = consumer.poll(10)
    assert message is not None
    assert message.job_id is None


def test_invalid_payload_is_acked_even_when_failed_event_cannot_publish() -> None:
    config = RedisBrokerConfig(max_publish_retries=0)
    client = MagicMock()
    client.xadd.side_effect = ConnectionError("redis down")
    runtime = RedisBrokerRuntime.__new__(RedisBrokerRuntime)
    runtime.config = config
    runtime._redis = client
    result = runtime._report_invalid(
        Message("job-1", {}, delivery_id="7-0"),
        "bad",
        "INVALID_PAYLOAD",
    )
    assert result.disposition == TaskDisposition.ACK


def test_missing_job_id_is_failed_and_acked_on_dedicated_stream() -> None:
    runtime = RedisBrokerRuntime.__new__(RedisBrokerRuntime)
    runtime.config = RedisBrokerConfig(allow_legacy_training_config=True)
    runtime._redis = MagicMock()
    runtime._consumer = MagicMock()
    result = runtime.handle(Message(None, {"raw": "{}"}, delivery_id="7-0"))
    assert result.disposition == TaskDisposition.ACK
    stream, values = runtime._redis.xadd.call_args.args[:2]
    assert stream == runtime.config.invalid_event_stream_key
    assert json.loads(values["payload"])["job_id"] is None


def test_runtime_passes_business_job_id_to_submission() -> None:
    config = RedisBrokerConfig(
        allow_legacy_training_config=True,
        extra_py_modules=["/provider/tributo_broker_redis"],
        worker_password_env="WORKER_REDIS_PASSWORD",
    )
    client = MagicMock()
    runtime = RedisBrokerRuntime.__new__(RedisBrokerRuntime)
    runtime.config = config
    runtime._redis = client
    runtime._consumer = MagicMock()
    object.__setattr__(runtime, "_is_cancelled", lambda _job_id: False)
    with patch(
        "tributo_broker_redis.runtime.submit_training_job_with_identity",
        return_value=MagicMock(
            run_id="business-job-1",
            attempt_id="attempt-1",
            job_id="ray-job-1",
            submission_id="submission-1",
        ),
    ) as submit:
        result = runtime.handle(
            Message(
                "business-job-1",
                {
                    "raw": json.dumps(
                        {
                            "protocol_version": "2.0",
                            "job_id": "payload-job",
                            "model_id": "model-1",
                            "version_id": "version-1",
                            "tenant_id": "tenant-1",
                            "training_config": {"data": {"type": "csv"}},
                        }
                    )
                },
            )
        )
    assert result.disposition == TaskDisposition.ACK
    assert submit.call_args.kwargs["run_id"] == "business-job-1"
    assert submit.call_args.kwargs["attempt_id"] == "attempt-1"
    assert submit.call_args.kwargs["project_root"] is None
    assert submit.call_args.kwargs["extra_py_modules"] == [
        "/provider/tributo_broker_redis"
    ]
    worker_config = json.loads(
        submit.call_args.kwargs["env_vars"]["TRIBUTO_BROKER_CONFIG_JSON"]
    )
    assert worker_config["password_env"] == "WORKER_REDIS_PASSWORD"


def test_runtime_rejects_legacy_training_config_without_explicit_opt_in() -> None:
    runtime = RedisBrokerRuntime.__new__(RedisBrokerRuntime)
    runtime.config = RedisBrokerConfig()
    runtime._redis = MagicMock()
    runtime._consumer = MagicMock()

    with patch(
        "tributo_broker_redis.runtime.submit_training_job_with_identity"
    ) as submit:
        outcome = runtime.handle(
            Message(
                "job-1",
                {
                    "raw": json.dumps(
                        {
                            "protocol_version": "2.0",
                            "training_config": {"data": {"type": "csv"}},
                        }
                    )
                },
                delivery_id="1-0",
            )
        )

    assert outcome.disposition == TaskDisposition.ACK
    submit.assert_not_called()
    event = json.loads(runtime._redis.xadd.call_args.args[1]["payload"])
    assert event["error_code"] == "INVALID_PAYLOAD"
    assert "legacy and disabled" in event["error_message"]


def test_runtime_maps_canonical_v2_request_without_training_config() -> None:
    runtime = RedisBrokerRuntime.__new__(RedisBrokerRuntime)
    runtime.config = RedisBrokerConfig()
    runtime._redis = MagicMock()
    runtime._consumer = MagicMock()
    object.__setattr__(runtime, "_is_cancelled", lambda _job_id: False)
    with patch(
        "tributo_broker_redis.runtime.submit_training_job_with_identity",
        return_value=MagicMock(
            run_id="canonical-job",
            attempt_id="attempt-1",
            job_id="ray-job-1",
            submission_id="submission-1",
        ),
    ) as submit:
        result = runtime.handle(
            Message(
                "canonical-job",
                {
                    "raw": json.dumps(
                        {
                            "protocol_version": "2.0",
                            "job_id": "payload-job",
                            "model_id": "model-1",
                            "version_id": "version-1",
                            "tenant_id": "tenant-1",
                            "algorithm": {
                                "algorithm_key": "xgboost",
                                "hyper_params": {
                                    "learning_rate": 0.2,
                                    "n_estimators": 3,
                                    "num_workers": 1,
                                },
                            },
                            "datasource": {
                                "type": "LOCAL",
                                "properties": {
                                    "path": "/provider-data/train.csv",
                                    "format": "csv",
                                },
                            },
                            "features": [{"feature_id": "f1", "result_column": "x1"}],
                            "target": {
                                "result_column": "label",
                                "task_type": "BINARY_CLASSIFICATION",
                            },
                            "feature_engineering": {
                                "default_missing_value_strategy": "NONE",
                                "default_outlier_strategy": "NONE",
                                "default_scaling_method": "NONE",
                                "default_encoding_method": "NONE",
                            },
                            "data_split": {
                                "train_ratio": 0.5,
                                "validation_ratio": 0.5,
                                "test_ratio": 0.0,
                            },
                            "storage_context": {
                                "type": "local",
                                "prefix": "/tmp/ray_results/bundles/",
                            },
                            "extensions": {
                                "future_metadata": {"authorization": "not-forwarded"}
                            },
                        }
                    )
                },
            )
        )

    assert result.disposition == TaskDisposition.ACK
    config = json.loads(
        submit.call_args.kwargs["env_vars"]["TRIBUTO_TRAINING_CONFIG_JSON"]
    )
    worker_request = json.loads(
        submit.call_args.kwargs["env_vars"]["TRIBUTO_BROKER_REQUEST_JSON"]
    )
    assert "extensions" not in worker_request
    assert config["data"]["path"] == "/provider-data/train.csv"
    assert config["model"]["eta"] == 0.2
    assert config["training"]["num_rounds"] == 3
    assert config["ray"]["storage_path"] == "/tmp/ray_results/bundles/_ray"
    assert config["output"]["onnx_path"] == "/tmp/ray_results/bundles/model.onnx"
    execution_context = submit.call_args.kwargs["execution_context"]
    assert execution_context == {
        "cancellation": {
            "broker_id": "knova-redis",
            "job_id": "canonical-job",
            "options": {"config_env": "TRIBUTO_BROKER_CONFIG_JSON"},
        },
        "event_reporter": {
            "broker_id": "knova-redis",
            "job_id": "canonical-job",
            "options": {"config_env": "TRIBUTO_BROKER_CONFIG_JSON"},
        },
    }
    assert "password" not in json.dumps(execution_context).lower()
    phase_events = [
        json.loads(call.args[1]["payload"])
        for call in runtime._redis.xadd.call_args_list
    ]
    assert [event["phase"] for event in phase_events] == ["QUEUED"]


def test_redelivery_reuses_same_ray_execution_attempt() -> None:
    config = RedisBrokerConfig(allow_legacy_training_config=True)
    runtime = RedisBrokerRuntime.__new__(RedisBrokerRuntime)
    runtime.config = config
    runtime._redis = MagicMock()
    runtime._consumer = MagicMock()
    object.__setattr__(runtime, "_is_cancelled", lambda _job_id: False)
    submissions = [
        MagicMock(
            run_id="business-job-1",
            attempt_id="attempt-1",
            job_id="ray-job-1",
            submission_id="submission-1",
        ),
        MagicMock(
            run_id="business-job-1",
            attempt_id="attempt-1",
            job_id="ray-job-1",
            submission_id="submission-1",
        ),
    ]
    with patch(
        "tributo_broker_redis.runtime.submit_training_job_with_identity",
        side_effect=submissions,
    ) as submit:
        first = runtime.handle(
            Message(
                "business-job-1",
                {
                    "raw": json.dumps(
                        {
                            "protocol_version": "2.0",
                            "training_config": {"data": {"type": "csv"}},
                        }
                    )
                },
                delivery_id="7-0",
                delivery_attempt=1,
            )
        )
        second = runtime.handle(
            Message(
                "business-job-1",
                {
                    "raw": json.dumps(
                        {
                            "protocol_version": "2.0",
                            "training_config": {"data": {"type": "csv"}},
                        }
                    )
                },
                delivery_id="7-0",
                delivery_attempt=2,
            )
        )
    assert first.disposition == TaskDisposition.ACK
    assert second.disposition == TaskDisposition.ACK
    assert [call.kwargs["attempt_id"] for call in submit.call_args_list] == [
        "attempt-1",
        "attempt-1",
    ]


def test_pre_submission_cancellation_is_acked_without_ray_submission() -> None:
    runtime = RedisBrokerRuntime.__new__(RedisBrokerRuntime)
    runtime.config = RedisBrokerConfig(allow_legacy_training_config=True)
    runtime._redis = MagicMock()
    runtime._consumer = MagicMock()
    object.__setattr__(runtime, "_is_cancelled", lambda _job_id: True)
    with patch(
        "tributo_broker_redis.runtime.submit_training_job_with_identity"
    ) as submit:
        result = runtime.handle(
            Message(
                "cancelled-job",
                {
                    "raw": json.dumps(
                        {
                            "protocol_version": "2.0",
                            "training_config": {"data": {"type": "csv"}},
                        }
                    )
                },
            )
        )
    assert result.disposition == TaskDisposition.ACK
    submit.assert_not_called()


def test_unsupported_task_type_is_failed_and_acked() -> None:
    runtime = RedisBrokerRuntime.__new__(RedisBrokerRuntime)
    runtime.config = RedisBrokerConfig()
    runtime._redis = MagicMock()
    runtime._consumer = MagicMock()
    result = runtime.handle(
        Message(
            "inference-job",
            {
                "raw": json.dumps(
                    {
                        "protocol_version": "2.0",
                        "task_type": "INFERENCE",
                        "training_config": {"data": {"type": "csv"}},
                    }
                )
            },
            delivery_id="7-0",
        )
    )
    assert result.disposition == TaskDisposition.ACK
    event = json.loads(runtime._redis.xadd.call_args.args[1]["payload"])
    assert event["event_type"] == "FAILED"
    assert event["error_code"] == "UNSUPPORTED_TASK_TYPE"


def test_oversized_payload_is_failed_and_acked() -> None:
    runtime = RedisBrokerRuntime.__new__(RedisBrokerRuntime)
    runtime.config = RedisBrokerConfig(max_payload_bytes=8)
    runtime._redis = MagicMock()
    runtime._consumer = MagicMock()
    result = runtime.handle(
        Message(
            "large-job",
            {"raw": "{}"},
            metadata={"payload_error": "payload exceeds limit"},
            delivery_id="7-0",
        )
    )
    assert result.disposition == TaskDisposition.ACK
    event = json.loads(runtime._redis.xadd.call_args.args[1]["payload"])
    assert event["error_code"] == "PAYLOAD_TOO_LARGE"


def test_temporary_ray_submission_failure_leaves_message_for_retry(
    caplog: pytest.LogCaptureFixture,
) -> None:
    runtime = RedisBrokerRuntime.__new__(RedisBrokerRuntime)
    runtime.config = RedisBrokerConfig(allow_legacy_training_config=True)
    runtime._redis = MagicMock()
    runtime._consumer = MagicMock()
    object.__setattr__(runtime, "_is_cancelled", lambda _job_id: False)
    with (
        patch(
            "tributo_broker_redis.runtime.submit_training_job_with_identity",
            side_effect=RuntimeError(
                "ray unavailable password=hunter2 Bearer opaque-token"
            ),
        ),
        caplog.at_level(logging.WARNING, logger="tributo_broker_redis.runtime"),
    ):
        result = runtime.handle(
            Message(
                "retry-job",
                {
                    "raw": json.dumps(
                        {
                            "protocol_version": "2.0",
                            "training_config": {"data": {"type": "csv"}},
                        }
                    )
                },
            )
        )
    assert result.disposition == TaskDisposition.RETRY
    assert result.error is not None
    assert "hunter2" not in result.error
    assert "opaque-token" not in result.error
    assert "hunter2" not in caplog.text
    assert "opaque-token" not in caplog.text


def test_ack_failure_enters_reconnecting_without_second_ack() -> None:
    consumer = MagicMock()
    consumer.ack.side_effect = ConnectionError("redis unavailable")
    runtime = MagicMock()
    runtime.consumer = consumer
    plugin = MagicMock()
    plugin.broker_id = "knova-redis"
    plugin.create_runtime.return_value = runtime
    runner = BrokerRunner(
        plugin,
        {},
        backoff_initial=0.001,
        backoff_max=0.001,
        sleep=lambda _delay: None,
    )
    runner.start()
    runner._apply_outcome(
        Message("job-1", {}, delivery_id="7-0"),
        TaskOutcome(disposition=TaskDisposition.ACK),
    )
    assert runner.state == BrokerRunnerState.RECONNECTING
    consumer.ack.assert_called_once()
    consumer.retry.assert_not_called()


def test_cancellation_checker_is_fail_open_when_redis_is_unavailable() -> None:
    client = MagicMock()
    client.exists.side_effect = ConnectionError("redis down")
    checker = RedisCancellationChecker(client, RedisBrokerConfig(), "job-1")
    assert checker.is_cancelled("job-1") is False
