from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from conftest import inference_request, training_config
from pydantic import ValidationError
from redis.exceptions import TimeoutError
from tributo.integrations.broker import TaskDisposition
from tributo.ray_jobs import RayJobSubmission

from tributo_broker_redis.cli import _parser, main
from tributo_broker_redis.config import OperationType, RedisBrokerConfig
from tributo_broker_redis.consumer import RedisTaskConsumer
from tributo_broker_redis.operations import MappingFailure, prepare_operation
from tributo_broker_redis.plugin import RedisBrokerPlugin
from tributo_broker_redis.protocol import ProtocolFailure, parse_request
from tributo_broker_redis.runtime import (
    RedisBrokerRuntime,
    validate_execution_environment,
)


def _payload(
    operation_type: str,
    execution_profile: str,
    spec: dict[str, Any],
    *,
    operation_id: str = "operation-1",
) -> str:
    return json.dumps(
        {
            "protocol_profile": "tributo-generic-v1",
            "protocol_version": "1.0",
            "operation_id": operation_id,
            "operation_type": operation_type,
            "execution_profile": execution_profile,
            "attempt_id": "attempt-1",
            "spec": spec,
        }
    )


def test_config_has_one_public_shape_and_independent_channels(
    raw_config: dict[str, Any],
) -> None:
    parsed = RedisBrokerConfig.model_validate(raw_config)
    assert parsed.broker_id == "tributo-redis"
    assert parsed.protocol.profile == "tributo-generic-v1"

    raw_config["channels"]["batch_inference"]["task_stream_key"] = raw_config[
        "channels"
    ]["training"]["task_stream_key"]
    with pytest.raises(ValidationError, match="distinct task_stream_key"):
        RedisBrokerConfig.model_validate(raw_config)


def test_config_uses_finite_polling_and_reads_preexisting_tasks_by_default(
    raw_config: dict[str, Any],
) -> None:
    raw_config["transport"]["block_ms"] = 0
    with pytest.raises(ValidationError, match="greater than or equal to 1"):
        RedisBrokerConfig.model_validate(raw_config)

    raw_config["transport"].pop("block_ms")
    for channel in raw_config["channels"].values():
        channel.pop("group_start_id")
    parsed = RedisBrokerConfig.model_validate(raw_config)
    assert parsed.transport.block_ms == 1000
    assert parsed.channels.training.group_start_id == "0-0"
    assert parsed.channels.batch_inference.group_start_id == "0-0"


def test_config_requires_explicit_core_root_and_driver_distribution(
    raw_config: dict[str, Any],
) -> None:
    raw_config["execution"].pop("project_root")
    with pytest.raises(ValidationError, match="project_root"):
        RedisBrokerConfig.model_validate(raw_config)

    raw_config["execution"]["project_root"] = "/deployment/tributo-core"
    raw_config["execution"]["runtime_pip_packages"] = []
    with pytest.raises(ValidationError, match="distribute the provider"):
        RedisBrokerConfig.model_validate(raw_config)


def test_execution_environment_fails_before_consumption_on_invalid_root(
    raw_config: dict[str, Any],
) -> None:
    raw_config["execution"]["project_root"] = "/missing/tributo-core"
    invalid_root = RedisBrokerConfig.model_validate(raw_config)
    with pytest.raises(ValueError, match="must contain the Tributo package"):
        validate_execution_environment(invalid_root)


def test_consumer_poll_forwards_exact_timeout_and_zero_is_nonblocking(
    config: RedisBrokerConfig,
) -> None:
    client = MagicMock()
    client.xreadgroup.return_value = []
    consumer = RedisTaskConsumer(
        client,
        config.transport,
        config.channels.training,
        "training",
    )

    assert consumer.poll(0) is None
    assert client.xreadgroup.call_args.kwargs["block"] is None
    assert consumer.poll(10_000) is None
    assert client.xreadgroup.call_args.kwargs["block"] == 10_000


def test_consumer_poll_treats_redis_socket_timeout_as_empty_poll(
    config: RedisBrokerConfig,
) -> None:
    client = MagicMock()
    client.xreadgroup.side_effect = TimeoutError("timed out")
    consumer = RedisTaskConsumer(
        client,
        config.transport,
        config.channels.training,
        "training",
    )

    assert consumer.poll(config.transport.block_ms) is None


def test_configured_outer_identity_field_is_normalized_for_tasks_and_events(
    raw_config: dict[str, Any],
) -> None:
    raw_config["channels"]["training"]["outer_identity_field"] = "request_id"
    config = RedisBrokerConfig.model_validate(raw_config)
    client = MagicMock()
    client.xreadgroup.return_value = [
        (
            config.channels.training.task_stream_key,
            [("1-0", {"request_id": "operation-1", "payload": "{}"})],
        )
    ]
    consumer = RedisTaskConsumer(
        client,
        config.transport,
        config.channels.training,
        "training",
    )

    message = consumer.poll(1)

    assert message is not None
    assert message.metadata["outer_operation_id"] == "operation-1"
    assert message.metadata["outer_identity_field"] == "request_id"

    raw_config["channels"]["training"]["outer_identity_field"] = "payload"
    with pytest.raises(ValidationError, match="must not be payload"):
        RedisBrokerConfig.model_validate(raw_config)


def test_consumer_bounds_outer_operation_identity(config: RedisBrokerConfig) -> None:
    client = MagicMock()
    client.xreadgroup.return_value = [
        (
            config.channels.training.task_stream_key,
            [("1-0", {"operation_id": "x" * 257, "payload": "{}"})],
        )
    ]
    consumer = RedisTaskConsumer(
        client,
        config.transport,
        config.channels.training,
        "training",
    )

    message = consumer.poll(1)

    assert message is not None
    assert message.metadata["outer_operation_id"] == ""


def test_runtime_rejects_unknown_outer_envelope_fields(
    config: RedisBrokerConfig,
) -> None:
    client = MagicMock()
    client.xreadgroup.return_value = [
        (
            config.channels.training.task_stream_key,
            [
                (
                    "1-0",
                    {
                        "operation_id": "operation-1",
                        "payload": _payload(
                            "training",
                            "single_worker",
                            {"algorithm": "xgboost", "config": training_config(1)},
                        ),
                        "unsupported": "value",
                    },
                )
            ],
        )
    ]

    def unexpected_submitter(entrypoint: str, **kwargs: Any) -> RayJobSubmission:
        raise AssertionError("invalid envelope must not be submitted")

    runtime = RedisBrokerRuntime(
        config,
        submitter=unexpected_submitter,
        redis_client=client,
    )
    message = runtime.consumers["training"].poll(1)
    assert message is not None

    outcome = runtime.handle(message)

    assert outcome.disposition == TaskDisposition.ACK
    assert outcome.error is not None
    assert outcome.error.code == "INVALID_ENVELOPE"
    runtime.close()


def test_config_rejects_credentials_and_unknown_compatibility_fields(
    raw_config: dict[str, Any],
) -> None:
    raw_config["transport"]["url"] = "redis://user:secret@localhost:6379/0"
    with pytest.raises(ValidationError, match="must not contain credentials"):
        RedisBrokerConfig.model_validate(raw_config)


@pytest.mark.parametrize(
    "name",
    [
        "TRIBUTO_ATTEMPT_ID",
        "TRIBUTO_REDIS_DRIVER_INPUT_B64",
        "TRIBUTO_RUN_ID",
        "TRIBUTO_SUBMISSION_ID",
    ],
)
def test_config_rejects_reserved_execution_env_names(
    raw_config: dict[str, Any], name: str
) -> None:
    raw_config["execution"]["env_vars"] = {name: "not-sensitive"}

    with pytest.raises(ValidationError, match="reserved runtime identity"):
        RedisBrokerConfig.model_validate(raw_config)


def test_config_allows_non_reserved_tributo_env_names(
    raw_config: dict[str, Any],
) -> None:
    raw_config["execution"]["env_vars"] = {"TRIBUTO_DATA_BACKEND": "ray-data"}

    parsed = RedisBrokerConfig.model_validate(raw_config)

    assert parsed.execution.env_vars == {"TRIBUTO_DATA_BACKEND": "ray-data"}


def test_provider_owns_validate_and_consume_cli(
    raw_config: dict[str, Any], tmp_path: Path
) -> None:
    config_path = tmp_path / "provider.json"
    config_path.write_text(json.dumps(raw_config), encoding="utf-8")

    assert main(["validate", "--config", str(config_path)]) == 0
    parsed = _parser().parse_args(["consume", "--config", str(config_path), "--once"])
    assert parsed.command == "consume"
    assert parsed.once is True

    raw_config["transport"]["url"] = "redis://localhost:6379/0"
    raw_config["execution"]["env_vars"] = {"API_TOKEN": "secret"}
    with pytest.raises(ValidationError, match="cannot carry sensitive"):
        RedisBrokerConfig.model_validate(raw_config)


def test_config_accepts_trusted_runtime_extension_forms(
    raw_config: dict[str, Any],
) -> None:
    raw_config["execution"]["extra_py_modules"] = [
        "/deployment/provider-module",
        "s3://public-artifacts/provider-driver.zip",
    ]
    raw_config["execution"]["runtime_pip_packages"] = [
        "driver-runtime>=1.2,<2",
        "/provider/tributo_broker_redis-0.1.0-py3-none-any.whl",
    ]

    parsed = RedisBrokerConfig.model_validate(raw_config)

    assert parsed.execution.extra_py_modules == tuple(
        raw_config["execution"]["extra_py_modules"]
    )
    assert parsed.execution.runtime_pip_packages == tuple(
        raw_config["execution"]["runtime_pip_packages"]
    )


def test_plugin_declares_only_validated_namespaced_capabilities() -> None:
    assert RedisBrokerPlugin.capabilities == frozenset(
        {
            "transport.redis.streams",
            "transport.redis.consumer-group",
            "operation.training.single_worker",
            "operation.training.distributed",
            "operation.batch_inference.single_worker",
            "operation.batch_inference.distributed",
            "event.basic",
        }
    )


@pytest.mark.parametrize(
    "value",
    [
        "",
        "https://user:opaque-secret@example.test/provider.zip",
        "s3://artifacts/provider.zip?X-Amz-Credential=opaque-secret",
        "ftp://example.test/provider.zip",
        "https://example.test/provider.tar.gz",
    ],
)
def test_config_rejects_unsafe_or_unsupported_extension_modules(
    raw_config: dict[str, Any], value: str
) -> None:
    raw_config["execution"]["extra_py_modules"] = [value]

    with pytest.raises(ValidationError) as error:
        RedisBrokerConfig.model_validate(raw_config)

    assert "extra_py_modules" in str(error.value)
    assert "opaque-secret" not in str(error.value)


@pytest.mark.parametrize(
    "value",
    [
        "",
        "--extra-index-url=https://example.test/simple",
        "driver-runtime @ https://user:opaque-secret@example.test/driver.whl",
        "https://example.test/driver.whl?token=opaque-secret",
        "/provider/driver-runtime.zip",
    ],
)
def test_config_rejects_unsafe_or_unsupported_runtime_pip_packages(
    raw_config: dict[str, Any], value: str
) -> None:
    raw_config["execution"]["runtime_pip_packages"] = [value]

    with pytest.raises(ValidationError) as error:
        RedisBrokerConfig.model_validate(raw_config)

    assert "runtime_pip_packages" in str(error.value)
    assert "opaque-secret" not in str(error.value)


@pytest.mark.parametrize(
    ("operation_type", "profile", "spec"),
    [
        (
            "training",
            "single_worker",
            {"algorithm": "xgboost", "config": training_config(1)},
        ),
        (
            "training",
            "distributed",
            {"algorithm": "xgboost", "config": training_config(2)},
        ),
        (
            "batch_inference",
            "single_worker",
            {"profile": "bundle-backed", "request": inference_request(1)},
        ),
        (
            "batch_inference",
            "distributed",
            {"profile": "bundle-backed", "request": inference_request(2)},
        ),
    ],
)
def test_four_operation_profiles_prepare_core_revalidatable_payloads(
    operation_type: str, profile: str, spec: dict[str, Any]
) -> None:
    request = parse_request(
        _payload(operation_type, profile, spec),
        outer_operation_id="operation-1",
        expected_operation_type=cast(OperationType, operation_type),
    )
    prepared = prepare_operation(request)
    if operation_type == "training":
        from tributo.training.xgboost_trainer import XGBoostTrainingConfig

        config = XGBoostTrainingConfig.model_validate(
            prepared.operation_payload["training_config"]
        )
        assert config.ray.num_workers == (1 if profile == "single_worker" else 2)
    else:
        from tributo.inference.contracts import InferenceRequest

        inference = InferenceRequest.model_validate(
            prepared.operation_payload["inference_request"]
        )
        assert inference.execution.concurrency == (
            1 if profile == "single_worker" else 2
        )


def test_protocol_fails_closed_on_identity_unknown_fields_and_plaintext_secret() -> (
    None
):
    with pytest.raises(ProtocolFailure) as identity:
        parse_request(
            _payload(
                "training",
                "single_worker",
                {"algorithm": "xgboost", "config": training_config(1)},
            ),
            outer_operation_id="different",
            expected_operation_type="training",
        )
    assert identity.value.code == "IDENTITY_MISMATCH"

    value = json.loads(
        _payload(
            "training",
            "single_worker",
            {"algorithm": "xgboost", "config": training_config(1)},
        )
    )
    value["unknown"] = True
    with pytest.raises(ProtocolFailure) as unknown:
        parse_request(
            json.dumps(value),
            outer_operation_id="operation-1",
            expected_operation_type="training",
        )
    assert unknown.value.code == "INVALID_REQUEST"

    value.pop("unknown")
    value["spec"]["password"] = "plaintext"
    with pytest.raises(ProtocolFailure) as credential:
        parse_request(
            json.dumps(value),
            outer_operation_id="operation-1",
            expected_operation_type="training",
        )
    assert credential.value.code == "PLAINTEXT_CREDENTIAL"


def test_protocol_rejects_non_digest_metadata_and_nested_credential_reference() -> None:
    value = json.loads(
        _payload(
            "training",
            "single_worker",
            {"algorithm": "xgboost", "config": training_config(1)},
        )
    )
    value["request_digest"] = "not-a-sha256-digest"
    with pytest.raises(ProtocolFailure) as digest:
        parse_request(
            json.dumps(value),
            outer_operation_id="operation-1",
            expected_operation_type="training",
        )
    assert digest.value.code == "INVALID_REQUEST"

    value.pop("request_digest")
    value["spec"]["config"]["model"]["credential_ref"] = "env:NESTED_SECRET"
    with pytest.raises(ProtocolFailure) as credential:
        parse_request(
            json.dumps(value),
            outer_operation_id="operation-1",
            expected_operation_type="training",
        )
    assert credential.value.code == "PLAINTEXT_CREDENTIAL"


@pytest.mark.parametrize(
    "credential_ref",
    ["env:PREPROVISIONED_SECRET", "mount:/var/run/secrets/object-store"],
)
def test_protocol_accepts_only_supported_root_credential_references(
    credential_ref: str,
) -> None:
    value = json.loads(
        _payload(
            "training",
            "single_worker",
            {
                "algorithm": "xgboost",
                "config": training_config(1),
                "credential_ref": credential_ref,
            },
        )
    )
    request = parse_request(
        json.dumps(value),
        outer_operation_id="operation-1",
        expected_operation_type="training",
    )

    assert prepare_operation(request).credential_ref == credential_ref


def test_protocol_rejects_unsupported_root_credential_reference() -> None:
    request = parse_request(
        _payload(
            "training",
            "single_worker",
            {
                "algorithm": "xgboost",
                "config": training_config(1),
                "credential_ref": "inline-opaque-value",
            },
        ),
        outer_operation_id="operation-1",
        expected_operation_type="training",
    )

    with pytest.raises(MappingFailure) as failure:
        prepare_operation(request)
    assert failure.value.code == "INVALID_TRAINING_REQUEST"
    assert "inline-opaque-value" not in failure.value.sanitized_message


@pytest.mark.parametrize(
    "uri",
    [
        "s3://access-key:opaque-secret@bucket/path",
        "https://storage.example/data?access_token=opaque-secret",
        "https://storage.example/data?X-Amz-Credential=opaque-secret",
    ],
)
def test_protocol_rejects_credentials_embedded_in_uri_values(uri: str) -> None:
    value = json.loads(
        _payload(
            "training",
            "single_worker",
            {"algorithm": "xgboost", "config": training_config(1)},
        )
    )
    value["spec"]["config"]["data"]["path"] = uri

    with pytest.raises(ProtocolFailure) as credential:
        parse_request(
            json.dumps(value),
            outer_operation_id="operation-1",
            expected_operation_type="training",
        )

    assert credential.value.code == "PLAINTEXT_CREDENTIAL"
    assert "opaque-secret" not in credential.value.sanitized_message


def test_unsupported_algorithm_and_profile_mismatch_have_stable_codes() -> None:
    request = parse_request(
        _payload(
            "training",
            "single_worker",
            {"algorithm": "lightgbm", "config": training_config(1)},
        ),
        outer_operation_id="operation-1",
        expected_operation_type="training",
    )
    with pytest.raises(MappingFailure) as unsupported:
        prepare_operation(request)
    assert unsupported.value.code == "UNSUPPORTED_ALGORITHM"

    request = parse_request(
        _payload(
            "training",
            "single_worker",
            {"algorithm": "xgboost", "config": training_config(2)},
        ),
        outer_operation_id="operation-1",
        expected_operation_type="training",
    )
    with pytest.raises(MappingFailure) as mismatch:
        prepare_operation(request)
    assert mismatch.value.code == "EXECUTION_PROFILE_MISMATCH"
