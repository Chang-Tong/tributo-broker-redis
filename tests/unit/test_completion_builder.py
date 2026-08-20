"""Golden protocol v2 terminal payloads from Core's neutral summary."""

from __future__ import annotations

import json

import pytest
from tributo.exceptions import (
    JobConfigurationError,
    ModelExportError,
    TrainingDataError,
)

from tributo_broker_redis.completion import (
    build_cancelled_payload,
    build_completed_payload,
    build_failed_payload,
    failure_phase,
)
from tributo_broker_redis.protocol import TrainingJobRequest


def _request() -> TrainingJobRequest:
    return TrainingJobRequest.model_validate(
        {
            "job_id": "job-1",
            "model_id": "model-1",
            "version_id": "version-1",
            "tenant_id": "tenant-1",
            "algorithm": {"algorithm_key": "xgboost"},
            "datasource": {
                "type": "LOCAL",
                "properties": {"path": "/data/train.csv"},
            },
            "features": [{"feature_id": "feature-1", "result_column": "x"}],
            "target": {
                "result_column": "label",
                "task_type": "BINARY_CLASSIFICATION",
            },
            "evaluation": {
                "primary_metric": "auc",
                "artifacts": {
                    "roc_curve": True,
                    "threshold_analysis": True,
                    "feature_importance": True,
                },
            },
            "storage_context": {
                "type": "s3",
                "bucket": "models",
                "prefix": "tenant/model/version/",
            },
        }
    )


def test_completed_payload_golden_contains_rich_summary_and_artifact_facts() -> None:
    payload = build_completed_payload(
        _request(),
        {
            "feature_columns": ["x"],
            "feature_id_map": {"x": "feature-1"},
            "row_counts": {"train": 70, "val": 10, "test": 20},
            "evaluation": {
                "eval_auc": 0.91,
                "eval_test_rows": 20,
                "eval_cm_tp": 8,
                "eval_cm_fp": 1,
                "eval_cm_fn": 2,
                "eval_cm_tn": 9,
                "eval_roc_fpr": [0.0, 1.0],
                "eval_roc_tpr": [0.0, 1.0],
                "feat_imp_rank": [1],
                "feat_imp_name": ["x"],
                "feat_imp_score": [0.75],
            },
            "artifact_refs": [
                {
                    "kind": "onnx",
                    "uri": "s3://models/tenant/model/version/model.onnx",
                    "sha256": "abc123",
                    "size_bytes": 4096,
                }
            ],
            "warnings": [{"code": "DUPLICATE_ENTITY_KEYS", "duplicate_rows": 2}],
        },
        duration_seconds=12.5,
    )

    assert payload["phase"] == "COMPLETED"
    assert payload["result_summary"] == {
        "primary_metric": {"name": "auc", "value": 0.91},
        "sample_rows": {"total": 100, "train": 70, "validation": 10, "test": 20},
    }
    assert payload["training_result"]["model_features"][0]["feature_id"] == "feature-1"
    manifest = payload["artifact_manifest"]
    assert manifest["total_size_bytes"] == 4096
    artifact_file = manifest["model_artifacts"]["model_weights"]["alternatives"][0][
        "files"
    ][0]
    assert artifact_file["path"] == "model.onnx"
    assert artifact_file["hash"] == "sha256:abc123"
    assert artifact_file["size"] == 4096
    assert payload["warnings"] == [
        {"code": "DUPLICATE_ENTITY_KEYS", "duplicate_rows": 2}
    ]
    json.dumps(payload, allow_nan=False)


def test_completed_payload_rejects_nan_and_infinity() -> None:
    for value in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(ValueError, match="must be finite"):
            build_completed_payload(
                _request(),
                {"evaluation": {"eval_auc": value}},
                duration_seconds=1.0,
            )

    with pytest.raises(ValueError, match=r"payload\.warnings\.0\.ratio must be finite"):
        build_completed_payload(
            _request(),
            {
                "evaluation": {"eval_auc": 0.9},
                "artifact_refs": [
                    {
                        "kind": "onnx",
                        "uri": "s3://models/tenant/model/version/model.onnx",
                        "sha256": "abc123",
                        "size_bytes": 1,
                    }
                ],
                "warnings": [{"ratio": float("nan")}],
            },
            duration_seconds=1.0,
        )


def test_artifact_manifest_requires_core_hash_and_uri_facts() -> None:
    with pytest.raises(ValueError, match=r"artifact_refs\.0\.sha256 is required"):
        build_completed_payload(
            _request(),
            {
                "evaluation": {"eval_auc": 0.9},
                "artifact_refs": [
                    {"kind": "onnx", "uri": "s3://models/model.onnx", "size_bytes": 1}
                ],
            },
            duration_seconds=1.0,
        )


def test_local_artifact_path_is_relative_to_storage_prefix() -> None:
    base_request = _request()
    assert base_request.storage_context is not None
    request = base_request.model_copy(
        update={
            "storage_context": base_request.storage_context.model_copy(
                update={"type": "local", "bucket": "", "prefix": "/models/job/"}
            )
        }
    )
    payload = build_completed_payload(
        request,
        {
            "evaluation": {"eval_auc": 0.9},
            "artifact_refs": [
                {
                    "kind": "onnx",
                    "uri": "/models/job/nested/model.onnx",
                    "sha256": "abc123",
                    "size_bytes": 1,
                }
            ],
        },
        duration_seconds=1.0,
    )
    artifact = payload["artifact_manifest"]["model_artifacts"]["model_weights"][
        "alternatives"
    ][0]["files"][0]
    assert artifact["path"] == "nested/model.onnx"


def test_artifact_outside_storage_prefix_fails_closed() -> None:
    with pytest.raises(ValueError, match="outside storage_context"):
        build_completed_payload(
            _request(),
            {
                "evaluation": {"eval_auc": 0.9},
                "artifact_refs": [
                    {
                        "kind": "onnx",
                        "uri": "s3://other-bucket/stolen/model.onnx",
                        "sha256": "abc123",
                        "size_bytes": 1,
                    }
                ],
            },
            duration_seconds=1.0,
        )


def test_evaluation_metrics_only_include_controlled_task_metrics() -> None:
    payload = build_completed_payload(
        _request(),
        {
            "evaluation": {
                "eval_auc": 0.9,
                "eval_test_rows": 10,
                "eval_cm_tp": 4,
                "eval_roc_fpr": [0.0, 1.0],
                "eval_thr_thresholds": [0.5],
            },
            "artifact_refs": [
                {
                    "kind": "onnx",
                    "uri": "s3://models/tenant/model/version/model.onnx",
                    "sha256": "abc123",
                    "size_bytes": 1,
                }
            ],
        },
        duration_seconds=1.0,
    )
    assert payload["training_result"]["evaluation"]["metrics"] == [
        {"metric_name": "auc", "value": 0.9}
    ]


def test_multiclass_internal_metric_names_map_to_wire_vocabulary() -> None:
    base_request = _request()
    assert base_request.target is not None
    request = base_request.model_copy(
        update={
            "target": base_request.target.model_copy(
                update={"task_type": "MULTICLASS_CLASSIFICATION"}
            ),
            "evaluation": base_request.evaluation.model_copy(
                update={
                    "primary_metric": "f1",
                    "additional_metrics": ["precision", "recall"],
                }
            ),
        }
    )
    payload = build_completed_payload(
        request,
        {
            "evaluation": {
                "eval_f1_macro": 0.8,
                "eval_precision_macro": 0.7,
                "eval_recall_macro": 0.9,
            },
            "artifact_refs": [
                {
                    "kind": "onnx",
                    "uri": "s3://models/tenant/model/version/model.onnx",
                    "sha256": "abc123",
                    "size_bytes": 1,
                }
            ],
        },
        duration_seconds=1.0,
    )
    assert payload["result_summary"]["primary_metric"] == {
        "name": "f1",
        "value": 0.8,
    }
    assert payload["training_result"]["evaluation"]["metrics"] == [
        {"metric_name": "f1", "value": 0.8},
        {"metric_name": "precision", "value": 0.7},
        {"metric_name": "recall", "value": 0.9},
    ]


def test_strict_onnx_completion_requires_artifact_refs() -> None:
    with pytest.raises(ValueError, match="artifact_refs.*required"):
        build_completed_payload(
            _request(),
            {"evaluation": {"eval_auc": 0.9}},
            duration_seconds=1.0,
        )


def test_completed_payload_recursively_redacts_warning_facts() -> None:
    payload = build_completed_payload(
        _request(),
        {
            "evaluation": {"eval_auc": 0.9},
            "artifact_refs": [
                {
                    "kind": "onnx",
                    "uri": "s3://models/tenant/model/version/model.onnx",
                    "sha256": "abc123",
                    "size_bytes": 1,
                }
            ],
            "warnings": [
                {
                    "message": "password=hunter2",
                    "nested": [
                        "redis://user:secret@redis",
                        {"accessToken": "opaque-value"},
                    ],
                }
            ],
        },
        duration_seconds=1.0,
    )

    encoded = json.dumps(payload["warnings"])
    assert "hunter2" not in encoded
    assert "user:secret" not in encoded
    assert "opaque-value" not in encoded


def test_failed_and_cancelled_payloads_are_controlled_and_redacted() -> None:
    failed = build_failed_payload(
        ValueError("password=hunter2 redis://user:secret@redis.internal token:abc"),
        phase="LOADING_DATA",
        duration_seconds=2.5,
    )
    assert failed["error_code"] == "INVALID_PAYLOAD"
    assert failed["phase"] == "LOADING_DATA"
    assert "hunter2" not in failed["error_message"]
    assert "user:secret" not in failed["error_message"]
    assert "token:abc" not in failed["error_message"]

    cancelled = build_cancelled_payload(
        phase="TRAINING", duration_seconds=3.0, has_best_model=True
    )
    assert cancelled == {
        "phase": "TRAINING",
        "duration_seconds": 3.0,
        "has_best_model": True,
    }
    assert failure_phase(TrainingDataError("bad split")) == "DATA_SPLITTING"
    assert failure_phase(ModelExportError("bad onnx")) == "EVALUATING"


@pytest.mark.parametrize(
    "secret_text",
    [
        "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.payload.signature",
        "authorization=Bearer opaque-token",
        '"Authorization": "Bearer json-token"',
        "Authorization: Basic dXNlcjpwYXNz",
        'request={"accessToken":"camel-token","privateKey":"pem-data"}',
        "Bearer standalone-token",
        "Basic c3RhbmRhbG9uZTpwYXNz",
    ],
)
def test_failed_payload_redacts_complete_authorization_and_secret_corpus(
    secret_text: str,
) -> None:
    payload = build_failed_payload(
        ValueError(secret_text),
        phase="TRAINING",
        duration_seconds=1.0,
    )
    message = payload["error_message"]
    for secret in (
        "eyJhbGciOiJIUzI1NiJ9.payload.signature",
        "opaque-token",
        "json-token",
        "dXNlcjpwYXNz",
        "camel-token",
        "pem-data",
        "standalone-token",
        "c3RhbmRhbG9uZTpwYXNz",
    ):
        assert secret not in message


@pytest.mark.parametrize(
    ("nested_name", "expected_code", "expected_phase"),
    [
        ("OperationalError", "DATA_SOURCE_UNREACHABLE", "LOADING_DATA"),
        ("ArtifactUploadError", "ARTIFACT_UPLOAD_FAILED", "EVALUATING"),
    ],
)
def test_ray_wrapped_controlled_failure_is_unwrapped_safely(
    nested_name: str, expected_code: str, expected_phase: str
) -> None:
    nested_type = type(nested_name, (RuntimeError,), {})
    nested = nested_type("password=hunter2")
    wrapper = RuntimeError("Ray wrapper")
    wrapper.__cause__ = nested

    payload = build_failed_payload(
        wrapper,
        phase=failure_phase(wrapper),
        duration_seconds=1.0,
        error_code=type(wrapper).__name__,
    )
    assert payload["error_code"] == expected_code
    assert payload["phase"] == expected_phase
    assert "hunter2" not in payload["error_message"]


def test_job_configuration_error_maps_through_wrapper() -> None:
    wrapper = RuntimeError("Ray wrapper")
    wrapper.__cause__ = JobConfigurationError("bad config")
    payload = build_failed_payload(
        wrapper,
        phase=failure_phase(wrapper),
        duration_seconds=1.0,
    )
    assert payload["error_code"] == "INVALID_PAYLOAD"
    assert payload["phase"] == "DATA_SPLITTING"
