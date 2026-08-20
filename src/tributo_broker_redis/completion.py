"""Translate Core's broker-neutral training summary into protocol v2 terminals."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence, Set
from datetime import UTC, datetime
from typing import Any

from tributo_broker_redis.protocol import TrainingJobRequest

_ERROR_CODES = {
    "ValueError": "INVALID_PAYLOAD",
    "TypeError": "INVALID_PAYLOAD",
    "KeyError": "INVALID_PAYLOAD",
    "TrainingDataError": "INVALID_PAYLOAD",
    "FileNotFoundError": "DATA_SOURCE_UNREACHABLE",
    "ConnectionError": "DATA_SOURCE_UNREACHABLE",
    "DataSourceError": "DATA_SOURCE_UNREACHABLE",
    "OperationalError": "DATA_SOURCE_UNREACHABLE",
    "EmptyDatasetError": "EMPTY_DATASET",
    "MemoryError": "TRAINING_OOM",
    "TimeoutError": "TRAINING_TIMEOUT",
    "ModelExportError": "MODEL_EXPORT_FAILED",
    "ArtifactUploadError": "ARTIFACT_UPLOAD_FAILED",
    "JobConfigurationError": "INVALID_PAYLOAD",
}
_PROTOCOL_CODES = frozenset(
    {
        "UNSUPPORTED_PROTOCOL_VERSION",
        "UNSUPPORTED_TASK_TYPE",
        "INVALID_JOB_ID",
        "PAYLOAD_TOO_LARGE",
        "INVALID_PAYLOAD",
        "DATA_SOURCE_UNREACHABLE",
        "DATA_QUERY_FAILED",
        "EMPTY_DATASET",
        "FEATURE_ENGINEERING_FAILED",
        "TRAINING_FAILED",
        "TRAINING_OOM",
        "TRAINING_TIMEOUT",
        "EVALUATION_FAILED",
        "MODEL_EXPORT_FAILED",
        "ARTIFACT_UPLOAD_FAILED",
        "UNKNOWN",
    }
)
_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(password|passwd|secret(?:[_-]?access[_-]?key)?|token|accessToken|"
    r"authorization|api[_-]?key|apiKey|private[_-]?key|privateKey)"
    r"\b[\"']?\s*[:=]\s*[\"']?([^\s,;\"']+)"
)
_AUTHORIZATION_ASSIGNMENT = re.compile(
    r"(?i)[\"']?authorization[\"']?\s*[:=]\s*[\"']?"
    r"(?:(?:bearer|basic)\s+)?[^\s,;}\"']+[\"']?"
)
_BARE_AUTHORIZATION = re.compile(r"(?i)\b(?:bearer|basic)\s+[A-Za-z0-9._~+/=-]+")
_URI_USERINFO = re.compile(r"(?P<scheme>[a-zA-Z][a-zA-Z0-9+.-]*://)[^/@\s]+@")
_EVALUATION_METRICS = {
    "BINARY_CLASSIFICATION": {
        "auc": "auc",
        "f1": "f1",
        "precision": "precision",
        "recall": "recall",
        "average_precision": "avg_precision",
    },
    "MULTICLASS_CLASSIFICATION": {
        "auc": "auc",
        "f1": "f1_macro",
        "precision": "precision_macro",
        "recall": "recall_macro",
    },
    "REGRESSION": {"rmse": "rmse", "mae": "mae", "r2": "r2"},
}


def finite_float(value: Any, path: str) -> float:
    """Return a finite protocol number or fail closed with its field path."""
    if isinstance(value, bool):
        raise ValueError(f"{path} must be a finite number")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{path} must be a finite number") from exc
    if not math.isfinite(result):
        raise ValueError(f"{path} must be finite")
    return result


def _reject_non_finite(value: Any, path: str = "payload") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            _reject_non_finite(child, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            _reject_non_finite(child, f"{path}.{index}")
    elif isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{path} must be finite")


def redact_sensitive(value: str) -> str:
    """Remove common credential forms from user-visible failure text."""
    redacted = _URI_USERINFO.sub(r"\g<scheme>[REDACTED]@", value)
    redacted = _AUTHORIZATION_ASSIGNMENT.sub("Authorization=[REDACTED]", redacted)
    redacted = _BARE_AUTHORIZATION.sub("Authorization=[REDACTED]", redacted)
    return _SECRET_ASSIGNMENT.sub(
        lambda match: f"{match.group(1)}=[REDACTED]", redacted
    )


def _redact_tree(value: Any) -> Any:
    if isinstance(value, str):
        return redact_sensitive(value)
    if isinstance(value, Mapping):
        result = {}
        for key, child in value.items():
            normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
            if any(
                fragment in normalized
                for fragment in (
                    "password",
                    "secret",
                    "token",
                    "authorization",
                    "apikey",
                    "privatekey",
                )
            ) and child not in (None, ""):
                result[key] = "[REDACTED]"
            else:
                result[key] = _redact_tree(child)
        return result
    if isinstance(value, list):
        return [_redact_tree(child) for child in value]
    if isinstance(value, tuple):
        return tuple(_redact_tree(child) for child in value)
    return value


def _controlled_error(error: BaseException) -> BaseException:
    """Find a controlled nested failure inside Ray/Python wrappers safely."""
    pending: list[object] = [error]
    seen: set[int] = set()
    fallback = error
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, BaseException):
            name = type(current).__name__
            if name in _ERROR_CODES or name in {
                "OperationalError",
                "ArtifactUploadError",
                "JobConfigurationError",
            }:
                return current
            if current.__cause__ is not None:
                pending.append(current.__cause__)
            if not current.__suppress_context__ and current.__context__ is not None:
                pending.append(current.__context__)
        if isinstance(current, Mapping):
            pending.extend(current.values())
            continue
        if isinstance(current, (Sequence, Set)) and not isinstance(
            current, (str, bytes, bytearray)
        ):
            pending.extend(current)
            continue
        for attribute in (
            "cause",
            "worker_failures",
            "controller_failure",
            "health_check_failure",
            "exceptions",
            "exception",
            "_base_exc",
        ):
            try:
                candidate = getattr(current, attribute)
            except (AttributeError, RuntimeError):
                continue
            if candidate is not current:
                pending.append(candidate)
    return fallback


def protocol_error_code(name: str) -> str:
    if name in _PROTOCOL_CODES:
        return name
    return _ERROR_CODES.get(name, "UNKNOWN")


def failure_phase(error: BaseException) -> str:
    """Map controlled Core/data failures to their most specific protocol phase."""
    name = type(_controlled_error(error)).__name__
    if name in {
        "DataSourceError",
        "EmptyDatasetError",
        "FileNotFoundError",
        "ConnectionError",
        "OperationalError",
    }:
        return "LOADING_DATA"
    if name in {"TrainingDataError", "JobConfigurationError"}:
        return "DATA_SPLITTING"
    if name in {"ModelExportError", "ArtifactUploadError"}:
        return "EVALUATING"
    return "TRAINING"


def _summary_metrics(summary: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for section in ("final_metrics", "evaluation", "metrics"):
        values = summary.get(section)
        if isinstance(values, Mapping):
            result.update(values)
    return result


def _row_counts(
    summary: Mapping[str, Any], metrics: Mapping[str, Any]
) -> dict[str, int]:
    raw = summary.get("row_counts")
    values = raw if isinstance(raw, Mapping) else {}
    train = int(values.get("train", metrics.get("row_count_train", 0)) or 0)
    validation = int(values.get("val", metrics.get("row_count_val", 0)) or 0)
    test = int(
        values.get(
            "test", metrics.get("row_count_test", metrics.get("eval_test_rows", 0))
        )
        or 0
    )
    return {
        "total": train + validation + test,
        "train": train,
        "validation": validation,
        "test": test,
    }


def _evaluation_payload(
    request: TrainingJobRequest,
    summary: Mapping[str, Any],
    metrics: Mapping[str, Any],
    rows: Mapping[str, int],
) -> dict[str, Any] | None:
    if not request.evaluation.enabled:
        return None
    assert request.target is not None
    allowed = _EVALUATION_METRICS[request.target.task_type.upper()]
    requested = {
        request.evaluation.primary_metric.lower(),
        *(name.lower() for name in request.evaluation.additional_metrics),
    }
    scalar_metrics = []
    for wire_name, source_name in allowed.items():
        if wire_name not in requested:
            continue
        value = metrics.get(f"eval_{source_name}")
        if value is None:
            continue
        scalar_metrics.append(
            {
                "metric_name": wire_name,
                "value": finite_float(
                    value, f"training_result.evaluation.metrics.eval_{source_name}"
                ),
            }
        )
    details: dict[str, Any] = {}
    artifacts = request.evaluation.artifacts
    if artifacts.confusion_matrix:
        if "eval_cm" in metrics:
            details["confusion_matrix"] = {
                "labels": [str(value) for value in metrics.get("eval_cm_labels", [])],
                "matrix": metrics["eval_cm"],
            }
        elif all(f"eval_cm_{key}" in metrics for key in ("tp", "fp", "fn", "tn")):
            details["confusion_matrix"] = {
                key: int(metrics[f"eval_cm_{key}"]) for key in ("tp", "fp", "fn", "tn")
            }
    if artifacts.roc_curve and metrics.get("eval_roc_fpr") is not None:
        details["roc_curve"] = {
            "fpr": [
                finite_float(value, "training_result.evaluation.details.roc_curve.fpr")
                for value in metrics.get("eval_roc_fpr", [])
            ],
            "tpr": [
                finite_float(value, "training_result.evaluation.details.roc_curve.tpr")
                for value in metrics.get("eval_roc_tpr", [])
            ],
        }
    if artifacts.threshold_analysis and metrics.get("eval_thr_thresholds") is not None:
        details["threshold_analysis"] = {
            "thresholds": metrics.get("eval_thr_thresholds", []),
            "precision_values": metrics.get("eval_thr_precision", []),
            "recall_values": metrics.get("eval_thr_recall", []),
            "f1_values": metrics.get("eval_thr_f1", []),
            "predicted_positive_rows": metrics.get("eval_thr_predicted_positive", []),
        }
    return {
        "eval_type": request.target.task_type if request.target else None,
        "sample_rows": rows["test"],
        "metrics": scalar_metrics,
        "details": details,
    }


def _feature_analysis(
    request: TrainingJobRequest,
    summary: Mapping[str, Any],
    metrics: Mapping[str, Any],
) -> dict[str, Any] | None:
    if not request.evaluation.artifacts.feature_importance:
        return None
    ranks = metrics.get("feat_imp_rank")
    names = metrics.get("feat_imp_name")
    scores = metrics.get("feat_imp_score")
    if (
        not isinstance(ranks, list)
        or not isinstance(names, list)
        or not isinstance(scores, list)
    ):
        return None
    feature_ids = summary.get("feature_id_map")
    id_map = feature_ids if isinstance(feature_ids, Mapping) else {}
    size = min(len(ranks), len(names), len(scores))
    return {
        "importance_ranking": [
            {
                "rank": int(ranks[index]),
                "feature_id": str(id_map.get(str(names[index]), "")),
                "model_feature_name": str(names[index]),
                "importance_score": finite_float(
                    scores[index], "training_result.feature_analysis.importance_score"
                ),
            }
            for index in range(size)
        ]
    }


def _artifact_manifest(
    request: TrainingJobRequest, summary: Mapping[str, Any]
) -> dict[str, Any] | None:
    refs = summary.get("artifact_refs")
    if not isinstance(refs, list) or not refs:
        raise ValueError("artifact_refs is required for strict ONNX completion")
    alternatives = []
    total_size = 0
    for index, raw in enumerate(refs):
        if not isinstance(raw, Mapping):
            continue
        if str(raw.get("kind", "")).lower() != "onnx":
            continue
        uri = str(raw.get("uri", ""))
        size = int(raw.get("size_bytes", 0) or 0)
        digest = str(raw.get("sha256", ""))
        if not uri:
            raise ValueError(f"artifact_refs.{index}.uri is required")
        if size < 0:
            raise ValueError(f"artifact_refs.{index}.size_bytes must be non-negative")
        if not digest:
            raise ValueError(f"artifact_refs.{index}.sha256 is required")
        total_size += size
        alternatives.append(
            {
                "format": str(raw.get("kind", "unknown")),
                "files": [
                    {
                        "path": _relative_artifact_path(request, uri, index),
                        "size": size,
                        "hash": f"sha256:{digest}",
                        "metadata": {"artifact_index": index},
                    }
                ],
            }
        )
    if not alternatives:
        raise ValueError("artifact_refs must contain a valid ONNX artifact")
    storage = request.storage_context
    return {
        "model_id": request.model_id,
        "version_id": request.version_id,
        "tenant_id": request.tenant_id,
        "created_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "algorithm_key": request.algorithm.algorithm_key,
        "storage": (
            {
                "type": storage.type,
                "bucket": storage.bucket,
                "prefix": storage.prefix,
                "properties": {},
            }
            if storage is not None
            else None
        ),
        "model_artifacts": {
            "model_weights": {
                "comment": "Core exported model artifact",
                "required_for_inference": True,
                "alternatives": alternatives,
            }
        },
        "total_size_bytes": total_size,
    }


def _relative_artifact_path(request: TrainingJobRequest, uri: str, index: int) -> str:
    storage = request.storage_context
    if storage is None:
        raise ValueError("storage_context is required for artifact publication")
    storage_type = storage.type.upper()
    prefix = storage.prefix.strip("/")
    if storage_type == "S3":
        root = (
            f"s3://{storage.bucket}/{prefix}/" if prefix else f"s3://{storage.bucket}/"
        )
    elif storage_type in {"FILE", "FILESYSTEM", "LOCAL"}:
        root = f"{storage.prefix.rstrip('/')}/"
    else:
        raise ValueError(f"Unsupported storage_context type: {storage.type!r}")
    if not uri.startswith(root):
        raise ValueError(f"artifact_refs.{index}.uri is outside storage_context prefix")
    relative = uri.removeprefix(root)
    if not relative or relative.startswith("/") or ".." in relative.split("/"):
        raise ValueError(f"artifact_refs.{index}.uri has an invalid relative path")
    return relative


def build_completed_payload(
    request: TrainingJobRequest,
    summary: Mapping[str, Any],
    *,
    duration_seconds: float,
) -> dict[str, Any]:
    if request.target is None:
        raise ValueError("target is required for completed training payload")
    metrics = _summary_metrics(summary)
    rows = _row_counts(summary, metrics)
    primary_name = request.evaluation.primary_metric.lower()
    primary_source = _EVALUATION_METRICS[request.target.task_type.upper()].get(
        primary_name, primary_name
    )
    primary_value = metrics.get(f"eval_{primary_source}", metrics.get(primary_source))
    if request.evaluation.enabled and primary_value is None:
        raise ValueError(
            "evaluation primary metric "
            f"{request.evaluation.primary_metric!r} is missing"
        )
    feature_columns = summary.get("feature_columns")
    columns = feature_columns if isinstance(feature_columns, list) else []
    feature_ids = summary.get("feature_id_map")
    id_map = {feature.result_column: feature.feature_id for feature in request.features}
    if isinstance(feature_ids, Mapping):
        id_map.update(
            {str(key): str(value) for key, value in feature_ids.items() if value}
        )
    missing_feature_ids = [str(name) for name in columns if not id_map.get(str(name))]
    if missing_feature_ids:
        raise ValueError(
            "feature_id is missing for model feature(s): "
            + ", ".join(missing_feature_ids)
        )
    payload: dict[str, Any] = {
        "phase": "COMPLETED",
        "progress_percent": 100,
        "duration_seconds": finite_float(duration_seconds, "duration_seconds"),
        "result_summary": {
            "primary_metric": (
                {
                    "name": primary_name,
                    "value": finite_float(
                        primary_value, "result_summary.primary_metric.value"
                    ),
                }
                if primary_value is not None
                else None
            ),
            "sample_rows": rows,
        },
        "training_result": {
            "algorithm_key": request.algorithm.algorithm_key,
            "task_type": request.target.task_type if request.target else None,
            "model_features": [
                {
                    "model_feature_index": index,
                    "model_feature_name": str(name),
                    "feature_id": str(id_map[str(name)]),
                    "transformation": "PASSTHROUGH",
                }
                for index, name in enumerate(columns)
            ],
            "evaluation": _evaluation_payload(request, summary, metrics, rows),
            "feature_analysis": _feature_analysis(request, summary, metrics),
            "tuning_result": None,
        },
        "artifact_manifest": _artifact_manifest(request, summary),
        "warnings": (
            _redact_tree(list(summary.get("warnings", [])))
            if isinstance(summary.get("warnings"), list)
            else []
        ),
    }
    _reject_non_finite(payload)
    return payload


def build_failed_payload(
    error: BaseException | str,
    *,
    phase: str,
    duration_seconds: float,
    error_code: str | None = None,
) -> dict[str, Any]:
    controlled = _controlled_error(error) if isinstance(error, BaseException) else error
    name = (
        type(controlled).__name__
        if isinstance(controlled, BaseException)
        else "UNKNOWN"
    )
    return {
        "phase": phase,
        "error_code": protocol_error_code(
            error_code if error_code in _PROTOCOL_CODES else name
        ),
        "error_message": redact_sensitive(str(controlled)),
        "duration_seconds": finite_float(duration_seconds, "duration_seconds"),
    }


def build_cancelled_payload(
    *, phase: str, duration_seconds: float, has_best_model: bool
) -> dict[str, Any]:
    return {
        "phase": phase,
        "duration_seconds": finite_float(duration_seconds, "duration_seconds"),
        "has_best_model": bool(has_best_model),
    }
