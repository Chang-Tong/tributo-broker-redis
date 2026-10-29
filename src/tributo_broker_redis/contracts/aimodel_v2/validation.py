"""Independent, offline wire oracle derived from the pinned protocol clauses.

These checks deliberately do not call Broker validators, reporters or executors.
JSON Schema describes structure; explicit checks cover cross-field semantics.
"""

from __future__ import annotations

import json
import math
import posixpath
import re
from functools import cache
from importlib.resources import files
from typing import Any

KINDS = (
    "training-request",
    "inference-request",
    "training-event",
    "inference-event",
    "inference-result",
)
TERMINALS = {"COMPLETED", "FAILED", "CANCELLED"}
METRIC_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


class ContractViolation(ValueError):
    """Safe field/code diagnostic: never interpolate a payload value."""

    def __init__(self, code: str, path: str = "$") -> None:
        self.code, self.path = code, path
        super().__init__(f"{code} at {path}")


def load_resource(name: str) -> Any:
    parts = name.split("/")
    if (
        any(part in {"", ".", ".."} for part in parts)
        or name.startswith("/")
        or any(not re.fullmatch(r"[A-Za-z0-9_.-]+", part) for part in parts)
    ):
        raise ValueError("resource path must be relative")
    return json.loads(
        files(__package__).joinpath("data", *parts).read_text(encoding="utf-8")
    )


def _finite(value: Any, path: str = "$") -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ContractViolation("NON_FINITE_NUMBER", path)
    if isinstance(value, dict):
        for key, item in value.items():
            _finite(item, f"{path}.{key}")
    if isinstance(value, list):
        for index, item in enumerate(value):
            _finite(item, f"{path}[{index}]")


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, item in pairs:
        if key in result:
            raise ContractViolation("DUPLICATE_JSON_KEY")
        result[key] = item
    return result


def parse_json(raw: str) -> Any:
    try:
        value = json.loads(raw, object_pairs_hook=_pairs)
    except (ValueError, TypeError) as error:
        if isinstance(error, ContractViolation):
            raise
        raise ContractViolation("INVALID_JSON") from None
    _finite(value)
    return value


@cache
def _validator(kind: str) -> Any:
    from jsonschema import Draft202012Validator, FormatChecker
    from referencing import Registry, Resource

    if kind not in KINDS:
        raise ValueError("unknown contract kind")
    registry = Registry()
    for name in ("common", *KINDS):
        schema = load_resource(f"schemas/{name}.schema.json")
        Draft202012Validator.check_schema(schema)
        registry = registry.with_resource(schema["$id"], Resource.from_contents(schema))
    schema = load_resource(f"schemas/{kind}.schema.json")
    return Draft202012Validator(
        schema, registry=registry, format_checker=FormatChecker()
    )


def _paths(value: dict[str, Any], path: str) -> None:
    # Artifact paths are relative object keys, never URL or filesystem traversal.
    if (
        "://" in path
        or "\\" in path
        or path.startswith("/")
        or any(p in {"", ".", ".."} for p in path.rstrip("/").split("/"))
    ):
        raise ContractViolation("UNSAFE_ARTIFACT_PATH")


def _topology(value: dict[str, Any]) -> None:
    tables = value["tables"]
    aliases = [t["table_alias"] for t in tables]
    primary = [t["table_alias"] for t in tables if t["role"] == "PRIMARY"]
    if len(set(aliases)) != len(aliases) or len(primary) != 1:
        raise ContractViolation("TABLE_TOPOLOGY")
    seen = {primary[0]}
    for relation in value.get("relations", []):
        if (
            relation["left_alias"] not in seen
            or relation["right_alias"] not in aliases
            or relation["right_alias"] in seen
        ):
            raise ContractViolation("RELATION_TOPOLOGY")
        seen.add(relation["right_alias"])
    if seen != set(aliases):
        raise ContractViolation("RELATION_TOPOLOGY")
    features = value["features"]
    if len({f["feature_id"] for f in features}) != len(features) or len(
        {f["result_column"] for f in features}
    ) != len(features):
        raise ContractViolation("DUPLICATE_FEATURE")
    columns = [*features]
    if value.get("entity_key"):
        columns.append(value["entity_key"])
    for column in columns:
        if column["origin"]["table_alias"] not in aliases:
            raise ContractViolation("UNKNOWN_ORIGIN_ALIAS")


def _query(value: dict[str, Any]) -> None:
    required = set(re.findall(r"\{([A-Za-z_][A-Za-z0-9_]*):[^{}]+\}", value["sql"]))
    if not required <= set(value.get("params") or {}):
        raise ContractViolation("MISSING_SQL_PARAMETER")


def _mapping(value: Any) -> None:
    if value and set(value.values()) != set(range(len(value))):
        raise ContractViolation("LABEL_MAPPING_CODES")


def _storage_artifacts(value: dict[str, Any]) -> None:
    storage = value["storage"]
    _paths(storage, storage["prefix"])
    for entry in value["model_artifacts"].values():
        for alternative in entry["alternatives"]:
            for artifact in alternative["files"]:
                _paths(artifact, artifact["path"])


def _category(model: dict[str, Any], category_key: str) -> None:
    allowed = {
        "CLASSIFICATION": {"BINARY_CLASSIFICATION", "MULTICLASS_CLASSIFICATION"},
        "REGRESSION": {"REGRESSION"},
        "CLUSTERING": {"CLUSTERING"},
        "TIMESERIES": {"TIMESERIES_FORECAST"},
    }
    if model["task_type"] not in allowed[model[category_key]]:
        raise ContractViolation("TASK_CATEGORY")
    _mapping(model.get("label_mapping"))
    positive = model.get("positive_label_value")
    if (
        positive is not None
        and model.get("label_mapping")
        and str(positive) not in model["label_mapping"]
    ):
        raise ContractViolation("POSITIVE_LABEL")


def validate_payload(kind: str, payload: Any) -> None:
    _finite(payload)
    errors = iter(_validator(kind).iter_errors(payload))
    error = next(errors, None)
    if error is not None:
        path = "$" + "".join(
            f"[{x}]" if isinstance(x, int) else f".{x}" for x in error.absolute_path
        )
        if error.validator == "required":
            missing = next(
                key for key in error.validator_value if key not in error.instance
            )
            path += "." + missing
        raise ContractViolation("SCHEMA_" + str(error.validator).upper(), path)
    if kind == "training-request":
        _topology(payload)
        query = payload["data_query"]
        for name in ("query", "entity_sampling_query", "feature_query", "target_query"):
            if query.get(name):
                _query(query[name])
        if (
            payload.get("relations") or query["mode"] == "TIMESERIES_PIVOT"
        ) and not query.get("entity_key"):
            raise ContractViolation("ENTITY_KEY_REQUIRED")
        for column in [payload.get("target"), query.get("entity_key")]:
            if column and column["origin"]["table_alias"] not in {
                t["table_alias"] for t in payload["tables"]
            }:
                raise ContractViolation("UNKNOWN_ORIGIN_ALIAS")
        if payload.get("target"):
            _category(
                {**payload["target"], "category": payload["algorithm"]["category"]},
                "category",
            )
            if payload["target"]["result_column"] in {
                f["result_column"] for f in payload["features"]
            }:
                raise ContractViolation("TARGET_LEAKAGE")
        split = payload.get("data_split", {})
        ratios = [
            split.get(name, default)
            for name, default in (
                ("train_ratio", 0.7),
                ("validation_ratio", 0.15),
                ("test_ratio", 0.15),
            )
        ]
        if not math.isclose(sum(ratios), 1, rel_tol=0, abs_tol=1e-6):
            raise ContractViolation("SPLIT_SUM")
        if split.get("stratify") and (
            split.get("strategy") == "TIME_ORDERED"
            or payload["algorithm"]["category"] != "CLASSIFICATION"
        ):
            raise ContractViolation("STRATIFY_MODE")
        _paths(payload["storage_context"], payload["storage_context"]["prefix"])
    elif kind == "inference-request":
        _topology(payload["input"])
        _query(payload["input"]["query"])
        _category(payload["model"], "algorithm_category")
        _storage_artifacts(payload["model"])
        result_filter = payload["output"].get("result_filter")
        if result_filter:
            if payload["model"]["task_type"] not in {
                "BINARY_CLASSIFICATION",
                "MULTICLASS_CLASSIFICATION",
                "CLUSTERING",
            }:
                raise ContractViolation("RESULT_FILTER_TASK")
            if not set(result_filter["labels"]) <= set(
                payload["model"].get("label_mapping") or {}
            ):
                raise ContractViolation("RESULT_FILTER_LABELS")
    elif kind.endswith("-event"):
        event = payload["event_type"]
        if (
            "processed_rows" in payload
            and "result_rows" in payload
            and payload["result_rows"] > payload["processed_rows"]
        ):
            raise ContractViolation("RESULT_ROWS_EXCEED_PROCESSED")
        if (
            "total_rows" in payload
            and payload.get("processed_rows", 0) > payload["total_rows"]
        ):
            raise ContractViolation("PROCESSED_ROWS_EXCEED_TOTAL")
        if event == "METRICS":
            current, total = payload["current_round"], payload["total_rounds"]
            if (
                current > total
                or abs(payload["progress_percent"] - round(current / total * 100, 1))
                > 0.051
            ):
                raise ContractViolation("ROUND_PROGRESS")
        if event == "COMPLETED" and kind == "training-event":
            _storage_artifacts(payload["artifact_manifest"])
            rows = payload["result_summary"]["sample_rows"]
            if rows["total"] != rows["train"] + rows["validation"] + rows["test"]:
                raise ContractViolation("SAMPLE_ROWS_SUM")
        if (
            event == "COMPLETED"
            and kind == "inference-event"
            and payload["processed_rows"] != payload["total_rows"]
        ):
            raise ContractViolation("INCOMPLETE_RESULT")


def validate_trace(
    records: list[dict[str, Any]],
    operation: str,
    *,
    require_terminal: bool = True,
    producer: bool = False,
) -> dict[str, Any]:
    if operation not in {"training", "inference"}:
        raise ValueError("operation must be training or inference")
    if not isinstance(records, list) or any(
        not isinstance(record, dict) for record in records
    ):
        raise ContractViolation("TRACE_RECORDS")
    identity_key = "job_id" if operation == "training" else "execution_id"
    known = {
        "LOG",
        *TERMINALS,
        *({"PHASE", "METRICS"} if operation == "training" else {"STARTED", "PROGRESS"}),
    }
    identity = stream = None
    seen: dict[tuple[int, int], dict[str, Any]] = {}
    last_id = (-1, -1)
    terminal = None
    counters: dict[str, float] = {}
    unknown = duplicates = unknown_codes = 0
    first_round = None
    for record_index, record in enumerate(records):
        fields = record.get("fields")
        if (
            not isinstance(fields, dict)
            or set(fields) != {identity_key, "payload"}
            or any(not isinstance(v, str) for v in fields.values())
        ):
            raise ContractViolation("WIRE_FIELDS")
        event = parse_json(fields["payload"])
        if not isinstance(event, dict) or not re.fullmatch(
            r"2\.[0-9]+", str(event.get("protocol_version", ""))
        ):
            raise ContractViolation("EVENT_VERSION")
        timestamp = event.get("timestamp")
        if (
            isinstance(timestamp, bool)
            or not isinstance(timestamp, int)
            or timestamp < 0
        ):
            raise ContractViolation("EVENT_TIMESTAMP")
        if ("execution_id" if operation == "training" else "job_id") in event:
            raise ContractViolation("EVENT_IDENTITY_EXCLUSIVITY")
        current_identity = fields[identity_key]
        current_stream = record.get("stream_key", "")
        if (
            not current_identity
            or not isinstance(current_stream, str)
            or event.get(identity_key) != current_identity
            or not current_stream.endswith(":" + current_identity)
        ):
            raise ContractViolation("WIRE_IDENTITY")
        if identity is None:
            identity, stream = current_identity, current_stream
        if identity != current_identity or stream != current_stream:
            raise ContractViolation("MIXED_TRACE")
        raw_id = record.get("entry_id", "")
        if not isinstance(raw_id, str) or not re.fullmatch(r"[0-9]+-[0-9]+", raw_id):
            raise ContractViolation("STREAM_ID")
        milliseconds, sequence = raw_id.split("-")
        entry_id = (int(milliseconds), int(sequence))
        if entry_id in seen:
            if record != seen[entry_id]:
                raise ContractViolation("REPLAY_CONTENT_CHANGED")
            duplicates += 1
            continue
        if entry_id <= last_id:
            raise ContractViolation("STREAM_ORDER")
        last_id = entry_id
        seen[entry_id] = record
        if terminal is not None:
            raise ContractViolation("EVENT_AFTER_TERMINAL")
        if not isinstance(event.get("event_type"), str) or not event["event_type"]:
            raise ContractViolation("EVENT_TYPE")
        if event["event_type"] not in known:
            unknown += 1
            continue  # Consumers must tolerate future event types.
        if producer:
            # Consumer fallback must never hide an invalid known producer event.
            try:
                validate_payload(operation + "-event", event)
            except ContractViolation as error:
                raise ContractViolation(
                    error.code, f"$[{record_index}]" + error.path[1:]
                ) from None
        if event["event_type"] == "FAILED":
            codes = load_resource(f"schemas/{operation}-event.schema.json")[
                "properties"
            ]["error_code"]["enum"]
            if event.get("error_code") not in codes:
                if not isinstance(event.get("error_code"), str):
                    raise ContractViolation("ERROR_CODE_TYPE")
                event = {**event, "error_code": "UNKNOWN"}
                unknown_codes += 1
        validate_payload(operation + "-event", event)
        for key in (
            "processed_rows",
            "result_rows",
            "current_round",
            "percent",
            "progress_percent",
        ):
            if key in event:
                if event[key] < counters.get(key, -1):
                    raise ContractViolation("PROGRESS_REGRESSION", "$." + key)
                counters[key] = event[key]
        if event["event_type"] == "METRICS" and first_round is None:
            first_round = event["current_round"]
            if first_round != 1:
                raise ContractViolation("MISSING_FIRST_ROUND")
        if event["event_type"] in TERMINALS:
            terminal = event["event_type"]
    if require_terminal and terminal is None:
        raise ContractViolation("MISSING_TERMINAL")
    return {
        "identity": identity,
        "terminal": terminal,
        "unique_events": len(seen),
        "duplicate_deliveries": duplicates,
        "unknown_events": unknown,
        "unknown_error_codes": unknown_codes,
    }


def validate_request_records(
    records: Any, operation: str, *, expected: dict[str, Any]
) -> None:
    """Check actual Redis task entries, not a reserialized DTO projection."""
    validate_payload(operation + "-request", expected)
    if not isinstance(records, list) or not records:
        raise ContractViolation("REQUEST_WIRE_REQUIRED")
    field = "job_id" if operation == "training" else "execution_id"
    for record in records:
        if not isinstance(record, dict):
            raise ContractViolation("REQUEST_WIRE_RECORD")
        fields = record.get("fields")
        if (
            not isinstance(record.get("stream_key"), str)
            or not record["stream_key"]
            or not isinstance(record.get("entry_id"), str)
            or not re.fullmatch(r"[0-9]+-[0-9]+", record["entry_id"])
            or not isinstance(fields, dict)
            or set(fields) != {field, "payload"}
            or any(not isinstance(value, str) for value in fields.values())
        ):
            raise ContractViolation("REQUEST_WIRE_FIELDS")
        payload = parse_json(fields["payload"])
        validate_payload(operation + "-request", payload)
        if fields[field] != expected[field] or payload != expected:
            raise ContractViolation("REQUEST_WIRE_MISMATCH")


def validate_feedback(
    request: dict[str, Any], records: Any, operation: str
) -> dict[str, Any]:
    """Strict known producer events plus request/feedback correlation."""
    validate_payload(operation + "-request", request)
    result = validate_trace(records, operation, producer=True)
    field = "job_id" if operation == "training" else "execution_id"
    if result["identity"] != request[field]:
        raise ContractViolation("FEEDBACK_REQUEST_IDENTITY")
    if operation == "training":
        for record in records:
            event = parse_json(record["fields"]["payload"])
            if event["event_type"] != "COMPLETED":
                continue
            manifest, training = event["artifact_manifest"], event["training_result"]
            for key in ("tenant_id", "model_id", "version_id"):
                if manifest[key] != request[key]:
                    raise ContractViolation(
                        "FEEDBACK_MANIFEST_IDENTITY", "$.artifact_manifest." + key
                    )
            allocated, actual = request["storage_context"], manifest["storage"]
            shared = {"nas", "nfs", "shared_fs"}
            expected_type = "nas" if allocated["type"] in shared else allocated["type"]
            bucket = allocated["bucket"]
            if allocated["type"] in shared:
                bucket = posixpath.normpath(bucket)
            # A concrete Bundle may extend the allocated prefix. The backend
            # must subsequently project that concrete location without changes.
            if (
                actual["type"] != expected_type
                or actual["bucket"] != bucket
                or not actual["prefix"].startswith(allocated["prefix"])
            ):
                raise ContractViolation(
                    "FEEDBACK_STORAGE_SCOPE", "$.artifact_manifest.storage"
                )
            if (
                training["algorithm_key"] != request["algorithm"]["algorithm_key"]
                or manifest["algorithm_key"] != request["algorithm"]["algorithm_key"]
                or training["task_type"]
                != (request.get("target") or {}).get("task_type", "CLUSTERING")
            ):
                raise ContractViolation("FEEDBACK_TRAINING_MODEL")
            features = {feature["feature_id"] for feature in request["features"]}
            if any(
                feature["feature_id"] not in features
                for feature in training["model_features"]
            ):
                raise ContractViolation("FEEDBACK_FEATURE_IDENTITY")
    return result


def validate_projection(completed: dict[str, Any], request: dict[str, Any]) -> None:
    validate_payload("training-event", completed)
    validate_payload("inference-request", request)
    if completed["event_type"] != "COMPLETED":
        raise ContractViolation("PROJECTION_REQUIRES_COMPLETED")
    manifest = completed["artifact_manifest"]
    for key in ("storage", "model_artifacts", "model_id", "version_id"):
        if manifest[key] != request["model"][key]:
            raise ContractViolation("MANIFEST_PROJECTION", "$.model." + key)
    if manifest["tenant_id"] != request["tenant_id"]:
        raise ContractViolation("MANIFEST_PROJECTION", "$.tenant_id")


def validate_result_rows(request: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    validate_payload("inference-request", request)
    if not isinstance(rows, list):
        raise ContractViolation("RESULT_SET_TYPE")
    model = request["model"]
    task = model["task_type"]
    labels = model.get("label_mapping") or (
        {"0": 0, "1": 1} if task == "BINARY_CLASSIFICATION" else {}
    )
    reverse = {code: label for label, code in labels.items()}
    seen = set()
    for row in rows:
        validate_payload("inference-result", row)
        for key, expected in (
            ("execution_id", request["execution_id"]),
            ("tenant_id", request["tenant_id"]),
            ("model_id", model["model_id"]),
            ("version_id", model["version_id"]),
        ):
            if row[key] != expected:
                raise ContractViolation("RESULT_IDENTITY", "$." + key)
        if row["entity_id"] in seen:
            raise ContractViolation("DUPLICATE_ENTITY")
        seen.add(row["entity_id"])
        if task == "BINARY_CLASSIFICATION":
            positive = str(model.get("positive_label_value", reverse.get(1, "1")))
            if (
                len(labels) != 2
                or positive not in labels
                or row["pred_probability"] is None
                or row["pred_value"] is not None
                or row["pred_probabilities"] is not None
            ):
                raise ContractViolation("BINARY_RESULT_COLUMNS")
            extension = request.get("extensions")
            threshold = (
                extension.get("decision_threshold", 0.5)
                if isinstance(extension, dict)
                else 0.5
            )
            if (
                isinstance(threshold, bool)
                or not isinstance(threshold, (int, float))
                or not 0 <= threshold <= 1
            ):
                threshold = 0.5  # Opaque malformed extension is ignored by this oracle.
            code = (
                labels[positive]
                if row["pred_probability"] >= threshold
                else 1 - labels[positive]
            )
            if row["pred_label"] != reverse[code]:
                raise ContractViolation("BINARY_LABEL_PROBABILITY")
        elif task == "MULTICLASS_CLASSIFICATION":
            vector = (
                parse_json(row["pred_probabilities"])
                if row["pred_probabilities"] is not None
                else None
            )
            if isinstance(vector, list) and not labels:
                labels = {str(i): i for i in range(len(vector))}
                reverse = {v: k for k, v in labels.items()}
            if (
                not isinstance(vector, list)
                or len(vector) != len(labels)
                or not vector
                or any(
                    isinstance(v, bool)
                    or not isinstance(v, (int, float))
                    or not 0 <= v <= 1
                    for v in vector
                )
                or not math.isclose(sum(vector), 1, abs_tol=1e-5)
            ):
                raise ContractViolation("MULTICLASS_PROBABILITIES")
            if (
                row["pred_label"]
                != reverse[max(range(len(vector)), key=vector.__getitem__)]
                or row["pred_probability"] is not None
                or row["pred_value"] is not None
            ):
                raise ContractViolation("MULTICLASS_RESULT_COLUMNS")
        elif task in {"REGRESSION", "TIMESERIES_FORECAST"}:
            if row["pred_value"] is None or any(
                row[k] is not None
                for k in ("pred_label", "pred_probability", "pred_probabilities")
            ):
                raise ContractViolation("REGRESSION_RESULT_COLUMNS")
        elif row["pred_label"] is None or any(
            row[k] is not None
            for k in ("pred_value", "pred_probability", "pred_probabilities")
        ):
            raise ContractViolation("CLUSTER_RESULT_COLUMNS")
        if row["pred_extra"] is not None:
            extra = parse_json(row["pred_extra"])
            values = (
                extra.get("inference_feature_values")
                if isinstance(extra, dict)
                else None
            )
            if (
                not isinstance(values, list)
                or any(not isinstance(item, dict) for item in values)
                or [item.get("feature_name") for item in values]
                != [f["result_column"] for f in request["input"]["features"]]
            ):
                raise ContractViolation("RESULT_FEATURE_ALIGNMENT")
        result_filter = request["output"].get("result_filter")
        if result_filter and (
            (row["pred_label"] in result_filter["labels"])
            != (result_filter["mode"] == "INCLUDE")
        ):
            raise ContractViolation("RESULT_FILTER")
