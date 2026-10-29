"""Offline protocol export, self-test and backend/runtime evidence CI commands."""

from __future__ import annotations

import argparse
import hashlib
import json
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from contextlib import contextmanager
from importlib.resources import files
from pathlib import Path
from typing import Any

from .suite import run_vectors
from .validation import (
    KINDS,
    ContractViolation,
    load_resource,
    parse_json,
    validate_feedback,
    validate_payload,
    validate_projection,
    validate_request_records,
    validate_result_rows,
    validate_trace,
)


def _write(path: Path | None, value: Any) -> None:
    if path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )


def _candidate_file(root: Path, name: Any) -> Any:
    if not isinstance(name, str) or not name:
        raise ContractViolation("CANDIDATE_FILE")
    path = (root / name).resolve()
    if not path.is_relative_to(root.resolve()) or not path.is_file():
        raise ContractViolation("CANDIDATE_FILE")
    return parse_json(path.read_text(encoding="utf-8"))


@contextmanager
def _location(path: str) -> Iterator[None]:
    try:
        yield
    except ContractViolation as error:
        raise ContractViolation(error.code, path + error.path[1:]) from None


def _ray_job(base: str, submission: str) -> dict[str, Any]:
    url = urllib.parse.urlsplit(base)
    if (
        url.scheme not in {"http", "https"}
        or not url.hostname
        or url.username
        or url.password
        or url.query
        or url.fragment
    ):
        raise ContractViolation("RAY_URL")
    target = base.rstrip("/") + "/api/jobs/" + urllib.parse.quote(submission, safe="")
    try:
        with urllib.request.urlopen(target, timeout=15) as response:
            value = parse_json(response.read(2 * 1024 * 1024).decode("utf-8"))
    except (OSError, ValueError):
        raise ContractViolation("RAY_PROBE_FAILED") from None
    if not isinstance(value, dict):
        raise ContractViolation("RAY_PROBE_FAILED")
    return value


def check_candidate(
    root: Path, *, level: str, ray_url: str | None = None
) -> dict[str, Any]:
    """Check app-produced JSON; runtime level additionally probes live Ray Jobs.

    Evidence proves the wire facts provided by the owning CI job, not ML numeric
    equivalence or exactly-once writes. Those require the runtime's own E2E gate.
    """
    if level not in {"backend", "runtime"}:
        raise ValueError("unknown evidence level")
    if level == "runtime" and not ray_url:
        raise ContractViolation("LIVE_RAY_REQUIRED")
    manifest = _candidate_file(root, "candidate.json")
    if (
        not isinstance(manifest, dict)
        or manifest.get("contract_revision")
        != load_resource("spec-lock.json")["contract_revision"]
    ):
        raise ContractViolation("CANDIDATE_REVISION")
    producer = manifest.get("producer", {})
    if not isinstance(producer, dict) or not all(
        isinstance(producer.get(k), str) and producer[k]
        for k in ("component", "revision")
    ):
        raise ContractViolation("PRODUCER_PROVENANCE")
    for name in ("requests", "projections", "traces", "result_sets"):
        items = manifest.get(name, [])
        if not isinstance(items, list) or any(
            not isinstance(item, dict) for item in items
        ):
            raise ContractViolation("CANDIDATE_LIST", "$." + name)
    requests: dict[str, dict[str, Any]] = {}
    operations: dict[str, str] = {}
    for index, item in enumerate(manifest.get("requests", [])):
        if not isinstance(item, dict) or item.get("operation") not in {
            "training",
            "inference",
        }:
            raise ContractViolation("CANDIDATE_REQUEST")
        operation = item["operation"]
        with _location(f"$.requests[{index}].payload"):
            payload = _candidate_file(root, item.get("file"))
            validate_payload(operation + "-request", payload)
        with _location(f"$.requests[{index}].wire"):
            validate_request_records(
                _candidate_file(root, item.get("wire_file")),
                operation,
                expected=payload,
            )
        identity = payload["job_id" if operation == "training" else "execution_id"]
        if identity != item.get("outer_identity") or identity in requests:
            raise ContractViolation("CANDIDATE_REQUEST_IDENTITY")
        requests[identity] = payload
        operations[identity] = operation
    if set(operations.values()) != {"training", "inference"}:
        raise ContractViolation("BOTH_REQUEST_OPERATIONS_REQUIRED")
    projections = manifest.get("projections", [])
    if not projections:
        raise ContractViolation("BACKEND_PROJECTION_REQUIRED")
    projected_events = []
    for item in projections:
        completed = _candidate_file(root, item.get("completed"))
        inference = _candidate_file(root, item.get("inference"))
        validate_projection(completed, inference)
        training = requests.get(completed["job_id"])
        if (
            training is None
            or operations[completed["job_id"]] != "training"
            or requests.get(inference["execution_id"]) != inference
            or any(
                training[key] != inference["model"][key]
                for key in ("model_id", "version_id")
            )
            or training["tenant_id"] != inference["tenant_id"]
        ):
            raise ContractViolation("PROJECTION_REQUEST_LINK")
        projected_events.append(completed)
    summary = {
        "level": level,
        "request_count": len(requests),
        "projection_count": len(projections),
        "live_ray_probed": False,
    }
    traces = {}
    for index, item in enumerate(manifest.get("traces", [])):
        identity = item.get("identity")
        if (
            not isinstance(identity, str)
            or identity not in requests
            or identity in traces
        ):
            raise ContractViolation("CANDIDATE_TRACE_IDENTITY")
        with _location(f"$.traces[{index}]"):
            records = _candidate_file(root, item.get("file"))
            result = validate_feedback(
                requests[identity], records, operations[identity]
            )
        traces[identity] = (records, result)
    if set(traces) != set(requests):
        raise ContractViolation("ALL_REQUEST_TRACES_REQUIRED")
    for completed in projected_events:
        actual = [
            parse_json(record["fields"]["payload"])
            for record in traces[completed["job_id"]][0]
        ]
        if completed not in actual:
            raise ContractViolation("PROJECTION_FEEDBACK_MISMATCH")
    summary.update(trace_count=len(traces), request_wire_checked=True)
    if level == "backend":
        return summary
    for item in manifest.get("traces", []):
        identity = item["identity"]
        result = traces[identity][1]
        submission = item.get("submission_id")
        if not isinstance(submission, str) or not submission:
            raise ContractViolation("RAY_SUBMISSION_REQUIRED")
        live = _ray_job(ray_url or "", submission)
        state = live.get("status")
        valid = {
            "COMPLETED": {"SUCCEEDED"},
            "FAILED": {"FAILED", "STOPPED"},
            "CANCELLED": {"FAILED", "STOPPED"},
        }
        if state not in valid[result["terminal"]]:
            raise ContractViolation("RAY_BUSINESS_STATE")
        metadata = live.get("metadata") or {}
        expected_entry = (
            "python -m tributo_broker_redis.profiles.aimodel_v2.execution_driver"
        )
        if (
            not isinstance(metadata, dict)
            or metadata.get("tributo.operation_id") != identity
            or metadata.get("tributo.protocol_profile") != "aimodel-v2"
            or live.get("entrypoint") != expected_entry
        ):
            raise ContractViolation("RAY_BROKER_IDENTITY")
    results = {}
    for item in manifest.get("result_sets", []):
        identity = item.get("identity")
        if (
            not isinstance(identity, str)
            or identity not in requests
            or operations[identity] != "inference"
            or identity in results
        ):
            raise ContractViolation("RESULT_SET_IDENTITY")
        rows = _candidate_file(root, item.get("file"))
        if not isinstance(rows, list):
            raise ContractViolation("RESULT_SET_TYPE")
        validate_result_rows(requests[identity], rows)
        # Redis deliveries may replay an older entry after the terminal delivery.
        # validate_trace already rejects distinct conflicting terminal entries.
        terminal = next(
            event
            for record in traces[identity][0]
            if (event := parse_json(record["fields"]["payload"]))["event_type"]
            in {"COMPLETED", "FAILED", "CANCELLED"}
        )
        if terminal["event_type"] == "COMPLETED" and terminal["result_rows"] != len(
            rows
        ):
            raise ContractViolation("WRITTEN_ROW_COUNT")
        results[identity] = len(rows)
    if set(results) != {k for k, v in operations.items() if v == "inference"}:
        raise ContractViolation("ALL_INFERENCE_RESULTS_REQUIRED")
    summary.update(
        live_ray_probed=True, trace_count=len(traces), result_rows=sum(results.values())
    )
    return summary


def verify_spec(root: Path) -> None:
    lock = load_resource("spec-lock.json")
    for document in lock["documents"]:
        path = root / document["path"]
        if (
            not path.is_file()
            or hashlib.sha256(path.read_bytes()).hexdigest() != document["sha256"]
        ):
            raise ContractViolation("SPEC_SNAPSHOT_DRIFT")


def export_bundle(output: Path) -> None:
    if output.exists() and any(output.iterdir()):
        raise ContractViolation("EXPORT_DIRECTORY_NOT_EMPTY")
    source = files(__package__).joinpath("data")

    def copy_tree(node: Any, destination: Path) -> None:
        destination.mkdir(parents=True, exist_ok=True)
        for child in node.iterdir():
            target = destination / child.name
            if child.is_dir():
                copy_tree(child, target)
            else:
                target.write_bytes(child.read_bytes())

    copy_tree(source, output)


def _junit(path: Path, results: list[dict[str, Any]]) -> None:
    suite = ET.Element(
        "testsuite",
        name="aimodel-v2-contracts",
        tests=str(len(results)),
        failures=str(sum(not r["passed"] for r in results)),
    )
    for result in results:
        case = ET.SubElement(
            suite, "testcase", name=result["case"], classname="protocol.contract"
        )
        if not result["passed"]:
            ET.SubElement(
                case, "failure", message=result.get("code") or "contract mismatch"
            )
    path.parent.mkdir(parents=True, exist_ok=True)
    ET.ElementTree(suite).write(path, encoding="utf-8", xml_declaration=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Pinned v2 protocol contracts; no execution dependencies"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("self-test")
    run.add_argument("--junit", type=Path)
    run.add_argument("--report", type=Path)
    export = commands.add_parser("export")
    export.add_argument("--output", type=Path, required=True)
    validate = commands.add_parser("validate")
    validate.add_argument("--kind", choices=KINDS, required=True)
    validate.add_argument("--input", type=Path, required=True)
    trace = commands.add_parser("trace")
    trace.add_argument("--operation", choices=["training", "inference"], required=True)
    trace.add_argument("--input", type=Path, required=True)
    trace.add_argument(
        "--consumer",
        action="store_true",
        help="Test tolerant consumer replay instead of strict producer output",
    )
    projection = commands.add_parser("projection")
    projection.add_argument("--completed", type=Path, required=True)
    projection.add_argument("--inference", type=Path, required=True)
    spec = commands.add_parser("verify-spec")
    spec.add_argument("--root", type=Path, required=True)
    coverage = commands.add_parser("coverage")
    coverage.add_argument("--require-full", action="store_true")
    candidate = commands.add_parser("candidate")
    candidate.add_argument("--directory", type=Path, required=True)
    candidate.add_argument("--level", choices=["backend", "runtime"], required=True)
    candidate.add_argument("--ray-url")
    candidate.add_argument("--report", type=Path)
    candidate.add_argument("--junit", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "self-test":
            results = run_vectors()
            for operation in ("training", "inference"):
                try:
                    validate_trace(
                        load_resource(f"fixtures/{operation}-trace.json"),
                        operation,
                        producer=True,
                    )
                    result = {"case": operation + "-trace", "passed": True}
                except ContractViolation as error:
                    result = {
                        "case": operation + "-trace",
                        "passed": False,
                        "code": error.code,
                    }
                results.append(result)
            for name, action in (
                (
                    "manifest-projection",
                    lambda: validate_projection(
                        load_resource("fixtures/training-completed.json"),
                        load_resource("fixtures/inference-request.json"),
                    ),
                ),
                (
                    "result-columns",
                    lambda: validate_result_rows(
                        load_resource("fixtures/inference-request.json"),
                        load_resource("fixtures/inference-rows.json"),
                    ),
                ),
            ):
                try:
                    action()
                    results.append({"case": name, "passed": True})
                except ContractViolation as error:
                    results.append({"case": name, "passed": False, "code": error.code})
            if args.junit:
                _junit(args.junit, results)
            _write(args.report, {"results": results, "full_protocol_compatible": False})
            print(
                json.dumps(
                    {
                        "checks": len(results),
                        "passed": sum(r["passed"] for r in results),
                        "full_protocol_compatible": False,
                    }
                )
            )
            return int(any(not r["passed"] for r in results))
        if args.command == "export":
            export_bundle(args.output)
        elif args.command == "validate":
            validate_payload(
                args.kind, parse_json(args.input.read_text(encoding="utf-8"))
            )
        elif args.command == "trace":
            print(
                json.dumps(
                    validate_trace(
                        parse_json(args.input.read_text(encoding="utf-8")),
                        args.operation,
                        producer=not args.consumer,
                    )
                )
            )
        elif args.command == "projection":
            validate_projection(
                parse_json(args.completed.read_text(encoding="utf-8")),
                parse_json(args.inference.read_text(encoding="utf-8")),
            )
        elif args.command == "verify-spec":
            verify_spec(args.root)
        elif args.command == "candidate":
            report = check_candidate(
                args.directory, level=args.level, ray_url=args.ray_url
            )
            _write(args.report, report)
            if args.junit:
                _junit(args.junit, [{"case": "candidate", "passed": True}])
            print(json.dumps(report))
        elif args.command == "coverage":
            report = load_resource("coverage.json")
            print(json.dumps(report, ensure_ascii=False, indent=2))
            return int(args.require_full and not report["full_protocol_compatible"])
        return 0
    except (ContractViolation, OSError, json.JSONDecodeError) as error:
        code = (
            error.code
            if isinstance(error, ContractViolation)
            else "CONTRACT_INPUT_ERROR"
        )
        path = error.path if isinstance(error, ContractViolation) else "$"
        report = {"passed": False, "code": code, "path": path}
        _write(getattr(args, "report", None), report)
        if args.command == "candidate" and args.junit:
            _junit(args.junit, [{"case": "candidate", "passed": False, "code": code}])
        print(json.dumps(report))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
