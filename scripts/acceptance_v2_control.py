"""Real v2 failure/recovery gates on the owned, isolated acceptance stack.

Run after the task/shape matrix. All requests use new business identities;
only child Consumer processes started here are killed or restarted.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import traceback
import uuid
from pathlib import Path

import redis
from ray.job_submission import JobSubmissionClient

from tributo_broker_redis.plugin import RedisBrokerPlugin

TERMINALS = {"COMPLETED", "FAILED", "CANCELLED"}


class Controls:
    def __init__(self):
        if os.environ.get("ACCEPT_MATRIX_ISOLATED") != "1":
            raise RuntimeError("isolated acceptance network is required")
        self.root = Path("/artifacts")
        self.out = self.root / "controls"
        self.out.mkdir(exist_ok=True)
        self.prefix = "control-" + uuid.uuid4().hex[:8]
        self.redis = redis.Redis(
            host="repro-redis", password="local-repro-only", decode_responses=True
        )
        self.jobs = JobSubmissionClient("http://repro-ray:8265")
        self.config = json.loads(Path("/etc/tributo-knova/config.json").read_text())
        self.config["broker_id"] = "tributo-redis"
        self.config["protocol"] = {"profile": "aimodel-v2", "protocol_version": "2.0"}
        self.config["transport"] = {
            "url": "redis://repro-redis:6379/0",
            "password_ref": "env:KNOVA_REDIS_PASSWORD",
            "block_ms": 100,
            "claim_idle_ms": 500,
            "max_driver_input_bytes": 262144,
        }
        self.config["execution"]["ray_dashboard_url"] = "http://repro-ray:8265"
        self.config["execution"]["extra_py_modules"] = [
            p
            for p in self.config["execution"]["extra_py_modules"]
            if not p.endswith("/tributo_knova")
        ]
        for kind, channel in self.config["channels"].items():
            for key in (
                "task_stream_key",
                "event_stream_prefix",
                "cancel_key_prefix",
                "consumer_group",
            ):
                channel[key] = f"{self.prefix}:{kind}:{key}"
            channel["group_start_id"] = "0-0"
        self.config_path = self.out / "consumer.json"
        self.config_path.write_text(json.dumps(self.config))
        self.runtime = self.make_runtime()
        self.results = []

    def make_runtime(self):
        runtime = RedisBrokerPlugin().create_runtime(self.config)
        runtime.start()
        return runtime

    def request(self, case, kind="training", matrix_case=None):
        matrix_case = matrix_case or (
            "train_binary_1000" if kind == "training" else "infer_binary_100000"
        )
        value = json.loads(
            (self.root / "cases" / matrix_case / "request.redacted.json").read_text()
        )

        def restore(item):
            if isinstance(item, dict):
                for key, field in item.items():
                    if key == "password":
                        item[key] = "local-test-only"
                    else:
                        restore(field)
            elif isinstance(item, list):
                for field in item:
                    restore(field)

        restore(value)
        value["job_id" if kind == "training" else "execution_id"] = (
            f"{self.prefix}-{case}"
        )
        if kind == "training":
            value["storage_context"]["prefix"] = value["job_id"] + "/"
        return value

    def channel(self, kind):
        return self.config["channels"][
            "training" if kind == "training" else "batch_inference"
        ]

    def identity(self, request, kind):
        return request["job_id" if kind == "training" else "execution_id"]

    def events(self, request, kind):
        identity = self.identity(request, kind)
        stream = self.channel(kind)["event_stream_prefix"] + ":" + identity
        entries = self.redis.xrange(stream)
        field = "job_id" if kind == "training" else "execution_id"
        assert all(set(fields) == {field, "payload"} for _, fields in entries)
        return [json.loads(fields["payload"]) for _, fields in entries]

    def enqueue(self, request, kind="training", raw=None):
        channel = self.channel(kind)
        field = "job_id" if kind == "training" else "execution_id"
        return self.redis.xadd(
            channel["task_stream_key"],
            {
                field: self.identity(request, kind),
                "payload": json.dumps(request) if raw is None else raw,
            },
        )

    def wait(self, request, kind="training", predicate=None, timeout=240):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            events = self.events(request, kind)
            terminal = [e for e in events if e["event_type"] in TERMINALS]
            assert len(terminal) <= 1, "duplicate terminal events"
            if predicate is not None and predicate(events):
                return events
            if terminal:
                assert predicate is None, terminal[0]
                return terminal[0]
            time.sleep(0.25)
        raise TimeoutError(self.identity(request, kind))

    def submit(self, request, kind="training", raw=None):
        self.enqueue(request, kind, raw)
        assert self.runtime.run_once(timeout_ms=1000)

    def matching_jobs(self, request, kind="training"):
        identity = self.identity(request, kind)
        return [
            j
            for j in self.jobs.list_jobs()
            if (j.metadata or {}).get("tributo.operation_id") == identity
        ]

    def save_evidence(self, request, kind="training"):
        identity = self.identity(request, kind)
        events = self.events(request, kind)
        (self.out / f"{identity}.events.json").write_text(json.dumps(events, indent=2))
        for job in self.matching_jobs(request, kind):
            deadline = time.monotonic() + 120
            while str(job.status) not in {"SUCCEEDED", "FAILED", "STOPPED"}:
                assert time.monotonic() < deadline, "Ray job did not stop"
                time.sleep(0.5)
                job = self.jobs.get_job_info(job.submission_id)
            terminal = next(e for e in events if e["event_type"] in TERMINALS)
            expected = (
                {"SUCCEEDED"}
                if terminal["event_type"] == "COMPLETED"
                else {"FAILED", "STOPPED"}
            )
            assert str(job.status) in expected, str(job.status)
            (self.out / f"{identity}.ray-status.json").write_text(
                json.dumps(
                    {"submission_id": job.submission_id, "status": str(job.status)}
                )
            )
            (self.out / f"{identity}.ray.log").write_text(
                self.jobs.get_job_logs(job.submission_id)
            )

    def rejection(self, name, mutate, code, raw=None):
        request = self.request(name)
        mutate(request)
        self.submit(request, raw=raw)
        terminal = self.wait(request)
        assert terminal["event_type"] == "FAILED" and terminal["error_code"] == code, (
            terminal
        )
        assert not self.matching_jobs(request), "invalid payload started a Ray job"
        self.save_evidence(request)

    def queued_cancel(self):
        request = self.request("queued-cancel")
        self.redis.set(
            self.channel("training")["cancel_key_prefix"] + ":" + request["job_id"], "0"
        )
        self.submit(request, raw="{")
        assert self.wait(request)["event_type"] == "CANCELLED"
        assert not self.matching_jobs(request)
        self.save_evidence(request)

    def pending_recovery(self):
        request = self.request("pending-recovery")
        channel = self.channel("training")
        self.enqueue(request)
        assert self.redis.xreadgroup(
            channel["consumer_group"],
            "dead-consumer",
            {channel["task_stream_key"]: ">"},
            count=1,
        )
        self.redis.set(channel["cancel_key_prefix"] + ":" + request["job_id"], "1")
        self.runtime.close()
        time.sleep(0.6)
        self.runtime = self.make_runtime()
        assert self.runtime.run_once(timeout_ms=0)
        assert self.wait(request)["event_type"] == "CANCELLED"
        assert (
            self.redis.xpending(channel["task_stream_key"], channel["consumer_group"])[
                "pending"
            ]
            == 0
        )
        self.save_evidence(request)

    def major_minor_and_large_transport(self):
        request = self.request("future-minor-large-input")
        request["protocol_version"] = "2.9"
        request["future_optional"] = {"ignored": True}
        request["extensions"]["opaque_padding"] = "x" * 90000
        self.submit(request)
        assert self.wait(request)["event_type"] == "COMPLETED"
        assert len(self.matching_jobs(request)) == 1
        # A late cancellation cannot replace or duplicate the completed event.
        self.redis.set(
            self.channel("training")["cancel_key_prefix"] + ":" + request["job_id"], "1"
        )
        time.sleep(1)
        assert [
            e["event_type"]
            for e in self.events(request, "training")
            if e["event_type"] in TERMINALS
        ] == ["COMPLETED"]
        self.save_evidence(request)

    def process_crash_recovery_and_redelivery(self):
        self.runtime.close()
        request = self.request("process-crash")
        request["algorithm"]["hyper_params"]["n_estimators"] = 100000
        command = [
            sys.executable,
            "-m",
            "tributo_broker_redis.cli",
            "consume",
            "--config",
            str(self.config_path),
        ]
        process = None
        with (self.out / "consumer-process.log").open("w") as log:
            try:
                process = subprocess.Popen(
                    command, stdout=log, stderr=subprocess.STDOUT
                )
                self.enqueue(request)
                self.wait(
                    request,
                    predicate=lambda events: any(
                        e["event_type"] == "METRICS" for e in events
                    ),
                )
                assert len(self.matching_jobs(request)) == 1
                process.kill()
                process.wait(timeout=10)
                # Recovery must reconstruct the admitted job before re-delivery.
                self.runtime = self.make_runtime()
                assert (
                    self.runtime.active_submissions.get(request["job_id"]) is not None
                )
                self.enqueue(request)
                assert self.runtime.run_once(timeout_ms=1000)
                assert len(self.matching_jobs(request)) == 1
                self.redis.set(
                    self.channel("training")["cancel_key_prefix"]
                    + ":"
                    + request["job_id"],
                    "1",
                )
                assert self.wait(request)["event_type"] == "CANCELLED"
                self.save_evidence(request)
            finally:
                if process is not None and process.poll() is None:
                    process.kill()
                    process.wait(timeout=10)
                for job in self.matching_jobs(request):
                    if str(job.status) not in {"SUCCEEDED", "FAILED", "STOPPED"}:
                        self.jobs.stop_job(job.submission_id)
                if self.runtime._closed:
                    self.runtime = self.make_runtime()

    def timeout(self):
        request = self.request("training-timeout")
        request["algorithm"]["hyper_params"]["n_estimators"] = 100000
        request["resource_limits"] = {
            "max_training_time_seconds": 60,
            "max_epochs": 100000,
        }
        self.submit(request)
        self.wait(
            request,
            predicate=lambda events: any(e["event_type"] == "METRICS" for e in events),
            timeout=90,
        )
        terminal = self.wait(request)
        assert (
            terminal["event_type"] == "FAILED"
            and terminal["error_code"] == "TRAINING_TIMEOUT"
        ), {key: terminal.get(key) for key in ("event_type", "error_code", "phase")}
        self.save_evidence(request)

    def inference_cancel(self):
        request = self.request("inference-cancel", "inference")
        self.submit(request, "inference")
        self.wait(
            request,
            "inference",
            predicate=lambda events: any(e["event_type"] == "STARTED" for e in events),
        )
        self.redis.set(
            self.channel("inference")["cancel_key_prefix"]
            + ":"
            + request["execution_id"],
            "1",
        )
        assert self.wait(request, "inference")["event_type"] == "CANCELLED"
        self.save_evidence(request, "inference")

    def corrupted_model(self):
        request = self.request("corrupt-model", "inference")
        for spec in request["model"]["model_artifacts"].values():
            for alternative in spec["alternatives"]:
                for file in alternative["files"]:
                    file["hash"] = "sha256:" + "0" * 64
        self.submit(request, "inference")
        terminal = self.wait(request, "inference")
        assert (
            terminal["event_type"] == "FAILED"
            and terminal["error_code"] == "MODEL_ARTIFACT_CORRUPTED"
        ), terminal
        self.save_evidence(request, "inference")

    def output_failure(self):
        request = self.request("write-failure", "inference", "infer_binary_7")
        request["output"]["table_name"] = self.prefix.replace("-", "_") + "_missing"
        self.submit(request, "inference")
        terminal = self.wait(request, "inference")
        assert (
            terminal["event_type"] == "FAILED"
            and terminal["error_code"] == "RESULT_WRITE_FAILED"
        ), terminal
        self.save_evidence(request, "inference")

    def run(self, selected=None):
        cases = [
            (
                "malformed_json",
                lambda: self.rejection(
                    "bad-json", lambda _r: None, "INVALID_PAYLOAD", "{"
                ),
            ),
            (
                "unsupported_major",
                lambda: self.rejection(
                    "major-3",
                    lambda r: r.update(protocol_version="3.0"),
                    "UNSUPPORTED_PROTOCOL_VERSION",
                ),
            ),
            (
                "missing_version",
                lambda: self.rejection(
                    "missing-version",
                    lambda r: r.pop("protocol_version"),
                    "UNSUPPORTED_PROTOCOL_VERSION",
                ),
            ),
            ("queued_cancel_before_payload", self.queued_cancel),
            ("pending_delivery_restart", self.pending_recovery),
            (
                "future_minor_large_transport_late_cancel",
                self.major_minor_and_large_transport,
            ),
            (
                "real_process_crash_redelivery_running_cancel",
                self.process_crash_recovery_and_redelivery,
            ),
            ("training_timeout", self.timeout),
            ("inference_running_cancel", self.inference_cancel),
            ("model_corruption", self.corrupted_model),
            ("result_write_failure", self.output_failure),
        ]
        if selected:
            assert selected <= {name for name, _ in cases}, "unknown control case"
            previous = self.out / "results.json"
            if previous.exists():
                previous.replace(self.out / "results.before-recheck.json")
                self.results = [
                    item
                    for item in json.loads(
                        (self.out / "results.before-recheck.json").read_text()
                    )
                    if item["case"] not in selected
                ]
            cases = [(name, action) for name, action in cases if name in selected]
        try:
            for name, action in cases:
                started = time.monotonic()
                try:
                    action()
                    result = {"case": name, "passed": True}
                except Exception as error:
                    (self.out / f"{name}.assertion.log").write_text(
                        traceback.format_exc()
                    )
                    result = {
                        "case": name,
                        "passed": False,
                        "error_type": type(error).__name__,
                        "error": str(error),
                    }
                result["seconds"] = round(time.monotonic() - started, 2)
                self.results.append(result)
                (self.out / "results.json").write_text(
                    json.dumps(self.results, indent=2)
                )
                print(result, flush=True)
        finally:
            self.runtime.close()
            self.redis.close()
        return int(any(not item["passed"] for item in self.results))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", action="append", default=[])
    args = parser.parse_args()
    raise SystemExit(Controls().run(set(args.case)))
