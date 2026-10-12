from __future__ import annotations

import json

import pytest
from conftest import FakeRedis

from tributo_broker_redis.reporter import RedisEventReporter


@pytest.mark.parametrize("summary", [None, "features must be a non-empty array"])
def test_failed_retains_default_and_accepts_caller_approved_summary(
    fake_redis: FakeRedis, summary: str | None
) -> None:
    reporter = RedisEventReporter(
        fake_redis,
        event_stream_prefix="events:training",
        operation_id="operation-1",
        operation_type="training",
        execution_profile="distributed",
        run_id="run-1",
        attempt_id="attempt-1",
    )
    kwargs = {} if summary is None else {"sanitized_message": summary}

    reporter.failed("EXECUTION_FAILED", "ConfigurationError", "PREPARING", **kwargs)

    event = json.loads(fake_redis.events[reporter.stream_key][-1]["payload"])
    assert event["phase"] == "PREPARING"
    assert event["payload"] == {
        "error_code": "EXECUTION_FAILED",
        "error_type": "ConfigurationError",
        "sanitized_message": "operation failed" if summary is None else summary,
        "retryable": False,
    }
