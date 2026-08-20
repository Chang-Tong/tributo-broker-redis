"""Durable per-job active records and terminal candidates."""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import asdict, dataclass
from typing import Any

from tributo_broker_redis.config import RedisBrokerConfig
from tributo_broker_redis.protocol import (
    MAX_EVENT_TIMESTAMP,
    MAX_TERMINAL_DURATION_SECONDS,
    quantize_duration_seconds,
    validate_terminal_event,
)

_SAVE_ACTIVE_LUA = r"""
local incoming = cjson.decode(ARGV[1])
local raw = redis.call('GET', KEYS[1])
if raw then
    local ok, existing = pcall(cjson.decode, raw)
    if ok and type(existing) == 'table' and existing.current_phase
        and existing.current_phase ~= '' and existing.current_phase ~= 'QUEUED' then
        incoming.current_phase = existing.current_phase
    end
end
redis.call('SET', KEYS[1], cjson.encode(incoming), 'EX', ARGV[2])
return incoming.current_phase
"""

_UPDATE_PHASE_LUA = r"""
local raw = redis.call('GET', KEYS[1])
if not raw then
    local placeholder = {placeholder=true, current_phase=ARGV[1]}
    redis.call('SET', KEYS[1], cjson.encode(placeholder), 'EX', ARGV[2])
    return 1
end
local ok, record = pcall(cjson.decode, raw)
if not ok or type(record) ~= 'table' then
    return redis.error_reply('invalid active job JSON')
end
record.current_phase = ARGV[1]
redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[2])
return 1
"""

_STAGE_TERMINAL_CANDIDATE_LUA = r"""
-- STAGE_TERMINAL_CANDIDATE
local function nonempty_string(value)
    return type(value) == 'string' and value ~= ''
end
local function nonnegative_number(value)
    return type(value) == 'number' and value >= 0
end
local function valid_terminal(value, job_id)
    if type(value) ~= 'table'
        or value.protocol_version ~= '2.0'
        or value.job_id ~= job_id
        or not nonnegative_number(value.timestamp)
        or value.timestamp > tonumber(ARGV[4])
        or value.timestamp ~= math.floor(value.timestamp)
        or not nonnegative_number(value.duration_seconds)
        or value.duration_seconds > tonumber(ARGV[5]) then
        return false
    end
    if value.event_type == 'FAILED' then
        return nonempty_string(value.phase)
            and nonempty_string(value.error_code)
            and nonempty_string(value.error_message)
    end
    if value.event_type == 'CANCELLED' then
        return nonempty_string(value.phase)
            and type(value.has_best_model) == 'boolean'
    end
    if value.event_type == 'COMPLETED' then
        return type(value.result_summary) == 'table'
            and type(value.training_result) == 'table'
            and type(value.artifact_manifest) == 'table'
    end
    return false
end
local raw = redis.call('GET', KEYS[1])
if raw then
    local ok, existing = pcall(cjson.decode, raw)
    if ok and valid_terminal(existing, ARGV[3]) then
        redis.call('EXPIRE', KEYS[1], ARGV[2])
        return raw
    end
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
return ARGV[1]
"""


def _decode(value: Any) -> Any:
    return value.decode() if isinstance(value, bytes) else value


@dataclass(frozen=True)
class ActiveJobRecord:
    job_id: str
    run_id: str
    attempt_id: str
    submission_id: str
    execution_id: str
    submitted_at: float
    deadline_at: float | None
    current_phase: str
    request_metadata: dict[str, Any]
    candidate_ref: str

    def encode(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"), allow_nan=False)

    @classmethod
    def decode(cls, raw: str | bytes) -> ActiveJobRecord:
        value = json.loads(_decode(raw))
        if not isinstance(value, dict):
            raise ValueError("active job record must be a JSON object")
        return cls(**value)


class ActiveJobStore:
    def __init__(self, redis_client: Any, config: RedisBrokerConfig) -> None:
        self._redis = redis_client
        self._config = config

    def save(self, record: ActiveJobRecord) -> None:
        result = self._redis.eval(
            _SAVE_ACTIVE_LUA,
            1,
            self._config.active_job_key(record.job_id),
            record.encode(),
            str(self._config.active_job_ttl_seconds),
        )
        if not result:
            raise RuntimeError("active job registration was not persisted")

    @staticmethod
    def _decode_record(raw: str | bytes) -> ActiveJobRecord | None:
        value = json.loads(_decode(raw))
        if isinstance(value, dict) and value.get("placeholder") is True:
            return None
        return ActiveJobRecord.decode(raw)

    def load(self, job_id: str) -> ActiveJobRecord | None:
        raw = self._redis.get(self._config.active_job_key(job_id))
        return self._decode_record(raw) if raw is not None else None

    def update_phase(self, job_id: str, phase: str) -> bool:
        result = self._redis.eval(
            _UPDATE_PHASE_LUA,
            1,
            self._config.active_job_key(job_id),
            phase,
            str(self._config.active_job_ttl_seconds),
        )
        return bool(result)

    def delete(self, job_id: str) -> None:
        self._redis.delete(self._config.active_job_key(job_id))

    def refresh(self, job_id: str) -> bool:
        return bool(
            self._redis.expire(
                self._config.active_job_key(job_id),
                self._config.active_job_ttl_seconds,
            )
        )

    def scan_keys(self) -> Iterator[str]:
        for raw_key in self._redis.scan_iter(
            match=f"{self._config.active_job_key_prefix}:*",
            count=self._config.supervisor_scan_count,
        ):
            yield str(_decode(raw_key))

    def load_key(self, key: str) -> ActiveJobRecord | None:
        raw = self._redis.get(key)
        return self._decode_record(raw) if raw is not None else None


class TerminalCandidateStore:
    def __init__(self, redis_client: Any, config: RedisBrokerConfig) -> None:
        self._redis = redis_client
        self._config = config

    def save(self, job_id: str, encoded_event: str) -> str:
        value = json.loads(encoded_event)
        if not isinstance(value, dict):
            raise ValueError("terminal candidate must be a JSON object")
        validate_terminal_event(value, job_id)
        value["duration_seconds"] = quantize_duration_seconds(value["duration_seconds"])
        encoded_event = json.dumps(
            value,
            separators=(",", ":"),
            allow_nan=False,
        )
        if len(encoded_event.encode("utf-8")) > self._config.max_event_bytes:
            raise ValueError("terminal candidate exceeds max_event_bytes")
        key = self._config.terminal_candidate_key(job_id)
        staged = self._redis.eval(
            _STAGE_TERMINAL_CANDIDATE_LUA,
            1,
            key,
            encoded_event,
            str(self._config.terminal_candidate_ttl_seconds),
            job_id,
            str(MAX_EVENT_TIMESTAMP),
            str(MAX_TERMINAL_DURATION_SECONDS),
        )
        return str(_decode(staged))

    def load(self, job_id: str) -> str | None:
        raw = self._redis.get(self._config.terminal_candidate_key(job_id))
        return str(_decode(raw)) if raw is not None else None

    def delete(self, job_id: str) -> None:
        self._redis.delete(self._config.terminal_candidate_key(job_id))
