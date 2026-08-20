"""Atomic Redis Stream terminal and phase publication guard."""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

TERMINAL_EVENT_TYPES = frozenset({"COMPLETED", "FAILED", "CANCELLED"})

_PUBLISH_EVENT_LUA = r"""
local stream_key = KEYS[1]
local job_id = ARGV[1]
local encoded = ARGV[2]
local event_type = ARGV[3]
local phase = ARGV[4]
local max_length = ARGV[5]
local terminal = {COMPLETED=true, FAILED=true, CANCELLED=true}
local terminal_seen = false
local phase_seen = false

if job_id ~= '' then
    local entries = redis.call('XRANGE', stream_key, '-', '+')
    for _, entry in ipairs(entries) do
        local fields = entry[2]
        local existing_job_id = nil
        local existing_payload = nil
        for index = 1, #fields, 2 do
            if fields[index] == 'job_id' then
                existing_job_id = fields[index + 1]
            elseif fields[index] == 'payload' then
                existing_payload = fields[index + 1]
            end
        end
        if existing_job_id == job_id and existing_payload ~= nil then
            local ok, existing = pcall(cjson.decode, existing_payload)
            if ok and type(existing) == 'table' and terminal[existing.event_type] then
                terminal_seen = true
            end
            if ok and type(existing) == 'table' and event_type == 'PHASE'
                and existing.event_type == 'PHASE'
                and existing.phase == phase then
                phase_seen = true
            end
        end
    end
end

if terminal_seen then
    if terminal[event_type] then
        return {'terminal_exists', ''}
    end
    return {'rejected_after_terminal', ''}
end
if phase_seen then
    return {'duplicate_phase', ''}
end

local event_id = redis.call(
    'XADD', stream_key, 'MAXLEN', '~', max_length, '*',
    'job_id', job_id, 'payload', encoded
)
return {'published', event_id}
"""


class PublishDecision(StrEnum):
    """Redis-side decision for one attempted event."""

    PUBLISHED = "published"
    TERMINAL_EXISTS = "terminal_exists"
    REJECTED_AFTER_TERMINAL = "rejected_after_terminal"
    DUPLICATE_PHASE = "duplicate_phase"

    @property
    def accepted(self) -> bool:
        return self in {
            PublishDecision.PUBLISHED,
            PublishDecision.TERMINAL_EXISTS,
            PublishDecision.DUPLICATE_PHASE,
        }


@dataclass(frozen=True)
class GuardResult:
    decision: PublishDecision
    event_id: str | None = None

    @property
    def accepted(self) -> bool:
        return self.decision.accepted


def _decode(value: Any) -> Any:
    return value.decode() if isinstance(value, bytes) else value


def _entry_fields(entry: Any) -> dict[str, Any]:
    if not isinstance(entry, (list, tuple)) or len(entry) < 2:
        return {}
    raw_fields = entry[1]
    if isinstance(raw_fields, dict):
        return {str(_decode(key)): _decode(value) for key, value in raw_fields.items()}
    if not isinstance(raw_fields, (list, tuple)):
        return {}
    return {
        str(_decode(raw_fields[index])): _decode(raw_fields[index + 1])
        for index in range(0, len(raw_fields) - 1, 2)
    }


class TerminalGuard:
    """Enforce global terminal uniqueness using one stream key per Lua call."""

    def __init__(self, redis_client: Any, *, max_stream_length: int) -> None:
        self._redis = redis_client
        self._max_stream_length = max_stream_length

    def publish(
        self,
        stream_key: str,
        *,
        job_id: str | None,
        encoded_event: str,
        event_type: str,
        phase: str | None,
    ) -> GuardResult:
        """Atomically decide and append using exactly one Redis key."""
        raw = self._redis.eval(
            _PUBLISH_EVENT_LUA,
            1,
            stream_key,
            job_id or "",
            encoded_event,
            event_type,
            phase or "",
            str(self._max_stream_length),
        )
        if not isinstance(raw, (list, tuple)) or not raw:
            raise RuntimeError(f"Unexpected terminal guard response: {raw!r}")
        decision = PublishDecision(str(_decode(raw[0])))
        event_id = str(_decode(raw[1])) if len(raw) > 1 and raw[1] else None
        return GuardResult(decision=decision, event_id=event_id)

    def terminal_event(
        self, stream_key: str, job_id: str, *, count: int | None = None
    ) -> dict[str, Any] | None:
        """Return the latest durable terminal for a job, if present."""
        entries = self._redis.xrevrange(
            stream_key,
            max="+",
            min="-",
            count=count or self._max_stream_length,
        )
        for entry in entries:
            fields = _entry_fields(entry)
            if fields.get("job_id") != job_id:
                continue
            encoded = fields.get("payload")
            if not isinstance(encoded, str):
                continue
            try:
                event = json.loads(encoded)
            except (TypeError, json.JSONDecodeError):
                continue
            if (
                isinstance(event, dict)
                and event.get("event_type") in TERMINAL_EVENT_TYPES
            ):
                return event
        return None


def assert_single_key_lua() -> None:
    """Fail development checks if the guard ever grows cross-slot key usage."""
    if "KEYS[2]" in _PUBLISH_EVENT_LUA:
        raise AssertionError("terminal guard Lua must use exactly one Redis key")
