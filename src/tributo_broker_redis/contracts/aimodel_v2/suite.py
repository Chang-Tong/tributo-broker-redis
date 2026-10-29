"""JSON test vectors shared by Python, Java and other protocol consumers."""

from __future__ import annotations

import copy
from typing import Any

from .validation import ContractViolation, load_resource, validate_payload


def materialize(case: dict[str, Any]) -> Any:
    value = copy.deepcopy(load_resource(case["fixture"]))
    for change in case["changes"]:
        parent = value
        for key in change["path"][:-1]:
            parent = parent[int(key)] if isinstance(parent, list) else parent[key]
        key = change["path"][-1]
        if isinstance(parent, list):
            index = int(key)
            if change["op"] == "remove":
                del parent[index]
            elif index == len(parent):
                parent.append(copy.deepcopy(change["value"]))
            else:
                parent[index] = copy.deepcopy(change["value"])
        elif change["op"] == "remove":
            parent.pop(key)
        else:
            parent[key] = copy.deepcopy(change["value"])
    return value


def run_vectors() -> list[dict[str, Any]]:
    results = []
    for case in load_resource("cases.json"):
        try:
            validate_payload(case["kind"], materialize(case))
            accepted, code = True, None
        except ContractViolation as error:
            accepted, code = False, error.code
        results.append(
            {
                "case": case["id"],
                "passed": accepted == case["accepted"],
                "accepted": accepted,
                "expected_accepted": case["accepted"],
                "code": code,
            }
        )
    return results
