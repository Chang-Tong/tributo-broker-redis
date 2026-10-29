from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from tributo_broker_redis.config import RedisBrokerConfig


class FakeRedis:
    def __init__(self) -> None:
        self.messages: dict[str, list[tuple[str, dict[str, str]]]] = {}
        self.events: dict[str, list[dict[str, str]]] = {}
        self.cancelled: set[str] = set()
        self.acked: list[tuple[str, str, str]] = []
        self.closed = False

    def xgroup_create(self, **_kwargs: Any) -> bool:
        return True

    def xreadgroup(self, *, streams: dict[str, str], **_kwargs: Any) -> list[Any]:
        stream = next(iter(streams))
        queue = self.messages.setdefault(stream, [])
        if not queue:
            return []
        return [(stream, [queue.pop(0)])]

    def xack(self, stream: str, group: str, delivery: str) -> int:
        self.acked.append((stream, group, delivery))
        return 1

    def xadd(self, stream: str, fields: dict[str, str], **_kwargs: Any) -> str:
        self.events.setdefault(stream, []).append(fields)
        return "1-0"

    def xrevrange(self, stream: str, *, count: int) -> list[Any]:
        values = self.events.get(stream, [])[-count:]
        return [(f"{index}-0", fields) for index, fields in enumerate(reversed(values))]

    def exists(self, key: str) -> int:
        return int(key in self.cancelled)

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def raw_config(tmp_path: Path) -> dict[str, Any]:
    core_root = tmp_path / "core"
    (core_root / "src" / "tributo").mkdir(parents=True)
    return {
        "broker_id": "tributo-redis",
        "api_version": 1,
        "transport": {
            "mode": "standalone",
            "url": "redis://127.0.0.1:6379/0",
            "driver_url": "redis://redis:6379/0",
            "block_ms": 1,
        },
        "channels": {
            "training": {
                "task_stream_key": "tasks:training",
                "event_stream_prefix": "events:training",
                "cancel_key_prefix": "cancel:training",
                "consumer_group": "group:training",
                "consumer_name": "consumer:training",
                "group_start_id": "0-0",
            },
            "batch_inference": {
                "task_stream_key": "tasks:inference",
                "event_stream_prefix": "events:inference",
                "cancel_key_prefix": "cancel:inference",
                "consumer_group": "group:inference",
                "consumer_name": "consumer:inference",
                "group_start_id": "0-0",
            },
        },
        "protocol": {
            "profile": "tributo-generic-v1",
            "protocol_version": "1.0",
        },
        "operations": {
            "training": {"execution_profiles": ["single_worker", "distributed"]},
            "batch_inference": {"execution_profiles": ["single_worker", "distributed"]},
        },
        "execution": {
            "ray_dashboard_url": "http://ray:8265",
            "project_root": str(core_root),
            "runtime_pip_packages": ["tributo-broker-redis==0.1.0"],
        },
    }


@pytest.fixture
def config(raw_config: dict[str, Any]) -> RedisBrokerConfig:
    return RedisBrokerConfig.model_validate(raw_config)


@pytest.fixture
def fake_redis() -> FakeRedis:
    return FakeRedis()


def inference_request(concurrency: int) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "model": {"kind": "bundle", "uri": "/models/bundle"},
        "input": {
            "source": {"type": "parquet", "path": "/data/input.parquet"},
            "engine": "ray",
        },
        "input_binding": {
            "tensors": [
                {
                    "tensor_name": "float_input",
                    "columns": ["x1", "x2"],
                    "dtype": "float32",
                }
            ]
        },
        "output_binding": {
            "tensors": [
                {
                    "tensor_name": "probabilities",
                    "column": "score",
                    "semantic": "probability",
                }
            ]
        },
        "result_sink": {"sink_id": "parquet-v1", "uri": "/data/output"},
        "execution": {
            "executor_id": "ray-map-batches-v1",
            "batch_size": 8,
            "concurrency": concurrency,
            "num_cpus_per_actor": 1.0,
            "num_gpus_per_actor": 0.0,
        },
    }


def training_config(num_workers: int) -> dict[str, Any]:
    return {
        "ingestion": {
            "source": {"type": "csv", "path": "/data/train.csv"},
            "engine": "ray",
        },
        "feature_names": ["x1", "x2"],
        "label_name": "label",
        "worker_count": num_workers,
        "model": {"objective": "binary:logistic", "max_depth": 2},
        "training": {
            "num_rounds": 2,
            "train_ratio": 1.0,
            "validation_ratio": 0.0,
            "test_ratio": 0.0,
        },
        "storage_path": "/tmp/ray",
        "bundle_uri": "/tmp/bundles",
    }
