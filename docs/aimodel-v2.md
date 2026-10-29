# AI model protocol v2

The Redis provider owns the training/inference protocol described by
`ai-model-design-docs/训练推理引擎任务描述协议/protocol-v2-2606/`:
`protocol-conventions.md`, `training-protocol.md`,
`training-protocol-appendix.md`, and `inference-protocol.md`.
The migration baseline is protocol repository commit `358ffc4`.

The implementation and its tests now belong to this repository, independently
of company deployment configuration. `tributo-knova` retains the old plugin
name, CLI, deployment assets, and compatibility module aliases only.

## Installation and configuration

Build the provider Wheel from this checkout. Install its `aimodel-v2` extra
together with compatible Core, official boosting, and ClickHouse connector
distributions. The base install and generic v1 do not import the optional
execution stack during discovery or configuration validation.

```bash
uv build --wheel
pip install 'dist/tributo_broker_redis-0.3.0-py3-none-any.whl[aimodel-v2]'
```

This is a source migration, not a claim that the new version or all compatible
algorithm/connector Wheels are already published to PyPI. The existing KnoVa
offline distribution supplies the compatible Core, patched boosting, and
connector Wheels. Its updated lock selects this provider's 0.3.0 Wheel.

Start from [the complete example](config.aimodel-v2.example.json), replace the
deployment paths, and use the standard provider command:

```bash
tributo-broker-redis validate --config /etc/tributo/aimodel-v2.json
tributo-broker-redis consume --config /etc/tributo/aimodel-v2.json
```

Select `broker_id: tributo-redis` and explicitly configure:

```json
{
  "protocol": {"profile": "aimodel-v2", "protocol_version": "2.0"},
  "operations": {
    "training": {"execution_profiles": ["distributed"]},
    "batch_inference": {"execution_profiles": ["distributed"]}
  }
}
```

Training channels require `outer_identity_field: job_id`; inference channels
require `outer_identity_field: execution_id`. Each Stream entry has exactly
that identity and `payload`. Existing configured Stream/group/key names are
preserved. Do not add generic-v1 fields to a v2 task. Task versions use
major-version compatibility; the profile configuration selects the supported
2.0 serializer, while higher 2.x request versions tolerate unknown fields.

The Ray driver is now
`python -m tributo_broker_redis.profiles.aimodel_v2.execution_driver`.
Ship the provider and official algorithms to Ray through the existing
`extra_py_modules`/runtime Wheel settings. New deployments do not need the
`tributo_knova` package on Ray nodes. The provider recognizes old
`python -m tributo_knova.execution_driver` metadata during active-job recovery;
already-submitted old drivers still need their original deployment artifacts.

## Preserved behavior and limits

This step moves the existing implementation and its regression suite. It
preserves the admission-ACK policy, best-effort informational events, v2
terminal-event arbitration, watchdog, queued/running cancellation, and
pending/active-job recovery behavior. It does not implement the later durable
admission/terminal ledger described by the full migration plan.

Execution remains distributed XGBoost with ClickHouse input/output, Bundle
publication, ONNX prediction, UBJ attribution, protocol evaluation projection,
parameterized scalar filters, per-execution thresholds, and adaptive output
batching. Existing restrictions remain: arbitrary JOIN/query execution,
TIMESERIES_PIVOT, nontrivial preprocessing, cross-restart training resume, and
full DNN/PU protocol routing are not added by this ownership change. AUTO
tuning retains the existing warning and MANUAL fallback.

Both generic v1 and v2 now target current Core `1eaae84`, the documented
patched official Boosting Wheel, and ray-clickhouse 1.0. Generic training uses
public ingestion/resource/result contracts. Removed in-Core trainer APIs and
the old generic trainer configuration are rejected; see the current
[generic configuration](training.current.example.json). No older-Core fallback
is kept. Install the declared extras and deploy the same verified Wheels to
Consumer and Ray.

The `KNOVA_*` credential and driver-log environment names remain supported for
deployment compatibility. Driver logs use the new Broker module namespace.
Python compatibility class names such as `KnovaRedisEventReporter` are kept;
they are defined by Broker and do not import the KnoVa package.

## Verification

The protocol/mapping/driver regression suite is `tests/aimodel_v2`. It includes
subprocess checks that block all `tributo_knova` imports. The isolated Redis
integration suite exercises both wire identities and COMPLETED/FAILED/CANCELLED
through the provider and execution driver; Ray initialization/execution is
substituted, so this suite is not a distributed model-training gate.

```bash
python -m pytest tests/aimodel_v2 -q
TRIBUTO_TEST_REDIS_SERVER=/path/to/redis-server \
  python -m pytest tests/integration/test_aimodel_v2_redis.py -q
```

Use the compatible installed dependencies or explicitly supplied sibling
source paths.

The [2026-10-27 acceptance report](test-reports/2026-10-27-aimodel-v2-acceptance.md)
records 54 real Redis/Ray/ClickHouse/NAS/S3-compatible business cases, 11
failure/recovery cases, and 8 real Sentinel cases, all passing after fixes.
The final Wheel also passed three real training/inference/SHAP checks through
KnoVa's thin wrapper with its pinned Core and connector. The primary v2
assembly uses Core `1eaae84`, ray-clickhouse 1.0, and the documented patched
boosting Wheel; the pinned KnoVa assembly uses Core `440da042` and its local
ray-clickhouse 0.1.0 Wheel. Core's public read gateway selects the unique
compatible Binding rather than relying on a historical connector Binding ID.

The earlier report's eight generic-v1 failures are historical. Broker 0.3
replaces that adapter and uses the current-Core baseline for both CI jobs;
the follow-up current-Core report records its replacement acceptance tests.

See the [current-Core 0.3 acceptance](test-reports/2026-10-27-current-core-compatibility.md) for the replacement adapter, current dependency lock, and real V1/V2 verification.
