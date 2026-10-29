# Tributo Redis Streams provider

`tributo-broker-redis` is an independently installed Redis Streams provider
for Tributo Broker API v1. The generic Alpha profile supports a public
`tributo-generic-v1` protocol and delegates execution to Tributo public APIs.
Redis remains outside Tributo Core.

Version 0.3 owns the optional `aimodel-v2` training/inference profile,
migrated from the KnoVa integration. Its parser, structural validators, event
reporter, request mapping, and Ray execution driver live in
`tributo_broker_redis.profiles.aimodel_v2`. No KnoVa installation is required.
See [the v2 profile guide](docs/aimodel-v2.md) for dependencies, configuration,
compatibility, and the current execution limits.

The generic v1 release gate covers these healthy paths:

- XGBoost training with `single_worker` and `distributed` profiles;
- Bundle-backed batch inference with `single_worker` and `distributed` profiles;
- Standalone or Sentinel Redis, Redis consumer groups, one provider
  execution-driver Ray Job, Bundle or ResultSink output, and best-effort
  terminal events;
- best-effort restart recovery from Redis pending deliveries and active Ray Job
  metadata, without resubmitting an exact recovered delivery;
- queued cancellation and running cancellation through Ray Jobs stop.

For generic v1, unknown protocol fields and unsupported algorithms or profiles fail closed.
This release does not promise cross-restart exactly-once behavior, durable
events, complete recovery, DLQ processing, Redis Cluster, or exactly-once HA.

## Installation

Install Tributo Core 1.0.0 containing the public Ray Jobs runtime-env extension
contract and this package:

```bash
pip install /path/to/validated/tributo-1.0.0-py3-none-any.whl \
  'dist/tributo_broker_redis-0.3.0-py3-none-any.whl[xgboost]'
```

The supported Core baseline is `Chang-Tong/Tributo@1eaae848ef7f6acbd600c87771f91f9291f00b3e`.
Both profiles use that current Core. Install the `xgboost` extra for generic
training, or `aimodel-v2` for the external protocol. These install the official
algorithm package; XGBoost is no longer provided by an in-Core legacy trainer.
Broad package version ranges do not establish compatibility with older Core
builds sharing version 1.0.0. Configuration validates deployment paths before
Redis delivery consumption.

## Configuration

The provider accepts one closed root shape. Training and batch inference must
have separate task, event, cancel, and consumer-group names. See
[`docs/config.example.json`](docs/config.example.json) for a complete example.

Redis URLs and Sentinel node addresses cannot contain credentials. Standalone
and Sentinel transports accept `password_ref`; Sentinel additionally accepts
`sentinel_password_ref`. Both resolve `env:NAME` or
`mount:/absolute/path` from the pre-provisioned process environment. The same
references must be available to the Consumer and Ray Job driver. Resolved
values are never copied into config serialization, driver input, Ray runtime
env, metadata, logs, events, Bundle manifests, or result receipts.

Sentinel mode requires `sentinel_nodes`, `sentinel_master`, and `database`.
When Ray nodes reach Sentinel through different addresses, set
`driver_sentinel_nodes`; otherwise the Consumer node list is reused. Sentinel
discovery and master failover are delegated to redis-py rather than reimplemented
by the provider. See [`docs/config.sentinel.example.json`](docs/config.sentinel.example.json).

`execution.env_vars` remains limited to non-sensitive settings. A task may
carry its own `credential_ref`; the Ray driver resolves it independently from
the Redis transport credentials.

`execution.extra_py_modules` and `execution.runtime_pip_packages` are trusted
deployment settings and cannot be supplied by task payloads. Extension modules
accept deployment-visible local paths or credential-free remote wheel/zip
URIs. Runtime pip entries accept PEP 508 requirements or deployment-visible
wheel paths; direct URLs and pip command options are rejected. A Core source
root containing `src/tributo` or `tributo` is required by
`execution.project_root`, and at least one driver distribution setting must be
non-empty. This is an explicit Alpha deployment contract, not task metadata.

Each channel may set `outer_identity_field` to a generic Redis field name. It
defaults to `operation_id`; the selected field is used by both task and event
entries while JSON payloads retain the canonical `operation_id` property.

## Runtime

Copy the example and replace its deployment paths before validation. Validate
without connecting:

```bash
tributo-broker-redis validate --config /path/to/redis-provider.json
```

Validate connectivity explicitly:

```bash
tributo-broker-redis validate --config /path/to/redis-provider.json --check-connectivity
```

Run the provider-owned consume loop:

```bash
tributo-broker-redis consume --config /path/to/redis-provider.json
```

Core provides discovery and validation through `tributo broker list` and
`tributo broker validate`; Core does not own the production consume loop.

## Wire protocol

Each task Stream entry contains exactly the outer business identity and JSON
payload:

```text
operation_id = operation-123
payload      = {"protocol_profile":"tributo-generic-v1", ...}
```

The payload identity must equal the outer `operation_id`. Training and batch
inference are sent to their configured independent Streams. The common payload
fields are:

```json
{
  "protocol_profile": "tributo-generic-v1",
  "protocol_version": "1.0",
  "operation_id": "operation-123",
  "operation_type": "training",
  "execution_profile": "single_worker",
  "run_id": "run-123",
  "attempt_id": "attempt-1",
  "request_digest": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "spec": {}
}
```

Training `spec` uses `algorithm: "xgboost"` and the current
[explicit training configuration](docs/training.current.example.json).
`config.ingestion` is Core's public `IngestionRequest`; feature/label names,
worker count, resources, storage, and Bundle destination are explicit.
`single_worker` requires one worker and `distributed` requires at least two.
Results contain the public `AlgorithmRunResult`: `result.execution.status`,
metrics/outputs, input provenance, and worker evidence, with a Bundle reference.

Version 0.3 deliberately rejects the retired `config.data`, `config.ray`,
`config.output`, `val_size`, and `test_size` trainer formats. Use split ratios
and the current fields instead. The removed Core trainer module and its result
methods are not restored. Batch inference continues to use
`profile: "bundle-backed"` and a public `InferenceRequest` under `request`.

After deterministic Ray admission the provider publishes `ACCEPTED` and ACKs
the Redis delivery. The single Ray execution driver publishes `PHASE`, `LOG`,
`METRICS` or `PROGRESS`, then `COMPLETED` or `FAILED`. Every admitted event
contains `submission_id`; `ray_job_id` remains optional execution metadata.
Nonterminal events are best-effort and cannot change a successful workload into
an execution failure. Ray state and Bundle or ResultSink receipts remain the
result facts. An optional `request_digest` must be a lower-case SHA-256 digest.

See [`docs/operations.md`](docs/operations.md) and
[`docs/generic-redis-stream-protocol-integration-test.md`](docs/generic-redis-stream-protocol-integration-test.md).

## Development validation

Unit and static checks do not require Redis or Docker. Build the attested Core
runtime image once with the Core-owned builder:

```bash
cd /path/to/tributo-core
uv run --locked --no-sync python tools/build_tributo_image.py \
  --config tools/tributo-runtime-full.json \
  --output-dir dist/tributo-runtime-full
```

The final integration entry point builds both wheels, validates that image's
Tributo/Ray/manifest labels, and runs one isolated Redis plus Docker Ray matrix:

```bash
TRIBUTO_CORE_ROOT=/path/to/tributo-core \
BROKER_RAY_IMAGE=tributo-runtime-full:local \
./scripts/run_it.sh
```

## External v2 contract CI

Broker currently executes a subset of the external protocol. Shared JSON Schema,
request vectors, event/result semantics, backend evidence and live Ray evidence
checks are available through `tributo-broker-contract` (`contracts` extra).
See [protocol conformance and CI integration](docs/protocol-conformance.md) for the
locked specification, explicit gaps, reusable workflows and evidence format.
Passing a schema or a substituted executor test is not full runtime compatibility.
