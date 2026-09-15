# Tributo Redis Streams provider

`tributo-broker-redis` is an independently installed Redis Streams provider
for Tributo Broker API v1. The v0.1 Alpha supports a public
`tributo-generic-v1` protocol and delegates execution to Tributo public APIs.
Redis remains outside Tributo Core.

The release gate covers these healthy paths:

- XGBoost training with `single_worker` and `distributed` profiles;
- Bundle-backed batch inference with `single_worker` and `distributed` profiles;
- Standalone Redis, Redis consumer groups, one provider execution-driver Ray
  Job, Bundle or ResultSink output, and best-effort terminal events;
- best-effort restart recovery from Redis pending deliveries and active Ray Job
  metadata, without resubmitting an exact recovered delivery;
- queued cancellation and running cancellation through Ray Jobs stop.

Unknown protocol fields and unsupported algorithms or profiles fail closed.
This release does not promise cross-restart exactly-once behavior, durable
events, complete recovery, DLQ processing, Sentinel, Cluster, or HA.

## Installation

Install Tributo Core 1.0.0 containing the public Ray Jobs runtime-env extension
contract and this package:

```bash
pip install /path/to/validated/tributo-1.0.0-py3-none-any.whl \
  tributo-broker-redis
```

The provider dependency range remains `tributo>=1.0,<2.0`, but v0.1 deployment
must select a Core 1.0.0 build containing the runtime-env extension. The
package registers the `tributo-redis` entry point in `tributo.brokers`.
Compatibility is verified against the pinned Core baseline by static typing,
the wheel contract, and the Redis/Ray integration matrix. Configuration
validation checks the Core project root and configured extension paths before
any Redis delivery can be consumed.

## Configuration

The provider accepts one closed root shape. Training and batch inference must
have separate task, event, cancel, and consumer-group names. See
[`docs/config.example.json`](docs/config.example.json) for a complete example.

Redis URLs cannot contain credentials. `execution.env_vars` is only for
non-sensitive settings. A task may carry `credential_ref`; the Ray driver
resolves `env:NAME` or `mount:/absolute/path` from its pre-provisioned runtime.
The resolved value is never copied into request-derived Ray runtime env,
metadata, logs, events, Bundle manifests, or result receipts.

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

Training `spec` uses `algorithm: "xgboost"` and a validated Tributo XGBoost
`config` whose output is `bundle_uri`. Batch inference uses
`profile: "bundle-backed"` and a strict Tributo `InferenceRequest` under
`request`.

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
Tributo/Ray/manifest labels, and runs one isolated Standalone Redis plus Docker
Ray matrix:

```bash
TRIBUTO_CORE_ROOT=/path/to/tributo-core \
BROKER_RAY_IMAGE=tributo-runtime-full:local \
./scripts/run_it.sh
```
