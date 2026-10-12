# Operations guide

## Runtime topology

The v0.1 topology has one business Ray Job per Redis task:

```text
Redis task Stream
  -> provider consume loop
  -> deterministic RayJobSubmission
  -> provider execution-driver Ray Job
  -> Tributo in-process training or batch-inference API
  -> Bundle or ResultSink
  -> provider event Stream
```

Operation mappings only validate and prepare driver input. They do not call a
training or inference submission API, so inference cannot create a nested Ray
Job.

## Admission and ACK

The consumer validates protocol, outer identity, supported operation/profile,
and queued cancellation before admission. It then submits or reconciles a
deterministic `submission_id`, records the in-memory active mapping, publishes
`ACCEPTED`, and calls `XACK`.

`ACK` means Ray accepted the execution, not that execution completed. An
ambiguous Ray response is queried by the same `submission_id`; if admission
cannot be confirmed, the Redis delivery remains pending. An invalid or
unsupported request publishes a sanitized `FAILED` event and is ACKed to avoid
a poison loop.

The event Stream is best effort. It is not an outbox or terminal ledger.
Failures to publish `PHASE`, `LOG`, `METRICS`, or `PROGRESS` are logged and do
not replace a successful workload result with `EXECUTION_FAILED`. The driver
makes at most one immediate retry for a transient nonterminal publication
failure; this is not a durable delivery guarantee.

New consumer groups start at `0-0`, so tasks already present in a newly
configured Stream are eligible for delivery. `block_ms` must be a positive,
finite timeout; explicit zero-timeout polls are issued without Redis `BLOCK`
and are therefore nonblocking.

On process restart, the provider first lists active Ray Jobs created by the
configured driver entry point and rebuilds its in-memory watcher state from
credential-free Ray metadata. It then claims eligible pending Redis deliveries.
When a claimed delivery exactly matches an active operation/run/attempt/digest,
the provider republishes `ACCEPTED` with the recovered submission identity and
ACKs it without creating a second Ray Job. This is a bounded best-effort repair
for the submit-before-ACK crash window, not a durable exactly-once protocol.

## Cancellation

Each operation has an independent cancel key prefix.

- Before admission, an existing cancel key produces `CANCELLED` and ACK; no
  Ray Job is created. A failed cancel-key check is fail-closed and leaves the
  delivery pending.
- After admission, the provider watcher calls `stop_job(submission_id)`. It
  publishes `CANCELLED` only after Ray reports `STOPPED`.
- If Ray already reports `SUCCEEDED` or `FAILED`, a later cancel key has no
  effect.
- If Ray reports `STOPPED` without a provider stop request, the watcher removes
  the process-local active entry and publishes `FAILED` with
  `RAY_JOB_STOPPED`; it does not invent a provider cancellation reason.
- If Ray reaches `FAILED` or `SUCCEEDED` without a terminal provider event, the
  watcher publishes a fail-closed `FAILED` event. For `SUCCEEDED`, the code is
  `TERMINAL_EVENT_MISSING` because a successful Ray status alone does not prove
  that the result receipt was published.

The active operation map remains process-local, but restart reconstructs active
entries whose Ray metadata exactly matches the current channels and driver
entry point. Missing, malformed, terminal, or foreign job metadata is ignored.
Cooperative worker cancellation and complete restart recovery are not v0.1
guarantees.

The watcher checks for an existing terminal event before requesting a stop,
but artifact persistence and event publication are not transactional in v0.1.
If cancellation lands in that narrow interval, consumers must treat a durable
Bundle or ResultSink receipt as the execution fact. Durable terminal
arbitration belongs to the later reliability phase.

## Credentials

Tasks may include only a reference such as `env:OBJECT_STORE_PROFILE` or
`mount:/var/run/secrets/object-store`. The execution driver verifies the
reference inside its pre-provisioned Ray runtime. It does not copy the value
into Ray metadata or request-derived runtime env.

Credential values are also rejected when embedded as URI user information or
sensitive query parameters. Arbitrary opaque values cannot be classified as
secrets generically; integrations must use `credential_ref` rather than
placing credential material in operation configuration maps.
`credential_ref` is accepted only as the direct `spec.credential_ref` field;
nested lookalike fields are rejected.

If execution succeeds but the `COMPLETED` event cannot be encoded or
published, the driver classifies the notification failure as
`TERMINAL_EVENT_PUBLICATION_FAILED`, not as an execution failure. Event
delivery remains best effort.

Redis URLs must be credential-free. Deployments that require authenticated
Redis should add an independently reviewed client/secret integration in a
later provider version; plaintext URL credentials are rejected in v0.1.

## Observability

Use the channel's configured outer identity field (default `operation_id`) to
route task and event entries, and use `submission_id` to query Ray Jobs. Event
JSON retains canonical `operation_id` and includes public protocol identity, event ID,
timestamp, operation/run/attempt identity, optional `ray_job_id`, phase, and a
bounded redacted payload.

Expected training events include `PHASE`, `LOG`, `METRICS`, and a terminal
event. Expected inference events include `PHASE`, `LOG`, `PROGRESS`, and a
terminal event. `COMPLETED` carries a credential-free Bundle or ResultSink
reference.

## Alpha failure boundary

An external supervisor may restart the consume process after a Redis outage.
XAUTOCLAIM remains a best-effort hook, but the release does not promise cursor
persistence, reconnect state restoration, DLQ behavior, HA, durable events,
or cross-restart exactly-once execution.

## Validation

Run configuration validation before starting the service:

```bash
tributo-broker-redis validate --config /etc/tributo/redis-provider.json
tributo-broker-redis validate --config /etc/tributo/redis-provider.json --check-connectivity
```

Configuration validation requires a Core source root usable by Ray runtime
packaging and an explicit provider driver distribution before the consumer
group is opened or any task is read. Compatibility with the Core
`submit_ray_job()` runtime-env extension contract is verified by the pinned
Core baseline, static typing, wheel contract, and Redis/Ray integration matrix.

Start the provider-owned loop under a process supervisor:

```bash
tributo-broker-redis consume --config /etc/tributo/redis-provider.json
```
