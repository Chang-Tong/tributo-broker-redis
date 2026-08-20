# tributo-broker-redis

`tributo-broker-redis` is an optional Tributo Broker API v1 provider for the
KnoVa Redis Streams control plane. It consumes training-task envelopes,
submits Tributo Ray Jobs, publishes lifecycle events, and checks cooperative
cancellation keys.

Redis Streams is used here as a control-plane transport. This package is not a
Tributo `StreamSource`, does not read training data from Redis, and is not a
streaming-inference input.

## Install

Install Tributo Core and this provider into the process that runs the broker
consumer. The same provider wheel must be available to the Ray Job runtime
when the job entrypoint or worker-side cancellation checker is used.

```bash
pip install 'tributo>=1.1,<2.0' tributo-broker-redis
tributo broker list
```

The provider is discovered through the `tributo.brokers` entry-point group.
Tributo Core does not import Redis or this package during normal startup.

## Configuration

`--config` accepts JSON only. Core passes the object through unchanged; this
provider validates all fields.

```json
{
  "mode": "standalone",
  "url": "redis://redis.example:6379",
  "password_env": "KNOVA_REDIS_PASSWORD",
  "worker_password_env": "KNOVA_REDIS_PASSWORD",
  "task_stream_key": "knova:training:tasks",
  "event_stream_prefix": "knova:training:events",
  "invalid_event_stream_key": "knova:training:events:invalid",
  "consumer_group": "tributo",
  "group_start_id": "$",
  "claim_idle_ms": 60000,
  "claim_count": 10,
  "max_payload_bytes": 1048576,
  "max_event_bytes": 1048576,
  "allow_legacy_training_config": false,
  "ray_dashboard_url": "http://ray-head:8265",
  "runtime_pip_packages": ["/provider/tributo_broker_redis-<version>-py3-none-any.whl"]
}
```

Replace `<version>` with the actual Provider wheel version; IT scripts discover
the built filename dynamically.

Do not put a Redis password in JSON, a URL, a task payload, or a log line.
Use `password_env` and, when the Ray worker uses a different environment,
`worker_password_env`. In Cluster mode set `db` to `0`; logical Redis
databases are not supported by Redis Cluster.

The default `consumer_name` is generated from host, process, and a random
suffix. Set it explicitly only when a stable Redis consumer identity is
required for an operational reason.

## Commands

```bash
tributo broker validate --broker knova-redis --config broker.json
tributo broker validate --broker knova-redis --config broker.json --check-connectivity
tributo broker consume --broker knova-redis --config broker.json
```

Discovery is fail-open for ordinary `tributo` commands. An explicitly selected
missing, filtered, or invalid provider fails closed. Redis connection failure
is logged and contained by the Core BrokerRunner; task processing resumes via
bounded reconnect and Redis pending recovery.

Tributo 1.1 is the minimum supported Core release because it introduces Broker
API v1, stable Ray submission identity, and worker execution context support.

## Delivery semantics

- Consumer Groups provide at-least-once task delivery.
- A valid task is acknowledged after the Ray Job submission is accepted.
- A temporary submission or transport failure leaves the message pending.
- Pending messages are reclaimed with `XAUTOCLAIM`; its cursor is retained and
  advanced between recovery rounds.
- Delivery retries reuse business `run_id` and `attempt-1`, so deterministic
  Ray submission reconciliation prevents a second Job for the same execution.
- Invalid payloads are reported as `FAILED` on a best-effort basis and then
  acknowledged. Missing outer `job_id` messages use the invalid-event stream
  and never receive a sentinel identity.
- Core phase events and sampled rank-0 round metrics are published in real
  time through the Provider worker bridge. The Provider sends `QUEUED` before
  submission and owns the single terminal event; it does not replay history.

See the [protocol-v2 capability matrix](docs/protocol-v2-capabilities.md) for
the supported execution surface and fail-fast unsupported field paths.

## Topology support

The provider supports standalone Redis, Sentinel master discovery, and Redis
Cluster. Redis keys must use compatible naming and hash-tagging conventions in
Cluster deployments when a deployment requires multi-key atomic operations;
the provider itself uses independent task, event, and cancellation commands.
When Sentinel announces a master address behind NAT or a container bridge, set
`sentinel_address_map` to map each advertised `host:port` to a client-reachable
address. The optional `sentinel_force_master_ip` setting is supported by the
redis-py range declared by this package. When Redis Cluster
announces internal node hosts, set `cluster_address_remap_host` to map them to
the client-reachable host while preserving their announced ports.
Sentinel and Cluster connectivity must be validated in the provider IT suite,
not inferred from client construction alone.

## Development

Run the fast provider checks with Tributo Core available on `PYTHONPATH` or
installed in the environment:

```bash
PYTHONPATH=/path/to/Tributo/src:$PWD/src \
  python -m pytest -q tests/unit
ruff format --check .
ruff check .
mypy src tests/unit
uv build
```

Local checks must resolve the declared `redis>=6,<8` dependency range; do not
reuse an environment that has redis-py 8.x installed.

The real Redis/Ray suite is intentionally separate because it starts a unique
Docker Compose project and requires a Ray cluster:

```bash
./scripts/run_it.sh
```

The IT script builds the wheel, uses an isolated test environment, and
ensures the test process and Ray workers import the installed wheel rather
than the provider source tree.
