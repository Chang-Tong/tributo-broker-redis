# Changelog

## Unreleased

- Standalone Redis Consumer Group delivery for independent training and batch
  inference channels.
- Public `tributo-generic-v1` protocol with four healthy execution profiles.
- One provider execution-driver Ray Job, structured Bundle/ResultSink events,
  queued cancellation, and Ray Jobs stop for running cancellation.
- Alpha scope explicitly excludes full recovery, durable events, Redis Cluster,
  and cross-restart exactly-once guarantees.
- Redis Sentinel discovery and master failover using redis-py, with independent
  Consumer/Ray node lists and credential references resolved from environment
  variables or mounted secret files.
