# Changelog

## Unreleased

- Initial standalone Redis Streams Broker API v1 provider.
- Standalone, Sentinel, and Cluster Redis topology support with Consumer Group
  pending recovery and deterministic Ray submission reconciliation.
- Wheel-only Provider discovery, Ray worker wheel injection, and JSON-safe
  cooperative cancellation support.
- Fail-open Redis outage handling, bounded event publication retries, invalid
  message acknowledgement semantics, and lifecycle metrics replay.
- Add restart-safe active-job supervision, atomic first-writer terminal
  candidates, and Redis topology outage classification. Job IDs now have one
  128-character ASCII contract across task, protocol, and worker boundaries;
  the minimum event-size configuration is derived from the emergency terminal
  schemas.
- Validate staged terminal candidates against their complete minimum wire
  schemas and quantize all terminal durations to millisecond precision.
