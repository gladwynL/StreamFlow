# ADR-0011: Dashboard data path

- **Status:** Accepted, 2026-11-10
- **Implemented:** Phase 1 (status page, polling); Phase 4 (WebSocket, live sections)

## Context

The dashboard must show real measurements without becoming a bottleneck during benchmarks.

## Decision

- The API never consumes the event topic. It reads per-partition aggregates and at most 20 sampled
  recent rows per second; lag comes from Kafka AdminClient every 2 s.
- One WebSocket snapshot per second through bounded send buffers; slow clients are disconnected.
- The frontend caps the live feed at 200 rows.
- Phase 1: the System Status page polls `GET /api/v1/system/status` every 5 s; every value is a
  live check made while answering the request.
- All operational values are real; synthetic activity is labelled synthetic.

## Consequences

The dashboard shows processed truth (from PostgreSQL), not in-flight messages. Benchmarks report
results with the dashboard open and closed.

## Verification

Phase 1: `SystemStatusIT`, `SystemStatusDegradedIT`, frontend component tests. Phase 4: a slow
WebSocket client test and end-to-end tests against the real stack.
