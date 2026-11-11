# ADR-0011: Dashboard data path

- **Status:** Accepted, 2026-11-10; revised 2026-11-11 (Phase 4 implementation)
- **Implemented:** Phase 1 (status page, polling); Phase 4 (WebSocket feed, six live sections)

## Context

The dashboard must show real measurements without becoming a bottleneck during benchmarks, and a slow or stalled
browser must never be able to slow down Kafka processing.

## Decision

- **The api never consumes the event topic.** It reads PostgreSQL aggregates (minute statistics, a per-minute
  latency histogram written in the processing transaction, worker heartbeat counters) and at most the 20 newest
  events and 10 newest dead letters per snapshot. Lag, ownership and log-end offsets come from Kafka AdminClient
  (`describeConsumerGroups`, `listConsumerGroupOffsets`, `listOffsets`).
- **One snapshot per second, computed on demand.** The live snapshot is computed at most once per 900 ms and only
  while someone asks (a connected WebSocket client or a REST call), then serialized once and pushed to every client.
  There are no per-event messages.
- **Slow clients are isolated, not waited for.** Sends run on a small bounded pool, never on the ticking thread.
  Each client has at most one snapshot in flight: a client still receiving the previous one skips the tick, and after
  5 consecutive skips (or a send slower than 3 s, or more than 1 MiB buffered) it is disconnected. At most 20 clients
  may connect. The feed runs in the api process; processing runs in separate worker processes.
- **The browser reconnects and resynchronises.** The frontend reconnects with capped exponential backoff and jitter,
  fetches `GET /api/v1/live` after every (re)connection, marks data older than 4 s as delayed, drops a connection
  silent for 10 s, pings every 15 s, and keeps at most 500 live-stream rows and 5 minutes of trend points in memory.
- **Measurements only.** A value that cannot be measured is `null` and shown as a dash, never as 0. Percentiles are
  histogram bucket upper bounds and labelled as such. Synthetic traffic is labelled synthetic.
- **Phase 1** status page: polls `GET /api/v1/system/status` every 5 s.

## Alternatives considered

- *Server-Sent Events:* simpler, but a WebSocket gives one bidirectional connection for keep-alive pings and an
  explicit close code for slow or refused clients.
- *Broadcasting each processed event:* cost would grow with throughput and couple browsers to the processing rate.
- *Consuming the event topic in the api:* would add a consumer group whose lag and rebalances interfere with the
  measurements the dashboard reports.

## Consequences

The dashboard shows processed truth (from PostgreSQL), not in-flight messages. At high throughput the live stream
is a sample of the newest events; the complete history is available through the paged event API. Snapshot cost is
constant in the number of clients (one set of queries per second). Benchmarks (Phase 5) report results with the
dashboard open and closed.

## Verification

- `LiveBroadcasterTest`: a stalled client never delays the ticking thread or other clients, holds at most one
  snapshot, and is disconnected after 5 skipped ticks; the client limit; nothing is computed without clients.
- `DashboardApiIT` (real api, worker, Kafka, PostgreSQL): about one snapshot per second, the origin check, the client
  limit, and a client that completes the handshake and never reads while a run is published, processed and
  reconciled as PASS, with a healthy client still receiving snapshots.
- Frontend: `useLiveFeed` tests (resync, ordering, stale detection, backoff, disposal); Playwright end-to-end tests
  against the Compose stack, including a dropped connection that recovers with a fresh REST snapshot.
