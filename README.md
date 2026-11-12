# StreamFlow

> **Work in progress.** Phases 1–6 of a seven-phase plan are complete: foundations, the event
> streaming core, reliability and recovery, the API and dashboard, performance, and hardening. This
> README only describes what exists and has been verified; measurements are labelled as local
> measurements, and limitations are listed at the end.

StreamFlow is a real-time distributed event streaming and analytics platform: synthetic e-commerce
events are published to Apache Kafka and processed by independent workers into PostgreSQL with
explicit, tested delivery semantics. The design is in
[`docs/planning/phase-0-architecture.md`](docs/planning/phase-0-architecture.md) and the decisions in
[`docs/adr/`](docs/adr/).

## Verified capabilities

Each capability is backed by automated tests ([`docs/reliability.md`](docs/reliability.md) maps
every claim to its test).

- **Event contract:** a versioned JSON envelope with six event types, strict decoding (unknown
  fields, versions and types rejected), explicit validation rules, and time-ordered UUIDv7 ids.
- **Deterministic generator:** a seed reproduces the event content exactly (verified by a SHA-256
  hash over 100,000 events); every run has its own event ids and timestamps.
- **Processing with database-enforced idempotency:** each event is applied in one PostgreSQL
  transaction together with its row in a deduplication ledger; Kafka offsets are committed only
  after that transaction. Redelivered events are suppressed and counted.
- **Run reconciliation:** every run's producer outcomes (acknowledged, definitely failed,
  ambiguously failed) are checked against the ledger after the topic drains — PASS, FAIL or
  INCOMPLETE, with duplicates reported separately.
- **Failure handling:** failures are classified as bad record (dead-lettered immediately), transient
  (3 attempts in place, then dead-lettered) or infrastructure (retried until the dependency returns;
  never dead-lettered, so a database outage pauses processing instead of flooding the dead-letter topic).
  A record's offset is committed only after its dead-letter record is acknowledged.
- **Dead letters and replay:** dead letters are stored in PostgreSQL with their original topic,
  partition, offset, bytes and failure reason; operators inspect, replay (original event identity, so
  replay is idempotent) or discard them through the REST API ([`docs/api.md`](docs/api.md)), behind an
  operator key and a rate limit.
- **Crash-recovery tests:** real JVMs are killed with `Runtime.halt()` before and after the database
  commit, after a dead-letter publication, and in the middle of a replay; each recovers with no lost and
  no doubly applied event. Every claim maps to a test: [`docs/reliability.md`](docs/reliability.md).
- **Retention:** event partitions, ledger rows, aggregates and resolved dead letters expire on schedule,
  without ever breaking the deduplication window or deleting unresolved recovery evidence.
- **Runtime roles** of one Spring Boot 4 application: `migrate` (schema, topics, consumer-group
  offsets), `worker` (processing, dead-letter ingestion, heartbeats), `generate` (one seeded run, then
  reconcile), `api` (REST API, live feed, simulations, dead-letter replay, retention maintenance).
- **Versioned REST API** ([`docs/api.md`](docs/api.md)): live snapshot, paged event history, analytics
  over 15 minutes to 24 hours, consumer groups and partition lag, workers, simulation runs and
  dead letters, with RFC 9457 problem responses and capped, indexed queries.
- **Live feed over WebSocket:** one snapshot per second, computed once whatever the number of
  clients; a slow browser skips snapshots and is disconnected instead of being waited for, and it
  never touches Kafka processing (tested with a client that never reads).
- **React dashboard** with six sections — Overview, Live stream, Kafka & processing, Analytics,
  Dead letters, Simulations — plus System Status. Every value comes from the backend; unmeasured
  values show as a dash. Bounded synthetic runs can be started, stopped and reconciled, and dead
  letters replayed or discarded, behind confirmation dialogs and an operator key held only in
  memory. Accessible (axe-checked, keyboard, reduced motion) and responsive.
- **Hardening (Phase 6):** a Kafka position deleted by retention is never skipped: the worker pauses
  that partition, records the gap with the number of events lost, and status explains the deliberate
  operator recovery; the dashboard warns before a lagging partition reaches its retention limit;
  broker and database outages are ridden out in both processing modes; operator-key guessing is
  rate-limited ([`docs/security.md`](docs/security.md)); an OpenAPI description
  ([`docs/openapi.yaml`](docs/openapi.yaml)) is checked against the controllers.
- **Docker Compose** stack and **GitHub Actions CI** that runs every integration test, a Compose
  smoke test publishing a run that must reconcile as PASS, and Playwright end-to-end tests of the
  dashboard against that stack.

## Local benchmark measurements

Measured with the benchmark harness ([`docs/benchmarks/`](docs/benchmarks/README.md)) on one laptop
(6-core Ryzen 5 PRO 7540U, Docker Desktop): medians of repeated runs, every run reconciled exactly.
These describe this machine, not production capacity.

- One worker drains **15,548 events/s** (14,276–16,598) in the default batch mode and **941 events/s**
  (783–983) in record mode (one transaction per record).
- At 500 events/s offered, batch-mode processing latency is p50 5.8 ms, p95 27.7 ms, p99 361 ms. One of
  three batch runs at 750 events/s showed an unexplained p95 of 7.7 s; it remains a documented
  uncertainty.
- The producer publishes **63,247 events/s** (61,679–69,683) acknowledged by Kafka (acks=all).
- Profiling showed record mode was bound by per-record commit and round-trip latency, not CPU: fewer
  round trips gave +30 % and batching one commit per poll batch gave 16×.

## Stack

Java 21 · Spring Boot 4.1 · Spring for Apache Kafka · Spring JDBC · Flyway · PostgreSQL 18 ·
Apache Kafka 4.3 (KRaft) · React 19 · TypeScript · Vite · Tailwind CSS · TanStack Query · Recharts ·
Testcontainers · Playwright · axe-core · Docker Compose · GitHub Actions. Exact versions: [`docs/versions.md`](docs/versions.md).

## Quick start

Requires Docker Desktop. See [`docs/runbook.md`](docs/runbook.md) for details and local development.

```bash
cp .env.example .env    # then set POSTGRES_PASSWORD to a random value
docker compose --profile app up -d --build --wait
docker compose run --rm --no-deps generate    # publish and reconcile a seeded run
```

Open http://localhost:8080 for the dashboard. To start runs or replay dead letters from it, set
`STREAMFLOW_OPERATOR_KEY` in `.env` first (see the runbook).

## Repository layout

```
backend/    Maven build: streamflow-events (contracts), streamflow-app (Spring Boot roles)
frontend/   React dashboard (Vite)
benchmarks/ benchmark harness (run.sh, scenarios, analyze.py) and raw results
docs/       architecture, ADRs, reliability claims, verification notes, runbook, versions
compose.yaml
```

## Known limitations

- Per-key order is not preserved for events that are dead-lettered and replayed.
- Events deleted by Kafka retention before they were read are lost; StreamFlow records and reports
  them, and recovery needs an operator ([`docs/runbook.md`](docs/runbook.md), "Offset gaps").
- At high throughput the live stream view samples the newest events (20 per second); the full
  history is in the event API and the analytics.
- Performance figures come from one laptop where every component shares one CPU and one Docker VM.

## Production deployment limitations

StreamFlow is built and verified as a local, single-user system:

- No user authentication; operator actions are protected by one shared key and a rate limit.
- One Kafka broker with replication factor 1, one PostgreSQL instance, no TLS: losing the Docker VM's
  disk before a flush can lose acknowledged events.
- Every port binds to 127.0.0.1; nothing is designed to be exposed to a network.

## Future improvements

Not implemented: multi-broker Kafka and replicated PostgreSQL, scaling across machines, 3- and
12-partition and KIP-848 consumer-protocol measurements, and a cheaper latency-histogram write in
batch mode (see the limitations in [`docs/benchmarks/README.md`](docs/benchmarks/README.md)).

## License

[MIT](LICENSE)
