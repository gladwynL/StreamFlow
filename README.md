# StreamFlow

> **Work in progress.** Phases 1–4 of a seven-phase plan are complete: foundations, the event
> streaming core, reliability and recovery, and the API and dashboard. Benchmarks (Phase 5) are
> designed but **not run yet**, so no performance figures are published. This README only describes
> what exists and has been verified.

StreamFlow is a real-time distributed event streaming and analytics platform: synthetic e-commerce
events are published to Apache Kafka and processed by independent workers into PostgreSQL with
explicit, tested delivery semantics. The design is in
[`docs/planning/phase-0-architecture.md`](docs/planning/phase-0-architecture.md) and the decisions in
[`docs/adr/`](docs/adr/).

## What exists today

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
- **Docker Compose** stack and **GitHub Actions CI** that runs every integration test, a Compose
  smoke test publishing a run that must reconcile as PASS, and Playwright end-to-end tests of the
  dashboard against that stack.

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
docs/       architecture, ADRs, reliability claims, verification notes, runbook, versions
compose.yaml
```

## Limitations (current)

- Per-key order is not preserved for events that are dead-lettered and replayed.
- Local use only: no user authentication (operator actions need a key); a single Kafka broker with
  replication factor 1.
- At high throughput the live stream view samples the newest events (20 per second); the full
  history is in the event API and the analytics.
- No performance figures are published until they are measured (Phase 5).

## License

[MIT](LICENSE)
