# StreamFlow

> **Work in progress.** Phase 1 (foundations) of a seven-phase plan. Event generation, processing,
> failure handling, analytics and benchmarks are designed but **not implemented yet**. This README
> only describes what exists and has been verified.

StreamFlow will be a real-time distributed event streaming and analytics platform: synthetic
e-commerce events published to Apache Kafka, processed by independent workers into PostgreSQL with
explicit delivery semantics, and shown on a live dashboard. The design is in
[`docs/planning/phase-0-architecture.md`](docs/planning/phase-0-architecture.md) and the decisions in
[`docs/adr/`](docs/adr/).

## What exists today (Phase 1)

- **One Spring Boot 4 application, three runtime roles** selected by Spring profile:
  - `migrate` — applies Flyway migrations, provisions Kafka topics (creates missing ones, resets
    drifted configs, refuses to change partition counts), then exits;
  - `api` — `GET /api/v1/system/status`, a live check of Kafka, PostgreSQL and worker heartbeats;
  - `worker` — records a heartbeat every 5 seconds (no event processing yet).
- **A React dashboard** with one page, System Status, refreshed every 5 seconds from the real API.
- **Docker Compose** stack: Kafka 4.3 (KRaft), PostgreSQL 18, the three roles and nginx; ports
  bound to 127.0.0.1, every container memory-limited, application containers non-root with
  read-only file systems.
- **Tests:** unit tests, ArchUnit role-boundary rules, and integration tests against real Kafka and
  PostgreSQL containers (Testcontainers); frontend component tests (Vitest).
- **CI:** GitHub Actions runs the backend build with integration tests, frontend checks, and a
  Compose smoke test.

## Stack

Java 21 · Spring Boot 4.1 · Spring for Apache Kafka · Spring JDBC · Flyway · PostgreSQL 18 ·
Apache Kafka 4.3 (KRaft) · React 19 · TypeScript · Vite · Tailwind CSS · TanStack Query ·
Testcontainers · Docker Compose · GitHub Actions. Exact versions: [`docs/versions.md`](docs/versions.md).

## Quick start

Requires Docker Desktop. See [`docs/runbook.md`](docs/runbook.md) for details and local development.

```bash
cp .env.example .env    # then set POSTGRES_PASSWORD to a random value
docker compose --profile app up -d --build --wait
```

Open http://localhost:8080.

## Repository layout

```
backend/    Maven build: streamflow-events (contracts), streamflow-app (Spring Boot roles)
frontend/   React dashboard (Vite)
docs/       Phase 0 architecture, ADRs, runbook, pinned versions
compose.yaml
```

## Limitations (current)

- No event processing, generator, reliability features or analytics yet (Phases 2–5).
- Local use only: no authentication; single Kafka broker with replication factor 1.
- No performance figures are published until they are measured (Phase 5).

## License

[MIT](LICENSE)
