# StreamFlow — Phase 0 Architecture and Planning (approved)

**Status:** Approved 2026-11-10, including five refinements requested in review.
**Nature of this document:** a design and plan. It describes what StreamFlow *will* do and how each
property will be proven. It does not claim that anything beyond Phase 1 is implemented. Per-phase
implementation status is tracked in the phase reports and the README.

Decision records derived from this document live in [`docs/adr/`](../adr/).

---

## 1. Purpose and portfolio fit

StreamFlow is a real-time distributed event streaming and analytics platform. Synthetic e-commerce
activity is generated, published to Kafka, processed by independent workers into PostgreSQL, and
shown on a live dashboard.

The project exists to demonstrate what the author's other projects (DevTrack, RepoPilot,
CloudPulse — all Python/FastAPI) do not: Java and Spring Boot, Apache Kafka, explicit delivery
semantics, fault-tolerant processing proven by crash tests, and measured performance.

The engineering value is in failure behaviour, the tests that prove it, and real measurements —
not in moving events into a table. The dashboard must look distinct from CloudPulse (partitions,
lag, recovery, reconciliation rather than uptime).

## 2. Technology decisions

| Area | Decision |
|---|---|
| Language / runtime | Java 21 (Temurin) |
| Framework | Spring Boot 4.1.x, Spring for Apache Kafka 4.1.x (Boot-managed) |
| Persistence | Spring JDBC (`JdbcClient`), not JPA — see §4.2 |
| Broker | Apache Kafka 4.3.x, KRaft, single node locally |
| Database | PostgreSQL 18.x |
| Migrations | Flyway (Boot-managed) |
| Build | Maven via Maven Wrapper |
| Quality | Spotless (palantir-java-format), ArchUnit, JaCoCo |
| Tests | JUnit Jupiter, AssertJ, Awaitility, Testcontainers (real Kafka and PostgreSQL) |
| Frontend | React 19, TypeScript, Vite, Tailwind CSS, TanStack Query, React Router; Recharts from Phase 4 |
| Frontend tests | Vitest, Testing Library; Playwright with axe-core from Phase 4 |
| Observability | Spring Actuator and Micrometer; no Prometheus/Grafana servers |
| Infrastructure | Docker Compose, GitHub Actions; local-first, no paid cloud |

Rejected: Schema Registry/Avro (extra service; versioned JSON with validation suffices), Kafka
Streams (state stores and changelog topics; PostgreSQL transactions are simpler to prove),
Kafka transactions (cannot include PostgreSQL, so they do not give end-to-end exactly-once),
Redis, Kubernetes, Elasticsearch, extra microservices.

Exact versions are recorded in [`docs/versions.md`](../versions.md).

## 3. Architecture

### 3.1 One codebase, separate runtime roles

A Maven multi-module build (`streamflow-events`: plain-Java contracts; `streamflow-app`: the Spring
Boot application) produces one image. Each process runs exactly one role, selected by activating
the Spring profile of the same name:

| Role | Responsibility | Instances |
|---|---|---|
| `migrate` | Flyway migrations, Kafka topic provisioning, then exit 0 | one-shot, runs first |
| `api` | REST and WebSocket gateway, simulation control (rate-capped), replay, lag via AdminClient, retention jobs | 1 |
| `worker` | Event consumption and processing, dead-letter publication, dead-letter ingestion | 1..N |
| `loadgen` | Headless benchmark producer (added in Phase 5) | benchmarks only |

Workers must be separate processes so they can be killed and scaled; the benchmark producer must be
outside the API process so measurements stay clean. ArchUnit tests forbid dependencies between role
packages.

### 3.2 Data flow

1. The generator builds an event (UUIDv7 id, run id, sequence number) and publishes it keyed by user
   id with `acks=all` and idempotence enabled.
2. A worker polls a batch.
3. Per record, in one PostgreSQL transaction: insert into the dedup ledger with
   `ON CONFLICT DO NOTHING`; if new, insert the event row and update per-partition aggregates; if a
   duplicate, change nothing.
4. The transaction commits.
5. Offsets are committed after the poll batch (`AckMode.BATCH`, auto-commit disabled).
6. The API reads aggregates and indexed recent rows and pushes one snapshot per second.

The API never consumes the event topic.

## 4. Reliability model

### 4.1 Guarantee

At-least-once delivery with duplicate-free effects in PostgreSQL, provided the event id is stable
and redelivery or replay happens within the deduplication retention window (§6.3). StreamFlow never
claims end-to-end exactly-once delivery.

### 4.2 Why Spring JDBC

The core write is `INSERT … ON CONFLICT DO NOTHING RETURNING` plus `ON CONFLICT DO UPDATE`
aggregates — native SQL in JPA. JPA's persistence context costs memory per message for no benefit;
identity generation defeats batching. Plain SQL keeps transaction and locking behaviour visible.

### 4.3 Crash semantics

- **Crash before the database commit:** PostgreSQL rolls back on connection loss; no offset was
  committed; the partition is redelivered from the last committed offset. No loss, no duplicate.
- **Crash after the database commit, before the offset commit:** redelivered records conflict on the
  ledger and are skipped; `duplicates_suppressed` increases. An ambiguous commit (lost reply) is
  covered the same way.

### 4.4 Failure classification

| Category | Examples | Handling |
|---|---|---|
| Bad record (non-retryable) | undeserializable bytes, validation failure, unknown schema version, data errors (SQLState class 22, class 23 except the ledger conflict), injected poison | Dead-letter on first attempt; offset advances |
| Record-scoped transient | `40001`, `40P01`, `55P03`, injected "fail N times" | Retry in place, max 3 attempts, backoff under 10 s total; then dead-letter |
| Infrastructure unavailable | class `08` errors, pool timeout, database down | Never dead-lettered. Unlimited backoff capped at 10 s; partition stalls; lag grows; health DOWN; resumes on recovery |

Retries are in place (blocking), not retry topics, to preserve per-key order. The retry counter is
per assignment: a crash during retries resets it (documented limit).

### 4.5 Dead-letter publication, offsets and replay

A failing record at offset *o*: retries exhausted → synchronous publish to the DLT (same partition
number, `acks=all`) → only after the acknowledgement is *o+1* committed. If DLT publication fails,
the offset is not committed and the partition stalls rather than losing the record. A separate
`dlt-ingestor` group copies dead letters into `dead_letter_events`, unique on the original
topic/partition/offset, so duplicate dead letters collapse.

Replay is per item and database-backed: claim (`PENDING → REPLAY_REQUESTED` with a conditional
update), republish original bytes and key with `sf-replay-of`, mark `REPLAYED` after the broker
acknowledgement. A crash between publish and mark leaves the item claimed; a sweeper returns it to
`PENDING`, and the duplicate publish is suppressed by the ledger because the event id is unchanged.
Replay is refused for dead letters older than the replay window and for records without an event
id.

### 4.6 Ordering

Per-key (per-user) order is preserved for first-delivery processing: one partition, one consumer,
in-place retries, idempotent producer. Redelivered prefixes are suppressed. A dead-lettered event is
overtaken by later events of the same key and lands after them when replayed; analytics are
order-independent and `occurred_at` is the authoritative event time. Partition counts of live topics
are never changed.

### 4.7 Run reconciliation (refinement 1)

Every record carries headers `sf-run-id`, `sf-event-id`, `sf-seq`, so even an undeserializable
dead letter can be attributed. The ledger stores `(run_id, run_seq)`.

**Generator outcomes, one per attempted sequence number:** `ACKED`, `FAILED_DEFINITE` (never reached
the broker: serialization, buffer timeout, record too large) or `FAILED_AMBIGUOUS` (timeout or
network error after send). Failures are stored by sequence number with a cap; exceeding the cap
aborts the run as `INCOMPLETE`. Acknowledged events are not stored: they are every attempted
sequence number not recorded as failed. At run end the generator flushes and awaits every callback;
a generator crash marks the run `ABORTED` and its reconciliation `INCOMPLETE`.

**Sink states, one per event, in precedence order:**

1. `PROCESSED` — in the ledger (first delivery, retry or successful replay — counted once);
2. `DISCARDED` — operator-discarded dead letter, not in the ledger;
3. `DEAD_UNRESOLVED` — a dead letter (pending, replay requested or expired), not in the ledger.

Multiple dead-letter rows of one event collapse to one event. A successfully replayed event counts
only as `PROCESSED`.

**Duplicates** are counted as suppressed *deliveries*, never added to event totals.
**Replay outcomes** are derived: succeeded = `REPLAYED` dead letters whose event is in the ledger;
in flight = `REPLAY_REQUESTED`/`REPLAYED` dead letters whose event is not yet in the ledger.
`run_stats(run_id, kafka_partition, processed, duplicates_suppressed)` is updated in the same
transaction as the ledger.

**Checks, after the run drains (zero lag in both groups, no replay in flight):**

1. No missing events: every acknowledged sequence number is `PROCESSED`, `DEAD_UNRESOLVED` or
   `DISCARDED`.
2. No unexpected events: none recorded as `FAILED_DEFINITE`, none never attempted.
3. Count identity: processed + dead-unresolved + discarded = acknowledged + ambiguous-that-landed
   (ambiguous outcomes are reported separately, not as failures).
4. No double effects: `run_stats` processed = ledger count = `events` count (while retained).
5. No reused sequence numbers.

Results: `PASS`, `FAIL` (with offending sequence numbers) or `INCOMPLETE` (not drained, generator
aborted, failure cap exceeded, or beyond retention). Results are stored in `run_reconciliations` so
evidence outlives the ledger window.

## 5. Events and topics

### 5.1 Envelope

```json
{ "schemaVersion": 1, "eventId": "<UUIDv7>", "eventType": "PURCHASE_COMPLETED",
  "occurredAt": "2026-11-09T18:22:31.123Z", "source": "simulator", "synthetic": true,
  "runId": "<uuid>", "sequence": 18234, "userId": "u-004211", "sessionId": "s-…",
  "payload": { "orderId": "…", "amountCents": 4599, "currency": "CAD", "itemCount": 2 },
  "simulation": { "fault": "NONE" } }
```

Types: `USER_LOGIN`, `PRODUCT_VIEWED`, `CART_UPDATED`, `PURCHASE_COMPLETED`, `PAYMENT_FAILED`,
`APPLICATION_ERROR`. `PAYMENT_FAILED` and `APPLICATION_ERROR` are business events that process
successfully and are shown separately from processing failures.

### 5.2 Deterministic simulation (refinement 2)

- **Reproduced by a seed:** event type, user, session, payload, fault directive, sequence number,
  and the scheduled offset from run start.
- **Unique per run:** `runId` (random), `eventId` (UUIDv7 from time and a secure random source,
  never the seed, with a monotonic counter within a millisecond), `occurredAt` (actual run start
  plus scheduled offset).
- Content is generated on one thread with `SplittableRandom(seed)` and per-user state; event *n*
  depends only on (seed, configuration, `contentVersion`, *n*) — not on rate, timing, send failures
  or scheduling. Money is integer cents.
- **Proof:** SHA-256 over the normalized JSON of the first 100,000 events matches across runs and
  JVMs for the same seed, configuration and version; differs for a different seed; two runs of the
  same seed share no event ids and no timestamps.
- **Limit:** reproducible content is not reproducible processing order or timing across partitions;
  benchmarks are statistically repeatable.

### 5.3 Topics

| Topic | Partitions | Settings |
|---|---|---|
| `streamflow.events.v1` | 6 | key `userId`; RF 1, `min.insync.replicas` 1 (single broker); retention 24 h or 1 GiB per partition; 128 MiB segments |
| `streamflow.events.v1.dlt` | 6 | same partition number as the source; retention 7 days or 256 MiB per partition |
| `bench.*` | 3/6/12 | one per benchmark configuration; deleted after the run |

Six partitions divide evenly across 1, 2, 3 and 6 consumers. Broker auto-creation is disabled; the
`migrate` role owns topics, recreates nothing, never changes a partition count, and resets drifted
managed configs.

Producer: `acks=all`, idempotence, `linger.ms=5`, `batch.size=64KB`, lz4,
`delivery.timeout.ms=30000`, `max.block.ms=5000`, `buffer.memory=32MB`.
Consumer: auto-commit off, `max.poll.records=500`, `session.timeout.ms=10000`, classic group
protocol (KIP-848 compared in Phase 5), `auto.offset.reset=none` for the processing group (§6.2).

## 6. Data model and retention

### 6.1 Tables (introduced from Phase 2)

| Table | Purpose / keys |
|---|---|
| `processed_events` | dedup ledger: PK `event_id`; `run_id`, `run_seq`, partition, offset, `processed_at` (BRIN) |
| `events` | daily range partitions on `ingested_at`; PK (`event_id`, `ingested_at`); typed columns + JSONB payload |
| `event_stats_minute` | PK (bucket, type, **kafka_partition**) — no cross-worker row contention |
| `latency_stats_minute` | mergeable log-scale latency histogram buckets per partition |
| `run_stats` | per run and partition: processed, duplicates suppressed |
| `dead_letter_events` | unique on original topic/partition/offset; status state machine |
| `simulation_runs`, `publish_failures`, `run_reconciliations` | run accounting (§4.7) |
| `worker_heartbeats` | worker liveness (Phase 1) |
| `fault_settings` | predefined, time-limited fault scenarios only |

Latency: broker latency = `ingested_at − kafka_ts`; event-time latency from the *scheduled*
`occurred_at` so generator lag is not hidden. All containers share one host clock (documented).

### 6.2 Kafka backlog and outage window (refinement 3)

Kafka deletes whole closed segments (128 MiB); the active segment is never deleted; checks run every
5 minutes. Time retention deletes a segment only when its newest record is older than 24 h, so time
never deletes early. **Size retention (1 GiB per partition) can delete unconsumed data well before
24 h at high throughput.**

Supported backlog window **W = min(24 h, (1 GiB − 128 MiB) ÷ peak per-partition ingress bytes/s)**.
Phase 5 measures bytes per event and publishes W for each tested rate; the dashboard shows
retention headroom from measured ingress. No figure is published before it is measured.

Overrun is loud, not silent: the processing group uses `auto.offset.reset=none` (the migrate role
initialises its offsets to earliest on first setup), so a committed position below the log start
offset raises an out-of-range error, turns health DOWN and fires an alert metric instead of skipping
ahead. Recovery is a deliberate, runbook-documented operator step; reconciliation also reports the
gap. The DLT has its own 256 MiB-per-partition cap; it only bounds the dead-letter ingestion backlog
because the database is the durable store for dead letters.

**Claim boundary:** no acknowledged event is lost while (a) the broker log is intact, (b) every
partition's backlog stays within W, and (c) ledger retention covers redelivery and replay windows.
**Not guaranteed:** with one broker and RF 1, durability depends on OS flushing; disk loss or a VM
or host crash before flush can lose acknowledged events. Events in the producer buffer at a crash
were never acknowledged and are outside the claim.

### 6.3 Retention and the deduplication invariant

Rule: ledger retention ≥ longest window in which an event can return + margin.

| Data | Retention |
|---|---|
| main topic | 24 h (or size cap) |
| DLT and replay window | 7 days |
| **ledger** | **8 days** |
| `events` | 7 days (drop daily partitions) |
| aggregates | 30 days |
| resolved dead letters | 14 days; unresolved become `EXPIRED` (not replayable) after 7 days |

The application refuses to start if configured retention violates the rule; replay refuses dead
letters older than 7 days. Retention jobs run in the `api` role under a PostgreSQL advisory lock.

## 7. Dashboard and API

The API reads aggregates and at most 20 sampled rows per second for the live feed; lag comes from
AdminClient every 2 s; WebSocket pushes one snapshot per second through bounded send buffers
(`ConcurrentWebSocketSessionDecorator`) so slow clients are dropped. The frontend caps the feed at
200 rows. Benchmarks are run with the dashboard open and closed and the difference is reported.
Mutating controls require a server-checked operator key (localhost-only use) and are rate-limited;
only predefined, bounded scenarios exist. All operational values come from real measurements;
synthetic activity is labelled synthetic.

## 8. Resource budget

The laptop has 16 GB installed but **11.7 GB usable by Windows** (the rest is reserved for
integrated graphics). Docker Desktop's WSL VM uses the default cap (measured 5.62 GiB).

| Component | Setting | Container limit |
|---|---|---|
| Kafka | heap 256–512 MB | 1 GB |
| PostgreSQL | `shared_buffers=256MB` | 768 MB |
| api | `MaxRAMPercentage=70` | 768 MB |
| worker | same | 512 MB each |
| web (nginx) | — | 64 MB |

Working rules: infrastructure-only Compose profile by default; stop other projects' containers;
close heavy applications during integration tests and benchmarks; named volumes (not Windows bind
mounts); shared Testcontainers containers per test JVM. Measured figures are recorded in
[`docs/runbook.md`](../runbook.md).

## 9. Spring Kafka behaviour to verify, not assume (refinement 4)

Each becomes a pinned integration test; results go to `docs/verification/spring-kafka-behavior.md`
with the tested versions.

1. With `AckMode.BATCH`, whether offsets before a failed record *k* are committed before the seek
   (correctness must not depend on it).
2. Retries seek back to the failed record; nothing after *k* is processed first.
3. DLT publication failure: redelivery, no commit, and the effect of `resetStateOnRecoveryFailure`.
4. The recovered record's offset is committed only after the DLT acknowledgement.
5. Per-exception backoff (`setBackOffFunction`); capped unlimited backoff never exceeds
   `max.poll.interval.ms`.
6. No Kafka transaction manager; the database transaction (`TransactionTemplate`) commits before the
   listener returns; proven by the crash-after-commit test.
7. Batch mode (Phase 5): `BatchListenerFailedException(index)` commits only offsets whose effects
   are committed (per-record fallback); proven with a positioned failure plus a crash.
8. Graceful shutdown commits offsets; zero duplicates on restart.
9. Offsets are committed on revocation during a batch.
10. Out-of-range behaviour with `auto.offset.reset=none`.

If any behaves differently than expected, work stops and it is reported before any workaround.

## 10. Minimum tests for the reliability claims

Normal run reconciles; double delivery → one effect; concurrent same-id race → one effect;
crash before commit (child JVM `Runtime.halt()`); crash after commit before offset commit; crash
after DLT publish before offset commit; poison pill; transient fault that clears; transient fault
that persists; PostgreSQL paused 20 s under load (no dead letters); DLT publication failure;
replay once, twice, concurrently and with a crash; graceful shutdown under load; worker join/leave
under load; startup retention-invariant check. Each public claim maps to its test in
`docs/reliability.md`.

## 11. Roadmap and acceptance criteria

| Phase | Measurable acceptance criteria |
|---|---|
| 1. Foundations | `mvnw verify` passes locally and in CI including Testcontainers tests of migrations and topic configs; clean-clone Compose stack healthy within 120 s with `migrate` exiting 0; measured memory recorded; status page shows real cluster id, PostgreSQL version and worker heartbeats; formatting and frontend checks in CI; versions pinned; ADRs written; no secrets committed. |
| 2. Streaming core | Same seed → identical content hash over 100k events with unique ids; 100k events at 1,000/s reconcile exactly; 1,000 events delivered twice → 1,000 rows and 1,000 suppressed; both crash tests pass; graceful shutdown → 0 duplicates; worker join/leave → no loss; concurrent race → one effect. |
| 3. Reliability | Classifier unit-tested; transient fault clears with no dead letter; persistent fault dead-lettered after exactly 3 attempts within the bound; poison pill dead-lettered on attempt 1; PostgreSQL pause → 0 dead letters, exact reconciliation; DLT failure stalls without loss; crash after DLT publish → one row; replay once/twice/concurrent/crash; "faulty release" demo reconciles; retention startup check. |
| 4. API and frontend | OpenAPI published; invalid input → 400; operator endpoints 401 without key, 429 when rate exceeded; slow WebSocket client does not slow broadcast; six sections on real data with E2E against the real stack; synthetic labels; responsive at 375/768/1280/1920; zero serious/critical axe violations; keyboard navigation. |
| 5. Performance | Reproducible scenario files; JSON results with automatic environment manifest; matrix ×3 with median and range; producer max, consumer drain by mode/partitions/consumers, p50/p95/p99 at three loads, lag, recovery after kill, resource use; every run reconciles; at least one bottleneck proven with JFR or `pg_stat_statements` and fixed with before/after numbers. |
| 6. Hardening | Integration suite green five consecutive CI runs; Compose + Playwright from clean CI checkout; security and concurrency checklists; non-root, read-only, capabilities dropped; Dependabot; `npm audit` clean or exceptions documented; docs reviewed against code. |
| 7. Final | Timed clean-clone walkthrough; every README claim linked to evidence; real screenshots; labelled recorded demo; static showcase labelled static; case study, LinkedIn, résumé review; final limitations. |

Phase 1 is limited to infrastructure, health and status, CI, tests and foundations (refinement 5).

## 12. Risks

| Risk | Mitigation |
|---|---|
| Spring Boot 4 ecosystem is new (Jackson 3, Spring Kafka 4, Testcontainers 2) | Phase 1 compatibility check; fallback 4.0.x; hand-written OpenAPI if springdoc lags |
| Batch processing commits offsets for rolled-back records | Record mode first; batch falls back per record on failure; same crash suite for both |
| Long blocking retries exceed `max.poll.interval` | Transient retries < 10 s; capped infrastructure backoff; verified in Phase 3 |
| Flaky asynchronous tests | No sleeps; Awaitility on database state; fixed seeds; per-test groups |
| Memory pressure on an 11.7 GB-usable laptop | Shared test containers; sequential runs; stop other stacks; close heavy apps |
| Windows specifics | `.gitattributes`, both wrapper scripts, Linux CI |
| Single-laptop benchmark noise | Repetitions, variance, fixed power mode, single-host label |
| Over-claiming | Claims-to-tests table; banned phrase "exactly-once delivery" |
| Hot keys / skew | Near-uniform users by default; skew as an explicit scenario |
| Disk growth | Size retention, bench topic deletion, dropped partitions |
| Visual overlap with CloudPulse | Distinct visual identity around partitions, lag and reconciliation |
| Dashboard scope creep | Six fixed sections; cut polish before correctness |
