# Reliability: claims and the tests that prove them

Every reliability claim StreamFlow makes is listed here with the automated test that verifies it
against real Kafka 4.3 and PostgreSQL 18 (Testcontainers). A claim without a passing test is not made.
Design rationale: [ADR-0004](adr/0004-delivery-semantics.md), [ADR-0005](adr/0005-offset-commit-after-db-commit.md),
[ADR-0006](adr/0006-failure-classification.md), [ADR-0007](adr/0007-dead-letters-and-replay.md),
[ADR-0008](adr/0008-retention-and-backlog-window.md), [ADR-0009](adr/0009-run-reconciliation.md).
Observed values from the runs: [`verification/spring-kafka-behavior.md`](verification/spring-kafka-behavior.md).

## Delivery model

**At-least-once delivery with duplicate-free effects in PostgreSQL.** Each event's effects (event row, minute
aggregate, run statistics) are written in the same transaction as its row in the deduplication ledger
(`processed_events`, primary key `event_id`). A redelivered or replayed event conflicts on the ledger and changes
nothing except the duplicate-delivery counter. Kafka offsets are committed only after the database transactions of
their records, or — for a record that is dead-lettered — after the dead-letter record is acknowledged.

**Every failed record ends in exactly one of three places:** processed after a retry, stored as a dead letter, or
still waiting (infrastructure outage). None is skipped. StreamFlow does **not** claim exactly-once delivery and uses
no Kafka transactions.

## Processing and delivery (Phase 2)

| # | Claim | Verified by |
|---|---|---|
| 1 | A seeded run publishes, processes and reconciles: every acknowledged event is in the ledger exactly once, with one event row and one aggregate increment | `GenerateRoleIT` |
| 2 | The same seed reproduces identical event content (SHA-256 over 100,000 events); runs never share event ids or timestamps | `EventContentGeneratorTest`, `GenerateRoleIT` |
| 3 | An event delivered twice takes effect once; the second delivery is counted as a suppressed duplicate | `DuplicateDeliveryIT` |
| 4 | Concurrent processing of the same event by several transactions applies it exactly once | `ConcurrentProcessingIT` |
| 5 | A worker crash **before** the database commit loses nothing; the event is processed once after restart | `CrashRecoveryIT` |
| 6 | A worker crash **after** the database commit and before the offset commit duplicates nothing | `CrashRecoveryIT` |
| 7 | Workers joining (rebalance) and leaving gracefully lose nothing and cause no redelivery | `WorkerRestartIT` |
| 8 | Every role refuses to start if ledger retention is shorter than the longest redelivery or replay window plus a margin | `RetentionPolicyCheckTest` |
| 9 | The processing and ingestion groups have a committed offset on every partition from the start | `MigrateRoleIT` |

## Failure handling and recovery (Phase 3)

| # | Claim | Verified by |
|---|---|---|
| 10 | Failures are classified as bad record, transient, or infrastructure; unknown failures are treated as transient (bounded, then dead-lettered) | `FailureClassifierTest` |
| 11 | A bad record (undecodable or invalid) is dead-lettered on its first attempt with its original topic, partition, offset, key, value bytes, identity headers, exception class, message and category; processing of the partition continues, and partitions sharing its consumer thread are not blocked | `DeadLetterIT` |
| 12 | A transient failure is retried in place and succeeds without a dead letter; one that persists is dead-lettered after exactly the configured attempts (3) as TRANSIENT | `RetryIT`, `RetryPolicyTest` |
| 13 | A PostgreSQL outage produces no dead letters: processing pauses with capped back-off, the consumer stays in its group (no rebalance), and processing resumes by itself; the run reconciles with every event applied once | `DatabaseOutageIT` |
| 14 | If dead-letter publication fails, the source offset is not committed and nothing after the record is processed; once publication succeeds the dead letter is published once and the partition moves on | `DeadLetterPublishFailureIT` |
| 15 | A crash after the dead-letter acknowledgement and before the source offset commit republishes the dead letter, but PostgreSQL stores it once | `CrashRecoveryIT` |
| 16 | Replaying a dead letter applies its event once; a REPLAYED, DISCARDED or EXPIRED dead letter cannot be replayed; a second publication of the same event is suppressed by the ledger | `ReplayIT` |
| 17 | Concurrent replay requests for the same dead letters replay each exactly once, without deadlock | `ReplayIT` |
| 18 | A replay interrupted by an api crash between publication and the REPLAYED update is recovered: the claim survives, the sweeper releases it, and replaying again leaves one effect | `ReplayCrashIT` |
| 19 | Dead letters older than the 7-day replay window are rejected for replay and become EXPIRED | `ReplayIT`, `RetentionIT` |
| 20 | Retention deletes ledger rows only outside the 8-day window and only for runs with a final reconciliation; never deletes unresolved dead letters; drops event partitions and aggregates after their retention | `RetentionIT` |
| 21 | Reconciliation counts processed, replayed, unresolved, discarded and ambiguous events once each; an open replay keeps a run INCOMPLETE; missing and unexpected events make it FAIL | `ReplayIT`, `ReconciliationIT`, `ReconciliationReportTest` |
| 22 | A position deleted by retention (out of range) is reported, and the consumer never skips to the new log start | `OffsetOutOfRangeIT` |
| 23 | Replay and discard require the operator key (503 when not configured, 401 when wrong) and are rate-limited (429) | `OperatorGuardTest` |
| 24 | Header values that are not printable UTF-8 are stored as Base64, never as text PostgreSQL rejects | `DeadLetterIngestionTest`, `DeadLetterIT` |

## Dashboard and API (Phase 4)

Design rationale: [ADR-0011](adr/0011-dashboard-data-path.md).

| # | Claim | Verified by |
|---|---|---|
| 25 | A WebSocket client that stops reading neither delays the snapshot tick nor other clients; it holds at most one snapshot and is disconnected after 5 skipped ticks; a client whose send fails is dropped alone | `LiveBroadcasterTest` |
| 26 | With a connected client that never reads, a run is still published, processed and reconciled as PASS while a healthy client keeps receiving about one snapshot per second | `DashboardApiIT` |
| 27 | The live feed pushes about one snapshot per second, refuses origins other than localhost and 127.0.0.1, and refuses clients beyond the limit (close code 1013) | `DashboardApiIT`, `LiveBroadcasterTest` |
| 28 | Starting a simulation requires the operator key (401 without or with a wrong key) and is bounded (500,000 events, 5,000/s, 600 s, 100,000 users); a second run cannot start while one is in progress (409) | `DashboardApiIT`, `SimulationControllerTest` |
| 29 | A run started over the API reconciles as PASS and reports its acknowledged, processed and per-type counts exactly; its events page by keyset cursor without gaps or repeats | `DashboardApiIT` |
| 30 | Latency percentiles are the upper bound of the histogram bucket that contains them; the SQL bucketing matches `LatencyBuckets` | `LatencyBucketsTest`, `LatencyHistogramIT` |
| 31 | The dashboard shows only values from the API: unmeasured values render as a dash, and the operator key is held in memory only and sent only in the request header | Frontend tests (`OverviewPage`, `OperatorKey`, `DeadLettersPage`, `SimulationsPage`), Playwright `e2e/` |

## Boundaries of the claims

- **Retry counts are per partition assignment.** Spring Kafka keeps attempt counts in memory. A worker restart or
  rebalance during retries starts the count again, so a transient record can be attempted up to
  3 × (number of assignments) times before it is dead-lettered. It is never retried without bound unless it fails
  for infrastructure reasons.
- **Order after dead-lettering.** A dead-lettered event is overtaken by later events of the same key, and a replayed
  event is processed after events published while it was dead-lettered. Per-key order holds only for events processed
  on first delivery. Analytics are order-independent; `occurred_at` is the authoritative event time.
- **Infrastructure outages block the partition.** By design: Kafka holds the backlog. The backlog window (below)
  bounds how long an outage can last without losing data.
- **Single broker, replication factor 1.** Durability of acknowledged events depends on the broker's disk and the OS
  flushing it; losing the disk or the Docker VM before a flush can lose acknowledged events.
- **Backlog window.** Kafka can delete unconsumed records once a partition exceeds 1 GiB (size-based retention), well
  before 24 hours. W = min(24 h, (1 GiB − 128 MiB) ÷ peak per-partition ingress) is measured in Phase 5. Beyond it the
  consumer reports an out-of-range error instead of skipping (claim 22) and reconciliation reports the gap; recovery is
  a deliberate operator step (runbook).
- **Deduplication window.** Duplicates are suppressed while ledger rows exist: at least 8 days, longer for runs that
  were never finally reconciled.
- **Undecodable records without identity headers** cannot be replayed (no event id to deduplicate on); they can only
  be inspected and discarded.
- **Producer buffer.** Events in the producer's memory when the generator crashes were never acknowledged and are
  outside the claims; such a run stays `RUNNING` and can never reconcile as PASS.
