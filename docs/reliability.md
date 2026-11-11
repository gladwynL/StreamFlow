# Reliability: claims and the tests that prove them

Every reliability claim StreamFlow makes is listed here with the automated test that verifies it
against real Kafka and PostgreSQL. A claim without a passing test is not made. Design rationale:
[ADR-0004](adr/0004-delivery-semantics.md), [ADR-0005](adr/0005-offset-commit-after-db-commit.md),
[ADR-0008](adr/0008-retention-and-backlog-window.md), [ADR-0009](adr/0009-run-reconciliation.md).

## Delivery model

**At-least-once delivery with duplicate-free effects in PostgreSQL.** Each event's effects (event
row, minute aggregate, run statistics) are written in the same transaction as its row in the
deduplication ledger (`processed_events`, primary key `event_id`). A redelivered event conflicts on
the ledger and changes nothing except the duplicate-delivery counter. Kafka offsets are committed
only after the database transactions of their records have committed.

StreamFlow does **not** claim exactly-once delivery.

## Claims (Phase 2)

| # | Claim | Verified by |
|---|---|---|
| 1 | A seeded run publishes, processes and reconciles: every acknowledged event is in the ledger exactly once, with one event row and one aggregate increment | `GenerateRoleIT` |
| 2 | The same seed reproduces identical event content (SHA-256 over 100,000 events); runs never share event ids or timestamps | `EventContentGeneratorTest`, `GenerateRoleIT` |
| 3 | An event delivered twice takes effect once; the second delivery is counted as a suppressed duplicate | `DuplicateDeliveryIT` |
| 4 | Concurrent processing of the same event by several transactions applies it exactly once | `ConcurrentProcessingIT` |
| 5 | A worker crash **before** the database commit loses nothing: the transaction rolls back, the offset is not committed, and the event is processed once after restart | `CrashRecoveryIT` |
| 6 | A worker crash **after** the database commit and before the offset commit duplicates nothing: the record is redelivered and suppressed by the ledger | `CrashRecoveryIT` |
| 7 | Workers joining (rebalance) and leaving gracefully lose nothing and cause no redelivery | `WorkerRestartIT` |
| 8 | A record that can never be processed stalls its partition; it is never skipped, and partitions owned by other consumer threads keep flowing (Phase 2 behaviour until the dead-letter topic in Phase 3; partitions on the *same* thread are blocked too — see the head-of-line finding) | `PartitionStallIT` |
| 9 | Reconciliation distinguishes PASS, FAIL and INCOMPLETE; a run whose generator did not finish, or whose topic has not drained, can never pass | `ReconciliationReportTest` |
| 10 | Every role refuses to start if ledger retention is shorter than the longest redelivery or replay window plus a margin | `RetentionPolicyCheckTest` |
| 11 | The processing group has a committed offset on every partition from the start, so `auto.offset.reset=none` never silently skips data | `MigrateRoleIT` |

Observed values from the test runs (offsets, duplicate counts) are recorded in
[`verification/spring-kafka-behavior.md`](verification/spring-kafka-behavior.md).

## Boundaries of the claims

- **Single broker, replication factor 1.** Durability of acknowledged events depends on the broker's
  disk and the OS flushing it. Losing the disk, or a crash of the Docker VM or host before a flush,
  can lose acknowledged events. Broker replication is out of scope for the local deployment.
- **Backlog window.** Kafka can delete unconsumed records once a partition exceeds 1 GiB
  (size-based retention), possibly well before the 24-hour time limit. The window W = min(24 h,
  (1 GiB − 128 MiB) ÷ peak per-partition ingress) is measured in Phase 5. Beyond it, events can be
  lost; the consumer then fails loudly (`auto.offset.reset=none`) and reconciliation reports the gap.
- **Deduplication window.** Duplicates are suppressed while ledger rows exist (8 days). Ledger
  retention jobs arrive in Phase 3; until then ledger rows are never deleted.
- **Producer buffer.** Events in the producer's memory when the generator crashes were never
  acknowledged and are outside the claims; the run stays `RUNNING` and can never reconcile as PASS.
- **Phase 2 failure handling.** A record that can never be processed blocks its partition — and
  every other partition owned by the same consumer thread — indefinitely. Phase 3 replaces this with
  classified retries and the dead-letter topic.
