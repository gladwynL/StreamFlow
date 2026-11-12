# Spring Kafka consumer behaviour: decision and verification

Phase 0 described the consumer twice in different words: "offsets are committed after the poll batch
(`AckMode.BATCH`)" and "record mode first, batch mode in Phase 5". These are two different settings and they do not
conflict. This document fixes the meaning, records the decisions, and lists what the integration tests showed.

## Two independent settings

| Setting | Options | What it controls |
|---|---|---|
| **Listener type** | record listener / batch listener | How many records the application processes per call, and therefore per **database transaction** |
| **Ack mode** | `RECORD`, `BATCH`, `MANUAL`, … | When the container **commits Kafka offsets** |

"Record mode" means a *record listener* with one database transaction per record. "Batch mode" (Phase 5) means a
*batch listener* with one database transaction per poll. Either can be combined with `AckMode.BATCH`.

## Decision

**Record listener + one database transaction per record + `AckMode.BATCH`, auto-commit disabled,
`auto.offset.reset=none`.**

- Each record is decoded, validated and written (ledger, event row, aggregates, run statistics) in its own
  transaction, committed by `TransactionTemplate` before the listener method returns.
- The container commits offsets after every record returned by a `poll()` has been handled. A record's offset is
  committed strictly after its database commit, or after its dead-letter record is acknowledged.
- A crash can redeliver up to one poll's worth of already-committed records per consumer (`max.poll.records` = 500).
  The deduplication ledger suppresses them.

`AckMode.RECORD` was rejected (a synchronous broker round-trip per record for a redelivery window deduplication
already makes harmless). Kafka transactions were rejected in Phase 0 (they cannot include PostgreSQL).

## Failure handling (Phase 3)

`DefaultErrorHandler` with a back-off chosen per attempt by the failure category (`FailureClassifier`):

| Category | Back-off | After the last attempt |
|---|---|---|
| Bad record | none (`FixedBackOff(0, 0)`) | dead-lettered |
| Transient | 200 ms, doubling, max 2 s; 3 attempts in total | dead-lettered |
| Infrastructure | 500 ms, doubling, capped at 10 s, unlimited | never — the partition waits |

Dead letters are published by `DeadLetterPublishingRecoverer` to the same partition number of the `.dlt` topic,
synchronously (`acks=all`, wait up to 15 s, fail on error), with `sf-failure-category` and `sf-delivery-attempts`
added and stack traces excluded. `setResetStateOnExceptionChange(true)` restarts the attempt count when the category
changes (for example a transient failure followed by an outage).

## Verified behaviour

Observed on 2026-11-11 with Spring for Apache Kafka 4.1.2, kafka-clients 4.2.2, Kafka 4.3.2 and PostgreSQL 18.6
(Testcontainers), 3 consumer threads per worker, 6 partitions. The "Observed" column comes from actual runs.

| # | Question (Phase 0 §9) | Test | Observed |
|---|---|---|---|
| 1 | When record *k* of a poll fails, are offsets of the records before *k* committed before the seek? | Phase 2 `PartitionStallIT` | **Yes.** With the bad record at offset 5, the committed offset was 5 (records 0–4 committed, position kept on the bad record). Correctness does not depend on it. |
| 2 | Is anything after *k* on the same partition processed before *k* is resolved? | `DeadLetterPublishFailureIT` | **No.** While *k* could not be dead-lettered, the record after it stayed unprocessed and the committed offset stayed at *k*. |
| 3 | If dead-letter publication fails, is the record redelivered without committing, and how does `resetStateOnRecoveryFailure` behave? | `DeadLetterPublishFailureIT` | **Redelivered, never committed.** The broker rejected the dead-letter record 3 times (topic `max.message.bytes` below the record size); the committed offset stayed 0 and nothing after the record ran. With the default `resetStateOnRecoveryFailure=true` the record is re-attempted and recovery retried on each redelivery. When the topic accepted records again, the dead letter was published once, the committed offset moved to 2 and the next record was processed. |
| 4 | Is the recovered record's offset committed only after the dead-letter acknowledgement? | `DeadLetterPublishFailureIT`, `CrashRecoveryIT` | **Yes.** No commit while publication failed (3). Halting the JVM right after the acknowledgement left the dead letter in the topic with the source offset still uncommitted; after restart the record was dead-lettered again (2 dead-letter records) and stored once in PostgreSQL. |
| 5 | Is the per-exception back-off applied, and does a long unlimited back-off avoid exceeding `max.poll.interval.ms`? | `RetryIT`, `DatabaseOutageIT` | **Yes.** Transient: exactly 2 failed attempts per record that succeeds on attempt 3, 3 per exhausted record. Infrastructure: PostgreSQL frozen mid-run — 6 infrastructure failures, 0 dead letters, **0 rebalances** during the outage, automatic recovery, 20,000 events each applied once (reconciliation PASS). Duplicate deliveries were 0 in some runs and 2 in another — consistent with commits that reached PostgreSQL while their acknowledgement was lost to the socket timeout; the retries were suppressed by the ledger. |
| 6 | Is the database commit complete before the offset commit? | `CrashRecoveryIT` | **Yes.** Halted right after the commit of a record: in the ledger, committed offset still 0; ~1,000 redeliveries suppressed; PASS with each of 3,000 events applied once. Halted before the commit: not in the ledger, processed once after restart, PASS. |
| 7 | Batch mode: does `BatchListenerFailedException(index)` commit only offsets whose effects are committed? | `BatchProcessingIT`, reliability suite in both modes | **Yes.** With an undecodable record at position 7 of a 30-record batch, the fallback committed records 0–6 one by one; halting the JVM right after the seventh commit left the committed offset at 0 (nothing committed before the listener returned). A restarted worker suppressed the 7 redelivered records as duplicates, dead-lettered the bad record once (offset 7, `BAD_RECORD`) and committed the partition to its end (spring-kafka 4.1.2). |
| 8 | Does a graceful shutdown commit offsets of processed records? | `WorkerRestartIT` | **Yes.** 0 duplicate deliveries. |
| 9 | Are offsets committed when partitions are revoked during a rebalance? | `WorkerRestartIT` | **Yes.** 30,000 events, 0 duplicate deliveries, PASS. |
| 10 | What happens with `auto.offset.reset=none` when the committed position was deleted by retention? | `OffsetOutOfRangeIT` | **Reported, not skipped.** After `deleteRecords` moved the log start past the committed offset, the client logged that the fetch position "is out of range … raising error to the application since no reset policy is configured"; the container passed it to the error handler (counted as `streamflow.consumer.errors`), the committed offset stayed 0 and none of the surviving records were consumed by skipping ahead. |

Item 7 (batch listener) belongs to Phase 5.

### Findings

- **Head-of-line blocking (Phase 2, resolved).** With unlimited retries for every failure, a stalled partition also
  blocked the other partition owned by the same consumer thread (0 or 2 of 5 records processed). Phase 3
  dead-letters bad records on the first attempt, so the thread moves on; `DeadLetterIT` verifies that a partition
  sharing the thread with a bad record is fully processed. An infrastructure outage still pauses every partition, by
  design.
- **Retry counts restart on reassignment, observed in CI.** In the first CI run of `RetryIT` the run was published
  before the worker's three consumers had all joined. A rebalance during the joins revoked a partition while one
  record was between retries; after reassignment its attempt count started again at 1, so it failed 2 more times
  than the per-record arithmetic predicts (264 transient failures instead of 262). Every outcome was still correct
  (processed, or dead-lettered with `attempts = 3`; reconciliation PASS). This is the documented per-assignment
  semantics; the test now publishes only after all partitions are assigned and asserts that no rebalance occurred,
  so its exact count is meaningful. The same start-up effect showed up once in `DatabaseOutageIT` (2 revocations
  while the third consumer was still joining, before the outage began); it now also measures only after every
  consumer owns its partitions.
- **`deliveryAttempt()` during recovery reports the next attempt.** While the recoverer runs, the error handler's
  `deliveryAttempt(tpo)` returns attempts made + 1 (2 for a bad record that failed once, 4 for a record exhausted
  after 3). The `sf-delivery-attempts` header therefore records `deliveryAttempt − 1`; `DeadLetterIT` and `RetryIT`
  assert 1 and 3.
- **The container's `kafka_deliveryAttempt` header is binary and is copied into dead letters.** With
  `deliveryAttemptHeader=true` it is a 4-byte integer. Decoding it as text put NUL characters into the stored JSON,
  which PostgreSQL rejects; ingestion retried indefinitely and stalled until fixed. Non-printable header values are
  now stored as `base64:…` (`DeadLetterIngestionTest`), and ingestion failures are logged and counted
  (`streamflow.deadletters.ingestion.failures`) so such a stall is visible.

### Findings (Phase 5, batch mode)

- **The batch container clears the error handler's failure state after a successful batch.** spring-kafka 4.1.2's
  `ListenerConsumer.doInvokeBatchListener` calls `CommonErrorHandler.clearThreadState()` when a batch succeeds after
  a failed one; record listeners never do before the consumer thread ends. The successful batch may contain only
  other partitions' records, so a record still being retried restarted its attempt count: `RetryIT` in batch mode
  saw a record fail attempts 1, 2, 1, 2 (264 failed attempts instead of 262). The event error handler now keeps its
  state (`clearThreadState()` is a no-op), giving batch mode record mode's semantics: one entry per partition,
  replaced when another record of the partition fails, removed when the record is recovered. `RetryIT` passes in
  both modes.
- **A batch failure that is not a `BatchListenerFailedException` pauses the consumer (Phase 6).** After a broker
  outage the offset commit following a successful batch failed (`CommitFailedException`, "the coordinator is not
  aware of this member"). spring-kafka handled it with `ErrorHandlingUtils.retryBatch`, which pauses the consumer's
  whole assignment while retrying in memory; the outage's rebalance then left the reassigned partitions unconsumed
  (`KafkaOutageIT` in batch mode: lag stayed above zero for 180 s). The event error handler now reports such failures
  as a failure of the batch's first record, so the batch is sought back and redelivered (duplicates suppressed by the
  ledger), and Kafka commit and retriable errors are classified as infrastructure (never dead-lettered).
- **The recoverer receives the `BatchListenerFailedException`, not its cause.** Dead letters recorded the wrapper as
  their exception class (`DeadLetterIT` in batch mode); the recoverer now unwraps it.
