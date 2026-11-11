# Spring Kafka consumer behaviour: decision and verification

Phase 0 described the consumer twice in different words: "offsets are committed after the poll
batch (`AckMode.BATCH`)" and "record mode first, batch mode in Phase 5". These are two different
settings and they do not conflict. This document fixes the meaning, records the decision made
before Phase 2 implementation, and lists what the integration tests showed.

## Two independent settings

| Setting | Options | What it controls |
|---|---|---|
| **Listener type** | record listener / batch listener | How many records the application processes per call, and therefore per **database transaction** |
| **Ack mode** | `RECORD`, `BATCH`, `MANUAL`, … | When the container **commits Kafka offsets** |

"Record mode" in the roadmap means a *record listener* with one database transaction per record.
"Batch mode" (Phase 5) means a *batch listener* with one database transaction per poll. Either
listener type can be combined with `AckMode.BATCH`.

## Decision (Phase 2)

**Record listener + one database transaction per record + `AckMode.BATCH`, auto-commit disabled.**

- Each record is decoded, validated and written (ledger, event row, aggregates, run statistics) in
  its own transaction, committed by `TransactionTemplate` before the listener method returns.
- The container commits offsets once, after every record returned by a `poll()` has been
  processed. A record's offset is therefore committed strictly after its database commit.
- Consequence: a crash can redeliver up to one poll's worth of already-committed records per
  consumer (`max.poll.records` = 500). The deduplication ledger suppresses them. This is the intended
  trade-off: one offset commit per poll instead of one per record.

`AckMode.RECORD` (commit after every record) was rejected: it adds a synchronous broker
round-trip per record to shrink a redelivery window that deduplication already makes harmless.
Kafka transactions were rejected in Phase 0 (they cannot include PostgreSQL).

**Phase 2 failure behaviour (temporary):** there is no retry classification or dead-letter topic
until Phase 3. Any processing exception is retried in place with exponential back-off (0.5 s,
doubling, capped at 10 s) without limit. A record that can never be processed therefore **stalls
its partition rather than being skipped** — no silent loss. See the head-of-line finding below.

## Verified behaviour

Observed on 2026-11-11 with Spring for Apache Kafka 4.1.2, kafka-clients 4.2.2, Kafka 4.3.2 and
PostgreSQL 18.6 (Testcontainers), 3 consumer threads per worker, 6 partitions.

| # | Question | Test | Observed |
|---|---|---|---|
| 1 | When record *k* of a poll fails, are offsets of the records before *k* committed before the seek? | `PartitionStallIT` | **Yes.** The bad record sat at offset 5; the committed offset of its partition was 5, i.e. records 0–4 were committed and the position stayed on the bad record. Correctness does not depend on this (the ledger suppresses redelivery either way). |
| 2 | After a failure, is anything after *k* on the same partition processed before *k* succeeds? | `PartitionStallIT` | **No.** The five records after the bad one stayed unprocessed through repeated retry cycles, and the committed offset never moved past the bad record. |
| 6 | Is the database commit complete before the offset commit? | `CrashRecoveryIT` | **Yes.** JVM halted right after the database commit of record p4@332: the record was in the ledger while the partition's committed offset was still 0. After restart all 1,000 already-processed records were redelivered and suppressed; the run reconciled PASS with 3,000 events, each applied once. Halting before the commit (record p0@334): the record was not in the ledger, was processed once after restart, and the run reconciled PASS. |
| 8 | Does a graceful shutdown commit offsets of processed records? | `WorkerRestartIT` | **Yes.** A worker closed mid-run caused 0 duplicate deliveries. |
| 9 | Are offsets committed when partitions are revoked during a rebalance? | `WorkerRestartIT` | **Yes.** A second worker joined mid-run (partitions revoked and reassigned): 30,000 events processed, 0 duplicate deliveries, reconciliation PASS. |

Items 3–5, 7 and 10 of Phase 0 §9 belong to Phases 3 and 5.

### Finding: head-of-line blocking within a consumer thread

`PartitionStallIT` also showed that a stalled partition blocks **every partition owned by the same
consumer thread**, not only its own. With three threads over six partitions each thread owns two;
while one thread retried the bad record on partition 0, its other partition (1) processed only 0 of
5 records in one run and 2 of 5 in another (depending on what the first poll returned) and then
stopped, while a partition owned by another thread (2) processed all of them. The cause: after each
failure the error handler seeks back, the next poll returns the bad record again before the other
partition's records are reached, and the back-off sleeps on the shared consumer thread.

No record is lost or skipped, but throughput of the co-assigned partition stops. Phase 3 resolves
this for bad records (they are dead-lettered after classification, so the thread moves on). During
an infrastructure outage (database down) every partition stalls by design anyway.
