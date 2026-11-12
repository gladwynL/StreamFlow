# ADR-0013: Batch processing mode

- **Status:** Accepted, 2026-11-12 (Phase 5); batch is the default processing mode
- **Implemented:** Phase 5

## Context

Phase 5 profiling (docs/benchmarks/README.md) showed that record mode is limited by per-record synchronous latency:
each record pays one database commit (a WAL flush) and several statement round trips, and the consumer threads
spend almost all of their time waiting for PostgreSQL, while neither PostgreSQL nor the worker is CPU-bound. The
Phase 0 plan (§9 item 7, §12) foresaw a batch alternative, provided it keeps every reliability guarantee.

## Decision

A second processing mode, selected per worker with `streamflow.worker.processing-mode` (`RECORD` by default, or
`BATCH`). Record mode stays unchanged as the reference.

In batch mode the listener receives the whole poll batch (up to `max.poll.records`, across the consumer's
partitions) and:

1. decodes and validates every record;
2. in **one transaction**, inserts all events into the ledger with one set-based statement
   (`INSERT … SELECT FROM unnest(…) ON CONFLICT (event_id) DO NOTHING RETURNING event_id`, rows sorted by event id so
   concurrent batches lock in the same order), then writes the effects of exactly the newly inserted events (event
   rows, minute aggregates, latency histogram, run statistics) with one statement per table;
3. if anything fails — a record that cannot be decoded, an injected fault, a constraint violation, a deadlock, a
   lost connection — rolls the batch transaction back and processes the records **one by one** with the record-mode
   processor, each in its own transaction, in offset order. The first record that fails is reported to Spring
   Kafka as `BatchListenerFailedException(index)`.

The container uses the same consumer settings (`AckMode.BATCH`, auto-commit off, `auto.offset.reset=none`) and the
same error handler as record mode. On `BatchListenerFailedException(index)` the handler commits only the offsets of
the records before `index` — each of which has committed its own transaction — and then applies the unchanged
classification to the failed record: bad records are dead-lettered (synchronously, before their offset is
committed), transient failures are retried up to the limit, infrastructure failures are retried without limit.
Delivery attempts (used by fault injection and the dead-letter attempts header) come from that handler's failure
tracker, as in record mode.

## Guarantees

| Guarantee | How batch mode keeps it |
|---|---|
| Database-enforced idempotency | Same ledger primary key; only events newly inserted into the ledger get effects; repeats within and across batches are counted as duplicates |
| No offset committed before its effects are durable | Offsets are committed by the container only after the listener returns (whole batch committed) or, on a failure, only for the records before the failed one, which committed individually |
| Partial-batch failure | The batch transaction is all-or-nothing; the fallback commits record by record and stops at the first failure |
| Retry and dead-letter semantics | Identical to record mode for the failed record (same handler, classifier and recoverer) |
| Crash safety | A crash before the batch commit leaves nothing applied and nothing committed; a crash after it causes redelivery, absorbed by the ledger |

## Verification

- `BatchProcessingIT`: idempotency within and across batches; a bad record in the middle (index reported, records
  before it committed, nothing after it applied); a failure inside the batch transaction rolls the whole batch back
  before the fallback; concurrent overlapping batches apply each event once; a crash right after the fallback has
  committed the records before a bad record (Phase 0 §9.7), recovered with no loss and no double effect.
- The reliability suite runs a second time with workers in batch mode (failsafe execution `batch-mode-it`):
  crash before/after commit and after dead-letter publication, duplicate delivery, database outage, dead letters,
  dead-letter publication failure, retries, replay, reconciliation, worker restart, out-of-range offsets and the
  generate role.

## Consequences

- One poll batch shares one database transaction and one commit, so the commit and round-trip cost is amortised.
  Measured effect: docs/benchmarks/README.md.
- A single failing record makes its batch fall back to record mode, so throughput under frequent failures
  approaches record mode; correctness does not change.
- Processing latency includes the time to accumulate a poll batch; at low traffic batches are small.
- Measured (docs/benchmarks/README.md): 15,548 events/s against 941 in record mode with one worker × 3 threads,
  latency equal or better at 250–750 events/s offered. On that evidence and with the reliability suite passing in
  both modes, batch is the default; record mode remains available (`STREAMFLOW_WORKER_PROCESSING_MODE=RECORD`) and
  tested.
- Spring Kafka specifics found while verifying it (failure state cleared after a successful batch, wrapped exception
  given to the recoverer) are documented in docs/verification/spring-kafka-behavior.md.
