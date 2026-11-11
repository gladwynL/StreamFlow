# ADR-0006: Three failure categories; retries in place

- **Status:** Accepted, 2026-11-10
- **Implemented:** Phase 3 (2026-11-11)

## Context

A two-way retryable/non-retryable split dead-letters every event while the database is down.
Retry topics would break per-key ordering.

## Decision

| Category | Handling |
|---|---|
| Bad record | Dead-letter on the first attempt |
| Record-scoped transient (`40001`, `40P01`, `55P03`) | Up to 3 attempts in place, under 10 s total, then dead-letter |
| Infrastructure unavailable (class `08`, pool timeout) | Never dead-lettered; unlimited backoff capped at 10 s; partition stalls; health DOWN |

Retries are blocking, in place, through Spring Kafka's `DefaultErrorHandler` with a per-exception
back-off. Non-blocking retry topics are rejected.

## Consequences

- A database outage pauses processing; Kafka holds the backlog (bounded by ADR-0008).
- The retry counter is per partition assignment; a crash during retries resets it.

## Verification

Classifier unit tests; transient fault that clears; persistent fault dead-lettered after exactly 3
attempts; PostgreSQL container paused under load until infrastructure failures are observed, with zero dead letters and exact
reconciliation.

## Implementation notes (Phase 3)

- `FailureClassifier` walks the cause chain. Unrecognized failures are classified as transient: retried a bounded
  number of times, then dead-lettered rather than retried forever (an unknown bug in one record must not stall a
  partition indefinitely).
- Defaults: transient 3 attempts (200 ms, doubling, max 2 s); infrastructure unlimited (500 ms, doubling, capped at
  10 s). Configurable under `streamflow.worker.retry`.
- Retry counts are kept by Spring Kafka in memory per partition assignment; a restart or rebalance during retries
  restarts the count (documented in `docs/reliability.md`).
- A JDBC socket timeout (30 s) turns an unresponsive database into connection errors (infrastructure) instead of a
  consumer thread blocked indefinitely.
- Metrics: `streamflow.processing.failures{category}`, `streamflow.infrastructure.outage`,
  `streamflow.infrastructure.recoveries`, `streamflow.consumer.rebalances`.
