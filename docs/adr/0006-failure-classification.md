# ADR-0006: Three failure categories; retries in place

- **Status:** Accepted, 2026-11-10
- **Implemented:** Phase 3

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
attempts; PostgreSQL container paused for 20 s under load with zero dead letters and exact
reconciliation.
