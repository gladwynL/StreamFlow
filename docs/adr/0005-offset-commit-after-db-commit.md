# ADR-0005: Commit offsets after the database commit; no Kafka transactions

- **Status:** Accepted, 2026-11-10
- **Implemented:** Phase 2 (record mode); Phase 5 (batch mode)

## Context

The order of the database commit and the Kafka offset commit decides what a crash can do.

## Decision

- Auto-commit disabled; `AckMode.BATCH`; the listener runs each database transaction with
  `TransactionTemplate`, which commits before the listener returns.
- No Kafka transaction manager.
- Batch mode (Phase 5) falls back to per-record processing when a batch fails, so offsets are never
  committed for records whose effects were rolled back.

## Consequences

- Crash before the database commit: rollback, redelivery, no loss.
- Crash after the database commit, before the offset commit: redelivery, suppressed by the ledger.
- Spring Kafka's exact batch and partial-failure behaviour is verified by tests, not assumed
  (Phase 0 §9).

## Verification

Crash tests stop a real worker JVM with `Runtime.halt()` at named points (before the database
commit; after it and before the offset commit) and check for zero loss and zero duplicate effects.
