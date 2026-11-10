# ADR-0004: At-least-once delivery with database-enforced idempotency

- **Status:** Accepted, 2026-11-10
- **Implemented:** Phase 2

## Context

Kafka redelivers after crashes and rebalances. Kafka transactions cannot include PostgreSQL, so
end-to-end exactly-once delivery is not available.

## Decision

Deliver at least once. Make effects duplicate-free in PostgreSQL: each event id is inserted into a
ledger (`processed_events`, primary key `event_id`) in the same transaction as its effects; a
conflict means "already processed" and the effects are skipped.

## Consequences

- The guarantee holds while event ids are stable and redelivery or replay happens within the ledger
  retention window (ADR-0008).
- The phrase "exactly-once delivery" is not used in any public claim.
- Suppressed duplicates are counted as deliveries, never as events.

## Verification

Double-delivery test (1,000 events sent twice → 1,000 rows, 1,000 suppressed), concurrent same-id
race against real PostgreSQL, and the crash tests in ADR-0005.
