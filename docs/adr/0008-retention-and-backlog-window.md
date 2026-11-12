# ADR-0008: Retention, the deduplication window and the Kafka backlog window

- **Status:** Accepted, 2026-11-10
- **Implemented:** Phase 1 (topic retention configs); Phase 2–3 (ledger, retention jobs, checks)

## Context

Deduplication only works while ledger entries outlive every way an event can come back. Kafka's
size-based retention can delete unconsumed data early.

## Decision

- Retention: main topic 24 h or 1 GiB per partition; DLT 7 days or 256 MiB per partition; ledger
  8 days; `events` 7 days; aggregates 30 days; dead letters per Phase 0 §6.3.
- The application refuses to start if ledger retention is shorter than the longest redelivery or
  replay window plus a margin.
- Supported backlog window W = min(24 h, (1 GiB − 128 MiB) ÷ peak per-partition ingress bytes/s),
  measured and published in Phase 5 (docs/benchmarks/README.md §8): about 110 bytes per event, so W is about
  14 h at 1,000 events/s. Phase 6: running past W produces a recorded, paused offset gap, never a silent skip.
- The processing group uses `auto.offset.reset=none` so a retention overrun fails loudly instead of
  skipping data.

## Consequences

Public claims are bounded: no loss of acknowledged events while the broker log is intact, backlog
stays within W and ledger retention covers redelivery and replay. A single broker with replication
factor 1 can lose acknowledged events on disk loss or a VM/host crash before flush; this is stated
as a limitation.

## Verification

Topic configs are asserted in `MigrateRoleIT` (Phase 1). Startup invariant check, retention jobs and
an out-of-range test follow in Phases 2–3.

## Implementation notes (Phase 3)

- Hourly in the api role (`RetentionMaintenance`): expire PENDING dead letters past the replay window; delete
  resolved dead letters after 14 days; delete ledger rows older than 8 days **only for runs with a final (PASS or
  FAIL) reconciliation**, in batches of 10,000; drop daily `events` partitions older than 7 days; delete minute
  aggregates older than 30 days. Every step is conditional and safe to run concurrently.
- Reconciliation skips the event-row check for runs older than the events retention, whose partitions may be gone.
- Verified by `RetentionIT` and `OffsetOutOfRangeIT`.
