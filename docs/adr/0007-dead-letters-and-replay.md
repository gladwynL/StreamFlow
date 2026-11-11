# ADR-0007: Dead-letter handling and database-backed replay

- **Status:** Accepted, 2026-11-10
- **Implemented:** Phase 3 (backend and REST API, 2026-11-11); Phase 4 (dashboard)

## Context

Failed events must never disappear silently, must be inspectable, and must be safely replayable.

## Decision

- Dead-letter publication is synchronous with `acks=all`; the failed record's offset is committed
  only after the acknowledgement. If publication fails, the partition stalls instead of losing it.
- A `dlt-ingestor` consumer group copies dead letters into `dead_letter_events`, unique on the
  original topic, partition and offset.
- Replay is per item: a conditional claim (`PENDING → REPLAY_REQUESTED`), republish of the original
  bytes and key, then `REPLAYED`. A sweeper returns stuck claims to `PENDING`; duplicate republishes
  are suppressed by the ledger because the event id is unchanged.
- Replay is refused after the 7-day window and for records without an event id (discard only).

## Consequences

Replay is crash-safe without distributed transactions. Replayed events land after later events of
the same key; analytics are order-independent.

## Verification

DLT publication failure test; crash after DLT publish before offset commit (one row); replay once,
twice, concurrently and with a crash between publish and mark.

## Implementation notes (Phase 3)

- Publication: Spring's `DeadLetterPublishingRecoverer`, same partition number on the `.dlt` topic, synchronous,
  stack traces excluded, plus `sf-failure-category` and `sf-delivery-attempts` headers.
- Ingestion: consumer group `streamflow-dlt-ingestor`; `dead_letter_events` is unique on the source
  topic/partition/offset. Header values that are not printable UTF-8 are stored as Base64.
- Claiming locks rows in id order (`SELECT … ORDER BY id FOR UPDATE`) so concurrent replay requests cannot deadlock;
  a waiting claim re-checks `status = 'PENDING'` and skips rows another claim took.
- The api's maintenance scheduler releases claims older than 2 minutes every 30 seconds.
- Replay and discard are REST operations (`docs/api.md`) behind a configured operator key and a rate limit;
  inspection is read-only and needs no key. Undecodable records without an event id can only be discarded.
- Unresolved dead letters (PENDING, EXPIRED) are never deleted automatically; REPLAYED and DISCARDED ones are
  deleted 14 days after resolution.
