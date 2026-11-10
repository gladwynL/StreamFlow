# ADR-0010: Deterministic content, unique identity

- **Status:** Accepted, 2026-11-10
- **Implemented:** Phase 2

## Context

Demonstrations and benchmarks must be reproducible, but reusing event ids across runs would make
the deduplication ledger suppress a second run entirely.

## Decision

A seed reproduces normalized content (type, user, session, payload, fault directive, sequence
number, scheduled offset). Identity is unique per run: random `runId`, UUIDv7 `eventId` from time
plus a secure random source with a monotonic in-millisecond counter, and `occurredAt` = actual run
start + scheduled offset. Content is generated on one thread with `SplittableRandom(seed)`; event
*n* depends only on (seed, configuration, `contentVersion`, *n*).

## Consequences

Changing generator logic requires bumping `contentVersion`. Processing order and timing remain
non-deterministic; benchmarks are statistically, not bit-for-bit, repeatable.

## Verification

SHA-256 over 100,000 normalized events: equal across runs and JVMs for the same seed, different
for a different seed; two same-seed runs share no event ids or timestamps.
