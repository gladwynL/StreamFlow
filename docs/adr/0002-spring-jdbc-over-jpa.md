# ADR-0002: Spring JDBC instead of JPA

- **Status:** Accepted, 2026-11-10
- **Implemented:** Phase 1 (heartbeats, status); event processing from Phase 2

## Context

The core write path is an idempotent insert into a deduplication ledger followed by aggregate
upserts, all in one transaction, at high message rates.

## Decision

Use Spring JDBC (`JdbcClient`) with explicit SQL. Do not use JPA.

## Consequences

- `INSERT … ON CONFLICT DO NOTHING RETURNING` and `ON CONFLICT DO UPDATE` are written directly.
- No persistence context or dirty checking per message; batching is under our control.
- Transaction and locking behaviour is visible in the code.
- Row mapping is written by hand; acceptable for a small schema.

## Verification

Repository behaviour is tested against real PostgreSQL 18 with Testcontainers, including (from
Phase 2) a concurrent same-event race that must produce exactly one effect.
