# ADR-0009: Per-run reconciliation

- **Status:** Accepted, 2026-11-10
- **Implemented:** Phase 2 (core); Phase 3 (dead letters and replay)

## Context

"No silent loss" must be checked on every run, without double counting retries, replays, duplicates
or ambiguous producer outcomes.

## Decision

Every record carries `sf-run-id`, `sf-event-id` and `sf-seq` headers. The generator records each
attempt as `ACKED`, `FAILED_DEFINITE` or `FAILED_AMBIGUOUS` (failures stored by sequence number,
with a cap). Each event has exactly one sink state, by precedence: `PROCESSED` (in the ledger) >
`DISCARDED` > `DEAD_UNRESOLVED`. Duplicates are counted as deliveries. After the run drains, five
checks run (no missing, no unexpected, count identity, no double effects, no reused sequence
numbers). Results — `PASS`, `FAIL` or `INCOMPLETE` — are stored in `run_reconciliations`.

## Consequences

Ambiguous producer outcomes are reported, not hidden and not treated as failures. A run whose
generator crashed can never pass.

## Verification

Reconciliation runs after every integration scenario that produces events and after every
benchmark; its results are part of the benchmark evidence.
