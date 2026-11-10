# Architecture decision records

Derived from the approved [Phase 0 architecture](../planning/phase-0-architecture.md). "Implemented"
names the phase in which each decision lands; later phases are plans until their phase report says
otherwise.

| ADR | Decision |
|---|---|
| [0001](0001-single-codebase-runtime-roles.md) | One codebase, separate runtime roles |
| [0002](0002-spring-jdbc-over-jpa.md) | Spring JDBC instead of JPA |
| [0003](0003-kafka-topology.md) | Kafka topology |
| [0004](0004-delivery-semantics.md) | At-least-once delivery with database-enforced idempotency |
| [0005](0005-offset-commit-after-db-commit.md) | Commit offsets after the database commit |
| [0006](0006-failure-classification.md) | Three failure categories; retries in place |
| [0007](0007-dead-letters-and-replay.md) | Dead-letter handling and database-backed replay |
| [0008](0008-retention-and-backlog-window.md) | Retention, deduplication window and backlog window |
| [0009](0009-run-reconciliation.md) | Per-run reconciliation |
| [0010](0010-deterministic-simulation.md) | Deterministic content, unique identity |
| [0011](0011-dashboard-data-path.md) | Dashboard data path |
| [0012](0012-local-first-resource-budget.md) | Local-first, memory-bounded infrastructure |
