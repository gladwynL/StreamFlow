# ADR-0003: Kafka topology

- **Status:** Accepted, 2026-11-10
- **Implemented:** Phase 1 (topic provisioning); keys and consumers from Phase 2

## Context

The project needs per-user ordering, room for consumer-scaling experiments on a laptop, and a
broker that fits a tight memory budget.

## Decision

- One Kafka 4.3 broker in KRaft mode (combined broker and controller).
- `streamflow.events.v1`: 6 partitions, key = user id, replication factor 1.
- `streamflow.events.v1.dlt`: 6 partitions (dead letters keep the source partition number).
- Broker auto-creation disabled. The `migrate` role creates missing topics, resets drifted managed
  configs, and fails instead of changing an existing topic's partition count or replication factor.

## Consequences

- Six partitions divide evenly across 1, 2, 3 and 6 consumers.
- A single broker cannot demonstrate durability against broker loss (see ADR-0008).
- Changing partitions on a live topic would remap keys and break ordering, so it is never automatic.

## Verification

`MigrateRoleIT`: topics created with the specified shape and configs; reruns are idempotent; drifted
configs are reset; a partition-count mismatch fails without modifying the topic.
