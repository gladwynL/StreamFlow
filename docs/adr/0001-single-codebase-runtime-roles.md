# ADR-0001: One codebase, separate runtime roles

- **Status:** Accepted, 2026-11-10
- **Implemented:** Phase 1 (`migrate`, `api`, `worker`); `loadgen` in Phase 5

## Context

Workers must be killable and scalable independently of the API, and benchmark load must come from
outside the API process. Separate Spring Boot applications would triple images, configuration and
migration ownership for little benefit at this size.

## Decision

A Maven multi-module build produces one Spring Boot application and one image. Each process runs
exactly one role, chosen by activating the Spring profile of the same name (`migrate`, `api`,
`worker`). Startup fails unless exactly one role profile is active. Only `migrate` changes the
schema; `api` and `worker` validate it and refuse to start against pending migrations.

## Consequences

- One build, one image, one configuration schema.
- Role code is isolated by package; ArchUnit forbids dependencies between role packages and from
  shared packages to roles.
- Phase 0 proposed a `streamflow.role` property; Spring profiles were chosen instead because they
  select role-specific configuration files natively.

## Verification

`RoleTest`, `ArchitectureTest`, and the integration tests, which start each role against real
Kafka and PostgreSQL.
