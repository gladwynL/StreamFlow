# ADR-0012: Local-first, memory-bounded infrastructure

- **Status:** Accepted, 2026-11-10
- **Implemented:** Phase 1

## Context

Development happens on a Windows laptop with 11.7 GB of usable memory and Docker Desktop's WSL VM
capped at about 5.6 GiB. No paid cloud is used.

## Decision

- Docker Compose with profiles: infrastructure only by default; `--profile app` for the full stack.
- Explicit memory limits per container; JVMs sized by `MaxRAMPercentage`; Kafka heap ≤ 512 MB.
- No Prometheus, Grafana, Redis, Kubernetes or Kafka admin UI.
- Every published port binds to 127.0.0.1; containers run as non-root with capabilities dropped and
  read-only root filesystems where the image allows.
- Integration tests share one Kafka and one PostgreSQL container per test JVM.
- Benchmarks and integration tests run with other heavy applications and stacks stopped.

## Consequences

Durability against broker loss and multi-host scaling are out of scope; results are labelled
single-host. Memory pressure on the host is a real risk and is documented in the runbook.

## Verification

Measured container memory and start-up time are recorded in `docs/runbook.md`; the Compose smoke
test runs in CI.
