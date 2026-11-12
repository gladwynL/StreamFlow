# Phase 6 hardening checklist

Audit of the repository at `c479799` (end of Phase 5), 2026-11-12. Items are ordered by priority; each has the
acceptance criterion that closes it. Status is updated as items land; the Phase 6 report links the evidence.

## P0 — correctness, data safety, security

| # | Finding | Acceptance criteria |
|---|---|---|
| 1 | **Out-of-range offset error loop.** When retention deletes records a consumer group has not read (`auto.offset.reset=none`), every poll raises the same error: CI logged ~75 identical errors per minute per consumer, with no record of which events were lost and no operator guidance. | Real-Kafka tests prove: the affected partition is paused after the first error (bounded errors per assignment, no flood); its committed offset never moves; other partitions keep processing; the gap (group, partition, committed offset, new log start, events lost) is stored durably and shown by `/api/v1/system/status` with operator guidance; nothing resets offsets automatically; after the documented operator reset the surviving events are processed and run reconciliation still reports the lost events as missing. |
| 2 | **Operator-key guessing is not rate-limited.** `OperatorGuard` checks the key before the rate limit, so wrong keys never consume a token. | Rejected keys consume the rate limit; a test sends more wrong keys than the limit and then gets 429. Key comparison does not depend on the presented key's length or content. |
| 3 | **Stale documentation.** README limitations still say no performance figures are published (Phase 5); `reliability.md` and ADR-0008 still say the backlog window "is measured in Phase 5". | Every public document matches the code and the measured evidence; verified capabilities, local measurements, known limitations, production limitations and future work are distinguished. |

## P1 — reliability and operations

| # | Finding | Acceptance criteria |
|---|---|---|
| 4 | **No Kafka broker outage test.** PostgreSQL outages are tested; a broker outage is not. | A real-Kafka test pauses the broker mid-run in both processing modes: no dead letters, processing resumes by itself, the run reconciles PASS. |
| 5 | **Profiler recordings capture environment variables**, including the benchmark database password (local, git-ignored files only; nothing in Git history). | Benchmark JFR recordings no longer contain environment variables (verified on a recording). |
| 6 | **Dependency monitoring.** No Dependabot; `npm audit` reports 10 moderate findings (all dev-only lint tooling, 0 in runtime dependencies). | Dependabot configured for Maven, npm, Docker and Actions; `npm audit --omit=dev` clean; dev findings fixed or documented as exceptions with reasons. |
| 7 | **Phase 0 CI criterion:** integration suite green five consecutive CI runs. | Five consecutive green runs of the full workflow, links in the report. |
| 8 | **Runbook gaps:** no procedures for offset gaps, broker outage, disk/backlog limits, safe shutdown/restart, cleanup of stopped test containers. | Runbook covers safe startup, shutdown, recovery, diagnostics and troubleshooting, including the offset-gap operator procedure. |

## P2 — completeness and quality

| # | Finding | Acceptance criteria |
|---|---|---|
| 9 | **Frontend lint warnings** (11 × `react-refresh/only-export-components`). | `npm run lint` reports 0 warnings; CI fails on new warnings. |
| 10 | **Phase 4 criterion "OpenAPI published"** was not met (hand-written Markdown only). | `docs/openapi.yaml` describes every `/api/v1` operation; a test fails if a controller mapping is missing from it. |
| 11 | **Phase 0 §6.2: the dashboard does not show retention headroom.** | The api reports each event-topic partition's size against `retention.bytes`; the Kafka page shows it; system status warns when a partition with lag is above 80 % of its size limit. |
| 12 | **Phase 0 Phase 6 criterion:** security and concurrency checklists. | `docs/security.md` (threat model, controls, verified findings) and the concurrency checklist in `docs/reliability.md`. |
| 13 | **Security headers are not tested.** nginx sets them, but no test asserts them. | The Compose smoke test asserts the headers on the page and the API through nginx. |

## Not in scope (documented, not changed)

- Multi-broker Kafka, TLS, user authentication: production limitations of the local-only deployment model.
- The 750 events/s batch-mode latency outlier: remains a documented uncertainty unless new evidence explains it.
- Partition counts 3 and 12, KIP-848 consumer protocol: untested configurations (docs/benchmarks).

## Status

| # | Resolution | Evidence |
|---|---|---|
| 1 | Done. The error handler pauses the partition, records the gap in `offset_gaps` (V5), logs one actionable error; status reports it; runbook procedure for the deliberate reset | `OffsetOutOfRangeIT` (both modes): 1 error, partition paused, 0 further errors over 8 s, gap = 10 events, survivors processed once after the reset, reconciliation FAIL with exactly the 10 lost events missing |
| 2 | Done. Every attempt consumes the rate limit; SHA-256 digests compared in constant time | `OperatorGuardTest` (6 tests) |
| 3 | Done. README restructured (verified capabilities, local measurements, known, production and future limitations); stale Phase 5 statements corrected in README, `reliability.md`, ADR-0008, runbook | Document review |
| 4 | Done, and it found a real bug: in batch mode a failed offset commit after a broker outage made Spring Kafka pause the consumer for an in-memory retry, and consumption never resumed. Fixed (redelivery instead; Kafka commit errors are infrastructure) | `KafkaOutageIT` (both modes), `FailureClassifierTest` |
| 5 | Done. `jdk.InitialEnvironmentVariable#enabled=false` for benchmark recordings | Verified on a local recording: the probe variable present without the option, absent with it |
| 6 | Done. Dependabot for Maven, npm, Docker, Actions; `minimatch` overridden to 11.0.4 | `npm audit`: 0 vulnerabilities |
| 7 | See the Phase 6 report (consecutive CI runs) | GitHub Actions |
| 8 | Done. Runbook "Operations": startup, shutdown/restart, offset gaps, backlog and disk, outages, cleanup; new troubleshooting rows | `docs/runbook.md` |
| 9 | Done. 0 warnings; `npm run lint` now fails on any warning | `npm run lint` |
| 10 | Done. `docs/openapi.yaml` (18 operations) | `OpenApiCoverageTest` |
| 11 | Done. Partition sizes against `retention.bytes` in the live snapshot and on the Kafka page; status warning at 80 % with lag | `SystemStatusTest`, `KafkaPage` test, `DashboardApiIT` |
| 12 | Done. `docs/security.md`; concurrency checklist in `docs/reliability.md` | Documents |
| 13 | Done. The Compose smoke test asserts CSP, X-Frame-Options, X-Content-Type-Options and Referrer-Policy on the page and the API | CI |
