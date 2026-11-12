# Security

StreamFlow is a **local, single-user** system: every published port binds to 127.0.0.1, there are no user accounts,
and all traffic is synthetic. This document is the Phase 6 security checklist: the threat model, the controls that
exist, how each is verified, and what is deliberately out of scope. It is not a claim that StreamFlow is fit for
public or production deployment.

## Threat model

| In scope | Out of scope (production limitations) |
|---|---|
| Another local process or a web page in the user's browser trying to change data (replay, discard, start runs) | Attackers on the network: nothing is reachable from it by design |
| Guessing the operator key | Multi-user access control, audit trails per user |
| Secrets leaking into Git, CI logs, benchmark artifacts or screenshots | TLS, encryption at rest, secret managers |
| Malformed or hostile input to the API and the WebSocket | Denial of service by a local process with full access to the machine |
| Vulnerable dependencies and container images | Kafka and PostgreSQL authentication (local containers, localhost only) |

## Controls

| Control | Implementation | Verified by |
|---|---|---|
| Localhost only | Compose publishes ports on `127.0.0.1` only; the api and worker actuator ports are not published; the benchmark stack uses separate localhost ports | `compose.yaml`, `compose.bench.yaml` review; CI Compose smoke test |
| Read/write separation | Every state-changing endpoint requires `X-StreamFlow-Operator-Key`; reads need no key | `OperatorGuardTest`, `DashboardApiIT`, `ReplayCrashIT`, OpenAPI `security` per operation |
| Operator key comparison | SHA-256 digests compared in constant time: neither content nor length of the configured key is revealed by timing | `OperatorGuardTest` (keys of any length) |
| Brute-force limit | 30 operator attempts per minute per api process; **rejected keys count** (fixed in Phase 6: they used not to) | `OperatorGuardTest.rejectedKeysConsumeTheRateLimitSoGuessingIsBounded` |
| Operator actions disabled by default | No key configured → 503, never executed | CI smoke test (replay without key → 503) |
| Browser key handling | Key held in React memory only; never in URLs, storage, cookies or logs; sent only as a header; auto-lock after 10 minutes | Frontend `OperatorKey`, `DeadLettersPage`, `SimulationsPage` tests; Playwright operator flows |
| Input validation | Bounded page sizes (≤ 100), enumerated ranges and event types, run limits (500,000 events, 5,000/s, 600 s, 100,000 users), replay batches ≤ 100 ids, discard reasons ≤ 200 characters; RFC 9457 problem responses | `SimulationControllerTest`, `DashboardControllerTest`, `DashboardApiIT` |
| SQL injection | All SQL uses bind parameters (`JdbcClient` named parameters, prepared statements, `unnest` arrays); no SQL is built from request input (only fixed constants such as the latency bucket bounds are formatted into statements) | Code review of `persistence/*`, `loadgen/BenchmarkQueries` |
| WebSocket | Origins limited to `localhost` and `127.0.0.1`; inbound messages ≤ 4 KiB; 20 clients maximum; slow clients disconnected | `DashboardApiIT`, `LiveBroadcasterTest` |
| HTTP security headers | nginx sets CSP (`default-src 'self'`, no inline scripts), `X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer` on the page and on API responses | CI Compose smoke test asserts them (Phase 6) |
| CORS | None configured: the dashboard is same-origin through nginx; cross-origin browser requests to the API are not granted | Configuration review |
| Containers | Application images run as a non-root user (10001), read-only root filesystem, all capabilities dropped, `no-new-privileges`, memory limits | `compose.yaml`, `backend/Dockerfile`, `frontend/Dockerfile` |
| Secrets in Git | `.env` and `benchmarks/.bench.env` are git-ignored; CI generates throwaway passwords and keys and never prints them (the e2e key is also masked in logs) | History scan (Phase 6: no configured password or key appears in any commit) |
| Secrets in artifacts | Benchmark results contain no environment values; profiler recordings exclude environment variables (`jdk.InitialEnvironmentVariable#enabled=false`) and are not committed; screenshots show no key | Phase 6 audit; methodology |
| Dependencies | Exact pins; Dependabot weekly for Maven, npm, Docker and GitHub Actions; `npm audit` clean (a dev-only `minimatch` advisory fixed by an override) | `.github/dependabot.yml`, `npm audit` |

## Findings of the Phase 6 review

| Finding | Severity (local model) | Resolution |
|---|---|---|
| Wrong operator keys did not consume the rate limit, so the key could be guessed without limit | Medium | Fixed; every attempt counts; regression test |
| Key comparison time depended on the presented key's length | Low | Fixed; digests compared |
| Local profiler recordings contained the benchmark database password (environment variables) | Low (local, git-ignored, benchmark-only password) | Fixed for future recordings; existing recordings stay local |
| `minimatch` ReDoS advisory in lint tooling (dev only) | Low | Overridden to a patched release |
| Security headers were set but not tested | Low | CI assertion |

## Known limitations

- A local process can reach `127.0.0.1` ports, including PostgreSQL and Kafka, whose credentials are in `.env`. The
  model assumes the machine and its user are trusted.
- The operator rate limit is shared by all callers: a local process sending wrong keys can delay the real operator
  for up to a minute.
- Requests to the WebSocket without an `Origin` header (non-browser clients) are accepted; browsers always send it.
