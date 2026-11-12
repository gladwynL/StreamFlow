# Runbook

How to set up, run, test and troubleshoot StreamFlow locally. Commands work in PowerShell and in
Git Bash unless noted.

## Prerequisites

| Tool | Version | Check |
|---|---|---|
| JDK | Temurin 21 | `java -version` |
| Docker Desktop | Engine 29+, Compose v2 | `docker compose version` |
| Node.js | 24.15 or newer (CI uses 24.22.2) | `node -v` |
| Git | any recent | `git --version` |

Maven is not required; the build uses the Maven Wrapper in `backend/`.

## First-time setup

```bash
cp .env.example .env
```

Then set `POSTGRES_PASSWORD` in `.env` to a random value, for example the output of
`[guid]::NewGuid().ToString('N')` (PowerShell) or `openssl rand -hex 16` (bash). `.env` is
git-ignored.

## Running

### Full stack in Docker

```bash
docker compose --profile app up -d --build --wait
```

Open http://localhost:8080. The stack is `kafka`, `postgres`, a one-shot `migrate` job, `api`,
`worker` and `web` (nginx serving the dashboard and proxying `/api`). Every published port is bound
to 127.0.0.1.

Stop it with `docker compose --profile app down` (add `--volumes` to delete Kafka and PostgreSQL
data).

### Publishing a simulation run

With the full stack running, the one-shot `generate` role publishes a seeded run of synthetic
events, waits for the workers to process it, reconciles it and exits — `0` PASS, `2` FAIL,
`3` INCOMPLETE:

```bash
docker compose run --rm --no-deps generate
```

Defaults: seed 42, 10,000 events at 1,000 events/s. Override with environment variables, for
example:

```bash
docker compose run --rm --no-deps -e STREAMFLOW_GENERATE_EVENTS=100000 -e STREAMFLOW_GENERATE_SEED=7 generate
```

The log ends with the run id, producer outcomes, the content hash (equal for equal seeds) and the
reconciliation verdict. Results are stored in `simulation_runs` and `run_reconciliations`.

### Using the dashboard

Open http://localhost:8080. Sections: **Overview** (throughput, outcomes, latency, workers, trends), **Live
stream**, **Kafka & processing** (partitions, ownership, lag, workers, retries), **Analytics** (15 min to 24 h, per
run), **Dead letters** and **Simulations**, plus **System** (Phase 1 status page). The badge in the header shows the
live feed state: *Live*, *Delayed* (no snapshot for 4 s) or *Reconnecting*; when it is not live, every section keeps
the last data with a notice saying how old it is.

Reading is open. Starting, stopping or reconciling runs and replaying or discarding dead letters need the operator
key: set `STREAMFLOW_OPERATOR_KEY` in `.env` (for example `openssl rand -hex 16`), recreate the api
(`docker compose --profile app up -d api`), then use **Unlock** in the header. The key stays in that tab's memory
only and is forgotten after 10 minutes without operator actions, on **Lock**, or on reload. Without a configured key
the api refuses operator actions (503).

Simulations started from the dashboard are bounded (500,000 events, 5,000 events/s, 600 s, 100,000 users) and one
runs at a time. A run publishes, waits for both consumer groups to drain, then reconciles; the results keep
publication, processing, duplicates, dead letters and reconciliation separate.

### Infrastructure in Docker, applications from the IDE or terminal

```bash
docker compose up -d
```

This starts only Kafka (host port 9094) and PostgreSQL (host port 5432). Then run each role from
`backend/` with the password from `.env`:

```bash
SPRING_DATASOURCE_PASSWORD=<password> SPRING_PROFILES_ACTIVE=migrate ./mvnw -pl streamflow-app spring-boot:run
SPRING_DATASOURCE_PASSWORD=<password> SPRING_PROFILES_ACTIVE=api     ./mvnw -pl streamflow-app spring-boot:run
SPRING_DATASOURCE_PASSWORD=<password> SPRING_PROFILES_ACTIVE=worker  ./mvnw -pl streamflow-app spring-boot:run
```

In PowerShell, set the variables first, e.g. `$env:SPRING_PROFILES_ACTIVE = 'api'`. Exactly one role
profile (`migrate`, `api`, `worker` or `generate`) must be active or the application refuses to
start.

Frontend dev server (proxies `/api` and the `/ws` live feed to `http://localhost:8080`, override with
`STREAMFLOW_API_URL`):

```bash
cd frontend
npm ci --ignore-scripts
npm run dev
```

## Ports

| Port (host) | Service |
|---|---|
| 8080 | web (dashboard + `/api` proxy) in Docker; or the `api` role when run locally |
| 8081 | actuator of a locally run `api` or `worker` role |
| 9094 | Kafka (external listener) |
| 5432 | PostgreSQL |
| 5173 | Vite dev server |

Inside Docker, `api` and `worker` serve actuator health on 8081 (not published).

## Tests

```bash
cd backend && ./mvnw verify      # unit + Testcontainers integration tests, Spotless, JaCoCo
cd frontend && npm test          # Vitest
```

Integration tests start their own Kafka and PostgreSQL containers (one of each for the whole run).
Stop the Compose stack first to save memory: `docker compose --profile app down`.

Formatting: `./mvnw spotless:apply` (backend), `npm run format` (frontend).

### End-to-end tests (Playwright)

The end-to-end tests drive Chromium against the real Compose stack. Start it with the e2e overlay, which sets a
throwaway operator key and lets the worker run the predefined fault scenarios (the tests use one to produce dead
letters):

```bash
export STREAMFLOW_E2E_OPERATOR_KEY=$(openssl rand -hex 16)
docker compose -f compose.yaml -f compose.e2e.yaml --profile app up -d --build --wait
cd frontend
npx playwright install chromium     # once
npm run e2e
```

PowerShell: `$env:STREAMFLOW_E2E_OPERATOR_KEY = [guid]::NewGuid().ToString('N')`. The suite covers every section
(axe accessibility scan, no horizontal scroll at 375, 768 and 1440 px), keyboard navigation, reduced motion, the live
feed cadence and reconnection, a seeded run that must reconcile as PASS, and dead-letter replay and discard. Use `E2E_BASE_URL` to target another address. Stop the stack afterwards
with the same `-f` options and `down --volumes`.

## Memory on this laptop

Windows can use 11.7 GB of the 16 GB installed. Docker Desktop's WSL VM uses its default cap
(5.62 GiB as reported by `docker info`).

During Phase 1, Windows recorded two "low virtual memory" events while the Docker VM, Roblox Studio
and Chrome were the top consumers; both times the Docker VM was restarted and Kafka/PostgreSQL
exited with code 255. Before running integration tests or the full stack:

- stop other projects' containers;
- close heavy applications (game engines, many browser tabs);
- run one heavy process at a time (integration tests *or* the Compose stack, not both).

## Failures, dead letters and recovery

How a worker handles a failed record (ADR-0006, [`reliability.md`](reliability.md)):

| Failure | What happens |
|---|---|
| Bad record (undecodable, invalid) | Dead-lettered immediately; the partition moves on |
| Transient (serialization conflict, deadlock, lock or statement timeout, unknown error) | Up to 3 attempts in place, then dead-lettered |
| Infrastructure (PostgreSQL unreachable) | Retried without limit (back-off capped at 10 s); the partition waits and resumes by itself; nothing is dead-lettered |

Dead letters land in the `.dlt` topic and are copied into `dead_letter_events`. Inspect them through the API
([`api.md`](api.md)):

```bash
curl -s "http://localhost:8080/api/v1/dead-letters?status=PENDING&limit=20"
curl -s http://localhost:8080/api/v1/dead-letters/<id>
```

**Replay and discard** need an operator key. Set `STREAMFLOW_OPERATOR_KEY` in `.env` (generate it like the
password) and restart the api (`docker compose --profile app up -d api`). Fix the cause first, then:

```bash
curl -s -X POST http://localhost:8080/api/v1/dead-letters/replay -H "Content-Type: application/json"   -H "X-StreamFlow-Operator-Key: <key>" -d '{"ids":[<id>]}'
curl -s -X POST http://localhost:8080/api/v1/dead-letters/<id>/discard -H "Content-Type: application/json"   -H "X-StreamFlow-Operator-Key: <key>" -d '{"reason":"<why>"}'
```

Replay keeps the event's identity, so replaying twice never applies it twice. Dead letters older than 7 days cannot
be replayed (they become `EXPIRED`); expired and pending dead letters are never deleted automatically.

**Worker metrics** are on each worker's management port (`/actuator/metrics`; not published by Docker Compose —
use `docker compose exec worker wget -qO- http://127.0.0.1:8081/actuator/metrics/<name>`):
`streamflow.processing.failures` (tag `category`), `streamflow.deadletters.published`,
`streamflow.deadletters.publish.failures`, `streamflow.deadletters.ingested`,
`streamflow.deadletters.ingestion.failures`, `streamflow.infrastructure.outage`,
`streamflow.infrastructure.recoveries`, `streamflow.consumer.rebalances`, `streamflow.consumer.errors`.

**Consumer position deleted by retention.** If a worker was down longer than the topic retention (24 hours, or less
under heavy traffic because of the 1 GiB per-partition cap), its committed offset can point at deleted records. The
worker does not skip ahead: it logs "out of range … no reset policy is configured" and counts
`streamflow.consumer.errors`. Recovering is a deliberate decision because the deleted events are lost (the run's
reconciliation will report them missing). With every worker stopped:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092   --group streamflow-processor --topic streamflow.events.v1 --reset-offsets --to-earliest --execute
```

**Fault injection** (tests and demonstrations only) is off unless a worker runs with
`STREAMFLOW_FAULT_INJECTION_ENABLED=true`. Scenarios are rows in `fault_scenarios`, limited to two predefined kinds
(`REJECT_EVENT_TYPE`, `TRANSIENT_FAILURES`) and to at most ten minutes each.

## Troubleshooting

| Symptom | Likely cause | Action |
|---|---|---|
| `kafka` or `postgres` exits with code 255 | Docker VM restarted under host memory pressure | Free memory (above) and start again |
| `api`/`worker` fail with a Flyway validation error | `migrate` has not run | `docker compose --profile app up migrate`, then start the others |
| `migrate` fails with "has N partitions but 6 are specified" | Topic exists with a different partition count | Intended safety stop; recreate the topic deliberately |
| Status page shows "No worker has sent a heartbeat" | No worker running | Start a `worker` |
| Startup error "No StreamFlow role is active" | No or multiple role profiles | Set `SPRING_PROFILES_ACTIVE` to one of `migrate`, `api`, `worker`, `generate` |
| Replay returns 503 | No operator key configured | Set `STREAMFLOW_OPERATOR_KEY` in `.env` and restart the api |
| Dead letters pile up in the topic but not in the API | Ingestion failing | Check `streamflow.deadletters.ingestion.failures` and the worker log ("Dead-letter ingestion of … failed") |
| Lag grows, no dead letters, worker readiness DOWN | PostgreSQL unreachable (infrastructure outage) | Restore the database; processing resumes by itself |

## Measurements

### Phase 1 Compose stack (idle, no event traffic)

Measured 2026-11-10 on the development laptop: Lenovo ThinkPad P16s, AMD Ryzen 5 PRO 7540U
(12 logical CPUs), 16 GB installed / 11.7 GB usable by Windows, Windows 11 Pro, Docker Desktop
engine 29.8.0 with the default WSL VM cap (5.62 GiB), Compose v5.5.1.

| Measurement | Result |
|---|---|
| Image build, cold BuildKit cache (`docker compose --profile app build`) | 93 s |
| Image sizes | `streamflow-app` 380 MB, `streamflow-web` 82 MB |
| Cold start with fresh volumes and built images (`up -d --wait`) to all services healthy, `migrate` exited 0 | 39 s |

Container memory from `docker stats`, two samples about 1 and 2.5 minutes after start:

| Container | Sample 1 | Sample 2 | Limit |
|---|---|---|---|
| kafka | 302 MiB | 308 MiB | 1 GiB |
| api | 192 MiB | 195 MiB | 768 MiB |
| worker | 177 MiB | 181 MiB | 512 MiB |
| postgres | 59 MiB | 61 MiB | 768 MiB |
| web | 11 MiB | 12 MiB | 64 MiB |
| **Total** | **741 MiB** | **756 MiB** | |

The Docker VM process (`vmmemWSL`) used about 1.26 GB of host memory at the second sample. These
are idle figures from two samples, not load measurements; behaviour under event traffic is
measured in Phase 5.

### Benchmarks (Phase 5)

Performance is measured with the benchmark harness, not with the development stack:
[`docs/benchmarks/README.md`](benchmarks/README.md) (results) and
[`docs/benchmarks/methodology.md`](benchmarks/methodology.md) (method and reproduction). In short:

```bash
docker compose build migrate
benchmarks/run.sh benchmarks/scenarios/drain-batch-1w-c3.env 3
python benchmarks/analyze.py
```

It runs on its own Compose project (`streamflow-bench`, ports 19094 and 15432) and never touches the development
stack or its data. Check memory first (the script refuses to start below 5 GB of commit headroom; override with
`BENCH_MIN_HEADROOM_GB`) and stop the benchmark stack afterwards.

Processing mode: workers default to batch mode since Phase 5; set `STREAMFLOW_WORKER_PROCESSING_MODE=RECORD` on the
worker for the record (reference) mode.

### Phase 4 observations under synthetic load (not benchmarks)

Observed 2026-11-11 on the same laptop while verifying the dashboard: the Compose stack with one `worker`
container (3 consumer threads, one PostgreSQL transaction per record) and the e2e overlay. These are single
observations taken to check the dashboard, not controlled measurements; Phase 5 benchmarks processing properly.

| Observation | Value |
|---|---|
| 240,000-event run at 2,000 events/s requested | published at about 1,590-2,020 records/s; processed about 490-680 events/s while publishing; reconciled PASS (240,000 processed, 0 duplicates) |
| 90,000-event run at 1,000 events/s requested | processed about 670 events/s while publishing with one dashboard tab open; reconciled PASS |
| Draining a backlog with no dashboard client connected | about 780-810 events/s (rows in `events` per 10 s); PostgreSQL about 120% CPU, worker about 65%, api under 1% |
| `GET /api/v1/live` | about 1 s for the first call after start (cold), then 18-30 ms (cached snapshot) |
| Container memory under that load (`docker stats`) | kafka 438-513 MiB, api 233-253 MiB, worker 249-259 MiB, postgres 76-221 MiB, web 12-15 MiB |

With publication above roughly 700-800 events/s the single worker falls behind, lag grows and processing latency
reaches the open-ended histogram bucket (over 60 s) until the backlog drains. PostgreSQL CPU is the visible limit;
whether the per-event latency histogram write added in Phase 4 contributes measurably has not been isolated.
