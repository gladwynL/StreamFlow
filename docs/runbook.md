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

Frontend dev server (proxies `/api` to `http://localhost:8080`, override with `STREAMFLOW_API_URL`):

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

## Memory on this laptop

Windows can use 11.7 GB of the 16 GB installed. Docker Desktop's WSL VM uses its default cap
(5.62 GiB as reported by `docker info`).

During Phase 1, Windows recorded two "low virtual memory" events while the Docker VM, Roblox Studio
and Chrome were the top consumers; both times the Docker VM was restarted and Kafka/PostgreSQL
exited with code 255. Before running integration tests or the full stack:

- stop other projects' containers;
- close heavy applications (game engines, many browser tabs);
- run one heavy process at a time (integration tests *or* the Compose stack, not both).

## Troubleshooting

| Symptom | Likely cause | Action |
|---|---|---|
| `kafka` or `postgres` exits with code 255 | Docker VM restarted under host memory pressure | Free memory (above) and start again |
| `api`/`worker` fail with a Flyway validation error | `migrate` has not run | `docker compose --profile app up migrate`, then start the others |
| `migrate` fails with "has N partitions but 6 are specified" | Topic exists with a different partition count | Intended safety stop; recreate the topic deliberately |
| Status page shows "No worker has sent a heartbeat" | No worker running | Start a `worker` |
| Startup error "No StreamFlow role is active" | No or multiple role profiles | Set `SPRING_PROFILES_ACTIVE` to one of `migrate`, `api`, `worker` |

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
