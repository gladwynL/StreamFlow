# Benchmark methodology

How StreamFlow's performance figures are produced, so that every number in [README.md](README.md) and
[results.md](results.md) can be reproduced and checked. Results are single-host measurements on a developer
laptop; they describe this system on this machine, not a production deployment.

## Principles

- **Every run reconciles.** A run counts only if its reconciliation is PASS: every acknowledged event is in the
  ledger exactly once, nothing is missing or unexpected. A run that does not reconcile stops the scenario and is
  reported, never averaged in.
- **Four quantities are kept apart:** offered traffic (the requested rate), acknowledged publications (producer
  acks), unique database effects (distinct events applied, from the ledger), and duplicate deliveries (suppressed
  by the ledger). None is derived from another.
- **Repetition.** Each configuration runs three times (unless stated otherwise); results are the median with the
  full range (minimum–maximum). With three repetitions the range, not a confidence interval, is the honest
  measure of spread.
- **Same conditions within a comparison.** Before/after and scaling comparisons use the same host, image build
  procedure, container limits, data volume and scenario parameters, and are run back to back. Preliminary
  observations from earlier phases are never mixed with benchmark results.
- **Bounded and safe.** Runs execute strictly one at a time, after a memory preflight; containers have memory
  limits; topics are deleted after each run and the benchmark database is emptied before each run.

## Isolation

Benchmarks run on a separate Docker Compose project, `streamflow-bench` (`compose.yaml` + `compose.bench.yaml`),
with its own containers, named volumes, network, generated password (`benchmarks/.bench.env`, git-ignored) and host
ports (Kafka 19094, PostgreSQL 15432). The development stack (project `streamflow`) and its data are never touched.

Within the benchmark stack, every repetition gets its own topics `bench.<scenario>-<stamp>-r<n>.events` / `.dlt`
and consumer groups, provisioned by the `migrate` role and deleted (topics and groups) when the repetition ends.
Before each repetition the benchmark database's run data is emptied (`TRUNCATE` of the event, ledger, aggregate,
dead-letter and run tables) and its statistics are reset, so every repetition starts from the same state and table
growth does not drift results. Only the `streamflow-bench` database is ever reset.

PostgreSQL in the benchmark stack has the same settings as the development stack plus `pg_stat_statements`,
`track_io_timing` and `track_wal_io_timing` for measurement. Durability settings are unchanged
(`synchronous_commit=on`, `fsync=on`).

## Components

| Component | Role |
|---|---|
| `loadgen` runtime role | Headless benchmark job (Spring profile `loadgen`, no web server). One invocation runs one phase and writes JSON: **PUBLISH** (publish a seeded run at the offered rate; acknowledged throughput and producer metrics), **OBSERVE** (sample lag and progress until both consumer groups have drained the run, then reconcile and record consumer throughput, exact latency percentiles, per-partition distribution and PostgreSQL statistics), **DASHBOARD** (hold live-feed WebSocket clients open). |
| `benchmarks/run.sh` | Orchestrates one scenario × N repetitions: preflight, provisioning, phases, worker start/kill/restart, container statistics sampling, optional Java Flight Recorder, environment manifest, cleanup. |
| `benchmarks/scenarios/*.env` | Scenario definitions (see below). |
| `benchmarks/analyze.py` | Turns the raw results into `docs/benchmarks/results.md` (medians, ranges, tables, SVG charts). Standard library only. |
| `benchmarks/results/` | Raw results, committed: `publish.json`, `observe.json` (including the lag timeline), `docker-stats.csv`, `manifest.json`, logs. JFR recordings are kept locally (large) and summarised in the docs. |

## Scenario parameters

| Variable | Meaning | Default |
|---|---|---|
| `WORKLOAD` | `backlog`: publish the whole run first, then start the workers (measures drain / maximum consumer throughput). `live`: workers run while events are published at the offered rate (measures latency under load). | `backlog` |
| `EVENTS`, `RATE` | Run size and offered rate (100,000/s = as fast as the producer can) | 60,000 / 100,000 |
| `SEED`, `USERS`, `HOT_USER_PERCENT` | Deterministic content; user population; share of events for one hot user (key skew) | 20261112 / 10,000 / 0 |
| `PARTITIONS` | Partitions of the run's topic | 6 |
| `WORKERS`, `CONCURRENCY` | Worker containers × consumer threads per worker | 1 × 3 |
| `PROCESSING_MODE` | `RECORD` (one transaction per record) or `BATCH` (one per poll batch, ADR-0013) | `RECORD` |
| `DASHBOARD_CLIENTS` | Live-feed WebSocket clients held open during the run (requires the api) | 0 |
| `KILL_WORKER_AFTER`, `RESTART_KILLED_AFTER` | Kill one worker (SIGKILL) this many seconds after the workers started; restart it after a delay | — |
| `JFR` | 1 = record Java Flight Recorder on the worker (profiling runs only; not used for throughput figures) | 0 |
| `WORKER_MEM` | Memory limit per worker container | 512m |

## Metrics and how they are measured

| Metric | Source |
|---|---|
| Producer acknowledged throughput | `acked ÷ publish duration` in the PUBLISH phase (acks=all, idempotent producer, lz4) |
| Consumer throughput | From the event rows: `(distinct events − 1) ÷ (last − first processing transaction start)`, database clock. Independent of sampling. In `backlog` runs this is the maximum sustained processing rate of the configuration. |
| Drain time | The same span: first to last event processed |
| Processing latency p50/p95/p99 | Exact percentiles over every event of the run of `ingested_at − Kafka append timestamp` (start of the processing transaction minus broker append time). In `backlog` runs latency is dominated by the time events waited in the backlog and is not a latency measurement; latency figures come from `live` runs. |
| Lag and backlog | Consumer-group lag per partition every second (AdminClient), in the OBSERVE timeline |
| Duplicates | Deliveries suppressed by the ledger for the run (`run_stats`) |
| CPU and memory | `docker stats` about once per second for every benchmark container; reported while events were being processed. CPU is in percent of one core. |
| PostgreSQL cost | `pg_stat_statements` (time and calls per statement), `pg_stat_database` (transactions), `pg_stat_io` (WAL writes, fsyncs and their time) for the run |
| Partition distribution | Events and processing span per partition |
| Recovery after a worker kill | From the lag timeline: the longest interval after the kill during which any partition's lag did not fall |
| Profiling | Java Flight Recorder (`settings=profile`) on the worker: where consumer threads spend time (native-method samples while blocked on I/O, execution samples, GC pauses) |

## Environment manifest

Every repetition writes `manifest.json`: Git commit and whether the tree was dirty, scenario parameters, host CPU,
OS and memory, Docker engine version, CPUs and memory of the Docker VM, Compose version, image id and container
limits. The OBSERVE result adds the JVM version, arguments, heap and processor count seen by the container,
PostgreSQL server version and durability settings.

## Reproducing

Requirements: Docker Desktop (or Docker Engine), Git Bash on Windows, Python 3, about 5 GB of free memory.

```bash
docker compose build migrate                        # the application image (streamflow-app:local)
benchmarks/run.sh benchmarks/scenarios/drain-record-1w-c3.env 3
python benchmarks/analyze.py                        # regenerates docs/benchmarks/results.md
```

`run.sh` stops with exit status 20 when the memory headroom is below `BENCH_MIN_HEADROOM_GB` (default 5 GB), and
with the loadgen's status if a run does not reconcile. Stop the benchmark stack afterwards with
`docker compose -p streamflow-bench --env-file benchmarks/.bench.env -f compose.yaml -f compose.bench.yaml stop`.

## Limitations of the method

- One laptop, one Kafka broker, one PostgreSQL instance, all in one Docker VM: producer, broker, database and
  workers compete for the same CPUs, memory and disk. Absolute numbers do not transfer to other hardware; ratios
  between configurations are more robust than absolute values.
- Docker Desktop on Windows runs containers in a WSL 2 VM; disk flushes (fsync) go through its virtual disk, which
  makes commits slower than on bare-metal Linux.
- Three repetitions show spread but are too few for formal confidence intervals; background activity on the host
  (other applications, Windows services) is not controlled beyond closing heavy applications.
- Latency percentiles are measured on the database clock against the broker's append timestamp; both run on the
  same host clock.
