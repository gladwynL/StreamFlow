#!/usr/bin/env bash
# Runs one benchmark scenario REPETITIONS times on the isolated benchmark stack (docs/benchmarks/methodology.md).
#
#   benchmarks/run.sh benchmarks/scenarios/drain-1w-c3.env [repetitions]
#
# Each repetition: memory preflight -> reset the benchmark database -> provision its own bench.* topics and consumer
# groups -> publish (loadgen PUBLISH) -> process (workers) -> observe until drained and reconcile (loadgen OBSERVE)
# -> collect container statistics, optional JFR recordings and an environment manifest -> delete the topics and
# groups. Runs are strictly sequential. The development stack (project "streamflow") is never touched.
set -euo pipefail
# Git Bash on Windows would rewrite container paths such as /results/... into Windows paths.
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SCENARIO_FILE="${1:?usage: benchmarks/run.sh <scenario.env> [repetitions]}"
REPETITIONS="${2:-3}"

# Scenario defaults; the scenario file overrides them.
NAME=""
WORKLOAD=backlog            # backlog: publish everything, then start the workers; live: workers run while publishing
EVENTS=60000
RATE=100000                 # offered events/s (100000 = as fast as the producer can)
USERS=10000
HOT_USER_PERCENT=0
SEED=20261112
PARTITIONS=6
WORKERS=1
CONCURRENCY=3
PROCESSING_MODE=BATCH
DASHBOARD_CLIENTS=0
KILL_WORKER_AFTER=""        # seconds after workers start; kills one worker (SIGKILL)
RESTART_KILLED_AFTER=""     # seconds after the kill; starts the killed container again
JFR=0
WORKER_MEM=512m
DRAIN_TIMEOUT=900s
# shellcheck disable=SC1090
source "$SCENARIO_FILE"
: "${NAME:?scenario must set NAME}"

PROJECT=streamflow-bench
ENV_FILE="benchmarks/.bench.env"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
# Provenance is captured once, before the first repetition (the image must be built from this tree).
GIT_COMMIT="$(git rev-parse HEAD)"
GIT_DIRTY="$( [ -n "$(git status --porcelain)" ] && echo true || echo false )"
OUT_ROOT="benchmarks/results/$STAMP-$NAME"
PY="$(command -v python3 2>/dev/null || true)"
if [ -z "$PY" ] || ! "$PY" -c 'import sys' >/dev/null 2>&1; then PY="$(command -v python)"; fi

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }

compose() {
  docker compose -p "$PROJECT" --env-file "$ENV_FILE" -f compose.yaml -f compose.bench.yaml \
    --profile app --profile bench "$@"
}

psql_bench() {
  compose exec -T postgres psql -v ON_ERROR_STOP=1 -U streamflow -d streamflow -tAc "$1"
}

ensure_env_file() {
  if [ ! -f "$ENV_FILE" ]; then
    log "Creating $ENV_FILE (benchmark-only password and host ports)"
    sed -e "s/^POSTGRES_PASSWORD=.*/POSTGRES_PASSWORD=$(openssl rand -hex 16)/" \
        -e "s/^STREAMFLOW_OPERATOR_KEY=.*/STREAMFLOW_OPERATOR_KEY=/" \
        -e "s/^KAFKA_HOST_PORT=.*/KAFKA_HOST_PORT=19094/" \
        -e "s/^POSTGRES_HOST_PORT=.*/POSTGRES_HOST_PORT=15432/" \
        -e "s/^WEB_HOST_PORT=.*/WEB_HOST_PORT=18080/" .env.example > "$ENV_FILE"
  fi
}

# Refuses to start a run when the host is short of memory (Windows: commit charge; Linux: available memory).
preflight() {
  local min_gb="${BENCH_MIN_HEADROOM_GB:-5}"
  if command -v powershell.exe >/dev/null 2>&1; then
    local headroom
    headroom=$(powershell.exe -NoProfile -Command \
      '$o=Get-CimInstance Win32_OperatingSystem; [math]::Round($o.FreeVirtualMemory/1MB,1)' | tr -d '\r')
    log "Commit-charge headroom: ${headroom} GB (minimum ${min_gb})"
    "$PY" -c "import sys; sys.exit(0 if float('$headroom') >= $min_gb else 1)" \
      || { log "Not enough memory headroom; stopping."; exit 20; }
  elif [ -r /proc/meminfo ]; then
    local avail
    avail=$(awk '/MemAvailable/ {printf "%.1f", $2/1048576}' /proc/meminfo)
    log "Available memory: ${avail} GB (minimum ${min_gb})"
    "$PY" -c "import sys; sys.exit(0 if float('$avail') >= $min_gb else 1)" \
      || { log "Not enough memory; stopping."; exit 20; }
  fi
}

ensure_infrastructure() {
  compose up --detach --wait --wait-timeout 180 kafka postgres
  psql_bench "CREATE EXTENSION IF NOT EXISTS pg_stat_statements" >/dev/null
}

# Empties the benchmark database's run data so every repetition starts from the same state. Only the
# streamflow-bench project's database is ever reset.
reset_database() {
  psql_bench "TRUNCATE events, processed_events, event_stats_minute, latency_stats_minute, run_stats,
                dead_letter_events, publish_failures, run_reconciliations, simulation_runs, worker_heartbeats,
                fault_scenarios RESTART IDENTITY CASCADE" >/dev/null
  psql_bench "CHECKPOINT" >/dev/null
  psql_bench "SELECT pg_stat_statements_reset(), pg_stat_reset(), pg_stat_reset_shared('io'),
                pg_stat_reset_shared('wal')" >/dev/null
}

sample_stats() {
  local file="$1"
  echo "epoch_ms,container,cpu_percent,mem_usage" > "$file"
  while true; do
    docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' 2>/dev/null \
      | grep "^$PROJECT-" | sed "s/^/$(date +%s%3N),/" >> "$file" || true
    sleep 1
  done
}

kafka_admin() {
  compose exec -T kafka "/opt/kafka/bin/$1" --bootstrap-server localhost:9092 "${@:2}"
}

stop_sampler() {
  if [ -n "${STATS_PID:-}" ]; then
    kill "$STATS_PID" 2>/dev/null || true
    wait "$STATS_PID" 2>/dev/null || true
    STATS_PID=""
  fi
}

# Kills one worker (SIGKILL) KILL_WORKER_AFTER seconds after the workers started, optionally restarting it.
schedule_kill() {
  KILL_PID=""
  if [ -n "$KILL_WORKER_AFTER" ]; then
    (
      sleep "$KILL_WORKER_AFTER"
      victim=$(compose ps -q worker | head -1)
      date +%s%3N > "$RUN_DIR/worker-killed-ms"
      docker kill "$victim" >/dev/null
      if [ -n "$RESTART_KILLED_AFTER" ]; then
        sleep "$RESTART_KILLED_AFTER"
        date +%s%3N > "$RUN_DIR/worker-restarted-ms"
        docker start "$victim" >/dev/null
      fi
    ) &
    KILL_PID=$!
  fi
}

cleanup_run() {
  compose rm --stop --force worker api >/dev/null 2>&1 || true
  kafka_admin kafka-topics.sh --delete --topic "$BENCH_EVENTS_TOPIC" >/dev/null 2>&1 || true
  kafka_admin kafka-topics.sh --delete --topic "$BENCH_DLT_TOPIC" >/dev/null 2>&1 || true
  kafka_admin kafka-consumer-groups.sh --delete --group "$BENCH_GROUP" >/dev/null 2>&1 || true
  kafka_admin kafka-consumer-groups.sh --delete --group "$BENCH_GROUP-dlt" >/dev/null 2>&1 || true
}

manifest() {
  local file="$1"
  local host_cpu host_os host_mem
  if command -v powershell.exe >/dev/null 2>&1; then
    host_cpu=$(powershell.exe -NoProfile -Command '(Get-CimInstance Win32_Processor).Name' | tr -d '\r' | head -1)
    host_os=$(powershell.exe -NoProfile -Command '(Get-CimInstance Win32_OperatingSystem).Caption' | tr -d '\r')
    host_mem=$(powershell.exe -NoProfile -Command \
      '[math]::Round((Get-CimInstance Win32_OperatingSystem).TotalVisibleMemorySize/1MB,1)' | tr -d '\r')
  else
    host_cpu=$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //')
    host_os=$(uname -sr)
    host_mem=$(awk '/MemTotal/ {printf "%.1f", $2/1048576}' /proc/meminfo)
  fi
  "$PY" - "$file" <<PYEOF
import json, subprocess, sys
def sh(*args):
    try:
        return subprocess.run(args, capture_output=True, text=True, timeout=30).stdout.strip()
    except Exception as e:
        return f"unavailable: {e}"
info = json.loads(sh("docker", "info", "--format", "{{json .}}") or "{}")
manifest = {
    "gitCommit": "$GIT_COMMIT",
    "gitDirty": "$GIT_DIRTY" == "true",
    "imageCreated": sh("docker", "image", "inspect", "--format", "{{.Created}}", "streamflow-app:local"),
    "scenarioFile": "$SCENARIO_FILE",
    "scenario": {k: v for k, v in {
        "NAME": "$NAME", "WORKLOAD": "$WORKLOAD", "EVENTS": "$EVENTS", "RATE": "$RATE", "USERS": "$USERS",
        "HOT_USER_PERCENT": "$HOT_USER_PERCENT", "SEED": "$SEED", "PARTITIONS": "$PARTITIONS",
        "WORKERS": "$WORKERS", "CONCURRENCY": "$CONCURRENCY", "PROCESSING_MODE": "$PROCESSING_MODE",
        "DASHBOARD_CLIENTS": "$DASHBOARD_CLIENTS", "KILL_WORKER_AFTER": "$KILL_WORKER_AFTER",
        "RESTART_KILLED_AFTER": "$RESTART_KILLED_AFTER", "JFR": "$JFR", "WORKER_MEM": "$WORKER_MEM"}.items()},
    "repetition": int("$REP"),
    "host": {"cpu": """$host_cpu""", "os": """$host_os""", "memoryGB": "$host_mem"},
    "docker": {k: info.get(k) for k in ("ServerVersion", "NCPU", "MemTotal", "OperatingSystem", "KernelVersion")},
    "compose": sh("docker", "compose", "version", "--short"),
    "images": sh("docker", "images", "--format", "{{.Repository}}:{{.Tag}} {{.ID}}", "streamflow-app"),
    "containerLimits": {"worker": "$WORKER_MEM", "loadgen": "768m", "kafka": "1g", "postgres": "768m", "api": "768m"},
}
json.dump(manifest, open(sys.argv[1], "w"), indent=2)
PYEOF
}

ensure_env_file
set -a; source "$ENV_FILE"; set +a
mkdir -p "$OUT_ROOT" benchmarks/jfr
log "Scenario $NAME x $REPETITIONS -> $OUT_ROOT"

for REP in $(seq 1 "$REPETITIONS"); do
  preflight
  ensure_infrastructure
  TAG="$(echo "$NAME" | tr -c 'a-z0-9\n' '-')-$STAMP-r$REP"
  TAG="$(echo "$TAG" | tr 'A-Z' 'a-z')"
  export BENCH_SCENARIO="$NAME" BENCH_PARTITIONS="$PARTITIONS" BENCH_WORKERS="$WORKERS"
  export BENCH_CONCURRENCY="$CONCURRENCY" BENCH_PROCESSING_MODE="$PROCESSING_MODE" BENCH_WORKER_MEM="$WORKER_MEM"
  export BENCH_EVENTS_TOPIC="bench.$TAG.events" BENCH_DLT_TOPIC="bench.$TAG.dlt" BENCH_GROUP="bench-$TAG"
  export BENCH_WORKER_JAVA_OPTS=""
  if [ "$JFR" = "1" ]; then
    rm -f benchmarks/jfr/*.jfr
    export BENCH_WORKER_JAVA_OPTS="-XX:StartFlightRecording=filename=/jfr/worker-%t.jfr,settings=profile,dumponexit=true"
  fi
  RUN_DIR="$OUT_ROOT/r$REP"
  mkdir -p "$RUN_DIR"
  STATS_PID=""
  trap 'stop_sampler; cleanup_run' EXIT

  log "[$REP/$REPETITIONS] provision $BENCH_EVENTS_TOPIC ($PARTITIONS partitions), reset database"
  compose run --rm --no-deps migrate >/dev/null
  reset_database

  if [ "$DASHBOARD_CLIENTS" -gt 0 ]; then
    compose up --detach --no-deps --wait --wait-timeout 180 api
  fi
  KILL_PID=""
  if [ "$WORKLOAD" = "live" ]; then
    date +%s%3N > "$RUN_DIR/workers-started-ms"
    compose up --detach --no-deps --wait --wait-timeout 180 --scale "worker=$WORKERS" worker
    schedule_kill
  fi

  sample_stats "$RUN_DIR/docker-stats.csv" &
  STATS_PID=$!
  DASH_PID=""
  if [ "$DASHBOARD_CLIENTS" -gt 0 ]; then
    compose run --rm --no-deps -e STREAMFLOW_LOADGEN_PHASE=DASHBOARD \
      -e STREAMFLOW_LOADGEN_DASHBOARD_CLIENTS="$DASHBOARD_CLIENTS" \
      -e STREAMFLOW_LOADGEN_DASHBOARD_DURATION=3600s \
      -e STREAMFLOW_LOADGEN_RESULT_FILE="/results/${RUN_DIR#benchmarks/results/}/dashboard.json" \
      loadgen >"$RUN_DIR/dashboard.log" 2>&1 &
    DASH_PID=$!
  fi

  log "[$REP/$REPETITIONS] publish $EVENTS events at $RATE/s ($WORKLOAD)"
  compose run --rm --no-deps -e STREAMFLOW_LOADGEN_PHASE=PUBLISH -e STREAMFLOW_LOADGEN_SCENARIO="$NAME" \
    -e STREAMFLOW_LOADGEN_SEED="$SEED" -e STREAMFLOW_LOADGEN_EVENTS="$EVENTS" \
    -e STREAMFLOW_LOADGEN_RATE_PER_SECOND="$RATE" -e STREAMFLOW_LOADGEN_USERS="$USERS" \
    -e STREAMFLOW_LOADGEN_HOT_USER_PERCENT="$HOT_USER_PERCENT" \
    -e STREAMFLOW_LOADGEN_RESULT_FILE="/results/${RUN_DIR#benchmarks/results/}/publish.json" \
    loadgen >"$RUN_DIR/publish.log" 2>&1
  RUN_ID=$("$PY" -c "import json,sys; print(json.load(open(sys.argv[1]))['publish']['runId'])" "$RUN_DIR/publish.json")

  if [ "$WORKLOAD" = "backlog" ]; then
    log "[$REP/$REPETITIONS] start $WORKERS worker(s) x $CONCURRENCY consumers ($PROCESSING_MODE)"
    date +%s%3N > "$RUN_DIR/workers-started-ms"
    compose up --detach --no-deps --wait --wait-timeout 180 --scale "worker=$WORKERS" worker
    schedule_kill
  fi

  log "[$REP/$REPETITIONS] observe run $RUN_ID until drained"
  set +e
  compose run --rm --no-deps -e STREAMFLOW_LOADGEN_PHASE=OBSERVE -e STREAMFLOW_LOADGEN_RUN_ID="$RUN_ID" \
    -e STREAMFLOW_LOADGEN_DRAIN_TIMEOUT="$DRAIN_TIMEOUT" \
    -e STREAMFLOW_LOADGEN_RESULT_FILE="/results/${RUN_DIR#benchmarks/results/}/observe.json" \
    loadgen >"$RUN_DIR/observe.log" 2>&1
  OBSERVE_EXIT=$?
  set -e
  [ -n "$KILL_PID" ] && wait "$KILL_PID" 2>/dev/null || true
  stop_sampler
  if [ -n "$DASH_PID" ]; then
    docker ps -q --filter "label=com.docker.compose.project=$PROJECT"       --filter "label=com.docker.compose.service=loadgen" | xargs -r docker stop >/dev/null 2>&1 || true
    wait "$DASH_PID" 2>/dev/null || true
  fi

  if [ "$JFR" = "1" ]; then
    compose stop -t 30 worker >/dev/null
    mv benchmarks/jfr/*.jfr "$RUN_DIR/" 2>/dev/null || log "no JFR recording was written"
  fi
  compose logs --no-color worker > "$RUN_DIR/workers.log" 2>&1 || true
  manifest "$RUN_DIR/manifest.json"
  cleanup_run
  trap - EXIT

  if [ "$OBSERVE_EXIT" -ne 0 ]; then
    log "[$REP/$REPETITIONS] run $RUN_ID did NOT reconcile as PASS (exit $OBSERVE_EXIT); stopping the scenario."
    exit "$OBSERVE_EXIT"
  fi
  log "[$REP/$REPETITIONS] run $RUN_ID reconciled PASS"
done
log "Scenario $NAME complete: $OUT_ROOT"
