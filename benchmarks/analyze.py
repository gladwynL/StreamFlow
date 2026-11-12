#!/usr/bin/env python3
"""Summarises benchmark results (benchmarks/results/<stamp>-<scenario>/r<n>/) into docs/benchmarks/results.md.

Standard library only. For every scenario it reports the median, minimum and maximum of each metric over the
repetitions, and draws SVG charts. Runs that did not reconcile as PASS are listed and excluded from the statistics.

    python benchmarks/analyze.py [results-dir] [output-markdown]
"""
import csv
import json
import math
import statistics
import sys
from datetime import datetime
from pathlib import Path

RESULTS = Path(sys.argv[1] if len(sys.argv) > 1 else "benchmarks/results")
OUTPUT = Path(sys.argv[2] if len(sys.argv) > 2 else "docs/benchmarks/results.md")
CHARTS = OUTPUT.parent / "charts"


def load(path):
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def epoch_ms(iso):
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000 if iso else None


def parse_mem(text):
    value = text.split("/")[0].strip()
    units = {"KiB": 1 / 1024, "MiB": 1, "GiB": 1024, "B": 1 / 1048576, "kB": 1 / 1000 / 1.048576}
    for unit, factor in units.items():
        if value.endswith(unit):
            return float(value[: -len(unit)]) * factor
    return None


def resources(run_dir, start_ms, end_ms):
    """Mean and peak CPU (% of one core) and peak memory (MiB) per container role while events were processed."""
    path = run_dir / "docker-stats.csv"
    if not path.exists() or start_ms is None:
        return {}
    per = {}
    with path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                t = int(row["epoch_ms"])
            except (TypeError, ValueError):
                continue
            if not (start_ms <= t <= end_ms + 1000):
                continue
            name = row["container"].replace("streamflow-bench-", "")
            role = name.rsplit("-", 1)[0] if name.rsplit("-", 1)[-1].isdigit() else name
            role = "loadgen" if role.startswith("loadgen") else role
            cpu = float(row["cpu_percent"].rstrip("%") or 0)
            mem = parse_mem(row["mem_usage"]) or 0
            per.setdefault(role, {}).setdefault(t, []).append((cpu, mem))
    out = {}
    for role, samples in per.items():
        cpus = [sum(c for c, _ in v) for v in samples.values()]
        mems = [sum(m for _, m in v) for v in samples.values()]
        out[role] = {"cpuMean": statistics.mean(cpus), "cpuMax": max(cpus), "memMaxMiB": max(mems)}
    return out


def recovery(observe, run_dir):
    """Longest time any partition's lag stopped falling after a worker was killed (seconds), from the timeline."""
    killed = run_dir / "worker-killed-ms"
    if not killed.exists() or not observe or "observeStartedEpochMs" not in observe:
        return None
    kill_t = int(killed.read_text().strip()) - observe["observeStartedEpochMs"]
    timeline = [s for s in observe["timeline"] if s.get("lagByPartition")]
    longest = 0.0
    partitions = len(timeline[0]["lagByPartition"]) if timeline else 0
    for p in range(partitions):
        stall_start = None
        last = None
        for s in timeline:
            lag = s["lagByPartition"][p]
            if s["t"] < kill_t:
                last = lag
                continue
            if lag > 0 and last is not None and lag >= last:
                stall_start = stall_start if stall_start is not None else s["t"]
                longest = max(longest, (s["t"] - stall_start) / 1000)
            else:
                stall_start = None
            last = lag
    return longest


def run_metrics(run_dir):
    publish, observe, manifest = (load(run_dir / f) for f in ("publish.json", "observe.json", "manifest.json"))
    if not (publish and observe and manifest):
        return None
    consumer = observe["consumer"]
    start, end = epoch_ms(consumer.get("firstProcessedAt")), epoch_ms(consumer.get("lastProcessedAt"))
    wal = [w for w in (observe.get("walIo") or []) if w["backend_type"] == "client backend"]
    statements = observe.get("topStatements") or []
    db_ms = sum(float(s["total_ms"]) for s in statements)
    partitions = observe.get("partitions") or []
    counts = [p["events"] for p in partitions]
    return {
        "scenario": manifest["scenario"]["NAME"] + " @" + manifest["gitCommit"][:7],
        "config": manifest["scenario"],
        "run": str(run_dir),
        "commit": manifest["gitCommit"][:7],
        "result": observe["reconciliation"]["result"],
        "acked": publish["publish"]["acked"],
        "producerPerSecond": publish["publish"]["ackedPerSecond"],
        "recordBytes": (publish.get("producerMetrics") or {}).get("record-size-avg"),
        "consumerPerSecond": consumer.get("eventsPerSecond"),
        "drainSeconds": consumer.get("spanSeconds"),
        "processed": observe["reconciliation"]["processed"],
        "duplicates": observe["reconciliation"]["duplicatesSuppressed"],
        "p50": observe["latency"]["p50Millis"],
        "p95": observe["latency"]["p95Millis"],
        "p99": observe["latency"]["p99Millis"],
        "commits": (observe.get("database") or {}).get("xact_commit"),
        "walFsyncs": sum(w["fsyncs"] or 0 for w in wal),
        "walFsyncMs": sum(w["fsync_time"] or 0 for w in wal),
        "dbStatementMs": db_ms,
        "topStatements": statements[:6],
        "partitionEvents": counts,
        "skew": (max(counts) / (sum(counts) / len(counts))) if counts else None,
        "resources": resources(run_dir, start, end),
        "recoverySeconds": recovery(observe, run_dir),
        "maxLatency": observe["latency"]["maxMillis"],
        "peakLag": max(((s.get("lag") or 0) for s in observe.get("timeline") or []), default=None),
    }


def stats(values):
    values = [v for v in values if v is not None and not (isinstance(v, float) and math.isnan(v))]
    if not values:
        return None
    return {"median": statistics.median(values), "min": min(values), "max": max(values), "n": len(values)}


def fmt(s, digits=0, unit=""):
    if not s:
        return "—"
    f = f"{{:,.{digits}f}}"
    if s["n"] == 1 or s["min"] == s["max"]:
        return f.format(s["median"]) + unit
    return f"{f.format(s['median'])}{unit} ({f.format(s['min'])}–{f.format(s['max'])})"


def svg_bars(path, title, groups, unit):
    """Grouped horizontal bars: groups = [(label, median, min, max)]. Whiskers show the range."""
    width, row, left = 760, 30, 260
    height = 50 + row * len(groups)
    top = max((g[3] or g[1] or 0) for g in groups) or 1
    scale = (width - left - 90) / top
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" font-family="system-ui, sans-serif"'
        f' font-size="12" role="img" aria-label="{title}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="12" y="22" font-size="14" font-weight="600" fill="#111">{title}</text>',
    ]
    for i, (label, median, low, high) in enumerate(groups):
        y = 40 + i * row
        parts.append(f'<text x="{left - 8}" y="{y + 14}" text-anchor="end" fill="#333">{label}</text>')
        w = (median or 0) * scale
        parts.append(f'<rect x="{left}" y="{y + 3}" width="{w:.1f}" height="16" rx="3" fill="#2a78d6"/>')
        if low is not None and high is not None and high > low:
            parts.append(
                f'<line x1="{left + low * scale:.1f}" x2="{left + high * scale:.1f}" y1="{y + 11}" y2="{y + 11}"'
                ' stroke="#111" stroke-width="1.5"/>'
            )
        parts.append(f'<text x="{left + w + 6:.1f}" y="{y + 15}" fill="#111">{median:,.0f} {unit}</text>')
    parts.append("</svg>")
    path.write_text("\n".join(parts), encoding="utf-8")

# ---- appended sections (inserted before the "Runs" table by main) ----

def latest(summary, name):
    """The newest commit's result for a scenario name (summary keys are 'name @commit')."""
    keys = [k for k in summary if k.split(" @")[0] == name]
    if not keys:
        return None
    # The newest run directory sorts last (UTC timestamp prefix).
    keys.sort(key=lambda k: max(r["run"] for r in summary[k][2]))
    return summary[keys[-1]]


def max_lag(observe_runs):
    return stats([max((s.get("lag") or 0) for s in r["timeline"]) if r.get("timeline") else None for r in observe_runs])


def extra_sections(summary):
    out = []
    # Before / after the record-mode optimisation.
    keys = sorted((k for k in summary if k.startswith("drain-record-1w-c3 @")),
                  key=lambda k: min(r["run"] for r in summary[k][2]))
    if len(keys) >= 2:
        out += ["", "## Record mode before and after the single-statement effects change", "",
                "| Commit | n | Consumer processed/s | Drain s | Statement time ms | WAL fsync time ms |",
                "|---|---|---|---|---|---|"]
        for k in keys:
            c, s, ok = summary[k]
            out.append(f"| `{k.split(' @')[1]}` | {len(ok)} | {fmt(s['consumerPerSecond'])} | {fmt(s['drainSeconds'], 1)} |"
                       f" {fmt(s['dbStatementMs'])} | {fmt(s['walFsyncMs'])} |")

    # Scaling matrix.
    rows = []
    for label, suffix in (("1 × 1", "1w-c1"), ("2 × 1", "2w-c1"), ("3 × 1", "3w-c1"), ("6 × 1", "6w-c1"),
                          ("1 × 6", "1w-c6")):
        rec, bat = latest(summary, f"scale-record-{suffix}"), latest(summary, f"scale-batch-{suffix}")
        rows.append((label, rec, bat))
    base_rec = rows[0][1][1]["consumerPerSecond"]["median"] if rows[0][1] else None
    base_bat = rows[0][2][1]["consumerPerSecond"]["median"] if rows[0][2] else None
    out += ["", "## Consumer scaling (6 partitions, backlog drain)", "",
            "Workers × consumer threads per worker. Speed-up is relative to 1 × 1 of the same mode.", "",
            "| Workers × threads | Record processed/s | Speed-up | Batch processed/s | Speed-up |", "|---|---|---|---|---|"]
    chart = []
    for label, rec, bat in rows:
        r = rec[1]["consumerPerSecond"] if rec else None
        b = bat[1]["consumerPerSecond"] if bat else None
        su_r = f"{r['median'] / base_rec:.2f}×" if r and base_rec else "—"
        su_b = f"{b['median'] / base_bat:.2f}×" if b and base_bat else "—"
        out.append(f"| {label} | {fmt(r)} | {su_r} | {fmt(b)} | {su_b} |")
        if r:
            chart.append((f"record {label}", r["median"], r["min"], r["max"]))
        if b:
            chart.append((f"batch {label}", b["median"], b["min"], b["max"]))
    if chart:
        svg_bars(CHARTS / "scaling.svg", "Backlog drain throughput by workers × threads (events/s)", chart, "/s")
        out += ["", "![Scaling](charts/scaling.svg)"]

    # Latency under steady load.
    live = sorted((k for k in summary if k.startswith("live-")), key=lambda k: (k.split("-")[1], int(k.split("-")[4].split(" ")[0])))
    if live:
        out += ["", "## Latency under steady load (live workload, 60 s)", "",
                "Kafka append to start of the processing transaction, exact percentiles over every event of the run.",
                "", "| Scenario | Offered/s | Processed/s | p50 ms | p95 ms | p99 ms | Max ms |",
                "|---|---|---|---|---|---|---|"]
        for k in live:
            c, s, ok = summary[k]
            out.append(f"| `{k}` | {c['RATE']} | {fmt(s['consumerPerSecond'])} | {fmt(s['p50'], 1)} | {fmt(s['p95'], 1)} |"
                       f" {fmt(s['p99'], 1)} | {fmt(stats([r['maxLatency'] for r in ok]))} |")
    return out


def main():
    runs = []
    for run_dir in sorted(RESULTS.glob("*/r*")):
        m = run_metrics(run_dir)
        if m:
            runs.append(m)
    scenarios = {}
    for r in runs:
        scenarios.setdefault(r["scenario"], []).append(r)
    CHARTS.mkdir(parents=True, exist_ok=True)

    lines = [
        "# Benchmark results (generated)",
        "",
        "Generated by `python benchmarks/analyze.py` from the raw results in `benchmarks/results/`. Do not edit by",
        "hand. Values are medians over the repetitions with the range (min–max) in brackets; *n* is the number of",
        "repetitions that reconciled as PASS. Methodology and interpretation: [methodology.md](methodology.md),",
        "[README.md](README.md).",
        "",
        "## Throughput, latency and accounting",
        "",
        "| Scenario | n | Mode | Workers × consumers | Producer acked/s | Consumer processed/s | Drain s |"
        " Latency p50 / p95 / p99 ms | Processed | Duplicates |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    failed = []
    summary = {}
    for name, rs in scenarios.items():
        ok = [r for r in rs if r["result"] == "PASS"]
        failed += [r for r in rs if r["result"] != "PASS"]
        if not ok:
            continue
        c = ok[0]["config"]
        s = {k: stats([r[k] for r in ok]) for k in (
            "producerPerSecond", "consumerPerSecond", "drainSeconds", "p50", "p95", "p99", "processed",
            "duplicates", "commits", "walFsyncs", "walFsyncMs", "dbStatementMs", "skew", "recoverySeconds",
            "recordBytes")}
        summary[name] = (c, s, ok)
        lines.append(
            f"| `{name}` | {len(ok)} | {c['PROCESSING_MODE']} | {c['WORKERS']} × {c['CONCURRENCY']} |"
            f" {fmt(s['producerPerSecond'])} | {fmt(s['consumerPerSecond'])} | {fmt(s['drainSeconds'], 1)} |"
            f" {fmt(s['p50'])} / {fmt(s['p95'])} / {fmt(s['p99'])} | {fmt(s['processed'])} |"
            f" {fmt(s['duplicates'])} |"
        )

    lines += ["", "## Database work per run", "",
              "| Scenario | Transactions committed | WAL fsyncs (client) | WAL fsync time ms | Statement time ms |"
              " Events per transaction |",
              "|---|---|---|---|---|---|"]
    for name, (c, s, ok) in summary.items():
        per_tx = stats([r["processed"] / r["commits"] for r in ok if r["commits"]])
        lines.append(f"| `{name}` | {fmt(s['commits'])} | {fmt(s['walFsyncs'])} | {fmt(s['walFsyncMs'])} |"
                     f" {fmt(s['dbStatementMs'])} | {fmt(per_tx, 1)} |")

    lines += ["", "## Container resources while events were processed", "",
              "CPU is in percent of one core (Docker stats, about one sample per second); memory is the peak.", "",
              "| Scenario | Worker CPU mean / peak | PostgreSQL CPU mean / peak | Kafka CPU mean | Worker memory peak MiB |"
              " PostgreSQL memory peak MiB |",
              "|---|---|---|---|---|---|"]
    for name, (c, s, ok) in summary.items():
        def res(role, key):
            return stats([r["resources"].get(role, {}).get(key) for r in ok])
        lines.append(
            f"| `{name}` | {fmt(res('worker', 'cpuMean'))} / {fmt(res('worker', 'cpuMax'))} |"
            f" {fmt(res('postgres', 'cpuMean'))} / {fmt(res('postgres', 'cpuMax'))} | {fmt(res('kafka', 'cpuMean'))} |"
            f" {fmt(res('worker', 'memMaxMiB'))} | {fmt(res('postgres', 'memMaxMiB'))} |")

    skewed = [(n, v) for n, v in summary.items() if v[1]["skew"]]
    lines += ["", "## Partition distribution", "",
              "Skew = events on the busiest partition ÷ mean events per partition (1.0 = perfectly even).", "",
              "| Scenario | Skew | Events per partition (first repetition) |", "|---|---|---|"]
    for name, (c, s, ok) in skewed:
        lines.append(f"| `{name}` | {fmt(s['skew'], 2)} | {', '.join(str(x) for x in ok[0]['partitionEvents'])} |")

    recov = [(n, v) for n, v in summary.items() if v[0].get("KILL_WORKER_AFTER")]
    if recov:
        lines += ["", "## Worker kill and recovery", "",
                  "One of two workers is killed (SIGKILL) and restarted 15 s later. *Partition stall* is the longest time any",
                  "partition's lag stopped falling after the kill, from the lag timeline (backlog runs only: in live runs the",
                  "timeline starts after publishing and does not cover the kill). *Max latency* is the longest any event",
                  "waited from its Kafka append to processing.", "",
                  "| Scenario | Workload | Partition stall s | Max latency ms | p99 ms | Duplicates suppressed | Result |",
                  "|---|---|---|---|---|---|---|"]
        for name, (c, s, ok) in recov:
            stall = fmt(s['recoverySeconds'], 1) if c["WORKLOAD"] == "backlog" else "n/a"
            lines.append(f"| `{name}` | {c['WORKLOAD']} | {stall} | {fmt(stats([r['maxLatency'] for r in ok]))} |"
                         f" {fmt(s['p99'])} | {fmt(s['duplicates'])} | {len(ok)} × PASS |")

    lines += ["", "## Top statements (first repetition of each scenario)", ""]
    for name, (c, s, ok) in summary.items():
        lines += [f"**`{name}`**", "", "| Total ms | Calls | Mean ms | Statement |", "|---|---|---|---|"]
        for st in ok[0]["topStatements"]:
            q = st["query"].replace("|", "\\|")[:110]
            lines.append(f"| {float(st['total_ms']):,.0f} | {st['calls']:,} | {float(st['mean_ms']):.3f} | `{q}` |")
        lines.append("")

    groups = [(n, v[1]["consumerPerSecond"]) for n, v in summary.items() if v[1]["consumerPerSecond"]]
    if groups:
        svg_bars(CHARTS / "consumer-throughput.svg", "Consumer throughput (events/s, median; line = range)",
                 [(n, g["median"], g["min"], g["max"]) for n, g in groups], "/s")
        lines += ["## Charts", "", "![Consumer throughput](charts/consumer-throughput.svg)", ""]

    lines += extra_sections(summary) + [""]
    if failed:
        lines += ["## Runs excluded (did not reconcile as PASS)", ""]
        lines += [f"- `{r['run']}`: {r['result']}" for r in failed]
    runs_table = ["", "## Runs", "", "| Run | Commit | Result |", "|---|---|---|"]
    runs_table += [f"| `{r['run']}` | {r['commit']} | {r['result']} |" for r in runs]
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text("\n".join(lines + runs_table) + "\n", encoding="utf-8")
    print(f"{len(runs)} runs, {len(summary)} scenarios -> {OUTPUT}")


if __name__ == "__main__":
    main()
