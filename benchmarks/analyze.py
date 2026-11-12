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
    return json.loads(path.read_text()) if path.exists() else None


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
    with path.open() as f:
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
        "scenario": manifest["scenario"]["NAME"],
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
    path.write_text("\n".join(parts))


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

    recov = [(n, v) for n, v in summary.items() if v[1]["recoverySeconds"]]
    if recov:
        lines += ["", "## Worker kill and recovery", "",
                  "Longest time any partition's lag stopped falling after one worker was killed (SIGKILL).", "",
                  "| Scenario | Longest partition stall s | Duplicates suppressed |", "|---|---|---|"]
        for name, (c, s, ok) in recov:
            lines.append(f"| `{name}` | {fmt(s['recoverySeconds'], 1)} | {fmt(s['duplicates'])} |")

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
