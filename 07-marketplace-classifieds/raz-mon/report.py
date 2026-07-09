"""Aggregate a run directory's JSONL metrics into a human-readable report."""

import json
from collections import defaultdict
from pathlib import Path

from common import percentile_from_hist

CORRECTNESS_EVENTS = {
    "stale_hit", "content_mismatch", "missing_from_search",
    "insert_not_visible", "update_not_visible",
}


def _load_lines(run_dir: Path):
    for path in sorted(run_dir.glob("*.jsonl")):
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        continue


def report(run_dir: Path):
    ops = defaultdict(lambda: {"count": 0, "errors": 0, "timeouts": 0,
                               "warnings": 0, "hist": defaultdict(int)})
    events = defaultdict(int)
    event_samples = defaultdict(list)
    span = {}
    monitor_rows = defaultdict(list)

    for row in _load_lines(run_dir):
        kind = row.get("kind")
        if row.get("type") == "window":
            for op, s in row["ops"].items():
                agg = ops[(kind, op)]
                for k in ("count", "errors", "timeouts", "warnings"):
                    agg[k] += s.get(k, 0)
                for b, c in s["hist"].items():
                    agg["hist"][int(b)] += c
            lo, hi = span.get(kind, (row["ts"], row["ts"]))
            span[kind] = (min(lo, row["ts"] - row.get("window_s", 0)), max(hi, row["ts"]))
        elif row.get("type") == "event":
            key = (kind, row["op"])
            events[key] += 1
            if len(event_samples[key]) < 3:
                event_samples[key].append(row)
        elif kind == "ftinfo":
            monitor_rows[row["index"]].append(row)
        elif kind == "server":
            monitor_rows["__server__"].append(row)

    print(f"\n=== bazaar report: {run_dir} ===")

    for kind in ("load", "churn", "storm", "verify"):
        rows = {op: s for (k, op), s in ops.items() if k == kind}
        if not rows:
            continue
        lo, hi = span.get(kind, (0, 0))
        dur = max(hi - lo, 1e-9)
        total = sum(s["count"] for s in rows.values())
        print(f"\n-- {kind} ({total} ops, {dur:.0f}s, {total / dur:.0f} ops/s) --")
        header = f"{'op':<22}{'count':>10}{'err':>7}{'tmo':>6}{'warn':>6}" \
                 f"{'p50ms':>9}{'p95ms':>9}{'p99ms':>9}"
        print(header)
        for op in sorted(rows, key=lambda o: -rows[o]["count"]):
            s = rows[op]
            p50 = percentile_from_hist(s["hist"], 0.50)
            p95 = percentile_from_hist(s["hist"], 0.95)
            p99 = percentile_from_hist(s["hist"], 0.99)
            print(f"{op:<22}{s['count']:>10}{s['errors']:>7}{s['timeouts']:>6}"
                  f"{s['warnings']:>6}{p50:>9.1f}{p95:>9.1f}{p99:>9.1f}")

    if events:
        print("\n-- events --")
        bad = False
        for (kind, op), n in sorted(events.items(), key=lambda kv: -kv[1]):
            flag = "  <-- CORRECTNESS" if op in CORRECTNESS_EVENTS else ""
            bad = bad or bool(flag)
            print(f"{kind}/{op}: {n}{flag}")
            for sample in event_samples[(kind, op)][:2 if flag else 1]:
                detail = {k: v for k, v in sample.items()
                          if k not in ("ts", "kind", "worker", "type", "op")}
                print(f"    e.g. {detail}")
        if not bad:
            print("(no correctness failures)")
    else:
        print("\n-- events: none --")

    if monitor_rows:
        print("\n-- footprint over time (first -> last; sizes are Speedb estimates) --")
        for index, rows in sorted(monitor_rows.items()):
            if index == "__server__":
                continue
            a, b = rows[0], rows[-1]
            print(f"{index}: num_docs {a.get('num_docs')} -> {b.get('num_docs')}, "
                  f"index_mem_mb {a.get('total_index_memory_sz_mb')} -> "
                  f"{b.get('total_index_memory_sz_mb')}, "
                  f"indexing_failures {b.get('hash_indexing_failures')}")
        srv = monitor_rows.get("__server__")
        if srv:
            a, b = srv[0], srv[-1]
            gb = 1024**3
            print(f"server: keys {a.get('total_keys')} -> {b.get('total_keys')}, "
                  f"used_memory {a.get('used_memory', 0) / gb:.2f}GB -> "
                  f"{b.get('used_memory', 0) / gb:.2f}GB, "
                  f"expired_keys {b.get('expired_keys')}")
    print()


def latest_run_dir(out_dir: Path) -> Path | None:
    runs = sorted(d for d in out_dir.glob("*") if d.is_dir())
    return runs[-1] if runs else None
