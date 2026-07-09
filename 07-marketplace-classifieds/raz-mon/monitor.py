"""Periodic sampler: FT.INFO per index + server INFO -> JSONL time series.

On disk indexes several FT.INFO memory fields are placeholders or Speedb
estimates — we record them anyway (the interesting question is the *trend*:
does the footprint plateau under steady-state churn?) and report.py labels
them as estimates.
"""

import json
import time
from pathlib import Path

from common import Config, make_client
from datagen import VERTICALS
from schema import index_name

FTINFO_FIELDS = [
    "num_docs", "num_terms", "num_records", "indexing", "percent_indexed",
    "hash_indexing_failures", "inverted_sz_mb", "doc_table_size_mb",
    "total_index_memory_sz_mb", "bytes_per_record_avg",
    "total_indexing_time", "gc_stats", "cursor_stats",
]

SERVER_FIELDS = [
    "used_memory", "used_memory_rss", "maxmemory", "mem_fragmentation_ratio",
    "expired_keys", "evicted_keys", "keyspace_hits", "keyspace_misses",
    "total_commands_processed", "instantaneous_ops_per_sec",
]


def _to_plain(v):
    if isinstance(v, dict):
        return {str(k): _to_plain(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        if len(v) % 2 == 0 and all(isinstance(k, (str, bytes)) for k in v[::2]):
            return {str(v[i]): _to_plain(v[i + 1]) for i in range(0, len(v), 2)}
        return [_to_plain(x) for x in v]
    return v


def sample_once(client, out_fh):
    ts = time.time()
    for v_idx in range(len(VERTICALS)):
        name = index_name(v_idx)
        try:
            info = _to_plain(client.execute_command("FT.INFO", name))
            row = {k: info.get(k) for k in FTINFO_FIELDS if k in info}
            out_fh.write(json.dumps(
                {"ts": ts, "kind": "ftinfo", "index": name, **row}) + "\n")
        except Exception as e:
            out_fh.write(json.dumps(
                {"ts": ts, "kind": "ftinfo_error", "index": name, "error": str(e)[:200]}) + "\n")
    try:
        info = client.info()
        row = {k: info.get(k) for k in SERVER_FIELDS}
        dbs = {k: v for k, v in info.items() if k.startswith("db")}
        row["total_keys"] = sum(d.get("keys", 0) for d in dbs.values() if isinstance(d, dict))
        out_fh.write(json.dumps({"ts": ts, "kind": "server", **row}) + "\n")
    except Exception as e:
        out_fh.write(json.dumps({"ts": ts, "kind": "server_error", "error": str(e)[:200]}) + "\n")
    out_fh.flush()


def run_monitor(cfg: Config, run_dir: Path, duration: float | None = None,
                interval_s: float = 30.0):
    client = make_client(cfg)
    path = Path(run_dir) / "monitor.jsonl"
    deadline = time.time() + duration if duration else None
    with open(path, "a", buffering=1) as fh:
        while deadline is None or time.time() < deadline:
            try:
                sample_once(client, fh)
            except Exception:
                time.sleep(5.0)
                client = make_client(cfg)
            time.sleep(interval_s)
