"""Resumable bulk loader.

The doc_id space [0, total_docs) is split into contiguous ranges, one per
worker. Each worker writes pipelined batches (HSET or JSON.SET depending on
the doc's vertical) and checkpoints the next unwritten doc_id after every
batch, so a killed load continues where it left off. Indexes must already
exist (SKIPINITIALSCAN — see schema.py); run.py enforces the order.
"""

import json
import multiprocessing as mp
import time
from pathlib import Path

from common import Config, StatsWriter, make_client
from datagen import make_generator
from schema import index_doctype

BATCH = 200


def _ckpt_path(cfg: Config, worker: int) -> Path:
    return cfg.state_dir / f"load_w{worker}.txt"


def _read_ckpt(path: Path, default: int) -> int:
    try:
        return int(path.read_text().strip())
    except (FileNotFoundError, ValueError):
        return default


def write_doc(pipe, key: str, v_idx: int, fields: dict):
    if index_doctype(v_idx) == "json":
        pipe.execute_command("JSON.SET", key, "$", json.dumps(fields))
    else:
        pipe.hset(key, mapping=fields)


def load_worker(worker: int, start: int, end: int, cfg: Config, run_dir: Path):
    gen = make_generator(cfg)
    client = make_client(cfg)
    stats = StatsWriter(run_dir, "load", worker)
    ckpt = _ckpt_path(cfg, worker)
    ckpt.parent.mkdir(parents=True, exist_ok=True)
    doc_id = max(start, _read_ckpt(ckpt, start))

    last_log = time.time()
    while doc_id < end:
        batch_end = min(doc_id + BATCH, end)
        t0 = time.perf_counter()
        pipe = client.pipeline(transaction=False)
        for d in range(doc_id, batch_end):
            key, v_idx, fields = gen.generate(d, 0)
            write_doc(pipe, key, v_idx, fields)
        try:
            pipe.execute()
        except Exception as e:
            stats.record("load", (time.perf_counter() - t0) * 1000,
                         n=batch_end - doc_id, error=True)
            stats.event("load_error", error=str(e)[:300], at_doc=doc_id)
            time.sleep(1.0)  # transient (connection, OOM-reject) — retry the batch
            client = make_client(cfg)
            continue
        stats.record("load", (time.perf_counter() - t0) * 1000, n=batch_end - doc_id)
        doc_id = batch_end
        ckpt.write_text(str(doc_id))
        if time.time() - last_log > 30:
            pct = 100.0 * (doc_id - start) / max(1, end - start)
            print(f"[load w{worker}] {doc_id - start}/{end - start} ({pct:.1f}%)", flush=True)
            last_log = time.time()

    stats.close()
    print(f"[load w{worker}] done ({end - start} docs)", flush=True)


def run_load(cfg: Config, run_dir: Path):
    n, w = cfg.total_docs, cfg.load_workers
    ranges = [(i * n // w, (i + 1) * n // w) for i in range(w)]
    procs = [
        mp.Process(target=load_worker, args=(i, s, e, cfg, run_dir), name=f"load-{i}")
        for i, (s, e) in enumerate(ranges)
    ]
    t0 = time.time()
    for p in procs:
        p.start()
    for p in procs:
        p.join()
    dt = time.time() - t0
    print(f"[load] {n} docs in {dt:.0f}s ({n / max(dt, 1e-9):.0f} docs/s)", flush=True)
