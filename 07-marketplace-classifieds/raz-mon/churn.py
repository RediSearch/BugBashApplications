"""Steady-state write churn: inserts, updates, deletes, and a TTL stream.

Each worker owns an interleaved stripe of the "new listing" id space above
total_docs (id = total_docs + slot + k * workers), with k checkpointed so a
restarted churn run never reuses an id. Updates are read-modify-write: read
the doc's current `ver`, regenerate the whole listing at ver+1, rewrite —
every update re-indexes the full document. Deletes UNLINK a random listing;
a configurable fraction of inserts carries a TTL so expiration runs as a
continuous background deletion stream.
"""

import json
import multiprocessing as mp
import random
import time
from pathlib import Path

from common import Config, StatsWriter, TokenBucket, classify_error, make_client
from datagen import make_generator
from load import write_doc
from schema import index_doctype


def _ckpt_path(cfg: Config, worker: int) -> Path:
    return cfg.state_dir / f"churn_w{worker}.txt"


def _read_ver(client, key: str, v_idx: int):
    """Current version of a doc, or None if the key is gone."""
    if index_doctype(v_idx) == "json":
        raw = client.execute_command("JSON.GET", key, "$.ver")
        if raw is None:
            return None
        vals = json.loads(raw)
        return int(vals[0]) if vals else None
    raw = client.hget(key, "ver")
    return int(raw) if raw is not None else None


def churn_worker(worker: int, cfg: Config, run_dir: Path, duration: float | None):
    gen = make_generator(cfg)
    client = make_client(cfg)
    stats = StatsWriter(run_dir, "churn", worker)
    rng = random.Random(f"churn:{cfg.seed}:{worker}:{time.time()}")
    bucket = TokenBucket(cfg.churn_ops_per_sec / cfg.churn_workers)

    ckpt = _ckpt_path(cfg, worker)
    ckpt.parent.mkdir(parents=True, exist_ok=True)
    try:
        k = int(ckpt.read_text().strip())
    except (FileNotFoundError, ValueError):
        k = 0

    c = cfg.churn
    ops = ["insert", "update", "delete"]
    weights = [c["insert_pct"], c["update_pct"], c["delete_pct"]]
    stride = cfg.churn_workers
    deadline = time.time() + duration if duration else None

    while deadline is None or time.time() < deadline:
        bucket.take()
        op = rng.choices(ops, weights)[0]
        t0 = time.perf_counter()
        try:
            if op == "insert":
                doc_id = cfg.total_docs + worker + k * stride
                k += 1
                key, v_idx, fields = gen.generate(doc_id, 0)
                pipe = client.pipeline(transaction=False)
                write_doc(pipe, key, v_idx, fields)
                with_ttl = rng.random() < c["ttl_fraction"]
                if with_ttl:
                    pipe.expire(key, rng.randint(c["ttl_min_s"], c["ttl_max_s"]))
                pipe.execute()
                ckpt.write_text(str(k))
                op = "insert_ttl" if with_ttl else "insert"

            elif op == "update":
                doc_id = rng.randrange(cfg.total_docs)
                key = gen.key(doc_id)
                v_idx = gen.vertical_of(doc_id)
                ver = _read_ver(client, key, v_idx)
                if ver is None:
                    # deleted earlier — relist it, which keeps the population flat
                    op = "relist"
                    ver = -1
                key, v_idx, fields = gen.generate(doc_id, ver + 1)
                pipe = client.pipeline(transaction=False)
                write_doc(pipe, key, v_idx, fields)
                pipe.execute()

            else:  # delete — mostly base docs, sometimes this worker's own inserts
                if k > 0 and rng.random() < 0.2:
                    doc_id = cfg.total_docs + worker + rng.randrange(k) * stride
                else:
                    doc_id = rng.randrange(cfg.total_docs)
                removed = client.unlink(gen.key(doc_id))
                if not removed:
                    op = "delete_miss"

            stats.record(op, (time.perf_counter() - t0) * 1000)
        except Exception as e:
            stats.record(op, (time.perf_counter() - t0) * 1000, error=True,
                         timeout=classify_error(e) == "timeout")
            stats.event("churn_error", op=op, error=str(e)[:300])
            if classify_error(e) == "connection":
                time.sleep(1.0)
                client = make_client(cfg)

    stats.close()


def run_churn(cfg: Config, run_dir: Path, duration: float | None = None):
    procs = [
        mp.Process(target=churn_worker, args=(i, cfg, run_dir, duration), name=f"churn-{i}")
        for i in range(cfg.churn_workers)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join()
