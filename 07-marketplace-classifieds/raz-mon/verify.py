"""Correctness probes: read-your-writes, update visibility, no stale hits.

Two probe kinds run in a slow loop (this is a correctness oracle, not a load
generator):

- Lifecycle probe: write a fresh listing in a reserved id space (>= 10^12, so
  it never collides with load/churn ids), poll until it is visible in a
  seller-scoped search, verify the stored doc matches the deterministic
  generator byte-for-byte, update it and poll for the new content, then
  delete it and poll until it disappears — a hit after the grace period is a
  stale hit. The recorded latency of each phase is the time-to-visibility.

- Sample probe: pick a random base doc, read it in one round trip, regenerate
  the expected content from its stored `ver`, and compare; then assert it is
  present in its seller's search results.
"""

import json
import multiprocessing as mp
import random
import time
from pathlib import Path

from common import Config, StatsWriter, make_client, reply_keys
from datagen import make_generator
from load import write_doc
from schema import index_doctype, index_name

VERIFY_ID_BASE = 10**12
VISIBILITY_GRACE_S = 10.0
POLL_INTERVAL_S = 0.2


def _read_doc(client, key: str, v_idx: int) -> dict | None:
    if index_doctype(v_idx) == "json":
        raw = client.execute_command("JSON.GET", key, "$")
        if raw is None:
            return None
        docs = json.loads(raw)
        return docs[0] if docs else None
    doc = client.hgetall(key)
    return doc or None


def _seller_keys(client, v_idx: int, seller: str) -> list:
    reply = client.execute_command(
        "FT.SEARCH", index_name(v_idx), f"@seller:{{{seller}}}",
        "NOCONTENT", "LIMIT", "0", "500")
    return reply_keys(reply)


def _poll(check, grace=VISIBILITY_GRACE_S) -> float | None:
    """Run `check` until true; returns elapsed ms, or None if grace expired."""
    t0 = time.perf_counter()
    while True:
        if check():
            return (time.perf_counter() - t0) * 1000
        if time.perf_counter() - t0 > grace:
            return None
        time.sleep(POLL_INTERVAL_S)


def _content_matches(stored: dict, expected: dict) -> bool:
    return stored is not None and all(str(stored.get(k)) == v for k, v in expected.items())


def lifecycle_probe(client, gen, stats, doc_id: int):
    key, v_idx, fields = gen.generate(doc_id, 0)
    seller = fields["seller"]

    # insert -> visible in the seller-scoped search, and stored verbatim
    write_doc(client, key, v_idx, fields)
    ms = _poll(lambda: key in _seller_keys(client, v_idx, seller))
    if ms is None:
        stats.event("insert_not_visible", key=key, index=index_name(v_idx))
        client.unlink(key)
        return
    stats.record("probe_insert_visible", ms)
    if not _content_matches(_read_doc(client, key, v_idx), fields):
        stats.event("content_mismatch", key=key, phase="insert")

    # update -> new content stored, doc still found (full re-index path)
    _, _, fields2 = gen.generate(doc_id, 1)
    write_doc(client, key, v_idx, fields2)
    ms = _poll(lambda: _content_matches(_read_doc(client, key, v_idx), fields2)
               and key in _seller_keys(client, v_idx, seller))
    if ms is None:
        stats.event("update_not_visible", key=key, index=index_name(v_idx))
    else:
        stats.record("probe_update_visible", ms)

    # delete -> must drop out of results (stale-hit detection)
    client.unlink(key)
    ms = _poll(lambda: key not in _seller_keys(client, v_idx, seller))
    if ms is None:
        stats.event("stale_hit", key=key, index=index_name(v_idx))
    else:
        stats.record("probe_delete_gone", ms)


def sample_probe(client, gen, stats, rng, total_docs: int):
    doc_id = rng.randrange(total_docs)
    v_idx = gen.vertical_of(doc_id)
    key = gen.key(doc_id)
    t0 = time.perf_counter()
    stored = _read_doc(client, key, v_idx)
    if stored is None:
        stats.record("sample_deleted", (time.perf_counter() - t0) * 1000)
        return
    _, _, expected = gen.generate(doc_id, int(stored.get("ver", 0)))
    if not _content_matches(stored, expected):
        stats.event("content_mismatch", key=key, phase="sample",
                    ver=stored.get("ver"), stored_title=str(stored.get("title"))[:80],
                    expected_title=expected["title"][:80])
        return
    # a doc that exists must be searchable — allow one retry for indexing lag
    seller = expected["seller"]
    if key not in _seller_keys(client, v_idx, seller):
        time.sleep(2.0)
        if _read_doc(client, key, v_idx) is not None \
                and key not in _seller_keys(client, v_idx, seller):
            stats.event("missing_from_search", key=key, seller=seller,
                        index=index_name(v_idx))
            return
    stats.record("sample_ok", (time.perf_counter() - t0) * 1000)


def verify_worker(worker: int, workers: int, cfg: Config, run_dir: Path,
                  duration: float | None):
    gen = make_generator(cfg)
    client = make_client(cfg)
    stats = StatsWriter(run_dir, "verify", worker)
    rng = random.Random(f"verify:{cfg.seed}:{worker}:{time.time()}")
    deadline = time.time() + duration if duration else None
    iteration = 0

    while deadline is None or time.time() < deadline:
        try:
            doc_id = VERIFY_ID_BASE + worker + iteration * workers
            lifecycle_probe(client, gen, stats, doc_id)
            sample_probe(client, gen, stats, rng, cfg.total_docs)
        except Exception as e:
            stats.event("verify_error", error=str(e)[:300])
            time.sleep(1.0)
            client = make_client(cfg)
        iteration += 1
        time.sleep(1.0)

    stats.close()


def run_verify(cfg: Config, run_dir: Path, duration: float | None = None, workers: int = 2):
    procs = [
        mp.Process(target=verify_worker, args=(i, workers, cfg, run_dir, duration),
                   name=f"verify-{i}")
        for i in range(workers)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join()
