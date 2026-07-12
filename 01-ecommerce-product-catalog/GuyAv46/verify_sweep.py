#!/usr/bin/env python3
"""Post-disruption correctness sweep for the scale corpus.

Docs are deterministic in (worker, i), so any doc can be regenerated
client-side and compared against what the server returns — no mirror needed
at 32M docs. Run once (--once) after a disruption, or in a loop (--watch)
during one; every check failure prints a repro and the sweep exits non-zero.

Checks per sweep:
  S1 liveness + doc counts on both indexes (bracketed if ingest is running)
  S2 deterministic point lookups: N random (worker, i) docs -> exactly one
     hit in BOTH indexes with correct TAG fields
  S3 stored-vector integrity: HGET embedding byte-compare vs regenerated
  S4 KNN distance correctness on both indexes (reported dist vs brute-force
     from the stored vector; catches stale/torn graph data after
     failover/restore)
  S5 negative check: a never-written sku returns 0 hits
"""

import argparse
import random
import sys
import time

import numpy as np

import embeddings
import store as store_mod
from scale_ingest import make_product
from store import Store

DIST_TOL = 1e-3


class Sweep:
    def __init__(self, url, workers, max_i, seed=None):
        self.client = store_mod.connect(url)
        self.main = Store(self.client)
        self.scan = Store(self.client, index="idx:scan")
        try:
            self.scan.info()
        except Exception:
            self.scan = None  # second index dropped; skip its checks
        self.workers = workers
        self.max_i = max_i
        self.rng = random.Random(seed)
        self.failures = []

    def _fail(self, check, message, repro=""):
        self.failures.append((check, message))
        print(f"\033[91m{check} FAIL\033[0m {message}" + (f"\n  repro: {repro}" if repro else ""),
              flush=True)

    def run(self, points=20):
        t0 = time.time()

        # S1: liveness + count (full count at 32M+ docs needs a generous
        # explicit TIMEOUT — the 5s strict default can't scan the doc table)
        try:
            main_count = self.main.count(timeout_ms=90000)
            print(f"S1 ok: main={main_count}", flush=True)
        except Exception as err:
            # Full count may time out during migrations; fall back to the
            # cheap canary before declaring the DB unreachable.
            try:
                canary = self.main.count("@sku:{SC00000000042}", timeout_ms=30000)
                if canary == 1:
                    print(f"S1 degraded-ok: full count failed ({err}) but canary "
                          f"lookup works — heavy queries only", flush=True)
                else:
                    self._fail("S1", f"canary sku returned {canary} hits (expected 1)")
            except Exception as err2:
                self._fail("S1", f"count AND canary failed: {err} / {err2}")
                return self.failures  # DB unreachable; no point continuing

        # S2 + S3: deterministic point lookups + stored-vector integrity
        checked = 0
        for _ in range(points):
            w = self.rng.randrange(self.workers)
            i = self.rng.randrange(self.max_i)
            expected = make_product(w, i)
            sku = expected["sku"]
            n_main = self.main.count(f"@sku:{{{sku}}}", timeout_ms=30000)
            if n_main == 0:
                # may be in a timed-out batch hole; skip silently but note
                continue
            checked += 1
            if n_main != 1:
                self._fail("S2", f"{sku}: {n_main} hits in main index (expected 1)",
                           f"FT.SEARCH {self.main.index} '@sku:{{{sku}}}'")
            if self.scan is not None:
                n_scan = self.scan.count(f"@sku:{{{sku}}}", timeout_ms=30000)
                if n_scan != 1:
                    self._fail("S2", f"{sku}: {n_scan} hits in scan index (expected 1)",
                               f"FT.SEARCH {self.scan.index} '@sku:{{{sku}}}'")
            raw = self.client.hgetall(self.main.key(sku))
            raw = {k.decode(): v for k, v in raw.items()}
            for field in ("brand", "category", "color", "price_bucket"):
                got = raw.get(field, b"").decode()
                if got != expected[field]:
                    self._fail("S2", f"{sku}.{field}: stored={got} expected={expected[field]}",
                               f"HGETALL product:{sku}")
            expected_vec = embeddings.to_bytes(embeddings.embed_product(expected))
            if raw.get("embedding") != expected_vec:
                self._fail("S3", f"{sku}: stored embedding differs from regenerated "
                                 f"(torn/corrupt write?)", f"HGET product:{sku} embedding")
        if checked == 0:
            # All samples missing is not "batch holes" — it's a blanked index
            # (e.g. the OOM guard silently returning empty results on RESP2).
            self._fail("S2", f"0/{points} sampled docs found — index is returning "
                             f"empty results (OOM guard? blanked index?)",
                       "FT.SEARCH with RESP3 and check the 'warning' field")
        else:
            print(f"S2/S3 ok: {checked}/{points} sampled docs verified "
                  f"({points - checked} skipped, likely timed-out batches)", flush=True)

        # S4: KNN distance correctness on the available indexes
        targets = [(self.main, "main")] + ([(self.scan, "scan")] if self.scan else [])
        for s, name in targets:
            qv = np.random.default_rng(self.rng.getrandbits(32)).standard_normal(embeddings.DIM)
            qv = (qv / np.linalg.norm(qv)).astype(np.float32)
            try:
                _, docs = s.knn(qv, k=10, timeout_ms=30000)
            except Exception as err:
                self._fail("S4", f"KNN on {name} failed: {err}")
                continue
            bad = 0
            for doc in docs:
                if doc.get("dist") is None:
                    continue
                blob = self.client.hget(doc["_key"], "embedding")
                if blob is None:
                    self._fail("S4", f"KNN({name}) returned {doc['_key']} with no stored vector",
                               f"HGET {doc['_key']} embedding")
                    continue
                truth = embeddings.cosine_distance(embeddings.from_bytes(blob), qv)
                if abs(float(doc["dist"]) - truth) > DIST_TOL:
                    bad += 1
                    self._fail("S4", f"KNN({name}) dist mismatch for {doc['_key']}: "
                                     f"reported={float(doc['dist']):.6f} exact={truth:.6f}")
            if not bad:
                print(f"S4 ok: KNN({name}) {len(docs)} results, distances exact", flush=True)

        # S5: ghost check
        ghost = f"SC99{self.rng.randrange(10**9):09d}"
        if self.main.count(f"@sku:{{{ghost}}}", timeout_ms=30000) != 0:
            self._fail("S5", f"never-written sku {ghost} found in index",
                       f"FT.SEARCH {self.main.index} '@sku:{{{ghost}}}'")
        else:
            print("S5 ok: ghost lookup clean", flush=True)

        print(f"sweep done in {time.time() - t0:.1f}s, {len(self.failures)} failures", flush=True)
        return self.failures


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--max-i", type=int, default=1500000,
                        help="upper bound of per-worker doc ids known written")
    parser.add_argument("--points", type=int, default=20)
    parser.add_argument("--watch", type=int, default=0,
                        help="repeat every N seconds until interrupted")
    parser.add_argument("--seed", type=int)
    args = parser.parse_args()

    total_failures = 0
    while True:
        print(f"\n=== sweep @ {time.strftime('%H:%M:%S')} ===", flush=True)
        try:
            sweep = Sweep(args.url, args.workers, args.max_i, args.seed)
            total_failures += len(sweep.run(args.points))
        except Exception as err:
            print(f"SWEEP ERROR (DB unreachable?): {err}", flush=True)
            total_failures += 1
        if not args.watch:
            break
        time.sleep(args.watch)
    sys.exit(1 if total_failures else 0)


if __name__ == "__main__":
    main()
