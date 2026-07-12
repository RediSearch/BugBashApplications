#!/usr/bin/env python3
"""Query stress + invariant checks that need NO client-side mirror, so they
work at 50M+ docs (where the chaos oracle's in-RAM mirror can't).

Invariants checked every round (violations printed with repro commands):
  I1  category counts partition the catalog: sum(count per category) == count(*)
  I2  FT.INFO num_docs == FT.SEARCH '*' total
  I3  tag-filtered results actually satisfy the filter (containment)
  I4  KNN returns <= k results, distances non-decreasing and within metric range
  I5  hybrid KNN (both policies) respects the filter
  I6  deterministic point lookup: regenerate doc (worker, i) client-side,
      verify the indexed copy has identical TAG fields
Latency is tracked per query family; timeouts (5s strict default) and every
distinct server error are recorded verbatim — those are findings, not noise.

Usage: ./stress_queries.py --url ... --duration 3600 [--written-per-worker N --workers 8]
"""

import argparse
import random
import time

import numpy as np
import redis

import embeddings
import store as store_mod
from scale_ingest import make_product
from store import FACET_VOCAB, Store

METRIC_RANGE = (-1e-6, 2.0 + 1e-6)  # cosine distance


class Stress:
    def __init__(self, s, rng, written_per_worker=0, workers=0):
        self.s = s
        self.rng = rng
        self.written = written_per_worker
        self.workers = workers
        self.lat = {}          # family -> list of seconds
        self.violations = []
        self.errors = {}       # verbatim error -> count

    def _fail(self, inv, message, repro):
        self.violations.append((inv, message))
        print(f"\n\033[91m{inv} VIOLATION\033[0m {message}\n  repro: {repro}",
              flush=True)

    def _timed(self, family, fn, repro):
        t0 = time.time()
        try:
            result = fn()
        except redis.RedisError as err:
            key = f"{type(err).__name__}: {err}"
            self.errors[key] = self.errors.get(key, 0) + 1
            if self.errors[key] == 1:
                print(f"\n[{time.strftime('%H:%M:%S')}] QUERY ERROR ({family}): "
                      f"{key}\n  repro: {repro}", flush=True)
            return None
        self.lat.setdefault(family, []).append(time.time() - t0)
        return result

    # ------------------------------------------------------------- rounds

    def round(self):
        rng, s = self.rng, self.s

        # I1 + I2: partition & info consistency. Counts race with concurrent
        # ingest, so bracket them between two '*' totals: any monotonic write
        # load keeps total_before <= sum(parts) <= total_after. Only readings
        # outside the bracket (or searchable > indexed) are real violations.
        total_before = self._timed("count", lambda: s.count(),
                                   "FT.SEARCH idx '*' LIMIT 0 0")
        if total_before is not None:
            parts = {}
            for cat in FACET_VOCAB["category"]:
                n = self._timed("count", lambda c=cat: s.count(f"@category:{{{c}}}"),
                                f"FT.SEARCH idx '@category:{{{cat}}}' LIMIT 0 0")
                if n is None:
                    parts = None
                    break
                parts[cat] = n
            total_after = self._timed("count", lambda: s.count(),
                                      "FT.SEARCH idx '*' LIMIT 0 0")
            if parts is not None and total_after is not None:
                if not (total_before <= sum(parts.values()) <= total_after):
                    self._fail("I1", f"sum(category counts)={sum(parts.values())} "
                                     f"outside [{total_before}, {total_after}] ({parts})",
                               "compare the five count queries above")
                try:
                    info = s.info()
                    num_docs = int(info.get(b"num_docs", info.get("num_docs")))
                except Exception:
                    num_docs = None
                # num_docs (indexed) may run ahead of searchable while the
                # indexing pipeline drains; searchable ahead of indexed is
                # an impossible state.
                if num_docs is not None and num_docs < total_before:
                    self._fail("I2", f"FT.INFO num_docs={num_docs} < searchable "
                                     f"total={total_before}", "FT.INFO idx")

        # I3: filter containment on a random tag pair
        field = rng.choice(["category", "color", "price_bucket", "in_stock"])
        value = rng.choice(FACET_VOCAB[field])
        got = self._timed("tag_search",
                          lambda: s.text_search(None, {field: value}, limit=20),
                          f"FT.SEARCH idx '@{field}:{{{value}}}' LIMIT 0 20")
        if got:
            for doc in got[1]:
                if doc.get(field) != value:
                    self._fail("I3", f"@{field}:{{{value}}} returned doc "
                                     f"{doc.get('sku')} with {field}={doc.get(field)}",
                               f"FT.SEARCH idx '@{field}:{{{value}}}'")

        # I4: pure KNN sanity with a random unit vector
        qv = np.random.default_rng(rng.getrandbits(32)).standard_normal(embeddings.DIM)
        qv = (qv / np.linalg.norm(qv)).astype(np.float32)
        k = rng.choice([1, 10, 100])
        got = self._timed("knn", lambda: s.knn(qv, k=k), f"KNN {k} random vector")
        if got:
            docs = got[1]
            if len(docs) > k:
                self._fail("I4", f"KNN k={k} returned {len(docs)} results", "KNN query")
            dists = [float(d["dist"]) for d in docs if d.get("dist") is not None]
            if any(b < a - 1e-9 for a, b in zip(dists, dists[1:])):
                self._fail("I4", f"KNN distances not sorted: {dists}", "KNN query")
            if any(not (METRIC_RANGE[0] <= d <= METRIC_RANGE[1]) for d in dists):
                self._fail("I4", f"KNN distance out of cosine range: {dists}", "KNN query")

        # I5: hybrid containment, both policies
        cat = rng.choice(FACET_VOCAB["category"])
        policy = rng.choice(["ADHOC_BF", "BATCHES"])
        got = self._timed(f"hybrid_{policy}",
                          lambda: s.knn(qv, k=10, filters={"category": cat}, policy=policy),
                          f"(@category:{{{cat}}})=>[KNN 10 ... HYBRID_POLICY {policy}]")
        if got:
            for doc in got[1]:
                if doc.get("category") != cat:
                    self._fail("I5", f"{policy} leaked: {doc.get('sku')} category="
                                     f"{doc.get('category')} filter={cat}",
                               f"hybrid {policy} @category:{{{cat}}}")

        # I6: deterministic point lookup (only when scale ingest bounds known)
        if self.written and self.workers:
            w = rng.randrange(self.workers)
            i = rng.randrange(self.written)
            expected = make_product(w, i)
            got = self._timed("sku_lookup",
                              lambda: s.text_search(None, {"sku": expected["sku"]}),
                              f"FT.SEARCH idx '@sku:{{{expected['sku']}}}'")
            if got:
                total, docs = got
                if total != 1:
                    self._fail("I6", f"sku {expected['sku']} (worker {w}, doc {i}): "
                                     f"expected exactly 1 hit, got {total}",
                               f"FT.SEARCH idx '@sku:{{{expected['sku']}}}'")
                else:
                    for f in ("brand", "category", "color", "price_bucket"):
                        if docs[0].get(f) != expected[f]:
                            self._fail("I6", f"{expected['sku']} field {f}: "
                                             f"indexed={docs[0].get(f)} expected={expected[f]}",
                                       f"HGETALL product:{expected['sku']}")

    def summary(self):
        lines = []
        for family, xs in sorted(self.lat.items()):
            arr = np.array(xs)
            lines.append(f"{family}: n={len(arr)} p50={np.percentile(arr, 50)*1000:.0f}ms "
                         f"p95={np.percentile(arr, 95)*1000:.0f}ms max={arr.max()*1000:.0f}ms")
        return lines


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--duration", type=int, default=600, help="seconds")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--written-per-worker", type=int, default=0,
                        help="enable I6 lookups for scale docs below this offset")
    parser.add_argument("--workers", type=int, default=0,
                        help="worker count used by scale_ingest")
    parser.add_argument("--report", type=int, default=60)
    args = parser.parse_args()

    s = Store(store_mod.connect(args.url))
    stress = Stress(s, random.Random(args.seed),
                    args.written_per_worker, args.workers)
    started = last = time.time()
    rounds = 0
    while time.time() - started < args.duration:
        stress.round()
        rounds += 1
        if time.time() - last >= args.report:
            last = time.time()
            print(f"[stress] rounds={rounds} violations={len(stress.violations)} "
                  f"distinct_errors={len(stress.errors)} | "
                  + " | ".join(stress.summary()), flush=True)
    print(f"\nDONE rounds={rounds} violations={len(stress.violations)} "
          f"errors={sum(stress.errors.values())}", flush=True)
    for line in stress.summary():
        print("  " + line, flush=True)
    for err, n in stress.errors.items():
        print(f"  error x{n}: {err}", flush=True)


if __name__ == "__main__":
    main()
