#!/usr/bin/env python3
"""Finding #3 verifier: does the reported KNN distance depend on HYBRID_POLICY?

Reseeds deterministically, runs the same hybrid query under ADHOC_BF and
BATCHES, and compares each reported distance against the exact cosine
distance recomputed from the vector stored in the hash. Whichever policy
deviates from ground truth is the buggy path.

Usage: ./verify_dist.py --url redis://default:<pass>@<host>:<port> [--count 5000]
"""

import argparse

import numpy as np

import catalog
import embeddings
import store as store_mod
from store import Store


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--count", type=int, default=5000)
    parser.add_argument("--no-reseed", action="store_true")
    args = parser.parse_args()

    s = Store(store_mod.connect(args.url))
    if not args.no_reseed:
        s.create_index(recreate=True)
        s.ingest(catalog.generate(args.count, seed=42))
        print(f"reseeded {args.count} docs (seed 42)")

    query_vec = embeddings.embed_query("warm suit for cold winter waves")
    results = {}
    for policy in ("ADHOC_BF", "BATCHES"):
        _, docs = s.knn(query_vec, k=10, filters={"category": "surfing"},
                        policy=policy)
        results[policy] = docs
        print(f"\n--- {policy} (dist source: {docs[0]['_dist_source'] if docs else '?'}) ---")
        worst = 0.0
        for doc in docs:
            stored = s.get_embedding(doc["sku"])
            truth = embeddings.cosine_distance(stored, query_vec)
            reported = float(doc["dist"])
            delta = abs(reported - truth)
            worst = max(worst, delta)
            flag = "  <-- MISMATCH" if delta > 1e-3 else ""
            print(f"  {doc['sku']}  reported={reported:.6f}  "
                  f"exact={truth:.6f}  delta={delta:.2e}{flag}")
        print(f"  worst delta: {worst:.2e}")

    common = ({d["sku"] for d in results["ADHOC_BF"]} &
              {d["sku"] for d in results["BATCHES"]})
    print(f"\n--- cross-policy comparison ({len(common)} docs in both) ---")
    for sku in sorted(common):
        a = float(next(d["dist"] for d in results["ADHOC_BF"] if d["sku"] == sku))
        b = float(next(d["dist"] for d in results["BATCHES"] if d["sku"] == sku))
        flag = "  <-- POLICY-DEPENDENT" if abs(a - b) > 1e-6 else ""
        print(f"  {sku}  adhoc_bf={a:.6f}  batches={b:.6f}  "
              f"delta={abs(a - b):.2e}{flag}")


if __name__ == "__main__":
    main()
