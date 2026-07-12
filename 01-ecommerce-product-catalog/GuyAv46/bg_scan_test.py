#!/usr/bin/env python3
"""Background initial-scan test: create a second index (vector field) over a
prefix that ALREADY holds millions of docs, while bulk ingest keeps writing.

Questions this answers:
  1. Does the async initial scan complete, and how long does it take at scale?
  2. What's the scan rate over time — and does foreground write throttling
     starve it (WBM contention)?
  3. Does the new index converge to the same doc count as the existing index
     on the same prefix (eventual completeness), with no
     hash_indexing_failures?
  4. Are early docs actually queryable in the new index (TAG + KNN spot
     checks), not just counted?

Usage: ./bg_scan_test.py --url ... [--poll 15]
"""

import argparse
import time

import embeddings
import store as store_mod
from scale_ingest import make_product
from store import Store

SCAN_INDEX = "idx:scan"


def ft_info(store, index):
    reply = store.r.execute_command("FT.INFO", index)
    info = {}
    for i in range(0, len(reply), 2):
        key = reply[i].decode() if isinstance(reply[i], bytes) else reply[i]
        info[key] = reply[i + 1]
    return info


def num(value):
    if isinstance(value, bytes):
        value = value.decode()
    return float(value)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--poll", type=int, default=15)
    parser.add_argument("--stall-limit", type=int, default=40,
                        help="polls with zero scan progress before flagging a stall")
    args = parser.parse_args()

    client = store_mod.connect(args.url)
    s = Store(client)  # main index handle (idx:products)
    scan_store = Store(client, index=SCAN_INDEX)

    main_docs_at_start = int(num(ft_info(s, s.index)["num_docs"]))
    print(f"[bg-scan] main index num_docs at start: {main_docs_at_start}", flush=True)

    t_create = time.time()
    # Same prefix as the live index; NO SKIPINITIALSCAN -> async backfill.
    client.execute_command(
        "FT.CREATE", SCAN_INDEX, "ON", "HASH", "PREFIX", "1", s.prefix,
        "SCHEMA",
        "sku", "TAG",
        "category", "TAG",
        "embedding", "VECTOR", "HNSW", "14",
        "TYPE", "FLOAT32", "DIM", str(embeddings.DIM),
        "DISTANCE_METRIC", "COSINE",
        "M", "16", "EF_CONSTRUCTION", "200", "EF_RUNTIME", "10",
        "RERANK", "TRUE")
    print(f"[bg-scan] {SCAN_INDEX} created (async initial scan started)", flush=True)

    prev_docs, stall_polls = 0, 0
    while True:
        time.sleep(args.poll)
        try:
            info = ft_info(scan_store, SCAN_INDEX)
            main_docs = int(num(ft_info(s, s.index)["num_docs"]))
        except Exception as err:
            print(f"[bg-scan] INFO ERROR: {err}", flush=True)
            continue
        docs = int(num(info["num_docs"]))
        indexing = int(num(info.get("indexing", 0)))
        pct = num(info.get("percent_indexed", 0))
        failures = int(num(info.get("hash_indexing_failures", 0)))
        rate = (docs - prev_docs) / args.poll
        gap = main_docs - docs
        print(f"[bg-scan] t={time.time() - t_create:.0f}s scan_docs={docs} "
              f"rate={rate:.0f}/s main_docs={main_docs} gap={gap} "
              f"indexing={indexing} pct={pct:.3f} failures={failures}", flush=True)
        if failures:
            print(f"[bg-scan] VIOLATION: hash_indexing_failures={failures}", flush=True)
        if docs == prev_docs and indexing:
            stall_polls += 1
            if stall_polls >= args.stall_limit:
                print(f"[bg-scan] VIOLATION: scan made no progress for "
                      f"{stall_polls * args.poll}s while indexing=1 "
                      f"(docs={docs}, gap={gap}) — starved by write throttling?",
                      flush=True)
                stall_polls = 0
        else:
            stall_polls = 0
        prev_docs = docs
        if not indexing and pct >= 1.0:
            break

    elapsed = time.time() - t_create
    print(f"\n[bg-scan] SCAN COMPLETE in {elapsed:.0f}s "
          f"({prev_docs / max(elapsed, 1):.0f} docs/s avg)", flush=True)

    # --- eventual completeness: counts should stay in lockstep (bracketed,
    # both indexes keep ingesting the live stream)
    a1 = int(num(ft_info(s, s.index)["num_docs"]))
    b = int(num(ft_info(scan_store, SCAN_INDEX)["num_docs"]))
    a2 = int(num(ft_info(s, s.index)["num_docs"]))
    if not (a1 <= b + 5000 and b <= a2 + 5000):
        print(f"[bg-scan] VIOLATION: scan index count {b} not converged to "
              f"main index [{a1}, {a2}]", flush=True)
    else:
        print(f"[bg-scan] counts converged: scan={b} main=[{a1}, {a2}]", flush=True)

    # --- spot checks: earliest scale docs must be queryable in the NEW index
    misses = 0
    for w in range(4):
        for i in (0, 1, 2, 100, 1000):
            sku = make_product(w, i)["sku"]
            n = scan_store.count(f"@sku:{{{sku}}}")
            if n != 1:
                misses += 1
                print(f"[bg-scan] VIOLATION: {sku} not found in {SCAN_INDEX} "
                      f"(count={n}) — backfilled doc missing", flush=True)
    print(f"[bg-scan] spot checks done ({misses} misses/20)", flush=True)

    _, docs = scan_store.knn(embeddings.embed_query("carbon surfboard"), k=5)
    print(f"[bg-scan] KNN on scan index returned {len(docs)} results "
          f"(top: {[d.get('sku') for d in docs[:3]]})", flush=True)
    print("[bg-scan] DONE", flush=True)


if __name__ == "__main__":
    main()
