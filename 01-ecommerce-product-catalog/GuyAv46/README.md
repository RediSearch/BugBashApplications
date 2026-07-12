# E-commerce product catalog — `GuyAv46`

**Ride & Tide** — a motorbikes / surfing / sailing gear store on Redis Search on
Disk. Beyond the demo storefront it is a **self-verifying stress harness**: every
document is deterministic in its id, so any doc (and its vector) can be
regenerated client-side and compared against what the server returns — full
correctness checking at tens of millions of docs with no client-side mirror.

## What this app does

Simulates a catalog customer: faceted browse (brand/category/color/price-bucket
TAGs), full-text search over names/descriptions, "similar items" + free-text
semantic search via a 384-dim FLOAT32 HNSW vector field, price sorting/stats via
`FT.AGGREGATE APPLY to_number()` over an unindexed hash field (the no-NUMERIC
workaround), and constant merchandising churn (restocks, reprices, description
rewrites, deletions).

## Schema

```
FT.CREATE idx:products ON HASH PREFIX 1 product: SCHEMA
  sku TAG  name TEXT WEIGHT 2  description TEXT
  brand TAG  category TAG  subcategory TAG  color TAG
  price_bucket TAG  in_stock TAG
  embedding VECTOR HNSW 14 TYPE FLOAT32 DIM 384 DISTANCE_METRIC COSINE
    M 16 EF_CONSTRUCTION 200 EF_RUNTIME 10 RERANK TRUE
```

(`price` is stored unindexed for `to_number()` aggregations. On builds where
`SKIPINITIALSCAN` is mandatory / `RETURN` fields or `FT.AGGREGATE` are blocked,
the data layer auto-detects and degrades to `NOCONTENT` + client-side hydration
and client-side facets.)

## Dataset & scale

- **Source:** synthetic generator (`catalog.py`), deterministic in a seed:
  `make_product(worker, i)` always yields the same product and embedding.
  Embeddings are seeded subcategory cluster centers + hashed bag-of-words —
  no model download, meaningful KNN neighborhoods, exact reproducibility.
- **Size:** tested to **32.8M docs (~500GB)** on a cloud Flex DB.
- **Ingestion:** `scale_ingest.py`, multiprocess pipelined HSETs
  (~5k docs/s from a single WAN client at 16 workers).

## How to run

```
pip install -r requirements.txt
./app.py --url redis://default:<pass>@<host>:<port> seed --count 5000 --recreate
./app.py --url ... search "waterproof jacket" --category sailing
./app.py --url ... semantic "warm suit for winter waves" --policy BATCHES
./app.py --url ... chaos --ops 5000 --verify-every 250        # oracle mode
./scale_ingest.py --url ... --target 50000000 --workers 16     # blow it up
./stress_queries.py --url ... --duration 14400                 # invariants + latency
./verify_sweep.py --url ... --max-i <written> --watch 120      # during cluster ops
./verify_dist.py --url ...                                     # rerank/distance oracle
```

## Workload / queries

TEXT search, exact TAG facets, `FT.AGGREGATE` GROUPBY facets + numeric reducers,
pure KNN, pre-filtered hybrid KNN under both `ADHOC_BF` and `BATCHES`
(explicit `HYBRID_POLICY`), point lookups, churn (add/delete/update), background
initial-scan backfill (`bg_scan_test.py`), and sustained multi-hour bulk writes.

## What to watch

- **chaos mode:** in-memory reference model; verifies counts, TAG-filter counts,
  point-lookup freshness, ghost docs, KNN distance correctness (reported vs
  brute-force from stored vectors) and hybrid filter containment.
- **verify_sweep / stress:** mirror-free invariants that hold at any scale —
  deterministic doc regeneration, stored-vector byte-compare, KNN distance
  recompute, sorted-order and metric-range checks, canary lookups.
  **Lesson learned:** always keep a known-present canary doc that must return
  exactly one hit — count-based invariants pass vacuously when the OOM guard
  silently blanks all results (RESP2 gives no other signal).
- Latency percentiles per query family; verbatim first-occurrence logging of
  every distinct server error (write-throttle stalls are findings, not noise).

## Findings

Found during the MS2 bug bash (tracking epic RED-207622):

- **MOD-16803** — `ADHOC_BF` hybrid KNN ignores index-level `RERANK TRUE`:
  SQ8-quantized distances returned as scores, wrong top-K membership/order.
  Root-caused to the rerank gate in `hybrid_reader.c`; `verify_dist.py` is the
  oracle that caught and isolates it.
- **MOD-16806** — `FT.CREATE` background initial scan starves foreground writes
  (~97% throughput loss for the entire scan; client socket timeouts). Root
  cause: flat-buffer full → `PostponeClients`, which the internal scanner
  bypasses. Follow-up experiment proved the flat buffer is a **soft** cap —
  O(dataset) transient RAM during scans, not O(1024).
- **MOD-16327** (comment) — field confirmation of the OOM `return`-policy
  blindness: at 100% DB capacity all searches silently return empty on RESP2
  while writes continue and `FT.INFO` looks healthy.
- Plus observations recorded in the internal findings log: master-vs-MS2
  surface divergences, `percent_indexed=1.0` while vectors are still staged in
  the RAM flat buffer, strict-TIMEOUT checkpoint granularity under disk load,
  and `used_memory` semantics on Flex.
