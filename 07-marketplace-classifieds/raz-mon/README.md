# bazaar — marketplace churn stress app (`raz-mon`)

## What this app does

Simulates a second-hand marketplace ("bazaar"): tens of millions of listings that are
posted, **constantly edited** (price drops, description tweaks — every edit re-indexes
the full document), and removed when sold or expired, while buyers hammer the search
side. All components are driven by a **deterministic generator** — every listing is a
pure function of `(seed, doc_id, version)` — so the loader is resumable, the churn
writers need no state store, and a verifier can independently recompute what any
document should contain and catch stale or corrupted index results.

## Schema

Six indexes, one per marketplace vertical, **alternating HASH and JSON** document
types (multi-index also stresses the shared write-buffer-manager path):

| Index | ON | Prefix | Share |
|---|---|---|---|
| `idx:bazaar_electronics` | HASH | `l0:` | 28% |
| `idx:bazaar_fashion` | JSON | `l1:` | 22% |
| `idx:bazaar_home` | HASH | `l2:` | 18% |
| `idx:bazaar_vehicles` | JSON | `l3:` | 14% |
| `idx:bazaar_sports` | HASH | `l4:` | 10% |
| `idx:bazaar_collectibles` | JSON | `l5:` | 8% |

Every index has the same logical schema (JSON via single-value JSONPaths):

- TEXT: `title`, `description` (~200 words, capped ~1M-term vocabulary — unique terms
  are pinned RAM on disk indexes, so this is a deliberate knob)
- TAG: `brand` (5k), `category` (~60), `city` (500), `condition` (5),
  `seller` (**2M — high cardinality**), `price_bucket` (50 — NUMERIC is unsupported
  on disk, so prices are bucketed)
- `price` / `ver`: raw fields **outside the schema**; `price` is used via
  `FT.AGGREGATE ... LOAD ... APPLY to_number(@price)`, `ver` is the doc's version.

Created with `SKIPINITIALSCAN` — on disk indexes pre-existing keys are never
back-indexed, so `setup` must run **before** `load` (run.py enforces this).

## Dataset & scale

- **Source:** synthetic, fully deterministic (see `datagen.py`).
- **Size (`full` profile):** 50M docs × ~2KB ≈ **100GB** raw keyspace + index,
  split across the six indexes. `smoke` profile: 100k docs for sanity runs.
- **Ingestion:** `load` — N processes × pipelined `HSET`/`JSON.SET`, doc-id-range
  partitioned, checkpointed after every batch (kill it and rerun to resume).

## How to run

Requires a **disk-index-capable Redis build** (Flex / RediSearchEnterprise); plain
OSS redis-server has no on-disk search.

```bash
pip install -r requirements.txt

# point at your DB: edit config.yaml, or use flags / env vars
# (--host/--port/--password/--tls or BAZAAR_HOST/BAZAAR_PORT/BAZAAR_PASSWORD/BAZAAR_TLS)

./run.py setup   --profile full --host <endpoint> --port <port> --password <pw>
./run.py load    --profile full ...            # resumable; rerun after any crash
./run.py mixed   --profile full --duration 14400 ...   # churn+storm+verify+monitor, 4h
./run.py report                                 # aggregate the latest run

# or end-to-end (smoke):
./run.py all --profile smoke --duration 300
```

## Workload / queries

- **churn** (default 3k ops/s total, `full` profile): 30% inserts (10% of them with a
  1–24h TTL → continuous expiry stream), 40% read-modify-write updates (full doc
  regenerated at `ver+1`), 30% `UNLINK` deletes — population stays roughly flat.
- **storm** (16 processes, max throughput): weighted mix over all indexes —
  single-term / AND / OR / phrase, TEXT prefix (`word*`) and fuzzy (`%word%`),
  text+tag facets, high-cardinality `@seller` scoping, negations, bounded deep
  pagination, and `FT.AGGREGATE` (facet counts; `to_number(@price)` averages).
  All queries stay inside the disk-index limits (no SLOP, no TAG wildcards,
  no WITHCURSOR, ...).
- **verify** (slow loop, the correctness oracle): lifecycle probes in a reserved
  id space — insert → visible in seller-scoped search & stored verbatim → update →
  new content visible → delete → **must disappear (stale-hit detection)**; plus
  random sampling of base docs (stored content == regenerated content, and
  existing docs must be searchable). Uses RESP3 so reply warnings
  (timeout/OOM partials) are counted.

## What to watch

`./run.py report` summarizes each run:

- **Correctness:** `stale_hit`, `content_mismatch`, `missing_from_search`,
  `*_not_visible` events — any of these is a bug-bash finding.
- **Latency:** per-template p50/p95/p99 (merged log-scale histograms), plus
  time-to-visibility for insert/update/delete probes.
- **Guardrails:** timeout rate (strict `ON_TIMEOUT FAIL`), RESP3 warnings,
  `SEARCH_DISK_*` error codes.
- **Footprint:** FT.INFO `num_docs` / index-size estimates + server memory over
  time — under steady-state churn the on-disk footprint should **plateau**, not grow.

## Findings

_TBD — will be filled during the bug bash._
