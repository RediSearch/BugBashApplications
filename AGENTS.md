# AGENTS.md

Guidance for AI coding agents (and humans) building applications in this repo.

## What this repo is

A collection of small, self-contained applications that stress-test **Redis Search
on disk-backed indexes** at scale. Each app simulates a realistic customer workload
and hammers it to surface bugs. See the root [`README.md`](README.md) for the
use-case catalog and folder layout.

## Where to put code

- Build inside `<use-case>/<your-github-username>/` — **not** at the repo root or
  directly in a use-case folder.
- Start by copying the use-case's `_TEMPLATE/` folder and renaming the copy to the
  GitHub username, then fill in that folder's `README.md`.
- Keep each app self-contained: its own dependencies, run instructions, and README.

## Hard constraints (easy to get wrong)

- **Only `TEXT`, `TAG`, and `VECTOR` field types are supported on disk.** Do **not**
  put `NUMERIC` or `GEO` fields in `FT.CREATE` schemas here — even though standard
  RediSearch supports them. If a workload needs a numeric range, model it as bucketed
  `TAG`s; for geo, use a geohash-prefixed `TAG`.
- **Lean on scale.** On-disk indexes exist for data that doesn't fit in RAM. Size
  datasets accordingly (large doc counts / total bytes) — a toy dataset won't
  exercise the disk path.
- Requires a **disk-index-capable Redis build**; a plain OSS `redis-server` has no
  on-disk search. Document the exact server and version your app needs in its README.

## Known limitations

Disk-backed search is more restricted than in-RAM RediSearch — several of these are
things an agent will otherwise get wrong. For the authoritative and complete list,
see the internal design doc **"Redis Search on Flex — MS2 — Limitations."**

**Schema / `FT.CREATE`**

- Field types: **`TEXT`, `TAG`, `VECTOR` only** — `NUMERIC`, `GEO`, and `GEOSHAPE`
  are rejected. Model numeric ranges as bucketed `TAG`s; geo as geohash-prefix `TAG`s.
- No per-field `SORTABLE`, `NOINDEX`, or `INDEXMISSING`; no `WITHSUFFIXTRIE`.
- JSON: **single-value JSONPath only** (no multi-value paths), for all field types.
- Only these `FT.CREATE` args are accepted: `ON`, `PREFIX`, `FILTER`,
  `LANGUAGE`/`LANGUAGE_FIELD`, `SCORE`/`SCORE_FIELD`, `STOPWORDS`, `SKIPINITIALSCAN`.
- **`SKIPINITIALSCAN` is required** — keys that already exist when the index is
  created are **not** back-indexed. Create the index *before* loading data.
- Max **10 indexes** per DB.
- `FT.DROPINDEX` does not accept `DD` (can't delete the doc keys along with the index).

**Vectors**

- **HNSW only** (no FLAT / SVS-VAMANA).
- Element type **`FLOAT32` or `FLOAT16` only** (no FLOAT64 / BF16 / UINT8 / INT8).
- `M`, `EF_CONSTRUCTION`, `EF_RUNTIME`, and `RERANK` are **all mandatory** on every
  vector field.
- Pre-filtered (hybrid) KNN requires an explicit **`HYBRID_POLICY`** — no auto-select.

**Queries**

- **TAG**: no prefix / suffix / infix, wildcard, or lexrange queries.
- Unsupported `FT.SEARCH` options: `SUMMARIZE`, `HIGHLIGHT`, `WITHOUTCOUNT`, `SLOP`,
  `INORDER`, `DIALECT 4`.
- `FT.AGGREGATE`: no `WITHCURSOR`. A numeric `SORTBY` sorts **lexicographically**
  (no `NUMERIC` in the schema) — wrap with `APPLY to_number(@field)` for real numeric
  ordering.
- Unsupported commands include `FT.HYBRID`, `FT.ALTER`, `FT.DICT*`, `FT.SYN*`,
  `FT.SPELLCHECK`, `FT.TAGVALS`, `FT.CURSOR`, the `FT.SUG*` suggestion family, and the
  deprecated `FT.ADD/DEL/GET` family.

**Runtime behavior to expect**

- **Hash-field TTL is not reflected in the index** — a doc won't drop from results
  just because a hash field expired. Whole-**key** `EXPIRE` *is* honored.
- Queries have a **default 500 ms timeout** and return **partial results** by default;
  set `ON_TIMEOUT FAIL` to turn timeouts into hard errors.
- The OOM guard only checks **before** a query starts, not during execution — large
  `LIMIT`/offset, wide `GROUPBY`, or big `LOAD`/`SORTBY` result sets can still drive
  real OOM. Keep result sets bounded.
- "Disk mode" is not zero-RAM: some structures stay pinned in process RAM, so provision
  RAM headroom rather than assuming the index lives entirely on disk.

## Conventions

- Any language is fine (Python, Node, Go, Rust, ...). Prefer whatever loads and
  queries data fastest for your workload.
- Don't commit datasets, `dump.rdb` / `*.aof`, `node_modules/`, virtualenvs, or build
  output — see [`.gitignore`](.gitignore).
- Record findings (bugs, surprising behavior, metrics) in your app's `README.md`.

## Definition of done for an app

- Loads a large dataset into a disk-backed index.
- Drives the use-case's query patterns (see the use-case's `README.md`).
- Reports what to watch: correctness, latency, on-disk footprint, recall, GC/compaction.
- Its `README.md` explains how to run it end-to-end.
