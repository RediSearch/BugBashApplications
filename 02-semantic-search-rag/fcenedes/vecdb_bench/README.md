# Semantic search / RAG knowledge base — `fcenedes/vecdb_bench`

> Location: [`02-semantic-search-rag/fcenedes/vecdb_bench/`](.) · part of
> [Bug Bash Applications](../../../README.md) · use case
> [Semantic search / RAG](../../README.md).

## What this app does

`vecdb_bench` is a fork of the
[vector-db-benchmark](https://github.com/redis-performance/vector-db-benchmark)
harness, trimmed down and adapted to stress a **Redis Flex (disk-backed HNSW)**
index in beta. It simulates the flagship semantic-search / RAG workload: bulk-load a
large corpus of embedding vectors into an on-disk vector index, then hammer it with
KNN queries at varying recall targets and concurrency levels, measuring ingestion
throughput, query latency (p50/p95/p99), and recall. All non-Redis engines
(Qdrant, Milvus, Elasticsearch, OpenSearch, pgvector, Weaviate, MongoDB) have been
removed so the harness targets Redis only.

The upstream harness docs are preserved in
[`UPSTREAM_README.md`](UPSTREAM_README.md).

## Schema

The index is created by
[`engine/clients/redis/configure.py`](engine/clients/redis/configure.py). The
single indexed field is a `VECTOR` field using **HNSW**, which is the only algorithm
Redis Flex supports on disk:

```
FT.CREATE <index> ... SCHEMA
  vector VECTOR HNSW 12
    TYPE FLOAT32
    DIM <dataset vector size>
    DISTANCE_METRIC <L2|COSINE|IP>
    M <16|32|64>
    EF_CONSTRUCTION <64|128|256|512>
    EF_RUNTIME 64
    RERANK TRUE
```

Redis Flex requires `M`, `EF_CONSTRUCTION`, `EF_RUNTIME`, and `RERANK` on every
vector field, and only `FLOAT32`/`FLOAT16` element types — all satisfied here.

## Dataset & scale

- **Source:** `gist-960-euclidean` (default), an ANN-benchmarks HDF5 dataset of
  1M × 960-dim GIST descriptors with Euclidean distance. Any dataset supported by
  the harness works (override with `BENCH_DATASETS`).
- **Size:** ~1M vectors × 960 dims — large enough to exercise the on-disk index
  path rather than staying resident in RAM.
- **Ingestion:** parallel bulk upload (`upload_params.parallel = 32`) via the
  Redis client in [`engine/clients/redis/`](engine/clients/redis/).

## How to run

We use a **single configuration**. All engine/HNSW variants live in one file,
[`experiments/configurations/benchmark.json`](experiments/configurations/benchmark.json),
which `run_bench.py` treats as the single source of truth (via `--engines-file`).
You don't select or edit anything else — just point the harness at your Redis Flex
endpoint and launch it.

```bash
# 1. Install dependencies
poetry install

# 2. Point at your Redis Flex (disk-index-capable) endpoint
export REDIS_HOST=<host>
export REDIS_PORT=<port>
export REDIS_AUTH=<password>      # optional
export REDIS_USER=<username>      # optional
# REDIS_DIALECT defaults to 2 — Redis Flex does not support DIALECT 4.

# 3. Run the benchmark (single command)
poetry run ./run_bench.py
```

Optional overrides (all have sensible defaults):

| Env var | Default | Purpose |
| ------- | ------- | ------- |
| `BENCH_DATASETS` | `gist-960-euclidean` | dataset to load / query |
| `BENCH_ENGINES` | `redis` | engine name |
| `BENCH_ENGINES_FILE` | `experiments/configurations/benchmark.json` | the single config file |
| `REDIS_DIALECT` | `2` | query dialect (Flex has no DIALECT 4) |

Results are written as JSON to [`results/`](results/) (sample runs are included so
you can preview the dashboard without running the benchmark first).

## Viewing the output — dashboard v2

View the results with **dashboard v2**, a Dash app that renders Pareto
latency/recall charts and a results table:

```bash
poetry run python dashboard_v2.py
```

Then open <http://localhost:8055>. It reads every run in `results/` automatically.

## Workload / queries

Pure KNN search driven from `run.py` at two concurrency levels — **serial**
(`parallel: 1`) and **high concurrency** (`parallel: 100`) — each swept across
`EF_RUNTIME`-equivalent search `ef` values of 64/128/256/512. The single config
file crosses this with HNSW build parameters `M ∈ {16, 32, 64}` and
`EF_CONSTRUCTION ∈ {64, 128, 256, 512}`, giving the full recall/latency frontier
per index build.

## What to watch

- **On-disk footprint** — the index should live on disk; watch process RAM vs.
  index size (Flex is not zero-RAM — some structures stay pinned).
- **Query latency** — p50 / p95 / p99 at `parallel: 1` and `parallel: 100`.
- **Recall** — reported per run; confirm KNN correctness against ground truth.
- **Ingestion throughput** — upload time at `parallel: 32`.
- **Flex-specific edge cases** — `FT.DROPINDEX DD` is rejected (the harness falls
  back to drop-without-DD + `FLUSHALL`, see `configure.py`); the default 500 ms
  query timeout returns partial results unless `ON_TIMEOUT FAIL` is set.

## Findings

_Record bugs, surprises, and observations from your Flex runs here. Link any tickets
you open (e.g. MOD-XXXXX)._
