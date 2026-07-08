# Chat / messaging with retention — `kei-nan`

`chatstress` is a stress-test harness for **use case 6** — a multi-tenant chat
archive where every message is written with a **whole-key TTL** and ingest +
expiration run continuously in parallel. It hammers the on-disk index on the
axes that matter for retention workloads and ships a **live web dashboard** so
you can watch the generated conversations expire and inspect DB stats in real
time.

## What this app does

It simulates a messaging backend: tenants → channels → users → threads →
messages. A pool of workers continuously **ingests** messages (each `HSET` + a
whole-key `PEXPIRE`), while other workers **refresh** TTLs on active threads
(sliding expiration), **edit** and **delete** messages, and run **channel- /
user- / thread-scoped full-text queries**. Messages expire on their own via
Redis key expiry, so at steady state the expire-rate converges on the
ingest-rate — exactly the "continuous deletion stream" this use case targets.

A client-side **correctness oracle** verifies the headline property — **no stale
hits**: a document that has expired (or was deleted comfortably in the past) must
never appear in query results. A **web dashboard** (served by the harness itself)
shows the live conversations, an interactive scoped-search box, and charts of the
DB stats (footprint over time, indexed docs vs records, throughput, latency).

## Schema

Only `TEXT` and `TAG` are indexable on disk, so:

```
FT.CREATE chat ON HASH PREFIX 1 msg: SKIPINITIALSCAN SCHEMA
  body       TEXT           # message content
  tenant_id  TAG            # t<id>
  channel_id TAG            # c<id>
  user_id    TAG            # u<id>
  thread_id  TAG            # th<id>
  plan       TAG            # retention tier name
```

- **`SKIPINITIALSCAN` is required** and the index is created **before** any data
  is loaded (existing keys are not back-indexed on disk).
- Each message HASH also carries unindexed `seq` and `ts` fields (used only for
  client-side "recent first" ordering — there is **no `NUMERIC` field**, per the
  disk constraints; retention is modeled as a `plan` **TAG**).
- Whole-key TTL via `PEXPIRE` (hash-field TTL is intentionally **not** used — it
  isn't reflected in the disk index).

## Dataset & scale

- **Source:** synthetic generator (`internal/gendata`) — bodies are drawn from a
  fixed content-word vocabulary (no stopwords, so any body word is a valid search
  term; a few unicode words exercise multilingual tokenization).
- **Size:** driven by config. `config/bugbash.yaml` uses a large id-space
  (5k tenants × 200 channels, millions of users/threads) and unbounded ingest so
  total bytes exceed the server's RAM budget — the point of on-disk indexes.
- **Ingestion:** pipelined `HSET`+`PEXPIRE` batches across N workers. TTLs are
  drawn from a weighted **retention-tier mix** (disappearing / free / pro /
  enterprise) so short- and long-lived docs coexist in one index.

## How to run

**Prerequisites:** Docker (the build uses a `golang:1.23-alpine` container — no
host Go needed) and a Redis to point at. The real disk path needs a
**Flex/BigRedis-capable** server (a plain OSS `redis-server` has no on-disk
search); see [`deploy/flex-redis.conf`](deploy/flex-redis.conf).

```bash
# 1. Build the static binary (via Docker Go toolchain)
./build.sh              # -> ./bin/chatstress    (or: make build)

# 2a. Real disk run: start a Flex server, then point the harness at it
redis-server deploy/flex-redis.conf              # loadmodule = your redisearch.so
./bin/chatstress run -config config/bugbash.yaml -addr 127.0.0.1:6379

# 2b. In-RAM smoke run (validates the harness logic only — NOT the disk path)
REDIS_MODULE=/path/to/redisearch.so PORT=6399 ./scripts/local-redis.sh start
./bin/chatstress run -flush -config config/smoke.yaml -addr 127.0.0.1:6399

# 3. Restart / RDB-reload correctness check
./bin/chatstress reload-check -config config/smoke.yaml -addr 127.0.0.1:6399
```

Then open the dashboard: **http://localhost:8080**. The dashboard stays up after
the run completes so you can inspect the final state (Ctrl-C to exit).

Confirm the disk path is actually live: `redis-cli INFO search | grep search_disk_usage`
(that field only appears for a disk-backed index).

Key flags: `-addr`, `-config`, `-duration`, `-web-addr`, `-flush`, `-no-web`,
`-seed`. Everything else (scale, worker counts, rates, TTL tiers, sampling,
oracle sampling) is in the YAML config.

## Workload / queries

| Worker | What it drives |
| ------ | -------------- |
| ingesters | `HSET` + whole-key `PEXPIRE` (pipelined), tier-weighted TTLs |
| sliding-TTL refresher | re-`PEXPIRE` messages on active threads (TTL-update path) |
| editor | `HSET body` on a live message (re-index) |
| deleter | `UNLINK` a live message (de-index) |
| query workers | `FT.SEARCH chat "@channel_id:{c…} <term>" NOCONTENT LIMIT 0 N DIALECT 2`; 15% run unscoped term queries to stress high-cardinality posting lists |

Rates, worker counts and durations are all configurable. Expiry itself needs no
worker — Redis expires keys and the index excludes them at query time.

## What to watch

- **No stale hits (correctness).** For every query the oracle captures `t0`
  before issuing it; a returned doc is a violation if it expired at/​before `t0`
  (strict — expiry has a synchronous read-time filter) or was deleted more than a
  grace window before `t0` (deletion de-indexes asynchronously). A separate probe
  repeatedly searches for **known-expired** docs and asserts they're absent. The
  dashboard shows a red badge and the summary reports `FAIL` if any occur.
- **Footprint plateau.** The sampler reads `search_disk_usage` and `FT.INFO`
  (`num_docs`, `num_records`, `inverted_sz_mb`) over time and fits a slope over
  the steady-state window → **PLATEAU vs GROWING**. Under expiry churn at steady
  state, on-disk footprint should plateau rather than grow unbounded.
- **GC / compaction.** Periodically forces `_FT.DEBUG DISK_FLUSH` +
  `GC_FORCEINVOKE` (best-effort; no-op off the disk path) and samples
  `compaction_total_cycles` / `estimate_pending_compaction_bytes` /
  `async_reads_expired`.
- **Query latency** (p50/p90/p99) and **throughput** (ingest/s, query/s).
- **Post-restart correctness** via `reload-check` (`DEBUG RELOAD` = RDB
  save+load): expired docs stay gone, live docs survive, no stale hits.

Artifacts are written to `./out/` (`summary.json`, `metrics.csv`).

## Local validation performed

Smoke-tested in-RAM against a local OSS `redis-server` + the disk-capable
`redisearch.so` (in-RAM mode — the harness's `search_disk_*` sampling degrades
gracefully when those fields are absent). Over a 30s run at 2k msg/s + 200 q/s:
index created, ingest/expiry churn observed, **0 stale hits** (524 expired-doc
probes, all absent), ~98% recall, and `reload-check` passed (live docs preserved,
expired docs absent before and after `DEBUG RELOAD`).

> The disk path itself must be exercised on a provisioned Flex/BigRedis server —
> it can't run against plain OSS Redis.

## Findings

_To be filled in during the bug bash. Link any tickets you open (e.g. MOD-XXXXX)._

- _(footprint plateau vs growth under sustained expiry churn — attach `metrics.csv` / dashboard screenshots)_
- _(any stale hits, recall anomalies, GC/compaction surprises, restart/RDB issues)_
