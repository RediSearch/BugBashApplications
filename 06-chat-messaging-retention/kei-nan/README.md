# Chat / messaging with retention — `kei-nan`

`chatstress` is a stress-test harness for **use case 6** — a multi-tenant chat
archive where every message is written with a **whole-key TTL** and ingest +
expiration run continuously in parallel. It hammers the on-disk index on the
axes that matter for retention workloads, deliberately probes the documented
Flex **weak-points**, and ships a **live web dashboard** with high-level status,
an index-status panel, tunable query controls, and an ad-hoc query editor.

## What this app does

It simulates a messaging backend: tenants → channels → users → threads →
messages. Workers continuously **ingest** messages (`HSET` + whole-key `PEXPIRE`),
**refresh** TTLs on active threads (sliding expiration), **edit** and **delete**
messages, and run a configurable **mix of query profiles**. Messages expire on
their own via Redis key expiry, so at steady state expire-rate ≈ ingest-rate.

A client-side **correctness oracle** verifies the headline property — **no stale
hits**: a document that has expired (or was deleted comfortably in the past) must
never appear in query results. The **web dashboard** (served by the harness)
shows high-level status, the live conversations, index status, and lets you
tune the query workload and fire ad-hoc queries at the cluster in real time.

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

- **`SKIPINITIALSCAN` is required**; the index is created **before** any data
  (existing keys aren't back-indexed on disk).
- **No `NUMERIC`** — retention is a `plan` **TAG**. Each message HASH also stores
  unindexed `seq` and `ts` fields (used for client-side ordering and for the
  `recent_timeline` aggregate, which `LOAD`s `@ts` and orders it with
  `APPLY to_number(@ts)` — numeric `SORTBY` sorts lexicographically on Flex).
- Whole-key TTL via `PEXPIRE` (hash-field TTL is **not** reflected in the index).

**Do we need offsets / which schema keywords?** On Flex the only accepted
`FT.CREATE` args are `ON, PREFIX, FILTER, LANGUAGE(_FIELD), SCORE(_FIELD),
STOPWORDS, SKIPINITIALSCAN` — `NOOFFSETS`, `NOFREQS`, `NOHL`, `NOFIELDS` are all
**rejected**. So term offsets are **always stored** and can't be turned off; they
live **on disk** in Speedb, not in pinned RAM. The pinned-RAM cost is instead the
**TEXT term dictionary**, which scales with the number of *distinct* terms
(~100 MB per 1M terms). That is the real schema-level lever and a Flex weak-point,
so the generator can inflate distinct-term count via `body.vocab_high` (see below)
rather than trying to trim offsets. Stemming is left on (chat search benefits).

## Dataset & scale

- **Source:** synthetic generator (`internal/gendata`) — bodies from a content-word
  vocabulary (no stopwords; a few unicode words exercise tokenization).
- **`body.vocab_high`:** sprinkles synthetic unique-ish terms into bodies to grow
  the on-disk term dictionary (the pinned-RAM weak-point). `bugbash.yaml` sets 5M.
- **Size:** `config/bugbash.yaml` uses a large id-space (5k tenants × 200 channels,
  millions of users/threads) and unbounded ingest so total bytes exceed RAM.
- **Ingestion:** pipelined `HSET`+`PEXPIRE`; TTLs from a weighted **retention-tier
  mix** (disappearing / free / pro / enterprise) so short- and long-lived docs
  coexist in one index.

## How to run

**Prerequisites:** Docker (build uses a `golang:1.23-alpine` container — no host
Go needed) and a Redis. The real disk path needs a **Flex/BigRedis** server (see
[`deploy/flex-redis.conf`](deploy/flex-redis.conf)); plain OSS Redis has no on-disk
search but works for exercising the harness in-RAM.

```bash
./build.sh              # -> ./bin/chatstress    (or: make build)

# Real disk run against a Flex server:
redis-server deploy/flex-redis.conf
./bin/chatstress run -config config/bugbash.yaml -addr 127.0.0.1:6379

# Demo / in-RAM (e.g. Redis Stack or OSS redis + module):
docker run -d --rm -p 6379:6379 redis/redis-stack-server:latest
./bin/chatstress run -flush -config config/smoke.yaml -addr 127.0.0.1:6379

# Restart / RDB-reload correctness check:
./bin/chatstress reload-check -config config/smoke.yaml -addr 127.0.0.1:6379
```

Open the dashboard at **http://localhost:8080**. It stays up after the run
completes so you can inspect the final state (Ctrl-C to exit). Confirm the disk
path is live with `redis-cli INFO search | grep search_disk_usage`.

Flags: `-addr`, `-config`, `-duration`, `-web-addr`, `-flush`, `-no-web`, `-seed`.

## Workload / query profiles

The query mix is a **weighted set of profiles** (in the config and **tunable live
from the dashboard**). Each targets a disk pattern / weak-point:

| Profile | Command | Targets |
| ------- | ------- | ------- |
| `channel_search` | `FT.SEARCH @channel_id:{c} term` | the common case; recall-checked |
| `thread_search` | `FT.SEARCH @thread_id:{th} term` | high-cardinality TAG posting lists |
| `tag_filter` | `FT.SEARCH @plan:{tier}\|@tenant_id:{t} term` | retention-tier / tenant filtering |
| `recent_timeline` | `FT.AGGREGATE … LOAD @ts FILTER exists(@ts) APPLY to_number SORTBY` | LOAD of unindexed field + numeric sort |
| `plan_analytics` | `FT.AGGREGATE * GROUPBY @<tag> REDUCE COUNT[_DISTINCT]` | wide GROUPBY (OOM / max-aggregate-groups) |

Within each profile the concrete query is **randomized** — scope field, term
operators (OR / negation / prefix `foo*` / fuzzy `%foo%` / wildcard `w'f?o'`),
GROUPBY field and reducer, sort direction, and page offset — so the load is a
varied stream, visible in the dashboard's live query feed. (`recent_timeline`
guards the numeric sort with `FILTER exists(@ts)` so an expired-but-unreclaimed
doc returned by the match doesn't make `to_number(@ts)` throw during `SORTBY`.)
| `deep_pagination` | `FT.SEARCH … LIMIT <big offset> n` | large-offset / OOM-during-execution |
| `text_prefix` | `FT.SEARCH @channel_id:{c} pre*` | TEXT prefix expansion |

Other workers: sliding-TTL refresher (`PEXPIRE`), editor (`HSET body`, re-index),
deleter (`UNLINK`, de-index). Ingest/edit/delete rates come from the config; the
**query thread-concurrency, rate, per-query `TIMEOUT`, result `LIMIT`, and profile
mix are all live-tunable from the UI** (a supervisor spawns/stops query workers to
match the live thread count, so you can dial load up and down without a restart).

### Disk weak-points this exercises

- **Term-dictionary RAM** (pinned, scales with distinct terms) via `vocab_high`.
- **Query timeout** — 500 ms default returns *partial results silently*; the
  harness flags queries ≥ `slow_ms` as "slow" and lets you set a per-query
  `TIMEOUT` from the UI.
- **OOM guardrail** — only checks *before* a query; `deep_pagination` (large
  offset) and `plan_analytics` (wide GROUPBY) push the during-execution path.
- **TTL churn** — continuous expiry as a deletion stream; footprint-plateau check.
- **GC under delete bursts** and **compaction** — sampled via `INFO`.

> `FT.AGGREGATE` on Flex is version-dependent (enabled by MOD-16604 /
> `e555decfb`); the harness executes aggregate profiles error-tolerantly, and
> they run fully on Redis Stack / OSS for the demo.

## Web dashboard

All panels are **collapsible** (click the header). Charts show a **hover tooltip**
with the timestamp and each series' value.

- **High-level KPIs** (hover any tile for an explanation): messages retained,
  ingesting/s, expiring/s, footprint (+ plateau badge), stale results
  (correctness), query p99, slow queries, recall.
- **Query load** — the single place to drive query traffic:
  - *Query types:* toggle each profile on/off and set its relative weight, or hit
    **🎲 Randomize types** to randomize the mix. This is the "control or
    randomization of the type of queries" knob.
  - *Load:* **threads** (live query concurrency — more threads = more concurrent
    connections = more load, up to `max_workers`) and **rate q/s** (0 = max). Also
    per-query **timeout** and result **limit**. **Apply** pushes changes to the
    running workload instantly — no restart.
- **Live query feed:** a rolling sample of the **actual randomized queries** being
  sent (profile · command · matches · latency) — so you can see the varied
  structure (TAG OR/negation, prefix/fuzzy/wildcard, `GROUPBY` on different fields
  with different reducers, timeline aggregates).
- **Live conversations:** browse channels; messages fetched live via `FT.SEARCH`
  + `HGETALL` with TTL countdowns, plus an inline text filter (scoped search).
- **Index status (FT.INFO):** `num_docs`, `num_records`, `inverted_sz_mb`,
  `doc_table_size_mb`, `total_index_memory_sz_mb`, `hash_indexing_failures`,
  `indexing`, `percent_indexed`, GC/cleaning — plus disk `INFO`
  (`search_disk_usage`, `async_reads_expired`, compaction). Placeholder-on-Flex
  fields (offset/key-table sizes, etc.) are intentionally omitted.
- **Charts:** footprint over time, docs vs records, ingest & expire /s, query p50/p99.

The charts are drawn from an **in-process time-series** the harness samples from
`INFO`/`FT.INFO` — it does **not** use RedisTimeSeries (`TS.*`), which isn't part
of the disk-search surface.

> `/api/control` (GET/POST), `/api/gen-queries`, `/api/run-queries` are also
> exposed for scripting the workload from the CLI.

### Reading "stale hits" — the headline correctness signal

A **stale hit** is a query result that should not exist: a message that had already
**expired** (its TTL elapsed) or been **deleted** *before* the query ran, yet still
appeared in the results. On-disk indexes don't physically remove a message's
postings the instant its key expires (that happens later via GC/compaction) — the
guarantee is that they're **filtered at query time** in the meantime. `stale_hits`
counts violations of that guarantee:

- **0** — correct: expired/deleted docs are hidden at read time. Expected on the
  disk build.
- **> 0** — the index is returning expired content (a retention/privacy bug worth a
  ticket). Stock OSS/in-RAM RediSearch (Redis Stack) shows many under churn.

Measured with a `t0`-before-query rule against a client oracle, plus a probe that
searches for known-expired docs and asserts they're absent (see below).

## What to watch

- **No stale hits (correctness).** Per query the oracle captures `t0` before
  issuing; a returned doc is a violation if it expired at/before `t0` (strict —
  expiry has a synchronous read-time filter) or was deleted more than a grace
  window before `t0` (deletion de-indexes asynchronously). A probe repeatedly
  searches for known-expired docs and asserts they're absent.
- **Footprint plateau.** Slope over the steady-state window → PLATEAU vs GROWING.
- **Index status & term dictionary**, **GC/compaction**, **query latency & slow/
  timeout counts**, and **post-restart correctness** via `reload-check`.

Artifacts: `./out/summary.json`, `./out/metrics.csv`.

## Local validation performed

Built via the Go container and smoke-tested in-RAM against both a local
`redisearch.so` and a **Redis Stack** container. All dashboard endpoints work
(stats, control GET/POST, gen-queries, run-queries, channels, messages, search);
the profile mix and query rate/timeout are live-tunable; the ad-hoc editor
generates, edits and runs queries. `reload-check` passes (live docs preserved,
expired absent before/after `DEBUG RELOAD`).

> **Finding (OSS vs Enterprise, in-RAM):** under sustained TTL churn, stock OSS
> RediSearch (Redis Stack) returns **expired-but-not-yet-reaped** documents —
> thousands of stale hits — while the RediSearchEnterprise/disk build filters
> expired docs at query time (**0 stale hits** on the identical workload). This is
> exactly the no-stale-hits guarantee use case 6 targets.

> The disk path itself (SpeedB, pinned-RAM term dictionary, `search_disk_usage`,
> compaction) must be exercised on a provisioned Flex/BigRedis server.

## Findings

_To be filled in during the bug bash. Link any tickets you open (e.g. MOD-XXXXX)._

- _(footprint plateau vs growth under sustained expiry churn — attach `metrics.csv` / screenshots)_
- _(term-dictionary RAM growth with `vocab_high`; RAM-quota write rejection?)_
- _(timeout partial-results / slow-query behavior; deep-pagination & wide-GROUPBY under load)_
- _(any stale hits, recall anomalies, GC/compaction surprises, restart/RDB issues)_
