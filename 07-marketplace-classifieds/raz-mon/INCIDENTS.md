# bazaar — live incident log (full ingest + mixed run)

Live-updated report of DB incidents observed while running the bazaar workload
against the QA cloud Flex DB
(`redis-19599.c127261.us-east-1-mz.ec2.qa-cloud.rlrcp.com:19599`,
Redis Search on Disk MS2). All times UTC, 2026-07-09.

Workload context: full ingest of 50M docs (≈100GB) via 16 pipelined workers,
sustained ~13–15k docs/s writes; 6 disk indexes (3 HASH / 3 JSON) indexing
everything as it lands.

**Pattern so far: sustained double-digit-k docs/s write load knocks the shards
over; the proxy stays up (PONG) while data commands fail; the DB self-recovers
in ~10 min with no observed data loss after recovery.** Possibly related to
known bugs on the limitations page: MOD-16648 (shutdown race), MOD-16701 (fork
deadlock), RED-208624 (replica join). Cluster-side events not visible through
the proxy — needs a look at the cluster logs by the DB owner.

---

## Incident 1 — 07:36–07:51, at ~6.4M keys (first ingest attempt)

- **Client errors:** 47 × `Timeout reading from socket` across all 16 load
  workers, 07:36:17–07:38:05.
- **Observed from probes:** intermittent `Server closed the connection`;
  `DBSIZE` returned **0** twice; summed `FT.INFO num_docs` **regressed
  6.4M → 4.3M** before recovering — consistent with a shard restart/failover
  losing not-yet-persisted index state, then rebuilding/rejoining.
- **Recovery:** self-recovered; by ~07:51 PONG stable, dbsize steady at
  8,642,765 and exactly equal to summed `num_docs` (the loaders had kept
  writing during part of the window). The `run.py load` process died during
  the incident; checkpoints were intact.
- **Misc:** proxy `used_memory` frozen at 76.5GB before/during/after —
  unreliable metric.

## Incident 2 — ~08:05–08:15, at ~15.1M keys (resumed ingest)

- **Onset:** last clean worker checkpoints written 08:04:40; first client
  error 08:06:43.
- **Client errors:** 80 × `Connection closed by server.` across all 16
  workers, 08:06:43–08:15:22. Workers retried in a loop and survived (no
  process death this time).
- **Observed from probes (during outage):** `PING` → PONG the whole time, but
  `DBSIZE` hung (>6s) or returned **0**, and `FT.INFO` on all 6 indexes hung
  or returned 0 — proxy up, shards unresponsive.
- **Recovery:** self-recovered ≤08:19:51, when `DBSIZE` returned 20,068,187
  vs summed `num_docs` 20,074,442 (gap ≈ in-flight indexing). Ingest resumed
  at full speed on its own; error count frozen at 127 afterwards.
- **Data-consistency check:** post-recovery dbsize/num_docs consistent; the
  jump 15.1M → 20M during/after the window suggests shards kept ingesting
  (or caught up) while the proxy-side view was broken.

---

## Health snapshots (digest)

| time (UTC) | dbsize | % of 50M | num_docs sum | load errs (cum) | note |
|---|---|---|---|---|---|
| 07:51 | 8,642,765 | 17.3% | 8,642,765 | 47 | pre-resume state, fully consistent |
| 07:55 | 10,275,590 | 20.6% | — | 47 | ingest resumed ~13k docs/s |
| 08:0x | 15,121,595 | 30.2% | 15,151,327 | 47 | last healthy sample before incident 2 |
| 08:06–08:15 | 0 / hang | — | 0 / hang | 47→127 | **incident 2** |
| 08:20 | 20,068,187 | 40.1% | 20,074,442 | 127 | recovered, loading |
| 08:30 | 25,507,958 | 51.0% | — | 127 | healthy, ~13k docs/s |
| 08:41 | 30,872,323 | 61.7% | 30,876,907 | 127 | healthy, no new errors since incident 2 |
| ~08:52 | 35,271,968 | 70.5% | 35,271,968 | 127 (final) | **ingest stopped** (load process died with tooling-session exit; per Raz we stop at ~35M anyway). dbsize == num_docs exactly. |

---

## Phase 2 — mixed stress run (churn + query storm + verifiers), started 09:02

- `run.py mixed --profile full --duration 21600 --run-name mixed-full`
  (8 churn workers @ 3k ops/s total, 16 query-storm workers, 2 verifiers,
  monitor; detached via setsid so it survives tooling restarts).
- Pre-launch sanity: per-index `num_docs` split exactly 28/22/18/14/10/8;
  TEXT query `jacket` → 1.37M hits; TAG `@condition:{good}` → 3.46M hits;
  TAG+TEXT combo → 224k hits. All fast when the DB is otherwise idle.

### Observation 3 — pervasive query timeouts under mixed load (from first minutes)

- In the first ~90s of the mixed run: **261 of 543 storm queries (~48%)
  failed with server-side `SEARCH_TIMEOUT Timeout limit was reached`**, spread
  across every template type (single-term text, AND/OR, prefix, fuzzy, phrase,
  tag facet, deep pagination, negation, aggregations).
- For comparison: the same query set had **zero timeouts** in the 100k-doc
  smoke run, and one-off queries at 35M docs with no churn returned instantly.
- Watching whether the rate settles (cold disk caches after ingest?) or stays
  at ~half of all queries. Verifier correctness events so far: **0**.

## Incident 3 — from ~09:10, read/query path collapse (writes unaffected)

Different failure mode from incidents 1–2:

- During the `mixed-full-t25` run: churn writes stayed **fast and clean**
  (60–70ms per op, 0 errors, ~3k ops/s) while **every** FT.SEARCH /
  FT.AGGREGATE — storm, verifier, and manual — died at the full 25s
  `TIMEOUT` budget. With `TIMEOUT 0` a single-term query didn't return
  within 40s (client gave up).
- **Persists on an idle DB**: after killing all workload at 09:17, single-term
  TEXT, 2-term AND, prefix, and pure TAG queries on multiple indexes each
  burned exactly 25.1s → `SEARCH_TIMEOUT`. So it is not (only) load
  starvation; the query path itself is degraded.
- `DBSIZE` and `FT.INFO` intermittently hang >8–10s from ad-hoc connections,
  while the 30s health watcher still gets answers; `PING` always fine.
- Contrast: at 08:59 (idle, post-ingest) the same queries returned instantly;
  at ~09:20 after ~15 min of churn+storm they cannot complete in 25s.
  `num_docs` only moved 35.27M → 35.31M in that time, so this is not about
  data volume — suspicion: index/GC state after delete/update churn, or
  shards degraded again (proxy hides cluster state).
- **09:26 — root cause surfaces.** After ~8 min of idle DB, FT.SEARCH stops
  timing out and instead fails instantly with:
  `ERR could not perform command 'ft.search' because shards topology update
  is either transient or has failed`.
  The cluster is stuck mid shard-topology update (failover/migration).
  Explains the whole incident: search fan-out blocked on unreachable/joining
  shards (25s timeouts, then explicit refusal), `DBSIZE` (also fan-out)
  hanging, single-key writes unaffected, `PING` served by the proxy.
  Likely related to RED-208624 (replica join) / the shard restarts already
  seen in incidents 1–2. **The topology update has NOT converged after
  15+ min** — much worse than the ~10 min self-recovery of incidents 1–2.
- **Not converging.** Still refusing FT.SEARCH at 09:36 (~26 min in);
  incidents 1–2 self-recovered in ~10 min. `DBSIZE` still hangs. The
  health-watch script wedged too (its redis-cli calls have no timeout).
- **09:37 probes** (saved: `out/incident3-info-0937.txt`):
  - Per-key path totally healthy: SET/GET/DEL instant.
  - `INFO keyspace` → **`db0:keys=2,986,906`** — ~1/12 of the 35.31M keys.
    Consistent with the proxy relaying/aggregating only one reachable shard.
    Cannot distinguish "stale proxy view" from real missing shards from here.
  - `INFO replication` → `role:master, connected_slaves:1,`
    **`master_repl_offset:0`** after ~2.5h of writes — replication state
    reset, fits a replica-(re)join gone wrong (RED-208624?).
  - `uptime_in_seconds:9051` (started ~07:06) — whichever shard INFO comes
    from was NOT restarted by incidents 1–3.
- **Escalation needed:** cluster-side logs / `rladmin status` from the DB
  owner; the proxy hides everything. If topology stays wedged the cluster
  likely needs a manual kick.
- 09:49 and 10:02 rechecks: unchanged (52+ min wedged). The
  `INFO keyspace` line is byte-identical across 25 min — even `expires` and
  `avg_ttl` frozen — so it is a **stale cached snapshot**, not a live
  single-shard view. Per-key R/W still healthy throughout.
- 10:44: first movement in 40 min — the INFO snapshot ticked once
  (keys +4,863 with zero client traffic; resync?), then froze again.
  11:04, 11:12: still refusing FT.SEARCH. **~2h+ wedged.**
- **11:15 — traffic restarted on Raz's call** (`out/mixed-full-t25b/`, then
  `t25c` after a harness fix, see below): 8 churn + 16 storm + 2 verify.
  Churn writes mostly fine (60–70ms) but a fraction hit 30s socket timeouts;
  storm queries all fail fast on the topology error (~300k+ errors logged in
  the first minutes — the refusal path itself is at least cheap/stable).
- **11:30 — keyspace partially unreachable, quantified:** probing 30
  spread-out keys with a 3s budget: 27 answered instantly, **3 hung (~10%)**.
  So per-key ops to ~1 shard's worth of slots hang indefinitely; everything
  else is fast. Explains churn's intermittent 30s timeouts and DBSIZE
  hanging (fan-out touches the dead slice), and means "writes are healthy"
  was only ~90% true.

- 12:15: still wedged (**3h10m**). t25c interim: churn running with
  intermittent 30s socket timeouts (the ~10% dead slice); storm
  **4,730,343 queries, 100% errors**; verifiers: 392 `verify_error`
  (all search-leg timeouts/refusals), **0 correctness events**.
- 12:20 harness ops note: the fail-fast error loop wrote ~1.7GB of identical
  `query_error` lines and nearly filled the box's disk (99%). Storm error
  events are now sampled (first 5 per template+class, then every 500th) with
  a 100ms backoff on sub-50ms failures; error *counts* remain exact in the
  window stats. Spam-era run dirs t25b/t25c deleted (summaries salvaged in
  `out/mixed-full-t25c-salvage/`), run relaunched as `mixed-full-t25d`.

- 12:51 / 13:21 / 13:52 / 14:23 checks: unchanged — FT.SEARCH refused,
  DBSIZE hung. **5h15m+ wedged.** t25d traffic (throttled storm + churn +
  verifiers) running stable throughout; disk steady at ~4.8G free.

- 14:54 / 15:21: unchanged. **6h15m+ wedged.**
- 15:25: second churn fleet launched (`out/churn-extra/`, +8 workers, 3h) to
  raise write pressure. Measured effect: **effective churn throughput is
  ~2–3 ops/s per fleet, not the 3k target** — ~10% of ops land on the dead
  slice and block for the full 30s socket timeout, so the average op costs
  ~3s and workers serialize (0.9×70ms + 0.1×30s). I.e. the wedge also
  collapses *write* throughput ~1000× for clients touching random keys,
  even though 90% of individual writes are fast.

- 15:45 responsiveness matrix (all verified with unique-nonce round trips,
  live socket inspection, and read-back of a churn-inserted doc):
  PING instant; per-key R/W instant on ~80–90% of keys; per-key ops on the
  dead slice (4/20 sampled) hang to client timeout; DBSIZE/FT.INFO hang;
  FT.SEARCH/FT.AGGREGATE fail instantly with the topology error.
- 15:57: unchanged. **~6h50m wedged.**
- **16:05 — all client traffic and watchers stopped (end of session).**
  Incident 3 was still open at stop time: FT.SEARCH had been unavailable
  since ~09:10 UTC (~7h), never self-recovered. Final state of the test:
  35.31M docs loaded, indexes intact per last consistent read (35.27M
  num_docs == dbsize at 08:59), wedge unresolved — needs cluster-side
  diagnosis (rladmin status / event logs for c127261, window 09:05–09:40).

### Harness bug found by incident 3 (fixed, not a DB issue)

All 8 churn workers died on their *first* op error:
`stats.event("churn_error", op=op, ...)` collided with `event()`'s positional
`op` arg → `TypeError` → worker crash. Never triggered before because churn
had a clean run until this incident. Fixed (`failed_op=`), run relaunched as
`mixed-full-t25c`; churn now survives errors and logs them.

### Mixed-run health snapshots

| time (UTC) | dbsize | queries done | q-timeout % | correctness events | note |
|---|---|---|---|---|---|
| 09:04 | ~35.27M | 543 | 48% | 0 | first sample, caches cold |

### Config change 09:2x — `TIMEOUT 25000` on all queries

First mixed run (`out/mixed-full/`, 09:02–09:2x) ran with the server's default
query timeout and ~48% of queries died with `SEARCH_TIMEOUT`. Per Raz, all
storm + verifier queries now carry an explicit `TIMEOUT 25000` (25s server
budget, under the 30s client socket timeout). Run restarted as
`out/mixed-full-t25/` (6h). Timeout-rate numbers before/after are not
comparable; Observation 3 stands as: **default query timeout is far too small
for this dataset under load — queries need a 25s budget to complete**. Watch
p50/p99 latencies in the t25 run to quantify how slow they actually are.

## Standing observations

- `used_memory` via the proxy is stuck around 76.1–76.5GB regardless of
  actual data volume — treat as unusable; only FT.INFO trends + dbsize matter.
- `instantaneous_ops_per_sec` from `INFO stats` also freezes at stale values
  during incidents (kept reporting 14,667 while shards were down).
- `hash_indexing_failures` = 0 and `expired_keys` = 0 throughout so far.
