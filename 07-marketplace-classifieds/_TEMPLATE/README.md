# Marketplace / classifieds with churn — `<your-github-username>`

> **Template.** Copy this `_TEMPLATE/` folder, rename the copy to your GitHub
> username, and fill in the sections below. Delete this note when you're done.

## What this app does

_One paragraph: the application and the customer scenario it simulates._

## Schema

_The `FT.CREATE` schema(s) you index with (TEXT / TAG / VECTOR fields only — those are the disk-supported types). This use case mixes HASH and JSON indexes — say which indexes you create and on which document type._

## Dataset & scale

- **Source:** _real dataset, synthetic generator, ..._
- **Size:** _doc count / total bytes — aim for "doesn't fit in RAM" (~100GB)_
- **Ingestion:** _how you load it (pipeline / bulk), throughput_

## How to run

```
# prerequisites, env vars, and commands to load data + drive the workload
```

## Workload / queries

_The churn mix (insert / update / delete rates, TTL policy) and the query patterns you drive (load generator, concurrency, duration)._

## What to watch

_Metrics and correctness checks you assert: on-disk footprint over time (does it plateau under churn?), no stale hits after delete/expire, query latency and timeout rate, GC / compaction behavior, HASH vs JSON parity, ..._

## Findings

_Bugs, surprises, observations. Link any tickets you open (e.g. MOD-XXXXX)._
