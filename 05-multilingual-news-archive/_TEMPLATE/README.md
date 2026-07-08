# Multilingual news / article archive — `<your-github-username>`

> **Template.** Copy this `_TEMPLATE/` folder, rename the copy to your GitHub
> username, and fill in the sections below. Delete this note when you're done.

## What this app does

_One paragraph: the application and the customer scenario it simulates._

## Schema

_The `FT.CREATE` schema you index with (TEXT / TAG / VECTOR fields only — those are the disk-supported types)._

## Dataset & scale

- **Source:** _real dataset, synthetic generator, ..._
- **Size:** _doc count / total bytes — aim for "doesn't fit in RAM"_
- **Ingestion:** _how you load it (pipeline / bulk), throughput_

## How to run

```
# prerequisites, env vars, and commands to load data + drive the workload
```

## Workload / queries

_The query patterns you drive, and how (load generator, concurrency, duration)._

## What to watch

_Metrics and correctness checks you assert: on-disk footprint, query latency, result correctness, recall, GC / compaction behavior, ..._

## Findings

_Bugs, surprises, observations. Link any tickets you open (e.g. MOD-XXXXX)._
