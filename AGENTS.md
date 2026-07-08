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
