# Use case 7 — Marketplace / classifieds with churn (HASH + JSON)

> Part of [Bug Bash Applications](../README.md) · use-case catalog: internal doc *Redis Search on Disk — MS2 — Use-cases*

* **Domain:** second-hand marketplace (classifieds) — tens of millions of listings across several verticals (electronics, fashion, vehicles, ...). Listings are posted, **edited constantly** (price drops, description tweaks), and removed when sold or expired, while buyers hammer the search side. Deliberately **churn-heavy**: update/delete pressure and query load run continuously in parallel.
* **Schema:** TEXT (title, description), TAG (brand, category, city, condition, seller — high cardinality, price_bucket — NUMERIC is unsupported on disk, so prices are bucketed TAGs; the raw price is an unindexed field loaded via `FT.AGGREGATE ... APPLY to_number()`). A **fleet of indexes, mixing HASH and JSON** document types (one index per vertical), to stress shared-resource (WBM) contention across indexes.
* **Queries:** full-text (AND/OR/phrase), TEXT prefix & fuzzy, text + tag facets, high-cardinality seller scoping, negations, bounded deep pagination, `FT.AGGREGATE` GROUPBY facet counts and `to_number` price averages.
* **Stress axes:** full-doc re-index on every update (the rewrite path); GC / compaction under steady-state churn where delete+expire rate ≈ insert rate (does the on-disk footprint plateau?); posting lists shrinking and growing concurrently; no stale hits after delete/expire under load; multi-index WBM contention; HASH vs JSON indexing parity at scale.

## Implementations

Each subfolder here is one contributor's implementation, named after their GitHub
username. To add yours, copy [`_TEMPLATE/`](_TEMPLATE/) and rename it to your GitHub
username, then build your app inside it.

| Contributor | Notes |
| ----------- | ----- |
| [`raz-mon`](raz-mon/) | `bazaar` — Python harness: deterministic 100GB synthetic listing generator, resumable bulk loader, rate-limited insert/update/delete churn with TTL stream, weighted query storm, read-your-writes / no-stale-hits verifier, FT.INFO+INFO monitor, and an aggregating report tool. |
