# Use case 6 — Chat / messaging with retention (HASH)

> Part of [Bug Bash Applications](../README.md) · use-case catalog: internal doc *Redis Search on Disk — MS2 — Use-cases*

* **Domain:** multi-tenant messaging archive — billions of messages across millions of channels/users, **each written with a TTL**. Covers disappearing messages (hours–days), free-tier retention windows (e.g. 90 days), and per-plan retention tiers (short- and long-lived docs coexisting in one index). Ingest and expiration run continuously in parallel.
* **Schema:** TEXT (message body), TAG (tenant_id, channel_id, user_id, thread_id). Per-message key TTL via `EXPIRE`; optional sliding TTL refreshed on thread activity.
* **Queries:** full-text scoped to a channel / user / thread via TAG; message edits and deletions; TTL refresh on active threads.
* **Stress axes:** TTL-driven expiration as a **continuous deletion stream** — GC / compaction under steady-state churn where expire-rate ≈ ingest-rate (does on-disk footprint plateau or grow?); query-time exclusion of expired-but-not-yet-reclaimed docs (no stale hits); high-cardinality TAG posting lists shrinking as messages expire; the TTL-update (sliding expiration) rewrite path; restart / RDB reload with expired docs still present.

## Implementations

Each subfolder here is one contributor's implementation, named after their GitHub
username. To add yours, copy [`_TEMPLATE/`](_TEMPLATE/) and rename it to your GitHub
username, then build your app inside it.

| Contributor | Notes |
| ----------- | ----- |
| [`kei-nan`](kei-nan/) | `chatstress` — Go harness: continuous ingest+TTL-expiry churn, sliding-TTL/edits/deletes, scoped full-text queries, a no-stale-hits correctness oracle, footprint-plateau analysis, `DEBUG RELOAD` check, and a live web dashboard (conversation browser + DB-stats charts). |
