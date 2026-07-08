# Use case 3 — Log / observability archive (HASH)

> Part of [Bug Bash Applications](../README.md) · [Confluence use-case catalog](https://redislabs.atlassian.net/wiki/spaces/DX/pages/6484329496/Redis+Search+on+Disk+-+MS2+-+Use-cases)

* **Domain:** very high-volume, append-only log lines.
* **Schema:** TEXT (message), TAG (service, level, host).
* **Queries:** text search within a service / level, filter by host or level.
* **Stress axes:** sustained ingestion throughput, GC / compaction under churn.

## Implementations

Each subfolder here is one contributor's implementation, named after their GitHub
username. To add yours, copy [`_TEMPLATE/`](_TEMPLATE/) and rename it to your GitHub
username, then build your app inside it.

| Contributor | Notes |
| ----------- | ----- |
| _add yourself_ | |
