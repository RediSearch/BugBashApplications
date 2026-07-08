# Use case 1 — E-commerce product catalog (HASH)

> Part of [Bug Bash Applications](../README.md) · [Confluence use-case catalog](https://redislabs.atlassian.net/wiki/spaces/DX/pages/6484329496/Redis+Search+on+Disk+-+MS2+-+Use-cases)

* **Domain:** millions of SKUs — the canonical "too big for RAM" catalog.
* **Schema:** TEXT (title, description), TAG (brand, category, color).
* **Queries:** full-text + tag filters, prefix / fuzzy, pagination with cursors.
* **Stress axes:** large ingestion, faceted filtering.

## Implementations

Each subfolder here is one contributor's implementation, named after their GitHub
username. To add yours, copy [`_TEMPLATE/`](_TEMPLATE/) and rename it to your GitHub
username, then build your app inside it.

| Contributor | Notes |
| ----------- | ----- |
| _add yourself_ | |
