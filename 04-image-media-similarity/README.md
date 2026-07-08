# Use case 4 — Image / media similarity (JSON or HASH)

> Part of [Bug Bash Applications](../README.md) · [Confluence use-case catalog](https://redislabs.atlassian.net/wiki/spaces/DX/pages/6484329496/Redis+Search+on+Disk+-+MS2+-+Use-cases)

* **Domain:** large-scale pure-vector similarity (image / audio embeddings), high dimensionality.
* **Schema:** VECTOR (embedding), TAG (metadata: type, owner).
* **Queries:** large-K KNN, KNN + tag filter.
* **Stress axes:** big vectors on disk, memory vs. disk footprint, recall correctness.

## Implementations

Each subfolder here is one contributor's implementation, named after their GitHub
username. To add yours, copy [`_TEMPLATE/`](_TEMPLATE/) and rename it to your GitHub
username, then build your app inside it.

| Contributor | Notes |
| ----------- | ----- |
| _add yourself_ | |
