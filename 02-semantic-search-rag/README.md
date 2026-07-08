# Use case 2 — Semantic search / RAG knowledge base (JSON)

> Part of [Bug Bash Applications](../README.md) · [Confluence use-case catalog](https://redislabs.atlassian.net/wiki/spaces/DX/pages/6484329496/Redis+Search+on+Disk+-+MS2+-+Use-cases)

* **Domain:** large corpus of chunked documents with embeddings — the flagship vector-on-disk story.
* **Schema:** TEXT (chunk content), TAG (source, doc_id), VECTOR (embedding, HNSW).
* **Queries:** pure KNN, vector range queries.
* **Stress axes:** vector index on disk, JSON path indexing.

## Implementations

Each subfolder here is one contributor's implementation, named after their GitHub
username. To add yours, copy [`_TEMPLATE/`](_TEMPLATE/) and rename it to your GitHub
username, then build your app inside it.

| Contributor | Notes |
| ----------- | ----- |
| _add yourself_ | |
