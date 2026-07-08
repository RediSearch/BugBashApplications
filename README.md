# Bug Bash Applications — Redis Search on Disk (MS2)

Example, customer-style applications built to exercise **Redis Search on Disk (MS2)**
during the bug bash. Each application drives realistic ingestion and query patterns
**at scale** against on-disk indexes, so we get broad coverage of the surface and
shake out bugs.

> **Use-case catalog (source of truth):**
> [Redis Search on Disk – MS2 – Use-cases](https://redislabs.atlassian.net/wiki/spaces/DX/pages/6484329496/Redis+Search+on+Disk+-+MS2+-+Use-cases)

## Guiding principle

On-disk indexes exist for datasets that **don't fit in RAM**, so lean on **scale**.
The supported disk field types are **TEXT**, **TAG**, and **VECTOR** — build the
applications around those.

## Repository layout

```
<use-case>/                    one folder per use case
├── README.md                  describes the use case (domain, schema, queries, stress axes)
├── _TEMPLATE/                 starting point — copy it and rename to your GitHub username
│   └── README.md
└── <your-github-username>/    your implementation lives here — one folder per contributor
    └── ...
README.md                      this file
```

Several people can work the same use case — each contributor gets their own folder,
so there are no collisions.

## Use cases

| # | Use case | Data type | Folder |
| - | -------- | --------- | ------ |
| 1 | E-commerce product catalog | HASH | [`01-ecommerce-product-catalog/`](01-ecommerce-product-catalog/) |
| 2 | Semantic search / RAG knowledge base | JSON | [`02-semantic-search-rag/`](02-semantic-search-rag/) |
| 3 | Log / observability archive | HASH | [`03-log-observability-archive/`](03-log-observability-archive/) |
| 4 | Image / media similarity | JSON or HASH | [`04-image-media-similarity/`](04-image-media-similarity/) |
| 5 | Multilingual news / article archive | JSON | [`05-multilingual-news-archive/`](05-multilingual-news-archive/) |
| 6 | Chat / messaging with retention | HASH | [`06-chat-messaging-retention/`](06-chat-messaging-retention/) |

## How to contribute an application

1. **Pick a use case** below (or add a new one — mirror it on the Confluence page and add a folder here following the same layout).
2. **Claim it** on the [Confluence page](https://redislabs.atlassian.net/wiki/spaces/DX/pages/6484329496/Redis+Search+on+Disk+-+MS2+-+Use-cases): add yourself as an **Owner** and flip the status to **IN PROGRESS**.
3. **Copy** that use case's `_TEMPLATE/` folder and rename the copy to your **GitHub username**.
4. **Build** your app inside that folder and fill in its `README.md`.
5. **Log findings** — link any bugs/tickets you open in your README, and flip the Confluence status to **DONE** when you're finished.
