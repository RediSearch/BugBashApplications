"""Redis Search on Flex data layer for the Ride & Tide store.

Everything here sticks to the MS2 supported surface
(https://redislabs.atlassian.net/wiki/spaces/DX/pages/6475644929):

- Fields: TEXT / TAG / VECTOR only (no NUMERIC/GEO, no SORTABLE/NOINDEX).
- VECTOR: HNSW with explicit M / EF_CONSTRUCTION / EF_RUNTIME / RERANK
  (all four are mandatory on Flex), FLOAT32, sent via the raw command API
  because high-level client builders don't know RERANK yet.
- FT.CREATE requires SKIPINITIALSCAN on Flex — indexes never backfill
  pre-existing keys, so create the index before ingesting.
- Pre-filtered (hybrid) KNN always passes an explicit HYBRID_POLICY.
- Price sorting/stats go through FT.AGGREGATE LOAD + APPLY to_number(@price)
  on an unindexed hash field (the documented workaround for no NUMERIC).
- DIALECT 2 everywhere (DIALECT 4 is unsupported); no SUMMARIZE/HIGHLIGHT/
  SLOP/INORDER/WITHCURSOR; TAG queries are exact-match only.

Newer builds are stricter than MS2 (FT.SEARCH demands NOCONTENT/RETURN 0,
FT.AGGREGATE is blocked outright), so every read path degrades
automatically: NOCONTENT + client-side HMGET hydration, and client-side
facets/price math. The active mode is detected from the server's errors.
"""

import numpy as np
import redis

import catalog
import embeddings

RETURN_FIELDS = ["sku", "name", "brand", "category", "subcategory", "color",
                 "price", "price_bucket", "in_stock"]

# Cap on docs pulled client-side when FT.AGGREGATE is unavailable.
CLIENT_SIDE_DOC_CAP = 5000

FACET_VOCAB = {
    "category": list(catalog.DEPARTMENTS),
    "brand": sorted({b for brands, _ in catalog.DEPARTMENTS.values() for b in brands}),
    "subcategory": sorted({s for _, subs in catalog.DEPARTMENTS.values() for s in subs}),
    "color": catalog.COLORS,
    "price_bucket": [name for _, name in catalog.PRICE_BUCKETS],
    "in_stock": ["yes", "no"],
}


def connect(url=None, host="localhost", port=6379, password=None, tls=False):
    # protocol=2: keep RESP2 reply shapes so positional FT.* parsing is stable.
    if url:
        client = redis.Redis.from_url(url, decode_responses=False, protocol=2)
    else:
        client = redis.Redis(host=host, port=port, password=password,
                             ssl=tls, decode_responses=False, protocol=2)
    client.ping()
    return client


def _text(value):
    return value.decode("utf-8", "replace") if isinstance(value, bytes) else value


class Store:
    def __init__(self, client, index="idx:products", prefix="product:"):
        self.r = client
        self.index = index
        self.prefix = prefix
        self.no_return_mode = False   # server demands NOCONTENT / RETURN 0
        self.no_aggregate = False     # server blocks FT.AGGREGATE

    # ------------------------------------------------------------- schema

    def create_index(self, recreate=False):
        if recreate:
            try:
                # No DD: unsupported on Flex — document keys stay.
                self.r.execute_command("FT.DROPINDEX", self.index)
            except redis.ResponseError:
                pass
            # SKIPINITIALSCAN is *mandatory* on Flex, so a new index never
            # backfills existing keys — purge them or they stay unindexed.
            self.purge_keys()
        self.r.execute_command(
            "FT.CREATE", self.index, "ON", "HASH",
            "PREFIX", "1", self.prefix,
            "SKIPINITIALSCAN",
            "SCHEMA",
            "sku", "TAG",
            "name", "TEXT", "WEIGHT", "2",
            "description", "TEXT",
            "brand", "TAG",
            "category", "TAG",
            "subcategory", "TAG",
            "color", "TAG",
            "price_bucket", "TAG",
            "in_stock", "TAG",
            "embedding", "VECTOR", "HNSW", "14",
            "TYPE", "FLOAT32",
            "DIM", str(embeddings.DIM),
            "DISTANCE_METRIC", "COSINE",
            "M", "16",
            "EF_CONSTRUCTION", "200",
            "EF_RUNTIME", "10",
            "RERANK", "TRUE",
        )

    def drop_index(self):
        self.r.execute_command("FT.DROPINDEX", self.index)

    def purge_keys(self):
        n = 0
        pipe = self.r.pipeline(transaction=False)
        for key in self.r.scan_iter(match=f"{self.prefix}*", count=1000):
            pipe.unlink(key)
            n += 1
            if len(pipe) >= 1000:
                pipe.execute()
        pipe.execute()
        return n

    # -------------------------------------------------------------- write

    def key(self, sku):
        return f"{self.prefix}{sku}"

    def upsert(self, product, pipe=None):
        vec = embeddings.embed_product(product)
        mapping = dict(product)
        mapping["embedding"] = embeddings.to_bytes(vec)
        (pipe or self.r).hset(self.key(product["sku"]), mapping=mapping)
        return vec

    def ingest(self, products, batch=200, progress=None):
        n = 0
        pipe = self.r.pipeline(transaction=False)
        for product in products:
            self.upsert(product, pipe)
            n += 1
            if n % batch == 0:
                pipe.execute()
                if progress:
                    progress(n)
        pipe.execute()
        return n

    def delete(self, sku):
        return self.r.delete(self.key(sku))

    def get_embedding(self, sku):
        blob = self.r.hget(self.key(sku), "embedding")
        return embeddings.from_bytes(blob) if blob else None

    # ------------------------------------------------------------- filters

    @staticmethod
    def tag_filter(filters):
        """dict of field -> value into an ANDed TAG filter expression."""
        return " ".join(f"@{field}:{{{value}}}"
                        for field, value in filters.items() if value)

    # ------------------------------------------------------------- queries

    def _search(self, *args, timeout_ms=None):
        args = list(args) + ["DIALECT", "2"]
        if timeout_ms is not None:
            args += ["TIMEOUT", str(timeout_ms)]
        return self.r.execute_command(*args)

    @staticmethod
    def _parse_docs(reply, extra_fields=()):
        total, docs = reply[0], []
        fields = RETURN_FIELDS + list(extra_fields)
        for i in range(1, len(reply), 2):
            raw = reply[i + 1]
            pairs = {_text(raw[j]): _text(raw[j + 1]) for j in range(0, len(raw), 2)}
            doc = {"_key": _text(reply[i])}
            doc.update({f: pairs.get(f) for f in fields if f in pairs})
            docs.append(doc)
        return total, docs

    def _hydrate(self, keys, with_embedding=False):
        """NOCONTENT fallback: fetch display fields per key via HMGET."""
        fields = RETURN_FIELDS + (["embedding"] if with_embedding else [])
        pipe = self.r.pipeline(transaction=False)
        for key in keys:
            pipe.hmget(key, fields)
        docs = []
        for key, values in zip(keys, pipe.execute()):
            doc = {"_key": _text(key)}
            for field, value in zip(fields, values):
                doc[field] = value if field == "embedding" else _text(value)
            docs.append(doc)
        return docs

    def _keys_only(self, query, limit, timeout_ms=None):
        reply = self._search("FT.SEARCH", self.index, query, "NOCONTENT",
                             "LIMIT", "0", str(limit), timeout_ms=timeout_ms)
        return reply[0], [_text(k) for k in reply[1:]]

    @staticmethod
    def _needs_nocontent(err):
        return "NOCONTENT" in str(err) or "RETURN 0" in str(err)

    def count(self, query="*", timeout_ms=None):
        return self._search("FT.SEARCH", self.index, query, "LIMIT", "0", "0",
                            timeout_ms=timeout_ms)[0]

    def text_search(self, text, filters=None, limit=10, timeout_ms=None):
        query = text or "*"
        tag_expr = self.tag_filter(filters or {})
        if tag_expr:
            query = f"({query}) {tag_expr}" if text else tag_expr
        if not self.no_return_mode:
            try:
                reply = self._search(
                    "FT.SEARCH", self.index, query,
                    "RETURN", str(len(RETURN_FIELDS)), *RETURN_FIELDS,
                    "LIMIT", "0", str(limit),
                    timeout_ms=timeout_ms)
                return self._parse_docs(reply)
            except redis.ResponseError as err:
                if not self._needs_nocontent(err):
                    raise
                self.no_return_mode = True
        total, keys = self._keys_only(query, limit, timeout_ms)
        return total, self._hydrate(keys)

    def knn(self, vector, k=10, filters=None, policy="ADHOC_BF",
            ef_runtime=None, timeout_ms=None):
        """KNN query. With filters this is a pre-filtered (hybrid) query,
        which on Flex requires an explicit HYBRID_POLICY.

        Each returned doc carries `dist` and `_dist_source`: "index" when
        the server yielded it, "client" (recomputed from the stored vector)
        in NOCONTENT mode.
        """
        query_vec = np.asarray(vector, dtype=np.float32)
        blob = embeddings.to_bytes(query_vec)
        tag_expr = self.tag_filter(filters or {})
        knn_params = f" EF_RUNTIME {int(ef_runtime)}" if ef_runtime else ""
        if tag_expr:
            query = (f"({tag_expr})=>[KNN $K @embedding $vec "
                     f"HYBRID_POLICY {policy}{knn_params} AS dist]")
        else:
            query = f"*=>[KNN $K @embedding $vec{knn_params} AS dist]"
        base = ["FT.SEARCH", self.index, query,
                "PARAMS", "4", "K", str(k), "vec", blob,
                "SORTBY", "dist"]
        if not self.no_return_mode:
            try:
                reply = self._search(
                    *base,
                    "RETURN", str(len(RETURN_FIELDS) + 1), *RETURN_FIELDS, "dist",
                    "LIMIT", "0", str(k),
                    timeout_ms=timeout_ms)
                total, docs = self._parse_docs(reply, extra_fields=("dist",))
                for doc in docs:
                    doc["_dist_source"] = "index"
                return total, docs
            except redis.ResponseError as err:
                if not self._needs_nocontent(err):
                    raise
                self.no_return_mode = True
        reply = self._search(*base, "NOCONTENT", "LIMIT", "0", str(k),
                             timeout_ms=timeout_ms)
        total, keys = reply[0], [_text(key) for key in reply[1:]]
        docs = self._hydrate(keys, with_embedding=True)
        for doc in docs:
            blob = doc.pop("embedding", None)
            doc["dist"] = (embeddings.cosine_distance(
                embeddings.from_bytes(blob), query_vec) if blob else None)
            doc["_dist_source"] = "client"
        return total, docs

    def similar(self, sku, k=10, filters=None, **kw):
        vec = self.get_embedding(sku)
        if vec is None:
            raise KeyError(f"no such product: {sku}")
        total, docs = self.knn(vec, k=k + 1, filters=filters, **kw)
        docs = [d for d in docs if d.get("sku") != sku][:k]
        return total, docs

    def semantic(self, text, k=10, filters=None, **kw):
        return self.knn(embeddings.embed_query(text), k=k, filters=filters, **kw)

    # -------------------------------------------------------- aggregations

    def _aggregate(self, *args):
        if self.no_aggregate:
            return None
        try:
            return self.r.execute_command("FT.AGGREGATE", self.index, *args,
                                          "DIALECT", "2")
        except redis.ResponseError as err:
            if "not supported" not in str(err):
                raise
            self.no_aggregate = True
            return None

    def _client_docs(self, base_query, fields):
        """Fallback source when FT.AGGREGATE is unavailable: match keys via
        NOCONTENT search, hydrate the needed fields (capped)."""
        total, keys = self._keys_only(base_query, CLIENT_SIDE_DOC_CAP)
        pipe = self.r.pipeline(transaction=False)
        for key in keys:
            pipe.hmget(key, fields)
        rows = [dict(zip(fields, map(_text, values))) for values in pipe.execute()]
        return total, rows

    def facets(self, field, base_query="*", limit=20):
        reply = self._aggregate(
            base_query,
            "GROUPBY", "1", f"@{field}",
            "REDUCE", "COUNT", "0", "AS", "count",
            "SORTBY", "2", "@count", "DESC",
            "LIMIT", "0", str(limit))
        if reply is not None:
            return [(_text(row[1]), int(_text(row[3]))) for row in reply[1:]]
        # Fallback: one count query per known tag value.
        counts = []
        for value in FACET_VOCAB[field]:
            q = f"@{field}:{{{value}}}"
            if base_query != "*":
                q = f"({base_query}) {q}"
            n = self.count(q)
            if n:
                counts.append((value, n))
        counts.sort(key=lambda t: -t[1])
        return counts[:limit]

    def price_stats_by_category(self):
        """MIN/MAX/AVG price per category — numeric reducers over an
        unindexed field via LOAD + APPLY to_number (documented pattern)."""
        reply = self._aggregate(
            "*",
            "LOAD", "1", "@price",
            "APPLY", "to_number(@price)", "AS", "p",
            "GROUPBY", "1", "@category",
            "REDUCE", "COUNT", "0", "AS", "count",
            "REDUCE", "MIN", "1", "@p", "AS", "min_price",
            "REDUCE", "MAX", "1", "@p", "AS", "max_price",
            "REDUCE", "AVG", "1", "@p", "AS", "avg_price")
        if reply is not None:
            rows = []
            for row in reply[1:]:
                rows.append({_text(row[i]): _text(row[i + 1])
                             for i in range(0, len(row), 2)})
            return rows
        rows = []
        for category in FACET_VOCAB["category"]:
            total, docs = self._client_docs(f"@category:{{{category}}}", ["price"])
            prices = [float(d["price"]) for d in docs if d.get("price")]
            if prices:
                rows.append({"category": category, "count": str(total),
                             "min_price": f"{min(prices):.2f}",
                             "max_price": f"{max(prices):.2f}",
                             "avg_price": f"{sum(prices) / len(prices):.2f}"})
        return rows

    def price_sorted(self, base_query="*", ascending=True, limit=15):
        """SORTBY a real number: to_number(@price), since TAG buckets only
        filter and lexicographic sort of the raw field would be wrong."""
        reply = self._aggregate(
            base_query,
            "LOAD", "4", "@sku", "@name", "@brand", "@price",
            "APPLY", "to_number(@price)", "AS", "p",
            "SORTBY", "2", "@p", "ASC" if ascending else "DESC",
            "LIMIT", "0", str(limit))
        if reply is not None:
            rows = []
            for row in reply[1:]:
                rows.append({_text(row[i]): _text(row[i + 1])
                             for i in range(0, len(row), 2)})
            return rows
        _, rows = self._client_docs(base_query, ["sku", "name", "brand", "price"])
        for row in rows:
            row["p"] = row.get("price") or "0"
        rows.sort(key=lambda r: float(r["p"]), reverse=not ascending)
        return rows[:limit]

    # ---------------------------------------------------------------- info

    def info(self):
        reply = self.r.execute_command("FT.INFO", self.index)
        return {_text(reply[i]): reply[i + 1] for i in range(0, len(reply), 2)}
