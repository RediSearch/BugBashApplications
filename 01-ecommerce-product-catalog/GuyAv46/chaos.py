"""Churn workload + correctness oracle for the bug bash.

Mirrors every write into an in-memory reference model, then periodically
verifies the index against it. Every mismatch is printed with the exact
command that exposes it, so a finding is immediately a bug report.

What each check targets on the disk engine:
- count / tag-count drift ....... doc_table vs inverted-index consistency
                                  under update/delete churn + GC
- ghost & missing sku lookups ... deleted docs resurfacing, lost index
                                  entries after HSET updates
- KNN distance correctness ...... disk HNSW returning a doc whose stored
                                  vector doesn't match the reported score
                                  (torn repair / stale disk read)
- KNN recall (soft) ............. graph connectivity degradation from
                                  delete+repair cycles. Reported, not
                                  failed: HNSW is approximate and small
                                  graphs have a known recall weakness.
"""

import functools
import random
import time

# Progress must be visible live when output is piped (e.g. background runs).
print = functools.partial(print, flush=True)

import numpy as np

import catalog
import embeddings

RECALL_WARN_THRESHOLD = 0.6
DIST_TOLERANCE = 1e-3


class Mirror:
    """Reference model: what the index *should* contain."""

    def __init__(self):
        self.products = {}   # sku -> product dict
        self.vectors = {}    # sku -> np.ndarray

    def upsert(self, product, vector):
        self.products[product["sku"]] = dict(product)
        self.vectors[product["sku"]] = vector

    def delete(self, sku):
        self.products.pop(sku, None)
        self.vectors.pop(sku, None)

    def count(self, **tag_filters):
        return sum(1 for p in self.products.values()
                   if all(p[f] == v for f, v in tag_filters.items()))

    def knn(self, vector, k, **tag_filters):
        candidates = [(sku, embeddings.cosine_distance(vec, vector))
                      for sku, vec in self.vectors.items()
                      if all(self.products[sku][f] == v
                             for f, v in tag_filters.items())]
        candidates.sort(key=lambda t: t[1])
        return candidates[:k]


class ChaosRunner:
    def __init__(self, store, seed=42, verbose=True):
        self.store = store
        self.rng = random.Random(seed)
        self.mirror = Mirror()
        self.next_id = 0
        self.failures = []
        self.warnings = []
        self.verbose = verbose

    # ------------------------------------------------------------- logging

    def _fail(self, message, repro):
        self.failures.append((message, repro))
        print(f"\n\033[91mFAIL\033[0m {message}\n     repro: {repro}")

    def _warn(self, message):
        self.warnings.append(message)
        if self.verbose:
            print(f"\033[93mwarn\033[0m {message}")

    # ---------------------------------------------------------------- ops

    def _fresh_products(self, n):
        products = list(catalog.generate(n, seed=self.rng.getrandbits(32)))
        for product in products:
            product["sku"] = f"CH{self.next_id:06d}"
            self.next_id += 1
        return products

    def seed(self, count):
        products = self._fresh_products(count)
        for product in products:
            vec = self.store.upsert(product)
            self.mirror.upsert(product, vec)
        print(f"seeded {count} products (mirror size {len(self.mirror.products)})")

    def _random_sku(self):
        return self.rng.choice(list(self.mirror.products)) if self.mirror.products else None

    def op_add(self):
        product = self._fresh_products(1)[0]
        vec = self.store.upsert(product)
        self.mirror.upsert(product, vec)

    def op_delete(self):
        sku = self._random_sku()
        if sku:
            self.store.delete(sku)
            self.mirror.delete(sku)

    def op_flip_stock(self):
        sku = self._random_sku()
        if not sku:
            return
        product = self.mirror.products[sku]
        product["in_stock"] = "no" if product["in_stock"] == "yes" else "yes"
        vec = self.store.upsert(product)
        self.mirror.upsert(product, vec)

    def op_change_price(self):
        sku = self._random_sku()
        if not sku:
            return
        product = self.mirror.products[sku]
        _, subcats = catalog.DEPARTMENTS[product["category"]]
        lo, hi = subcats[product["subcategory"]]
        price = round(self.rng.uniform(lo, hi), 2)
        product["price"] = f"{price:.2f}"
        product["price_bucket"] = catalog.bucket_for(price)
        vec = self.store.upsert(product)
        self.mirror.upsert(product, vec)

    def op_rewrite_text(self):
        """Changes description -> changes embedding + inverted index terms."""
        sku = self._random_sku()
        if not sku:
            return
        product = self.mirror.products[sku]
        template = self.rng.choice(
            catalog.DESCRIPTION_TEMPLATES[product["category"]])
        series = self.rng.choice(catalog.SERIES)
        product["description"] = template.format(
            brand=product["brand"].replace("_", " "), series=series,
            sub=product["subcategory"].replace("_", " "),
            color=product["color"])
        vec = self.store.upsert(product)
        self.mirror.upsert(product, vec)

    OPS = [(op_add, 20), (op_delete, 15), (op_flip_stock, 25),
           (op_change_price, 20), (op_rewrite_text, 20)]

    # ------------------------------------------------------------- verify

    def verify(self):
        m = self.mirror

        # 1. Total document count.
        got = self.store.count()
        if got != len(m.products):
            self._fail(f"doc count: index={got} mirror={len(m.products)}",
                       f"FT.SEARCH {self.store.index} * LIMIT 0 0")

        # 2. Random TAG-filter counts.
        for field in ("category", "brand", "price_bucket", "in_stock"):
            if not m.products:
                break
            value = self.rng.choice(list(m.products.values()))[field]
            got = self.store.count(f"@{field}:{{{value}}}")
            expected = m.count(**{field: value})
            if got != expected:
                self._fail(
                    f"tag count @{field}:{{{value}}}: index={got} mirror={expected}",
                    f"FT.SEARCH {self.store.index} '@{field}:{{{value}}}' LIMIT 0 0")

        # 3. Point lookups: live sku found with fresh fields, dead sku gone.
        sku = self._random_sku()
        if sku:
            total, docs = self.store.text_search(None, filters={"sku": sku})
            repro = f"FT.SEARCH {self.store.index} '@sku:{{{sku}}}'"
            if total != 1 or not docs:
                self._fail(f"sku lookup {sku}: expected 1 hit, got {total}", repro)
            else:
                expected = m.products[sku]
                stale = {f: (docs[0].get(f), expected[f])
                         for f in ("in_stock", "price_bucket", "color")
                         if docs[0].get(f) != expected[f]}
                if stale:
                    self._fail(f"stale fields for {sku}: {stale}", repro)
        dead = f"CH{self.rng.randrange(self.next_id):06d}" if self.next_id else None
        if dead and dead not in m.products:
            got = self.store.count(f"@sku:{{{dead}}}")
            if got != 0:
                self._fail(f"ghost doc: deleted {dead} still indexed",
                           f"FT.SEARCH {self.store.index} '@sku:{{{dead}}}'")

        # 4. KNN: distance correctness (hard) + recall (soft) + no ghosts.
        sku = self._random_sku()
        if sku:
            query_vec = m.vectors[sku]
            k = min(10, len(m.products))
            _, docs = self.store.knn(query_vec, k=k)
            truth = dict(m.knn(query_vec, k * 3))
            for doc in docs:
                dsku = doc.get("sku")
                if dsku not in m.products:
                    self._fail(f"KNN returned ghost doc {dsku}",
                               f"KNN {k} around embedding of {sku}")
                    continue
                if doc.get("dist") is None:
                    self._fail(f"KNN hit {dsku} has no stored vector",
                               f"HGET {self.store.key(dsku)} embedding")
                    continue
                expected_dist = embeddings.cosine_distance(
                    m.vectors[dsku], query_vec)
                got_dist = float(doc["dist"])
                if abs(got_dist - expected_dist) > DIST_TOLERANCE:
                    self._fail(
                        f"KNN distance mismatch for {dsku}: index={got_dist:.6f} "
                        f"brute-force={expected_dist:.6f} (stale vector?)",
                        f"HGET {self.store.key(dsku)} embedding vs reported dist")
            top = set(dict(m.knn(query_vec, k)))
            returned = {d.get("sku") for d in docs}
            recall = len(top & returned) / max(1, len(top))
            if recall < RECALL_WARN_THRESHOLD:
                self._warn(f"KNN recall@{k} = {recall:.2f} around {sku} "
                           f"(soft check, HNSW is approximate)")

        # 5. Hybrid (pre-filtered) KNN with explicit policy: subset check.
        sku = self._random_sku()
        if sku:
            category = m.products[sku]["category"]
            _, docs = self.store.knn(m.vectors[sku], k=5,
                                     filters={"category": category},
                                     policy=self.rng.choice(["ADHOC_BF", "BATCHES"]))
            for doc in docs:
                dsku = doc.get("sku")
                if dsku in m.products and m.products[dsku]["category"] != category:
                    self._fail(
                        f"hybrid KNN leaked filter: {dsku} has category "
                        f"{m.products[dsku]['category']}, filtered on {category}",
                        f"(@category:{{{category}}})=>[KNN 5 ...] returned {dsku}")

    # ----------------------------------------------------------------- run

    def run(self, ops=1000, verify_every=100):
        weights = [w for _, w in self.OPS]
        funcs = [f for f, _ in self.OPS]
        start = time.time()
        for i in range(1, ops + 1):
            self.rng.choices(funcs, weights)[0](self)
            if i % verify_every == 0:
                self.verify()
                print(f"[{i}/{ops}] docs={len(self.mirror.products)} "
                      f"failures={len(self.failures)} "
                      f"warnings={len(self.warnings)} "
                      f"({time.time() - start:.0f}s)")
        self.verify()
        print(f"\ndone: {ops} ops, {len(self.failures)} failures, "
              f"{len(self.warnings)} soft warnings")
        return self.failures
