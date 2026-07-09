"""Weighted query templates — all legal on disk indexes.

Kept inside the documented disk-index limits: no SLOP/INORDER, no
SUMMARIZE/HIGHLIGHT, no TAG prefix/wildcard queries, no WITHCURSOR on
aggregations, bounded pagination offsets. Query terms are sampled with the
same skew the generator writes with, so queries hit real data.
"""

import random

from datagen import NUM_BRANDS, NUM_CITIES, VERTICALS, Generator
from schema import index_name


class QueryMix:
    def __init__(self, gen: Generator, cfg):
        self.gen = gen
        self.limit = str(cfg.storm["limit"])
        self.max_offset = int(cfg.storm["max_offset"])
        self.templates = [
            ("text_single", 15, self.text_single),
            ("text_and2", 12, self.text_and2),
            ("text_or3", 8, self.text_or3),
            ("phrase2", 5, self.phrase2),
            ("text_prefix", 8, self.text_prefix),
            ("text_fuzzy", 5, self.text_fuzzy),
            ("text_tags", 15, self.text_tags),
            ("tag_facet", 10, self.tag_facet),
            ("seller_scope", 8, self.seller_scope),
            ("negation", 4, self.negation),
            ("paginate_deep", 3, self.paginate_deep),
            ("agg_facet_count", 4, self.agg_facet_count),
            ("agg_avg_price", 3, self.agg_avg_price),
        ]
        self._names = [t[0] for t in self.templates]
        self._weights = [t[1] for t in self.templates]
        self._fns = {t[0]: t[2] for t in self.templates}
        self._vertical_weights = [v[2] for v in VERTICALS]

    # ---- term/tag helpers (same skew as the data)

    def _word(self, rng, top=5000):
        return self.gen.zipf_word(rng, top=top)

    def _brand(self, rng):
        return self.gen.brands[self.gen.zipf_index(rng, NUM_BRANDS)]

    def _city(self, rng):
        return self.gen.cities[self.gen.zipf_index(rng, NUM_CITIES)]

    def _category(self, rng, v_idx):
        name, _, _, subcats = VERTICALS[v_idx]
        return f"{name}_{subcats[self.gen.zipf_index(rng, len(subcats))]}"

    def _search(self, query, *extra):
        return ["FT.SEARCH", None, query, "LIMIT", "0", self.limit, *extra]

    # ---- FT.SEARCH templates (index name is filled in by sample())

    def text_single(self, rng, v_idx):
        return self._search(self._word(rng), "RETURN", "2", "title", "brand")

    def text_and2(self, rng, v_idx):
        return self._search(f"{self._word(rng)} {self._word(rng)}", "NOCONTENT")

    def text_or3(self, rng, v_idx):
        return self._search(f"({self._word(rng)}|{self._word(rng)}|{self._word(rng)})",
                            "NOCONTENT")

    def phrase2(self, rng, v_idx):
        return self._search(f'"{self._word(rng, top=500)} {self._word(rng, top=500)}"',
                            "NOCONTENT")

    def text_prefix(self, rng, v_idx):
        w = self.gen.vocab[rng.randrange(2500, min(60000, self.gen.vocab_size))]
        return self._search(f"{w[:4]}*", "NOCONTENT")

    def text_fuzzy(self, rng, v_idx):
        return self._search(f"%{self._word(rng, top=2000)}%", "NOCONTENT")

    def text_tags(self, rng, v_idx):
        q = f"{self._word(rng)} @city:{{{self._city(rng)}}}"
        if rng.random() < 0.5:
            q += " @condition:{good}"
        return self._search(q, "RETURN", "2", "title", "city")

    def tag_facet(self, rng, v_idx):
        if rng.random() < 0.5:
            q = f"@brand:{{{self._brand(rng)}}} @city:{{{self._city(rng)}}}"
        else:
            q = f"@category:{{{self._category(rng, v_idx)}}} @condition:{{{rng.choice(('new', 'good', 'fair'))}}}"
        return self._search(q, "NOCONTENT")

    def seller_scope(self, rng, v_idx):
        seller = f"s{rng.randrange(self.gen.num_sellers)}"
        return self._search(f"@seller:{{{seller}}}", "RETURN", "2", "title", "seller")

    def negation(self, rng, v_idx):
        return self._search(f"{self._word(rng)} -@condition:{{for_parts}}", "NOCONTENT")

    def paginate_deep(self, rng, v_idx):
        offset = rng.randrange(100, self.max_offset)
        return ["FT.SEARCH", None, self._word(rng, top=200), "NOCONTENT",
                "LIMIT", str(offset), self.limit]

    # ---- FT.AGGREGATE templates

    def agg_facet_count(self, rng, v_idx):
        return ["FT.AGGREGATE", None, f"@city:{{{self._city(rng)}}}",
                "GROUPBY", "1", "@category", "REDUCE", "COUNT", "0", "AS", "cnt",
                "SORTBY", "2", "@cnt", "DESC", "LIMIT", "0", "20"]

    def agg_avg_price(self, rng, v_idx):
        # `price` is not in the schema: LOAD it raw and APPLY to_number() —
        # the documented pattern for numeric work on disk indexes.
        if VERTICALS[v_idx][1] == "json":
            load = ["LOAD", "3", "$.price", "AS", "price"]
        else:
            load = ["LOAD", "1", "@price"]
        return ["FT.AGGREGATE", None, f"@city:{{{self._city(rng)}}}", *load,
                "APPLY", "to_number(@price)", "AS", "p",
                "GROUPBY", "1", "@category", "REDUCE", "AVG", "1", "@p", "AS", "avg_price",
                "LIMIT", "0", "10"]

    # ---- sampling

    def sample(self, rng: random.Random):
        """Returns (template_name, index_v_idx, command args ready to execute)."""
        v_idx = rng.choices(range(len(VERTICALS)), self._vertical_weights)[0]
        name = rng.choices(self._names, self._weights)[0]
        args = self._fns[name](rng, v_idx)
        args[1] = index_name(v_idx)
        # 25s server-side budget (< the 30s client socket_timeout), so slow
        # disk queries complete instead of failing with SEARCH_TIMEOUT.
        args += ["TIMEOUT", "25000"]
        return name, v_idx, args
