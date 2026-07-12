"""Deterministic synthetic embeddings (384-dim float32, unit-normalized).

No model download required: each subcategory gets a stable random cluster
center, and the text contributes hashed bag-of-words features. Products of
the same subcategory land close together (so "similar items" is
meaningful), while text tokens shared between a query and a product pull
them together (so free-text semantic search behaves sensibly).

Everything is seeded by content, never by global state, so the same
product always produces the same vector — a requirement for the chaos
oracle's brute-force ground truth.

Set RIDE_TIDE_REAL_EMBEDDINGS=1 to use sentence-transformers
(all-MiniLM-L6-v2, also 384-dim) if it is installed.
"""

import hashlib
import os
import re

import numpy as np

DIM = 384

_TOKEN_RE = re.compile(r"[a-z0-9]+")

_real_model = None
if os.getenv("RIDE_TIDE_REAL_EMBEDDINGS"):
    from sentence_transformers import SentenceTransformer
    _real_model = SentenceTransformer("all-MiniLM-L6-v2")


_vector_cache = {}


def _seeded_vector(seed_text):
    # Token/subcategory vectors recur constantly during bulk generation —
    # memoize them. Per-doc "sku:" seeds are unique; caching those would
    # only bloat the cache, so they bypass it.
    cacheable = not seed_text.startswith("sku:")
    if cacheable and seed_text in _vector_cache:
        return _vector_cache[seed_text]
    seed = int.from_bytes(hashlib.md5(seed_text.encode()).digest()[:8], "big")
    rng = np.random.default_rng(seed)
    vec = rng.standard_normal(DIM).astype(np.float32)
    if cacheable:
        _vector_cache[seed_text] = vec
    return vec


def _bow_vector(text):
    vec = np.zeros(DIM, dtype=np.float32)
    tokens = _TOKEN_RE.findall(text.lower())
    for token in tokens:
        vec += _seeded_vector("tok:" + token)
    norm = np.linalg.norm(vec)
    return vec / norm if norm > 0 else vec


def _normalize(vec):
    norm = np.linalg.norm(vec)
    return (vec / norm).astype(np.float32) if norm > 0 else vec


def embed_product(product):
    if _real_model is not None:
        text = f"{product['name']}. {product['description']}"
        return _normalize(np.asarray(_real_model.encode(text), dtype=np.float32))
    center = _seeded_vector("subcat:" + product["subcategory"])
    bow = _bow_vector(f"{product['name']} {product['description']} "
                      f"{product['category']}")
    noise = _seeded_vector("sku:" + product["sku"])
    return _normalize(0.55 * _normalize(center) + 0.35 * bow + 0.10 * _normalize(noise))


def embed_query(text):
    if _real_model is not None:
        return _normalize(np.asarray(_real_model.encode(text), dtype=np.float32))
    vec = _bow_vector(text)
    # Pull the query toward any subcategory cluster it names, mirroring how
    # a real sentence encoder maps "wetsuit for cold water" near wetsuits.
    tokens = set(_TOKEN_RE.findall(text.lower()))
    from catalog import DEPARTMENTS
    for _, subcats in DEPARTMENTS.values():
        for subcategory in subcats:
            if set(subcategory.split("_")) & tokens:
                vec = vec + 0.8 * _normalize(_seeded_vector("subcat:" + subcategory))
    return _normalize(vec)


def to_bytes(vec):
    return vec.astype(np.float32).tobytes()


def from_bytes(blob):
    return np.frombuffer(blob, dtype=np.float32)


def cosine_distance(a, b):
    return 1.0 - float(np.dot(_normalize(a), _normalize(b)))
