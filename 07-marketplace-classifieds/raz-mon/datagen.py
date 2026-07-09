"""Deterministic synthetic listing generator.

Every listing is a pure function of (seed, doc_id, version): the loader, the
churn writers, and the verifier can all regenerate the exact same document
independently, so there is no client-side state store and every component is
resumable. Static identity (vertical, seller) depends on doc_id only; content
(title, description, price, ...) depends on the version too, so an update is
just "bump the version and rewrite".

The TEXT vocabulary is a fixed-size list of pseudo-words sampled with a
log-uniform (Zipf-like) rank distribution — unique-term count is a pinned-RAM
knob on disk indexes (~100MB per 1M terms), so it is capped deliberately.
"""

import math
import random

# 48 syllables -> word index i maps to a unique syllable sequence. Low ranks
# (the most frequently sampled) get the shortest words, like a real corpus.
SYLLABLES = [
    "ba", "be", "bo", "da", "de", "di", "fa", "fe", "ga", "go", "ka", "ke",
    "ki", "la", "le", "lo", "ma", "me", "mi", "na", "ne", "no", "pa", "pe",
    "po", "ra", "re", "ri", "sa", "se", "so", "ta", "te", "ti", "va", "ve",
    "vo", "za", "ze", "zu", "cha", "sha", "tra", "pla", "gra", "sta", "bri", "clo",
]

# Default RediSearch stopwords: they are dropped at indexing time, and a quoted
# phrase containing one is a query syntax error — keep them out of the vocab.
# The 'x' suffix cannot collide with another generated word (syllables end in
# vowels), so the mapping stays bijective.
STOPWORDS = frozenset(
    "a is the an and are as at be but by for if in into it no not of on or "
    "such that their then there these they this to was will with".split()
)

CONDITIONS = ["new", "like_new", "good", "fair", "for_parts"]
CONDITION_WEIGHTS = [0.15, 0.20, 0.35, 0.20, 0.10]

NUM_BRANDS = 5000
NUM_CITIES = 500
PRICE_BUCKETS = 50

# (name, doc_type, weight, subcategories) — weight skews index sizes on purpose.
VERTICALS = [
    ("electronics", "hash", 0.28,
     ["phones", "laptops", "tablets", "cameras", "audio", "tv", "consoles",
      "wearables", "components", "accessories"]),
    ("fashion", "json", 0.22,
     ["shoes", "jackets", "dresses", "jeans", "bags", "watches", "jewelry",
      "sportswear", "kids", "vintage"]),
    ("home", "hash", 0.18,
     ["furniture", "kitchen", "garden", "lighting", "appliances", "decor",
      "tools", "bedding", "storage", "heating"]),
    ("vehicles", "json", 0.14,
     ["cars", "motorcycles", "bicycles", "scooters", "parts", "tires",
      "trailers", "boats", "vans", "trucks"]),
    ("sports", "hash", 0.10,
     ["fitness", "camping", "fishing", "ski", "climbing", "running",
      "cycling", "ballgames", "water", "hunting"]),
    ("collectibles", "json", 0.08,
     ["coins", "stamps", "cards", "comics", "vinyl", "art", "antiques",
      "models", "memorabilia", "books"]),
]


def word_from_index(i: int) -> str:
    n = len(SYLLABLES)
    parts = [SYLLABLES[i % n]]
    i //= n
    while i:
        i -= 1  # so lengths nest without collisions
        parts.append(SYLLABLES[i % n])
        i //= n
    w = "".join(reversed(parts))
    return w + "x" if w in STOPWORDS else w


def _name_list(count: int, salt: str, suffixes: list[str]) -> list[str]:
    rng = random.Random(f"names:{salt}")
    names = []
    seen = set()
    while len(names) < count:
        w = word_from_index(rng.randrange(0, 60000))
        name = w.capitalize() + rng.choice(suffixes)
        if name not in seen:
            seen.add(name)
            names.append(name)
    return names


class Generator:
    """Built once per process from the config; all methods are deterministic."""

    def __init__(self, seed: int, vocab_size: int, num_sellers: int, description_words: int):
        self.seed = seed
        self.vocab_size = vocab_size
        self.num_sellers = num_sellers
        self.description_words = description_words
        self.vocab = [word_from_index(i) for i in range(vocab_size)]
        self.brands = _name_list(NUM_BRANDS, "brands", ["", "", "tech", "works", "co", "lab"])
        self.cities = _name_list(NUM_CITIES, "cities", ["", "ville", "burg", "port", "field"])
        cum, acc = [], 0.0
        for _, _, w, _ in VERTICALS:
            acc += w
            cum.append(acc)
        self._vertical_cum = cum

    # -------- identity (doc_id only — stable across versions)

    def vertical_of(self, doc_id: int) -> int:
        u = random.Random(f"{self.seed}:{doc_id}:vertical").random()
        for i, c in enumerate(self._vertical_cum):
            if u <= c:
                return i
        return len(VERTICALS) - 1

    def key(self, doc_id: int) -> str:
        return f"l{self.vertical_of(doc_id)}:{doc_id}"

    def seller_of(self, doc_id: int) -> str:
        return f"s{doc_id % self.num_sellers}"

    # -------- sampling helpers

    def zipf_word(self, rng: random.Random, top: int | None = None) -> str:
        """Log-uniform rank => frequency ~ 1/rank, over the whole vocab or its head."""
        v = top or self.vocab_size
        return self.vocab[min(v - 1, int(v ** rng.random()) - 1) if v > 1 else 0]

    def zipf_index(self, rng: random.Random, n: int) -> int:
        return min(n - 1, int(n ** rng.random()) - 1) if n > 1 else 0

    # -------- the document

    def generate(self, doc_id: int, version: int) -> tuple[str, int, dict]:
        """Returns (key, vertical_index, fields)."""
        v_idx = self.vertical_of(doc_id)
        v_name, _, _, subcats = VERTICALS[v_idx]
        rng = random.Random(f"{self.seed}:{doc_id}:v{version}")

        brand = self.brands[self.zipf_index(rng, NUM_BRANDS)]
        city = self.cities[self.zipf_index(rng, NUM_CITIES)]
        subcat = subcats[self.zipf_index(rng, len(subcats))]
        condition = rng.choices(CONDITIONS, CONDITION_WEIGHTS)[0]

        price = round(rng.lognormvariate(4.0 + v_idx * 0.7, 1.2) + 1.0, 2)
        bucket = min(PRICE_BUCKETS - 1, int(math.log(price, 1.3)))

        title_words = [self.zipf_word(rng, top=5000) for _ in range(rng.randint(3, 5))]
        title = f"{brand} {subcat} {' '.join(title_words)}"

        n_words = self.description_words + rng.randint(-30, 30)
        words = []
        for i in range(n_words):
            if i % 40 == 25:  # sprinkle entities into the text body
                words.append(rng.choice((brand.lower(), city.lower(), subcat)))
            else:
                words.append(self.zipf_word(rng))
        description = " ".join(words)

        fields = {
            "title": title,
            "description": description,
            "brand": brand,
            "category": f"{v_name}_{subcat}",
            "city": city,
            "condition": condition,
            "seller": self.seller_of(doc_id),
            "price_bucket": f"b{bucket:02d}",
            "price": f"{price:.2f}",
            "ver": str(version),
        }
        return f"l{v_idx}:{doc_id}", v_idx, fields


def make_generator(cfg) -> Generator:
    return Generator(cfg.seed, cfg.vocab_size, cfg.num_sellers, cfg.description_words)
