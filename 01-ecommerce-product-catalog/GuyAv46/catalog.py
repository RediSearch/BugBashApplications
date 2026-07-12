"""Deterministic product catalog generator for the Ride & Tide store.

Three departments: motorbikes, surfing, sailing. Products are generated
deterministically from a seed so a bug found with `--seed 42 --count 5000`
is exactly reproducible.

Schema note (Flex/RoF limitations): NUMERIC fields are unsupported, so the
price is stored twice — as a `price_bucket` TAG for filtering, and as a raw
(unindexed) `price` hash field for FT.AGGREGATE `to_number()` sorting/stats.
Tag values use snake_case (no spaces/dashes) to avoid TAG-query escaping.
"""

import random

PRICE_BUCKETS = [
    (50, "under_50"),
    (100, "50_100"),
    (250, "100_250"),
    (500, "250_500"),
    (1000, "500_1000"),
    (5000, "1000_5000"),
    (float("inf"), "over_5000"),
]

COLORS = ["black", "red", "blue", "white", "yellow", "green", "orange",
          "silver", "teal", "navy"]

SERIES = ["Apex", "Vortex", "Storm", "Carbon", "Pro", "Elite", "Classic",
          "Sport", "Touring", "Horizon", "Drift", "Nomad", "Falcon", "Tide"]

# category -> (brands, {subcategory: (min_price, max_price)})
DEPARTMENTS = {
    "motorbikes": (
        ["Ducati", "Yamaha", "Kawasaki", "Triumph", "KTM", "Honda",
         "Royal_Enfield", "Harley_Davidson"],
        {
            "motorcycle": (4000, 22000),
            "helmet": (80, 900),
            "riding_jacket": (120, 800),
            "gloves": (30, 250),
            "boots": (90, 500),
            "exhaust": (300, 2500),
            "saddlebag": (100, 700),
        },
    ),
    "surfing": (
        ["Rip_Curl", "Quiksilver", "Billabong", "ONeill", "Firewire",
         "Channel_Islands", "FCS", "Dakine"],
        {
            "surfboard": (350, 1400),
            "wetsuit": (100, 600),
            "leash": (20, 60),
            "fins": (40, 200),
            "board_bag": (60, 300),
            "surf_wax": (5, 15),
            "rash_guard": (25, 80),
        },
    ),
    "sailing": (
        ["Musto", "Gill", "Helly_Hansen", "Harken", "Lewmar", "Ronstan",
         "Spinlock", "Zhik"],
        {
            "sailing_jacket": (150, 900),
            "winch": (400, 3000),
            "mooring_rope": (30, 200),
            "life_vest": (60, 350),
            "deck_shoes": (70, 220),
            "mainsail": (800, 6000),
            "tiller_extension": (50, 250),
        },
    ),
}

DESCRIPTION_TEMPLATES = {
    "motorbikes": [
        "Built for the open road, this {sub} from {brand} delivers "
        "confident handling and all-weather durability. The {series} line "
        "features reinforced stitching, ventilated panels and a {color} "
        "finish riders love on long touring days.",
        "The {brand} {series} {sub} combines lightweight construction with "
        "race-proven protection. Ideal for commuters and track-day "
        "enthusiasts alike, finished in {color}.",
        "Aggressive styling meets everyday comfort in this {color} {sub}. "
        "Part of the {series} collection, engineered by {brand} for "
        "maximum grip and abrasion resistance on asphalt.",
    ],
    "surfing": [
        "Shaped for speed down the line, the {brand} {series} {sub} thrives "
        "in punchy beach breaks and clean point waves. The {color} deck "
        "keeps you visible in the lineup.",
        "Cold-water sessions demand gear like this {color} {sub} from "
        "{brand}. Flexible, warm and quick-drying, the {series} model is a "
        "favorite for dawn patrol surfers.",
        "The {series} {sub} by {brand} offers a lively, responsive feel "
        "under your feet. Perfect for progressing surfers chasing bigger "
        "swell, finished in {color}.",
    ],
    "sailing": [
        "Trusted offshore, the {brand} {series} {sub} is fully waterproof "
        "and shrugs off spray and heavy weather. Corrosion-resistant "
        "hardware and a {color} shell make it a staple for coastal "
        "cruising crews.",
        "Regatta-ready performance: this {color} {sub} from {brand} is cut "
        "for freedom of movement on deck. The {series} range is proven "
        "across ocean racing campaigns.",
        "The {brand} {series} {sub} balances durability with low weight, "
        "ideal for club racers and blue-water sailors alike. Supplied in "
        "{color} with marine-grade fittings.",
    ],
}


def bucket_for(price):
    for limit, name in PRICE_BUCKETS:
        if price < limit:
            return name
    return PRICE_BUCKETS[-1][1]


def generate(count, seed=42, start_index=0):
    """Yield `count` product dicts, deterministic in `(seed, start_index)`.

    The category rotates with the positional index; single-doc callers
    (scale ingest) must pass a varying `start_index` or every doc lands in
    the first category.
    """
    rng = random.Random(seed)
    categories = list(DEPARTMENTS)
    for i in range(count):
        category = categories[(start_index + i) % len(categories)]
        brands, subcats = DEPARTMENTS[category]
        brand = rng.choice(brands)
        subcategory = rng.choice(list(subcats))
        lo, hi = subcats[subcategory]
        price = round(rng.uniform(lo, hi), 2)
        series = rng.choice(SERIES)
        color = rng.choice(COLORS)
        sub_words = subcategory.replace("_", " ")
        brand_words = brand.replace("_", " ")
        template = rng.choice(DESCRIPTION_TEMPLATES[category])
        yield {
            "sku": f"RT{i:06d}",
            "name": f"{brand_words} {series} {sub_words}",
            "description": template.format(
                brand=brand_words, series=series, sub=sub_words, color=color),
            "brand": brand,
            "category": category,
            "subcategory": subcategory,
            "color": color,
            "price": f"{price:.2f}",
            "price_bucket": bucket_for(price),
            "in_stock": "yes" if rng.random() < 0.9 else "no",
        }
