"""Index fleet definition and creation.

One index per marketplace vertical, alternating HASH and JSON document types —
several indexes on one DB also stress the shared write-buffer-manager path.
Disk-index constraints honored here: TEXT/TAG fields only, no SORTABLE /
NOINDEX / WITHSUFFIXTRIE, JSON attributes use single-value JSONPaths, and
SKIPINITIALSCAN is passed (pre-existing keys are never back-indexed, so
indexes must be created before loading).
"""

import redis

from datagen import VERTICALS

TEXT_FIELDS = ["title", "description"]
TAG_FIELDS = ["brand", "category", "city", "condition", "seller", "price_bucket"]


def index_name(v_idx: int) -> str:
    return f"idx:bazaar_{VERTICALS[v_idx][0]}"


def index_doctype(v_idx: int) -> str:
    return VERTICALS[v_idx][1]


def prefix(v_idx: int) -> str:
    return f"l{v_idx}:"


def create_args(v_idx: int) -> list[str]:
    name, doctype, _, _ = VERTICALS[v_idx]
    args = ["FT.CREATE", index_name(v_idx), "ON", doctype.upper(),
            "PREFIX", "1", prefix(v_idx), "SKIPINITIALSCAN", "SCHEMA"]
    for f in TEXT_FIELDS:
        if doctype == "json":
            args += [f"$.{f}", "AS", f, "TEXT"]
        else:
            args += [f, "TEXT"]
    for f in TAG_FIELDS:
        if doctype == "json":
            args += [f"$.{f}", "AS", f, "TAG"]
        else:
            args += [f, "TAG"]
    return args


def create_indexes(client: redis.Redis) -> list[str]:
    """Create the whole fleet; existing indexes are left as-is."""
    created = []
    for v_idx in range(len(VERTICALS)):
        try:
            client.execute_command(*create_args(v_idx))
            created.append(index_name(v_idx))
        except redis.exceptions.ResponseError as e:
            if "already exists" not in str(e).lower():
                raise
    return created


def drop_indexes(client: redis.Redis) -> list[str]:
    """FT.DROPINDEX the fleet (no DD on disk indexes — doc keys survive)."""
    dropped = []
    for v_idx in range(len(VERTICALS)):
        try:
            client.execute_command("FT.DROPINDEX", index_name(v_idx))
            dropped.append(index_name(v_idx))
        except redis.exceptions.ResponseError as e:
            if "unknown index" not in str(e).lower() and "no such index" not in str(e).lower():
                raise
    return dropped
