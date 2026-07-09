"""Shared plumbing: config, Redis client factory, rate limiting, and metrics.

Metrics model: every worker aggregates operations into fixed log-scale latency
histograms per (kind, op) and flushes a JSON line every few seconds to its own
file under the run directory. report.py merges the histograms exactly, so
percentiles survive multi-process aggregation.
"""

import json
import math
import os
import time
from pathlib import Path

import redis
import yaml

APP_DIR = Path(__file__).resolve().parent

# ---------------------------------------------------------------- config


class Config:
    """Flat view over config.yaml with the chosen scale profile applied."""

    def __init__(self, raw: dict, profile: str | None, overrides: dict):
        self.raw = raw
        self.profile_name = profile or raw.get("profile", "smoke")
        prof = raw["profiles"][self.profile_name]

        r = raw["redis"]
        self.host = overrides.get("host") or os.environ.get("BAZAAR_HOST") or r["host"]
        self.port = int(overrides.get("port") or os.environ.get("BAZAAR_PORT") or r["port"])
        self.password = (
            overrides.get("password") or os.environ.get("BAZAAR_PASSWORD") or r.get("password")
        )
        tls = overrides.get("tls")
        if tls is None:
            tls = os.environ.get("BAZAAR_TLS", "").lower() in ("1", "true", "yes") or r.get("tls")
        self.tls = bool(tls)
        self.tls_ca = overrides.get("tls_ca") or r.get("tls_ca")

        self.total_docs = int(prof["total_docs"])
        self.vocab_size = int(prof["vocab_size"])
        self.num_sellers = int(prof["num_sellers"])
        self.load_workers = int(prof["load_workers"])
        self.churn_workers = int(prof["churn_workers"])
        self.churn_ops_per_sec = float(prof["churn_ops_per_sec"])
        self.storm_workers = int(prof["storm_workers"])

        self.description_words = int(raw["doc"]["description_words"])
        self.churn = raw["churn"]
        self.storm = raw["storm"]
        self.seed = int(raw["seed"])
        self.out_dir = APP_DIR / raw.get("out_dir", "out")
        self.state_dir = APP_DIR / raw.get("state_dir", "state") / self.profile_name


def load_config(args) -> Config:
    """Build a Config from --config plus CLI overrides (argparse namespace)."""
    path = Path(getattr(args, "config", None) or APP_DIR / "config.yaml")
    raw = yaml.safe_load(path.read_text())
    overrides = {
        k: getattr(args, k, None) for k in ("host", "port", "password", "tls", "tls_ca")
    }
    return Config(raw, getattr(args, "profile", None), overrides)


def make_client(cfg: Config, decode: bool = True) -> redis.Redis:
    """RESP3 client so OOM/timeout warnings in FT replies are visible."""
    kwargs = dict(
        host=cfg.host,
        port=cfg.port,
        password=cfg.password,
        protocol=3,
        decode_responses=decode,
        socket_timeout=30,
        socket_connect_timeout=10,
        health_check_interval=30,
    )
    if cfg.tls:
        kwargs.update(ssl=True, ssl_cert_reqs="required" if cfg.tls_ca else "none")
        if cfg.tls_ca:
            kwargs["ssl_ca_certs"] = cfg.tls_ca
    return redis.Redis(**kwargs)


# ---------------------------------------------------------------- rate limiting


class TokenBucket:
    """Simple token bucket; refills continuously, take() blocks until a token."""

    def __init__(self, rate_per_sec: float, burst: float | None = None):
        self.rate = rate_per_sec
        self.capacity = burst if burst is not None else max(1.0, rate_per_sec / 10)
        self.tokens = self.capacity
        self.last = time.monotonic()

    def take(self):
        while True:
            now = time.monotonic()
            self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
            self.last = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            time.sleep(max((1.0 - self.tokens) / self.rate, 0.001))


# ---------------------------------------------------------------- latency histograms

# Log-scale buckets, ~12% resolution: bucket i covers latencies around 1.12^i ms.
_LOG_BASE = math.log(1.12)


def lat_bucket(ms: float) -> int:
    return max(0, int(math.log(max(ms, 0.01)) / _LOG_BASE) + 40)


def bucket_upper_ms(bucket: int) -> float:
    return math.exp((bucket - 40 + 1) * _LOG_BASE)


def percentile_from_hist(hist: dict, q: float) -> float:
    """hist: {bucket(int|str): count}. Returns approx latency ms at quantile q."""
    items = sorted((int(b), c) for b, c in hist.items())
    total = sum(c for _, c in items)
    if total == 0:
        return 0.0
    target = q * total
    acc = 0
    for b, c in items:
        acc += c
        if acc >= target:
            return bucket_upper_ms(b)
    return bucket_upper_ms(items[-1][0])


# ---------------------------------------------------------------- stats writer


class StatsWriter:
    """Per-worker metrics aggregator; flushes JSONL windows to its own file."""

    def __init__(self, run_dir: Path, kind: str, worker: int, flush_every_s: float = 10.0):
        run_dir.mkdir(parents=True, exist_ok=True)
        self.path = run_dir / f"{kind}-{worker}.jsonl"
        self.kind = kind
        self.worker = worker
        self.flush_every_s = flush_every_s
        self._fh = open(self.path, "a", buffering=1)
        self._reset()

    def _reset(self):
        self.window_start = time.time()
        self.ops = {}  # op -> {"count", "errors", "timeouts", "warnings", "hist"}

    def _op(self, op: str) -> dict:
        return self.ops.setdefault(
            op, {"count": 0, "errors": 0, "timeouts": 0, "warnings": 0, "hist": {}}
        )

    def record(self, op: str, lat_ms: float, *, n=1, error=False, timeout=False, warning=False):
        s = self._op(op)
        s["count"] += n
        s["errors"] += int(error)
        s["timeouts"] += int(timeout)
        s["warnings"] += int(warning)
        b = lat_bucket(lat_ms)
        s["hist"][b] = s["hist"].get(b, 0) + 1
        if time.time() - self.window_start >= self.flush_every_s:
            self.flush()

    def event(self, op: str, **fields):
        """One-off structured event (e.g. a verification failure)."""
        line = {"ts": time.time(), "kind": self.kind, "worker": self.worker,
                "type": "event", "op": op, **fields}
        self._fh.write(json.dumps(line) + "\n")

    def flush(self):
        if self.ops:
            line = {
                "ts": time.time(),
                "kind": self.kind,
                "worker": self.worker,
                "type": "window",
                "window_s": round(time.time() - self.window_start, 1),
                "ops": self.ops,
            }
            self._fh.write(json.dumps(line) + "\n")
        self._reset()

    def close(self):
        self.flush()
        self._fh.close()


# ---------------------------------------------------------------- FT reply helpers


def classify_error(exc: Exception) -> str:
    """Map an exception from a query to a coarse class for metrics."""
    msg = str(exc)
    if "imeout" in msg:
        return "timeout"
    if "SEARCH_DISK" in msg or "Disk" in msg:
        return "disk"
    if isinstance(exc, redis.exceptions.ConnectionError):
        return "connection"
    return "other"


def reply_warning(reply) -> bool:
    """True if a RESP3 FT.SEARCH/FT.AGGREGATE reply carries a warning."""
    if isinstance(reply, dict):
        w = reply.get("warning") or reply.get(b"warning")
        return bool(w)
    return False


def reply_keys(reply) -> list:
    """Document keys from an FT.SEARCH reply, RESP3 map or RESP2 array."""
    if isinstance(reply, dict):
        return [r.get("id") for r in reply.get("results", [])]
    # RESP2: [total, key, fields, key, fields, ...] (or [total, key, key...] with NOCONTENT)
    keys = []
    for item in reply[1:]:
        if isinstance(item, (str, bytes)):
            keys.append(item)
    return keys


def reply_total(reply) -> int:
    if isinstance(reply, dict):
        return int(reply.get("total_results", 0))
    return int(reply[0]) if reply else 0


def new_run_dir(cfg: Config, name: str | None = None) -> Path:
    run = name or time.strftime("%Y%m%d-%H%M%S")
    d = cfg.out_dir / run
    d.mkdir(parents=True, exist_ok=True)
    return d
