"""Query storm: N processes hammering the index fleet with the weighted mix.

Queries run unpipelined so each recorded latency is a realistic round trip.
Timeouts (strict ON_TIMEOUT FAIL errors), disk errors, and RESP3 reply
warnings (partial results / OOM) are counted per template as first-class
metrics alongside the latency histograms.
"""

import multiprocessing as mp
import random
import time
from pathlib import Path

from common import Config, StatsWriter, classify_error, make_client, reply_warning
from datagen import make_generator
from queries import QueryMix


def storm_worker(worker: int, cfg: Config, run_dir: Path, duration: float | None):
    gen = make_generator(cfg)
    mix = QueryMix(gen, cfg)
    client = make_client(cfg)
    stats = StatsWriter(run_dir, "storm", worker)
    rng = random.Random(f"storm:{cfg.seed}:{worker}:{time.time()}")
    deadline = time.time() + duration if duration else None
    err_seen = {}  # (template, cls) -> count; errors stay fully counted in
    # the window stats, but identical error *events* are sampled so a
    # fail-fast outage can't fill the disk with duplicate lines.

    while deadline is None or time.time() < deadline:
        name, _, args = mix.sample(rng)
        t0 = time.perf_counter()
        try:
            reply = client.execute_command(*args)
            stats.record(name, (time.perf_counter() - t0) * 1000,
                         warning=reply_warning(reply))
        except Exception as e:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            cls = classify_error(e)
            stats.record(name, elapsed_ms, error=True, timeout=cls == "timeout")
            n = err_seen[(name, cls)] = err_seen.get((name, cls), 0) + 1
            if n <= 5 or n % 500 == 0:
                stats.event("query_error", template=name, cls=cls, seen=n,
                            error=str(e)[:300], query=" ".join(map(str, args[:8])))
            if cls == "connection":
                time.sleep(1.0)
                client = make_client(cfg)
            elif elapsed_ms < 50:
                time.sleep(0.1)

    stats.close()


def run_storm(cfg: Config, run_dir: Path, duration: float | None = None):
    procs = [
        mp.Process(target=storm_worker, args=(i, cfg, run_dir, duration), name=f"storm-{i}")
        for i in range(cfg.storm_workers)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join()
