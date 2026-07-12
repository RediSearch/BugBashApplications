#!/usr/bin/env python3
"""Bulk ingest toward tens of millions of docs — the DB-blow-up tool.

Multiprocess writers, pipelined HSETs, deterministic content (worker w,
doc i always produces the same product + vector, so any doc can be
regenerated for verification later without a client-side mirror).

Write errors are the interesting part at scale (RAM-quota write rejection,
L0 stalls, timeouts): every distinct server error is counted and echoed
verbatim to stdout/errors.log the first time it appears, with a timestamp
and the current doc count — that's a bug-bash finding, not noise.

Usage:
  ./scale_ingest.py --url ... --target 50000000 --workers 8 [--start 0]

Progress lines go to stdout every --report seconds:
  [ingest] docs=1234567 rate=8123/s used_mem=1.2G errors=0 eta=1.6h
"""

import argparse
import multiprocessing as mp
import sys
import time

import redis

import catalog
import embeddings
import store as store_mod

SKU_PREFIX = "SC"  # scale docs; seed/demo docs use RT, chaos uses CH


def make_product(worker, i):
    # One deterministic product per (worker, i): reuse catalog templates with
    # a seed derived from the global doc number.
    n = worker * 1_000_000_000 + i
    product = next(catalog.generate(1, seed=n, start_index=n))
    product["sku"] = f"{SKU_PREFIX}{worker:02d}{i:09d}"
    return product


def writer(worker, url, start, count, batch, queue):
    try:
        client = store_mod.connect(url)
        s = store_mod.Store(client)
        pipe = client.pipeline(transaction=False)
        done, errors = 0, {}
        last_report = time.time()
        for i in range(start, start + count):
            s.upsert(make_product(worker, i), pipe)
            if len(pipe) >= batch:
                try:
                    pipe.execute()
                except redis.RedisError as err:
                    key = f"{type(err).__name__}: {err}"
                    errors[key] = errors.get(key, 0) + 1
                    if errors[key] == 1:
                        queue.put(("error", worker, done, key))
                    pipe = client.pipeline(transaction=False)
                    time.sleep(2)  # back off; quota errors don't clear instantly
                    continue
                done += batch
                if time.time() - last_report > 2:
                    queue.put(("progress", worker, done, None))
                    last_report = time.time()
        try:
            pipe.execute()
            done += len(pipe)
        except redis.RedisError:
            pass
        queue.put(("done", worker, done, errors))
    except Exception as err:  # crash visibility beats a silent dead worker
        queue.put(("crash", worker, 0, repr(err)))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--target", type=int, required=True,
                        help="total docs to write across all workers")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--start", type=int, default=0,
                        help="per-worker start offset (resume support)")
    parser.add_argument("--batch", type=int, default=500)
    parser.add_argument("--report", type=int, default=30)
    args = parser.parse_args()

    per_worker = args.target // args.workers
    queue = mp.Queue()
    procs = [mp.Process(target=writer,
                        args=(w, args.url, args.start, per_worker, args.batch, queue),
                        daemon=True)
             for w in range(args.workers)]
    for p in procs:
        p.start()

    monitor = store_mod.connect(args.url)
    progress = [0] * args.workers
    finished = 0
    started = time.time()
    last_line = started
    last_total = 0
    while finished < args.workers:
        try:
            kind, worker, done, extra = queue.get(timeout=args.report)
        except Exception:
            kind = None
        if kind == "progress":
            progress[worker] = done
        elif kind == "error":
            print(f"[{time.strftime('%H:%M:%S')}] WRITE ERROR (worker {worker}, "
                  f"after {done} docs): {extra}", flush=True)
        elif kind == "done":
            progress[worker] = done
            finished += 1
            if extra:
                print(f"[worker {worker}] error summary: {extra}", flush=True)
        elif kind == "crash":
            finished += 1
            print(f"[worker {worker}] CRASHED: {extra}", flush=True)

        now = time.time()
        if now - last_line >= args.report:
            total = sum(progress)
            rate = (total - last_total) / (now - last_line)
            last_total, last_line = total, now
            try:
                mem = monitor.info("memory").get("used_memory_human", "?")
                dbsize = monitor.dbsize()
            except redis.RedisError as err:
                mem, dbsize = f"INFO failed: {err}", "?"
            eta = ((args.target - total) / rate / 3600) if rate > 0 else float("inf")
            print(f"[ingest] written={total} dbsize={dbsize} rate={rate:.0f}/s "
                  f"used_mem={mem} eta={eta:.1f}h", flush=True)

    total = sum(progress)
    elapsed = time.time() - started
    print(f"finished: {total} docs in {elapsed / 3600:.2f}h "
          f"({total / max(elapsed, 1):.0f}/s)", flush=True)


if __name__ == "__main__":
    main()
