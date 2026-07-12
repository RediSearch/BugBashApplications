#!/usr/bin/env python3
"""Ride & Tide — motorbikes, surfing & sailing gear store on Redis Search
(Flex/RoF). Bug-bash driver app: every subcommand exercises a supported
query path; `chaos` runs the churn workload with a correctness oracle.

Examples:
    ./app.py --url redis://:pass@host:port seed --count 5000
    ./app.py search "waterproof jacket" --category sailing
    ./app.py semantic "warm suit for winter waves" --in-stock yes
    ./app.py similar RT000042
    ./app.py facets --field brand
    ./app.py sort-by-price "@category:{motorbikes}" --desc
    ./app.py chaos --ops 2000 --verify-every 200
"""

import argparse
import sys

import redis as redis_lib

import catalog
import store as store_mod
from store import Store


def add_connection_args(parser):
    parser.add_argument("--url", help="redis://[:password@]host:port")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--password")
    parser.add_argument("--tls", action="store_true")


def add_filter_args(parser):
    parser.add_argument("--brand")
    parser.add_argument("--category", choices=list(catalog.DEPARTMENTS))
    parser.add_argument("--subcategory")
    parser.add_argument("--color")
    parser.add_argument("--bucket", dest="price_bucket",
                        help="e.g. under_50, 100_250, over_5000")
    parser.add_argument("--in-stock", dest="in_stock", choices=["yes", "no"])


def filters_from(args):
    return {f: getattr(args, f, None)
            for f in ("brand", "category", "subcategory", "color",
                      "price_bucket", "in_stock")
            if getattr(args, f, None)}


def print_docs(total, docs, show_dist=False):
    print(f"total matches: {total}")
    for doc in docs:
        dist = f"  dist={float(doc['dist']):.4f}" if show_dist and "dist" in doc else ""
        stock = "" if doc.get("in_stock") == "yes" else "  [OUT OF STOCK]"
        print(f"  {doc.get('sku')}  ${doc.get('price'):>9}  "
              f"{doc.get('name')} ({doc.get('color')}){dist}{stock}")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    add_connection_args(parser)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("seed", help="create index and load the catalog")
    p.add_argument("--count", type=int, default=3000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--recreate", action="store_true")

    p = sub.add_parser("search", help="full-text search with TAG facet filters")
    p.add_argument("text", nargs="?")
    p.add_argument("--limit", type=int, default=10)
    add_filter_args(p)

    p = sub.add_parser("semantic", help="embed free text, KNN (hybrid if filtered)")
    p.add_argument("text")
    p.add_argument("-k", type=int, default=10)
    p.add_argument("--policy", default="ADHOC_BF", choices=["ADHOC_BF", "BATCHES"])
    p.add_argument("--ef-runtime", type=int)
    add_filter_args(p)

    p = sub.add_parser("similar", help="similar items by product vector")
    p.add_argument("sku")
    p.add_argument("-k", type=int, default=10)
    p.add_argument("--policy", default="ADHOC_BF", choices=["ADHOC_BF", "BATCHES"])
    add_filter_args(p)

    p = sub.add_parser("facets", help="facet counts via FT.AGGREGATE GROUPBY")
    p.add_argument("--field", default="brand",
                   choices=["brand", "category", "subcategory", "color",
                            "price_bucket", "in_stock"])
    p.add_argument("--query", default="*")

    sub.add_parser("prices", help="price stats per category (to_number reducers)")

    p = sub.add_parser("sort-by-price", help="numeric sort via APPLY to_number")
    p.add_argument("query", nargs="?", default="*")
    p.add_argument("--desc", action="store_true")
    p.add_argument("--limit", type=int, default=15)

    p = sub.add_parser("chaos", help="churn workload + correctness oracle")
    p.add_argument("--ops", type=int, default=1000)
    p.add_argument("--verify-every", type=int, default=100)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--initial", type=int, default=500,
                   help="products seeded by the runner before churn starts")
    p.add_argument("--recreate", action="store_true")

    sub.add_parser("info", help="FT.INFO highlights")
    sub.add_parser("drop", help="drop the index (keys are kept; DD unsupported)")

    args = parser.parse_args()
    client = store_mod.connect(args.url, args.host, args.port, args.password, args.tls)
    s = Store(client)

    if args.cmd == "seed":
        s.create_index(recreate=args.recreate)
        n = s.ingest(catalog.generate(args.count, args.seed),
                     progress=lambda n: print(f"  ingested {n}...", end="\r"))
        print(f"index ready, ingested {n} products")

    elif args.cmd == "search":
        total, docs = s.text_search(args.text, filters_from(args), args.limit)
        print_docs(total, docs)

    elif args.cmd == "semantic":
        total, docs = s.semantic(args.text, k=args.k, filters=filters_from(args),
                                 policy=args.policy, ef_runtime=args.ef_runtime)
        print_docs(total, docs, show_dist=True)

    elif args.cmd == "similar":
        total, docs = s.similar(args.sku, k=args.k, filters=filters_from(args),
                                policy=args.policy)
        print_docs(total, docs, show_dist=True)

    elif args.cmd == "facets":
        for value, count in s.facets(args.field, args.query):
            print(f"  {value:<20} {count}")

    elif args.cmd == "prices":
        for row in s.price_stats_by_category():
            print(f"  {row['category']:<12} n={row['count']:<6} "
                  f"min=${float(row['min_price']):.2f} "
                  f"avg=${float(row['avg_price']):.2f} "
                  f"max=${float(row['max_price']):.2f}")

    elif args.cmd == "sort-by-price":
        for row in s.price_sorted(args.query, ascending=not args.desc,
                                  limit=args.limit):
            print(f"  ${float(row['p']):>9.2f}  {row.get('sku')}  "
                  f"{row.get('name')}")

    elif args.cmd == "chaos":
        from chaos import ChaosRunner
        if args.recreate:
            s.create_index(recreate=True)
        else:
            try:
                s.info()
            except redis_lib.ResponseError:
                s.create_index()
        runner = ChaosRunner(s, seed=args.seed)
        runner.seed(args.initial)
        failures = runner.run(args.ops, args.verify_every)
        sys.exit(1 if failures else 0)

    elif args.cmd == "info":
        info = {k: v for k, v in s.info().items()}
        for field in ("num_docs", "num_terms", "num_records",
                      "vector_index_sz_mb", "inverted_sz_mb",
                      "doc_table_size_mb", "total_index_memory_sz_mb",
                      "hash_indexing_failures", "indexing"):
            if field in info:
                value = info[field]
                print(f"  {field:<28} {value.decode() if isinstance(value, bytes) else value}")
        print("  (memory fields other than vector_index_sz_mb are "
              "estimates/placeholders on Flex)")

    elif args.cmd == "drop":
        s.drop_index()
        print("index dropped (document keys retained)")


if __name__ == "__main__":
    main()
