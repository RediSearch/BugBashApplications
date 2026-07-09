#!/usr/bin/env python3
"""bazaar — CLI orchestrator for the marketplace churn stress app.

Typical flows:
    ./run.py setup                          # create the index fleet (before loading!)
    ./run.py load                           # bulk load (resumable)
    ./run.py mixed --duration 3600          # churn + storm + verify + monitor together
    ./run.py report                         # summarize the latest run
    ./run.py all --duration 600             # setup + load + mixed, end to end
"""

import argparse
import multiprocessing as mp
import sys
import time
from pathlib import Path

import common
import schema
from churn import run_churn
from load import run_load
from monitor import run_monitor
from query_storm import run_storm
from report import latest_run_dir, report
from verify import run_verify


def add_common_args(p):
    p.add_argument("--config", help="path to config.yaml (default: alongside run.py)")
    p.add_argument("--profile", help="scale profile from config.yaml (smoke/full)")
    p.add_argument("--host")
    p.add_argument("--port")
    p.add_argument("--password")
    p.add_argument("--tls", action="store_const", const=True, default=None)
    p.add_argument("--tls-ca", dest="tls_ca")
    p.add_argument("--run-name", help="run directory name under out/ (default: timestamp)")
    p.add_argument("--duration", type=float, default=None,
                   help="seconds to run open-ended modes (default: until killed)")


def cmd_setup(cfg, _args):
    client = common.make_client(cfg)
    client.ping()
    created = schema.create_indexes(client)
    print(f"created: {created or '(all indexes already exist)'}")


def cmd_drop(cfg, args):
    if not args.yes:
        sys.exit("refusing to drop indexes without --yes")
    dropped = schema.drop_indexes(common.make_client(cfg))
    print(f"dropped: {dropped}")


def _run_dir(cfg, args):
    d = common.new_run_dir(cfg, args.run_name)
    print(f"run dir: {d}")
    return d


def cmd_mixed(cfg, args, run_dir=None):
    run_dir = run_dir or _run_dir(cfg, args)
    parts = [
        mp.Process(target=run_churn, args=(cfg, run_dir, args.duration), name="churn"),
        mp.Process(target=run_storm, args=(cfg, run_dir, args.duration), name="storm"),
        mp.Process(target=run_verify, args=(cfg, run_dir, args.duration), name="verify"),
        mp.Process(target=run_monitor, args=(cfg, run_dir, args.duration), name="monitor"),
    ]
    for p in parts:
        p.start()
    try:
        # monitor runs on the same duration; churn/storm/verify define the run
        for p in parts[:3]:
            p.join()
        parts[3].terminate()
    except KeyboardInterrupt:
        print("\ninterrupted — terminating workers", flush=True)
        for p in parts:
            p.terminate()
    report(run_dir)


def main():
    parser = argparse.ArgumentParser(prog="bazaar", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    cmds = {}
    for name, help_ in [
        ("setup", "create the index fleet (must run before load)"),
        ("load", "bulk load total_docs listings (resumable)"),
        ("churn", "run insert/update/delete churn"),
        ("storm", "run the query storm"),
        ("verify", "run correctness probes"),
        ("monitor", "sample FT.INFO / INFO to JSONL"),
        ("mixed", "churn + storm + verify + monitor together"),
        ("all", "setup + load + mixed"),
        ("report", "aggregate a run directory into a report"),
        ("drop", "drop all bazaar indexes"),
    ]:
        p = sub.add_parser(name, help=help_)
        add_common_args(p)
        cmds[name] = p
    cmds["drop"].add_argument("--yes", action="store_true")
    cmds["report"].add_argument("--run", help="run dir (default: latest under out/)")
    cmds["monitor"].add_argument("--interval", type=float, default=30.0)

    args = parser.parse_args()
    cfg = common.load_config(args)
    print(f"target: {cfg.host}:{cfg.port} (tls={cfg.tls}) profile={cfg.profile_name} "
          f"docs={cfg.total_docs}")

    if args.cmd == "setup":
        cmd_setup(cfg, args)
    elif args.cmd == "drop":
        cmd_drop(cfg, args)
    elif args.cmd == "load":
        cmd_setup(cfg, args)  # SKIPINITIALSCAN: indexes must exist before docs
        run_load(cfg, _run_dir(cfg, args))
    elif args.cmd == "churn":
        run_churn(cfg, _run_dir(cfg, args), args.duration)
    elif args.cmd == "storm":
        run_storm(cfg, _run_dir(cfg, args), args.duration)
    elif args.cmd == "verify":
        run_verify(cfg, _run_dir(cfg, args), args.duration)
    elif args.cmd == "monitor":
        run_monitor(cfg, _run_dir(cfg, args), args.duration, args.interval)
    elif args.cmd == "mixed":
        cmd_mixed(cfg, args)
    elif args.cmd == "all":
        cmd_setup(cfg, args)
        t0 = time.time()
        run_dir = _run_dir(cfg, args)
        run_load(cfg, run_dir)
        print(f"load finished in {time.time() - t0:.0f}s; starting mixed phase")
        cmd_mixed(cfg, args, run_dir)
    elif args.cmd == "report":
        run_dir = Path(args.run) if args.run else latest_run_dir(cfg.out_dir)
        if not run_dir:
            sys.exit("no runs found")
        report(run_dir)


if __name__ == "__main__":
    main()
