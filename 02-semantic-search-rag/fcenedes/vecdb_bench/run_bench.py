#!/usr/bin/env python3
"""
Benchmark runner script - Python version of run_bench.sh
Executes Redis and MongoDB benchmarks with various configurations.
"""

import os
import re
import subprocess
import sys


def extract_from_connection_string(connection_string, pattern):
    """Extract a component from MongoDB connection string using regex."""
    match = re.search(pattern, connection_string)
    return match.group(1) if match else ""


def get_poetry_venv_path():
    """Get the poetry virtual environment path."""
    try:
        result = subprocess.run(
            ["poetry", "env", "info", "--path"],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error getting poetry environment: {e}", file=sys.stderr)
        sys.exit(1)


def run_experiments(engines_file, dataset, host, description=""):
    """Run all benchmark experiments defined in a single engines file.

    Using --engines-file makes the given file the single source of truth for
    engine configs, bypassing run.py's glob-and-merge over experiments/configurations/*.json
    (which merges duplicate engine names across files with last-wins order and would
    otherwise ignore edits to benchmark.json)."""
    print("-----------------------------------")
    if description:
        print(f"Running experiments from: {engines_file} - {description}")
    else:
        print(f"Running experiments from: {engines_file}")

    cmd = ["python", "run.py", "--engines-file", engines_file, "--datasets", dataset, "--host", host]

    try:
        subprocess.run(cmd, check=True)
        print(f"Completed experiments from: {engines_file}")
    except subprocess.CalledProcessError as e:
        print(f"Error running experiments from {engines_file}: {e}", file=sys.stderr)
        sys.exit(1)

    print("-----------------------------------")


def main():
    # ============== Redis ENV VARS ==============
    
    # ============== General ENV VARS ==============
    os.environ["DATASETS"] = os.getenv("BENCH_DATASETS", "gist-960-euclidean")
    os.environ["ENGINES"] = os.getenv("BENCH_ENGINES", "redis")

    # Redis Flex (Disk HNSW) does not support DIALECT 4, so default the query
    # dialect to 2. Override by exporting REDIS_DIALECT before launching.
    os.environ.setdefault("REDIS_DIALECT", "2")

    
    # Print configuration (with sensitive values redacted)
    print("REDIS_PORT:", os.environ["REDIS_PORT"])
    print("REDIS_HOST:", os.environ["REDIS_HOST"])
    print("REDIS_USER: [REDACTED]")
    print("REDIS_AUTH: [REDACTED]")
    print("DATASETS:", os.environ["DATASETS"])
    print("ENGINES:", os.environ["ENGINES"])
    print("REDIS_DIALECT:", os.environ["REDIS_DIALECT"])
    print()
    
    # Activate poetry virtual environment
    venv_path = get_poetry_venv_path()
    activate_script = os.path.join(venv_path, "bin", "activate")
    
    # Note: We don't need to source activate in Python, just ensure we're using the right Python
    # The subprocess calls will inherit the environment
    
    # Define experiments
    datasets = os.environ["DATASETS"]
    redis_host = os.environ["REDIS_HOST"]
    
    
    
    # Redis experiments are defined in this file (single source of truth).
    engines_file = os.getenv(
        "BENCH_ENGINES_FILE", "experiments/configurations/benchmark.json"
    )

    # Run Redis experiments
    run_experiments(engines_file, datasets, redis_host, "Redis")

    print("All experiments completed!")


if __name__ == "__main__":
    main()

