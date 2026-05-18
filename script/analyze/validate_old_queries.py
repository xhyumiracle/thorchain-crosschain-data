#!/usr/bin/env python3
"""
Validate that old query IDs (from benchmark_output_orchtool) exist in new mini dataset.

Usage:
    python script/analyze/validate_old_queries.py
"""

import json
from pathlib import Path

# Use absolute paths from script location
# Script is at: data/thorchain/script/analyze/validate_old_queries.py
# So parent.parent gives us data/thorchain/
SCRIPT_DIR = Path(__file__).parent
THORCHAIN_DIR = SCRIPT_DIR.parent.parent
REPO_ROOT = THORCHAIN_DIR.parent.parent
BENCHMARK_DIR = REPO_ROOT / "benchmark_output_orchtool"
MINI_DIR = THORCHAIN_DIR / "data" / "thorchain-2025-high-fast-mini"

# Map benchmark directory names to mini dataset file names
PAIR_MAPPING = {
    "btc_doge": "BTC-DOGE.ndjson",
    "doge_btc": "DOGE-BTC.ndjson",
}

def load_mini_ids(mini_file: Path) -> set:
    """Load all IDs from a mini dataset file."""
    ids = set()

    if not mini_file.exists():
        return ids

    with open(mini_file) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    record = json.loads(line)
                    ids.add(record.get('id'))
                except:
                    pass

    return ids

def get_benchmark_query_ids(benchmark_subdir: Path) -> list:
    """Get query IDs from benchmark results directory."""
    results_dir = benchmark_subdir / "results"

    if not results_dir.exists():
        return []

    # Directory names are query IDs
    return [d.name for d in results_dir.iterdir() if d.is_dir()]

def main():
    print("=" * 70)
    print("Validating old query IDs against new mini dataset")
    print("=" * 70)
    print()
    print(f"BENCHMARK_DIR: {BENCHMARK_DIR}")
    print(f"BENCHMARK_DIR exists: {BENCHMARK_DIR.exists()}")
    print(f"MINI_DIR: {MINI_DIR}")
    print(f"MINI_DIR exists: {MINI_DIR.exists()}")
    print()

    total_old_queries = 0
    total_missing = 0

    for pair_name, mini_filename in PAIR_MAPPING.items():
        benchmark_subdir = BENCHMARK_DIR / pair_name
        mini_file = MINI_DIR / mini_filename

        print(f"Checking {pair_name} -> {mini_filename}")
        print("-" * 70)

        # Get old query IDs from benchmark
        old_query_ids = get_benchmark_query_ids(benchmark_subdir)

        if not old_query_ids:
            print(f"  No benchmark data found for {pair_name}")
            print()
            continue

        # Load new mini dataset IDs
        mini_ids = load_mini_ids(mini_file)

        if not mini_ids:
            print(f"  Mini file not found or empty: {mini_file}")
            print()
            continue

        # Check coverage
        missing_ids = [qid for qid in old_query_ids if qid not in mini_ids]

        total_old_queries += len(old_query_ids)
        total_missing += len(missing_ids)

        print(f"  Old queries: {len(old_query_ids)}")
        print(f"  Mini dataset size: {len(mini_ids)}")
        print(f"  Missing: {len(missing_ids)}")

        if missing_ids:
            print()
            print("  Missing query IDs:")
            for mid in missing_ids:
                print(f"    {mid}")
        else:
            print("  ✓ All old query IDs are present in new mini dataset")

        print()

    print("=" * 70)
    print(f"Summary: {total_old_queries} old query IDs, {total_missing} missing in new mini")

    if total_missing == 0:
        print("✓ All old query IDs are present in the new mini dataset")
        print("✓ Safe to regenerate queries from new mini dataset")
    else:
        print("✗ Some old query IDs are missing from new mini dataset")
        print("✗ Regenerated queries will NOT have full coverage")

    print("=" * 70)

if __name__ == "__main__":
    main()
