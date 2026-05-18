#!/usr/bin/env python3
"""
Check if all query IDs are present in the mini dataset.

Usage:
    python script/analyze/check_query_coverage.py
"""

import json
import yaml
from pathlib import Path

QUERIES_DIR = Path("queries/thorchain-2025-high-fast-mini")
MINI_DIR = Path("data/thorchain-2025-high-fast-mini")

def main():
    query_files = sorted(QUERIES_DIR.glob("*.yaml"))

    if not query_files:
        print(f"No query files found in {QUERIES_DIR}")
        return

    print("=" * 60)
    print("Checking query coverage in mini dataset")
    print("=" * 60)
    print()

    total_queries = 0
    total_missing = 0

    for query_file in query_files:
        pair = query_file.stem
        mini_file = MINI_DIR / f"{pair}.ndjson"

        if not mini_file.exists():
            print(f"{pair}: Mini file not found")
            continue

        # Load mini dataset IDs
        mini_ids = set()
        with open(mini_file) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        record = json.loads(line)
                        mini_ids.add(record.get('id'))
                    except:
                        pass

        # Load query file
        with open(query_file) as f:
            query_data = yaml.safe_load(f)

        # Extract query IDs
        query_ids = set()
        if isinstance(query_data, list):
            for item in query_data:
                if isinstance(item, dict) and 'id' in item:
                    query_ids.add(item['id'])

        # Check coverage
        missing_ids = query_ids - mini_ids

        total_queries += len(query_ids)
        total_missing += len(missing_ids)

        if missing_ids:
            print(f"{pair}: {len(query_ids)} queries, {len(missing_ids)} MISSING")
            for mid in sorted(missing_ids):
                print(f"  Missing ID: {mid}")
        else:
            print(f"{pair}: {len(query_ids)} queries, all present ✓")

    print()
    print("=" * 60)
    print(f"Total: {total_queries} query IDs, {total_missing} missing")
    if total_missing == 0:
        print("✓ All query IDs are present in mini dataset")
    print("=" * 60)


if __name__ == "__main__":
    main()
