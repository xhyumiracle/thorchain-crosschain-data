#!/usr/bin/env python3
"""
Fill mini dataset to 100 records per pair (except ETH-DOGE which only has 87 total).

Usage:
    python script/process/fill_mini_to_100.py
"""

import json
import random
from pathlib import Path

# Configuration
INPUT_DIR = Path("data/thorchain-2025-high-fast")
MINI_DIR = Path("data/thorchain-2025-high-fast-mini")
TARGET_COUNT = 100
RANDOM_SEED = 42  # for reproducibility
SKIP_FILES = {"ETH-DOGE.jsonl"}  # Skip files with insufficient source data

def main():
    random.seed(RANDOM_SEED)

    # Find all mini files
    mini_files = sorted(MINI_DIR.glob("*.jsonl"))

    if not mini_files:
        print(f"No files in {MINI_DIR}")
        return

    print("=" * 60)
    print(f"Filling mini dataset to {TARGET_COUNT} records per pair")
    print("=" * 60)
    print()

    total_added = 0

    for mini_file in mini_files:
        filename = mini_file.name

        # Skip files that don't have enough source data
        if filename in SKIP_FILES:
            current_count = sum(1 for _ in open(mini_file))
            print(f"{filename:15} {current_count:3} records (skipped - insufficient source data)")
            continue

        # Read existing mini records
        existing_records = []
        existing_ids = set()

        with open(mini_file) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        record = json.loads(line)
                        existing_records.append(record)
                        existing_ids.add(record.get('id'))
                    except:
                        pass

        current_count = len(existing_records)

        # Check if already at target
        if current_count >= TARGET_COUNT:
            print(f"{filename:15} {current_count:3} records (already at target)")
            continue

        # Calculate how many more we need
        needed = TARGET_COUNT - current_count

        # Read source file
        source_file = INPUT_DIR / filename

        if not source_file.exists():
            print(f"{filename:15} {current_count:3} records (source file not found)")
            continue

        all_records = []
        with open(source_file) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        all_records.append(json.loads(line))
                    except:
                        pass

        # Filter out existing records
        available_records = [r for r in all_records if r.get('id') not in existing_ids]

        if len(available_records) < needed:
            print(f"{filename:15} {current_count:3} + {len(available_records):3} = {current_count + len(available_records):3} (source exhausted)")
            needed = len(available_records)

        if needed == 0:
            print(f"{filename:15} {current_count:3} records (no new samples available)")
            continue

        # Sample additional records
        new_samples = random.sample(available_records, needed)

        # Merge and write with updated idx
        merged = existing_records + new_samples
        with open(mini_file, "w") as f:
            for idx, record in enumerate(merged):
                # Update idx field at the beginning
                record_with_idx = {"idx": idx}
                # Remove old idx if exists, then update with record
                record_without_idx = {k: v for k, v in record.items() if k != "idx"}
                record_with_idx.update(record_without_idx)
                f.write(json.dumps(record_with_idx, ensure_ascii=False) + "\n")

        total_added += needed
        print(f"{filename:15} {current_count:3} + {needed:3} = {len(merged):3} records")

    print()
    print(f"Total: {total_added} new records added")


if __name__ == "__main__":
    main()
