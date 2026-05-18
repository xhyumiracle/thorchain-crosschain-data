#!/usr/bin/env python3
"""
Merge new samples into existing mini dataset with deduplication by ID.

Usage:
    python script/process/merge_mini.py
"""

import json
from pathlib import Path

EXISTING_DIR = Path("data/thorchain-2025-high-fast-mini")
NEW_DIR = Path("data/thorchain-2025-high-fast-mini-temp")

def main():
    new_files = sorted(NEW_DIR.glob("*.ndjson"))

    if not new_files:
        print(f"No files in {NEW_DIR}")
        return

    print("=" * 60)
    print("Merging with deduplication by ID")
    print("=" * 60)
    print()

    total_added = 0

    for new_file in new_files:
        existing_file = EXISTING_DIR / new_file.name

        # Read existing records
        existing_records = []
        existing_ids = set()

        if existing_file.exists():
            with open(existing_file) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            record = json.loads(line)
                            existing_records.append(record)
                            existing_ids.add(record.get('id'))
                        except:
                            pass

        # Read new records
        new_records = []
        with open(new_file) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        record = json.loads(line)
                        # Only add if not already exists
                        if record.get('id') not in existing_ids:
                            new_records.append(record)
                            existing_ids.add(record.get('id'))
                    except:
                        pass

        # Merge and write with updated idx
        merged = existing_records + new_records
        with open(existing_file, "w") as f:
            for idx, record in enumerate(merged):
                # Update idx field at the beginning
                record_with_idx = {"idx": idx}
                # Remove old idx if exists, then update with record
                record_without_idx = {k: v for k, v in record.items() if k != "idx"}
                record_with_idx.update(record_without_idx)
                f.write(json.dumps(record_with_idx, ensure_ascii=False) + "\n")

        before = len(existing_records)
        added = len(new_records)
        after = len(merged)

        total_added += added
        print(f"{new_file.name:15} {before:3} + {added:3} = {after:3} records")

    print()
    print(f"Total: {total_added} new records added")
    print(f"Output: {EXISTING_DIR}")


if __name__ == "__main__":
    main()
