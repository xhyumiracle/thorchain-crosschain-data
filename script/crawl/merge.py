#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Merge two THORChain raw datasets with deduplication.

This tool merges two directories containing ndjson files from different time periods,
removing duplicates and sorting by timestamp.

Usage:
    python merge.py --dir1 raw --dir2 raw_new --outdir raw_merged

Features:
- Deduplicates using canonical_action_key from utils.py
- Sorts output by timestamp (date field)
- Preserves all unique records from both datasets
- Handles overlapping time ranges correctly
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

from utils import canonical_action_key


def load_all_records(ndjson_path: Path) -> List[Dict[str, Any]]:
    """Load all records from an ndjson file."""
    if not ndjson_path.exists():
        return []

    records = []
    with ndjson_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                records.append(obj)
            except Exception as e:
                print(f"[WARN] Failed to parse line: {e}")
                continue
    return records


def merge_records(records1: List[Dict[str, Any]], records2: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Merge two lists of records with deduplication.

    Returns:
        Sorted list of unique records (by timestamp)
    """
    seen_keys = set()
    merged = []

    for record in records1 + records2:
        key = canonical_action_key(record)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        merged.append(record)

    # Sort by timestamp (date field in nanoseconds)
    merged.sort(key=lambda r: int(r.get("date", "0")), reverse=True)

    return merged


def write_ndjson(path: Path, records: List[Dict[str, Any]]) -> None:
    """Write records to ndjson file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Merge two THORChain raw datasets with deduplication"
    )
    parser.add_argument("--dir1", required=True, help="First dataset directory (e.g., raw/data)")
    parser.add_argument("--dir2", required=True, help="Second dataset directory (e.g., raw_new/data)")
    parser.add_argument("--outdir", required=True, help="Output directory")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without writing")

    args = parser.parse_args()

    dir1 = Path(args.dir1)
    dir2 = Path(args.dir2)
    outdir = Path(args.outdir)

    if not dir1.exists():
        raise SystemExit(f"Error: {dir1} does not exist")
    if not dir2.exists():
        raise SystemExit(f"Error: {dir2} does not exist")

    # Find all ndjson files in dir1
    ndjson_files = sorted(dir1.glob("*.ndjson"))

    if not ndjson_files:
        raise SystemExit(f"Error: No .ndjson files found in {dir1}")

    print(f"[INFO] Merging datasets:")
    print(f"  dir1: {dir1.resolve()}")
    print(f"  dir2: {dir2.resolve()}")
    print(f"  outdir: {outdir.resolve()}")
    print(f"  Found {len(ndjson_files)} files to process")
    print()

    total_records_1 = 0
    total_records_2 = 0
    total_merged = 0
    total_duplicates = 0

    for ndjson_file in ndjson_files:
        filename = ndjson_file.name
        file1 = dir1 / filename
        file2 = dir2 / filename
        outfile = outdir / filename

        print(f"[INFO] Processing: {filename}")

        # Load records from both files
        records1 = load_all_records(file1)
        records2 = load_all_records(file2)

        print(f"  dir1: {len(records1)} records")
        print(f"  dir2: {len(records2)} records")

        # Merge with deduplication
        merged = merge_records(records1, records2)
        duplicates = (len(records1) + len(records2)) - len(merged)

        print(f"  merged: {len(merged)} records ({duplicates} duplicates removed)")

        # Write output
        if not args.dry_run:
            write_ndjson(outfile, merged)
            print(f"  written to: {outfile}")
        else:
            print(f"  [DRY RUN] would write to: {outfile}")

        print()

        total_records_1 += len(records1)
        total_records_2 += len(records2)
        total_merged += len(merged)
        total_duplicates += duplicates

    print("[INFO] Summary:")
    print(f"  Total records in dir1: {total_records_1}")
    print(f"  Total records in dir2: {total_records_2}")
    print(f"  Total merged records: {total_merged}")
    print(f"  Total duplicates removed: {total_duplicates}")

    if args.dry_run:
        print("\n[DRY RUN] No files were written. Remove --dry-run to perform the merge.")
    else:
        print(f"\n[INFO] Merge complete! Output saved to {outdir.resolve()}")


if __name__ == "__main__":
    main()
