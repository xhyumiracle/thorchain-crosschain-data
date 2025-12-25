#!/usr/bin/env python3
"""
Validate THORChain swap data.
- Check for duplicate records by ID
"""

import json
from pathlib import Path


DATA_DIR = Path(__file__).parent.parent.parent / "data"


def load_ndjson(filepath: Path) -> list[dict]:
    """Load records from an ndjson file."""
    records = []
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def check_duplicates(records: list[dict], filename: str) -> dict:
    """Check for duplicate records by ID."""
    seen_ids = set()
    duplicates = []
    for record in records:
        rid = record.get("id")
        if rid in seen_ids:
            duplicates.append(rid)
        else:
            seen_ids.add(rid)
    return {
        "file": filename,
        "total_records": len(records),
        "unique_ids": len(seen_ids),
        "duplicate_count": len(duplicates),
        "duplicate_ids": duplicates[:10] if duplicates else [],  # Show first 10
    }


def print_report(results: list[dict]):
    """Print validation report."""
    print("=" * 80)
    print("THORChain Swap Data Validation Report")
    print("=" * 80)

    has_duplicates = False
    for result in results:
        status = "✓ OK" if result["duplicate_count"] == 0 else "✗ DUPLICATES FOUND"
        print(f"\n  {result['file']}: {status}")
        print(f"    Total records: {result['total_records']}")
        print(f"    Unique IDs: {result['unique_ids']}")
        if result["duplicate_count"] > 0:
            has_duplicates = True
            print(f"    Duplicates: {result['duplicate_count']}")
            print(f"    Sample duplicate IDs: {result['duplicate_ids']}")

    if not has_duplicates:
        print("\n  ✓ All files have no duplicate records!")

    print("\n" + "=" * 80)


def main():
    # Find all ndjson files
    ndjson_files = sorted(DATA_DIR.glob("*.ndjson"))

    if not ndjson_files:
        print(f"No .ndjson files found in {DATA_DIR}")
        return

    print(f"Found {len(ndjson_files)} data files in {DATA_DIR}\n")

    results = []
    for filepath in ndjson_files:
        print(f"Checking {filepath.name}...")
        records = load_ndjson(filepath)
        result = check_duplicates(records, filepath.name)
        results.append(result)

    print()
    print_report(results)


if __name__ == "__main__":
    main()
