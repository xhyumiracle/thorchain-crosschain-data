#!/usr/bin/env python3
"""
Add 'idx' field to each record in all datasets.

This is a one-time script that:
1. Reads all jsonl files from data/ directories
2. Adds an 'idx' field (0-indexed, sequential) to each record
3. Writes to data-idx/ directory with same structure
4. Validates that no data is lost or added

Usage:
    python script/process/add_idx_field.py
"""

import json
from pathlib import Path
from typing import Dict, List

# Source and destination directories - will auto-discover all data/ subdirs
DATA_DIRS = None  # Will be populated dynamically

def add_idx_to_dataset(input_dir: Path, output_dir: Path) -> Dict[str, int]:
    """
    Add idx field to all jsonl files in a dataset directory.

    Returns dict mapping filename -> record count.
    """
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)

    if not input_dir.exists():
        print(f"[SKIP] Input directory does not exist: {input_dir}")
        return {}

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Find all jsonl files
    jsonl_files = sorted(input_dir.glob("*.jsonl"))

    if not jsonl_files:
        print(f"[SKIP] No jsonl files in {input_dir}")
        return {}

    file_counts = {}

    for jsonl_file in jsonl_files:
        records = []

        # Read records
        with open(jsonl_file) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        record = json.loads(line)
                        records.append(record)
                    except json.JSONDecodeError as e:
                        print(f"[WARN] Failed to parse line in {jsonl_file.name}: {e}")

        # Add idx field at the beginning (before 'id')
        for idx, record in enumerate(records):
            # Create new dict with idx first
            new_record = {"idx": idx}
            new_record.update(record)
            records[idx] = new_record

        # Write to output
        output_file = output_dir / jsonl_file.name
        with open(output_file, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

        file_counts[jsonl_file.name] = len(records)
        print(f"  {jsonl_file.name}: {len(records)} records")

    return file_counts


def validate_datasets(original_dir: Path, new_dir: Path, dataset_name: str) -> bool:
    """
    Validate that new dataset matches original (except for idx field).

    Returns True if validation passes.
    """
    print(f"\n[VALIDATE] {dataset_name}")
    print("=" * 70)

    original_dir = Path(original_dir)
    new_dir = Path(new_dir)

    if not original_dir.exists() or not new_dir.exists():
        print("[SKIP] Directory does not exist")
        return True

    # Get all jsonl files
    original_files = {f.name for f in original_dir.glob("*.jsonl")}
    new_files = {f.name for f in new_dir.glob("*.jsonl")}

    # Check file list matches
    if original_files != new_files:
        print(f"[ERROR] File list mismatch!")
        print(f"  Original only: {original_files - new_files}")
        print(f"  New only: {new_files - original_files}")
        return False

    all_valid = True

    for filename in sorted(original_files):
        original_file = original_dir / filename
        new_file = new_dir / filename

        # Load records
        original_records = []
        with open(original_file) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        original_records.append(json.loads(line))
                    except:
                        pass

        new_records = []
        with open(new_file) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        new_records.append(json.loads(line))
                    except:
                        pass

        # Check count
        if len(original_records) != len(new_records):
            print(f"[ERROR] {filename}: Record count mismatch!")
            print(f"  Original: {len(original_records)}, New: {len(new_records)}")
            all_valid = False
            continue

        # Check each record
        for idx, (orig, new) in enumerate(zip(original_records, new_records)):
            # New record should have idx field
            if "idx" not in new:
                print(f"[ERROR] {filename}: Record {idx} missing idx field")
                all_valid = False
                continue

            # idx should match position
            if new["idx"] != idx:
                print(f"[ERROR] {filename}: Record {idx} has wrong idx value: {new['idx']}")
                all_valid = False
                continue

            # Remove idx from new record for comparison
            new_without_idx = {k: v for k, v in new.items() if k != "idx"}

            # Check that all other fields match
            if orig != new_without_idx:
                print(f"[ERROR] {filename}: Record {idx} data mismatch!")
                print(f"  Original keys: {set(orig.keys())}")
                print(f"  New keys: {set(new_without_idx.keys())}")

                # Check for missing/extra keys
                missing_keys = set(orig.keys()) - set(new_without_idx.keys())
                extra_keys = set(new_without_idx.keys()) - set(orig.keys())
                if missing_keys:
                    print(f"  Missing keys: {missing_keys}")
                if extra_keys:
                    print(f"  Extra keys: {extra_keys}")

                # Check for value mismatches
                for key in set(orig.keys()) & set(new_without_idx.keys()):
                    if orig[key] != new_without_idx[key]:
                        print(f"  Key '{key}' value mismatch")

                all_valid = False
                continue

        print(f"  ✓ {filename}: {len(new_records)} records validated")

    if all_valid:
        print("\n✓ All validation checks passed!")
    else:
        print("\n✗ Validation failed - see errors above")

    return all_valid


def main():
    script_dir = Path(__file__).parent
    thorchain_dir = script_dir.parent.parent  # data/thorchain/

    # Auto-discover all directories under data/
    data_root = thorchain_dir / "data"
    data_dirs = sorted([d for d in data_root.iterdir() if d.is_dir()])

    print("=" * 70)
    print("Adding idx field to all datasets")
    print("=" * 70)
    print(f"Found {len(data_dirs)} directories in data/:")
    for d in data_dirs:
        print(f"  - {d.name}")
    print()

    all_counts = {}

    for input_dir in data_dirs:
        # Convert data/xxx to data-idx/xxx
        output_dir = thorchain_dir / "data-idx" / input_dir.name

        dataset_name = input_dir.name
        print(f"[PROCESS] {dataset_name}")
        print(f"  Input:  {input_dir}")
        print(f"  Output: {output_dir}")

        counts = add_idx_to_dataset(input_dir, output_dir)

        if counts:
            all_counts[dataset_name] = counts
            total = sum(counts.values())
            print(f"  Total: {total} records in {len(counts)} files")

        print()

    # Validation
    print("\n" + "=" * 70)
    print("Validating new datasets")
    print("=" * 70)

    all_valid = True
    for input_dir in data_dirs:
        output_dir = thorchain_dir / "data-idx" / input_dir.name
        dataset_name = input_dir.name

        is_valid = validate_datasets(input_dir, output_dir, dataset_name)
        all_valid = all_valid and is_valid

    print("\n" + "=" * 70)
    if all_valid:
        print("✓ All datasets processed and validated successfully!")
        print("✓ New data written to data-idx/ directories")
        print("✓ Original data in data/ directories unchanged")
    else:
        print("✗ Validation failed - please review errors above")
        print("✗ Do NOT use the new data until issues are resolved")
    print("=" * 70)


if __name__ == "__main__":
    main()
