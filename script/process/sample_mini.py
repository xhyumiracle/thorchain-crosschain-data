#!/usr/bin/env python3
"""
Sample mini dataset from high-fast data.

Usage:
    # Sample all pairs (default)
    python script/sample_mini.py

    # Sample specific pairs only
    python script/sample_mini.py --pairs BTC-LTC ETH-LTC LTC-BTC LTC-ETH DOGE-LTC LTC-DOGE

    # Custom sample size
    python script/sample_mini.py --sample-size 100

    # Force overwrite existing files
    python script/sample_mini.py --pairs BTC-LTC --force
"""

import json
import random
import argparse
from pathlib import Path

# Configuration
INPUT_DIR = Path("data/thorchain-2025-high-fast")
OUTPUT_DIR = Path("data/thorchain-2025-high-fast-mini")
DEFAULT_SAMPLE_SIZE = 100  # samples per file
RANDOM_SEED = 42  # for reproducibility

def main():
    parser = argparse.ArgumentParser(description="Sample mini dataset from high-fast data")
    parser.add_argument("--pairs", nargs="+", help="Specific pairs to sample (e.g., BTC-LTC ETH-LTC)")
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE, help=f"Sample size per pair (default: {DEFAULT_SAMPLE_SIZE})")
    parser.add_argument("--seed", type=int, default=RANDOM_SEED, help=f"Random seed (default: {RANDOM_SEED})")
    parser.add_argument("--force", action="store_true", help="Force overwrite existing files")
    args = parser.parse_args()

    random.seed(args.seed)
    sample_size = args.sample_size

    # Find input files
    if args.pairs:
        # Sample specific pairs
        files = []
        for pair in args.pairs:
            filename = f"{pair}.jsonl"
            filepath = INPUT_DIR / filename
            if filepath.exists():
                files.append(filepath)
            else:
                print(f"Warning: {filename} not found in {INPUT_DIR}")
        files = sorted(files)
    else:
        # Sample all pairs
        files = sorted(INPUT_DIR.glob("*.jsonl"))

    if not files:
        print(f"No files to process")
        return

    # Check for existing files
    if not args.force:
        existing_files = []
        for file in files:
            out_file = OUTPUT_DIR / file.name
            if out_file.exists():
                existing_files.append(file.name)

        if existing_files:
            print("=" * 60)
            print("ERROR: Output files already exist!")
            print("=" * 60)
            print(f"\nThe following files already exist in {OUTPUT_DIR}:")
            for filename in existing_files:
                print(f"  - {filename}")
            print(f"\nTo overwrite existing files, use --force flag:")
            print(f"  python script/process/sample_mini.py --pairs {' '.join([f.stem for f in files])} --force")
            print("=" * 60)
            return

    # Create output dir
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print(f"Sampling {sample_size} records per file (seed={args.seed})")
    if args.pairs:
        print(f"Processing {len(files)} specific pair(s): {', '.join([f.stem for f in files])}")
    else:
        print(f"Processing all {len(files)} pairs")
    print("=" * 60)
    print()

    total_sampled = 0

    for file in files:
        # Read all records
        records = []
        with open(file) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except:
                        pass

        # Sample
        n = min(sample_size, len(records))
        sampled = random.sample(records, n)

        # Write with idx field
        out_file = OUTPUT_DIR / file.name
        with open(out_file, "w") as f:
            for idx, record in enumerate(sampled):
                # Add/update idx field at the beginning
                record_with_idx = {"idx": idx}
                # Remove old idx if exists, then update with record
                record_without_idx = {k: v for k, v in record.items() if k != "idx"}
                record_with_idx.update(record_without_idx)
                f.write(json.dumps(record_with_idx, ensure_ascii=False) + "\n")

        total_sampled += n
        print(f"{file.name:15} {len(records):6,} -> {n:3} samples")

    print()
    print(f"Total: {total_sampled} samples")
    print(f"Output: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
