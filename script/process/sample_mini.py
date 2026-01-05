#!/usr/bin/env python3
"""
Sample mini dataset from high-fast data.

Usage:
    python script/sample_mini.py
"""

import json
import random
from pathlib import Path

# Configuration
INPUT_DIR = Path("data/thorchain-2025-high-fast")
OUTPUT_DIR = Path("data/thorchain-2025-high-fast-mini")
SAMPLE_SIZE = 10  # samples per file
RANDOM_SEED = 42  # for reproducibility

def main():
    random.seed(RANDOM_SEED)

    # Find input files
    files = sorted(INPUT_DIR.glob("*.ndjson"))

    if not files:
        print(f"No files in {INPUT_DIR}")
        return

    # Create output dir
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print(f"Sampling {SAMPLE_SIZE} records per file (seed={RANDOM_SEED})")
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
        n = min(SAMPLE_SIZE, len(records))
        sampled = random.sample(records, n)

        # Write
        out_file = OUTPUT_DIR / file.name
        with open(out_file, "w") as f:
            for record in sampled:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

        total_sampled += n
        print(f"{file.name:15} {len(records):6,} -> {n:3} samples")

    print()
    print(f"Total: {total_sampled} samples")
    print(f"Output: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
