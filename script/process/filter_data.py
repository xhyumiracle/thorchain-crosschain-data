#!/usr/bin/env python3
"""
Filter THORChain data by amount and time thresholds.

Usage:
    python script/filter_data.py
"""

import json
from pathlib import Path

# =============================================================================
# Configuration: Edit thresholds here
# =============================================================================

# Fee rate presets (amounts in 1e8 units)
FEE_RATE_PRESETS = {
    "0.01": {"BTC": 100_000_000, "ETH": 2_000_000_000, "DOGE": 1_000_000_000_000},      # 1.0 BTC, 20.0 ETH, 10k DOGE
    "0.02": {"BTC": 50_000_000, "ETH": 1_000_000_000, "DOGE": 500_000_000_000},         # 0.5 BTC, 10.0 ETH, 5k DOGE
    "0.05": {"BTC": 20_000_000, "ETH": 400_000_000, "DOGE": 200_000_000_000},           # 0.2 BTC, 4.0 ETH, 2k DOGE
    "0.1":  {"BTC": 10_000_000, "ETH": 200_000_000, "DOGE": 100_000_000_000},           # 0.1 BTC, 2.0 ETH, 1k DOGE
}

# Custom thresholds (add/modify assets here)
CUSTOM = {
    "BTC": 10_000_000,      # 0.1 BTC
    "ETH": 200_000_000,     # 2.0 ETH
    "DOGE": 100_000_000_000 # 1k DOGE
}

# Choose which thresholds to use:
USE_PRESET = "0.1"          # Use preset: "0.01", "0.02", "0.05", "0.1", or None for CUSTOM
THRESHOLDS = FEE_RATE_PRESETS[USE_PRESET] if USE_PRESET else CUSTOM

# Time threshold: max height diff (blocks)
MAX_HEIGHT_DIFF = 300       # 300 blocks ≈ 30 minutes (6s per block)

# Paths
INPUT_DIR = Path("data/thorchain-2025")
OUTPUT_DIR = Path("data/thorchain-2025-filtered")

# =============================================================================


def should_keep(record):
    """Return True if record passes filters."""
    in_list = record.get("in", [])
    out_list = record.get("out", [])

    if len(in_list) != 1 or len(out_list) != 1:
        return False

    in_entry = in_list[0]
    out_entry = out_list[0]

    # Check amount
    asset = in_entry.get("asset", "")
    amount = int(in_entry.get("amount", 0))
    threshold = THRESHOLDS.get(asset, 0)
    if amount < threshold:
        return False

    # Check time (height diff)
    in_height = int(in_entry.get("thorchainHeight", 0))
    out_height = int(out_entry.get("thorchainHeight", 0))
    if in_height and out_height:
        if out_height - in_height > MAX_HEIGHT_DIFF:
            return False

    return True


def main():
    # Find input files
    files = [f for f in INPUT_DIR.glob("*.ndjson") if not f.stem.startswith("multi-")]

    if not files:
        print(f"No ndjson files in {INPUT_DIR}")
        return

    # Create output dir
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Print config
    print("=" * 70)
    print(f"Filtering with: {f'preset {USE_PRESET}' if USE_PRESET else 'custom'}")
    print(f"Thresholds: BTC={THRESHOLDS['BTC']/1e8:.1f}, "
          f"ETH={THRESHOLDS['ETH']/1e8:.1f}, "
          f"DOGE={THRESHOLDS['DOGE']/1e8:.0f}")
    print(f"Max height diff: {MAX_HEIGHT_DIFF} blocks (~{MAX_HEIGHT_DIFF*6/60:.0f}min)")
    print("=" * 70)
    print()

    # Process files
    total_in = 0
    total_out = 0

    for file in sorted(files):
        count_in = 0
        count_out = 0

        out_file = OUTPUT_DIR / file.name

        with open(file) as f_in, open(out_file, "w") as f_out:
            for line in f_in:
                line = line.strip()
                if not line:
                    continue

                try:
                    record = json.loads(line)
                except:
                    continue

                count_in += 1

                if should_keep(record):
                    f_out.write(line + "\n")
                    count_out += 1

        total_in += count_in
        total_out += count_out

        pct = count_out / count_in * 100 if count_in > 0 else 0
        print(f"{file.name:15} {count_in:6,} -> {count_out:6,} ({pct:5.1f}%)")

    # Summary
    pct = total_out / total_in * 100 if total_in > 0 else 0
    print("-" * 70)
    print(f"{'Total':15} {total_in:6,} -> {total_out:6,} ({pct:5.1f}%)")
    print()
    print(f"Output: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
