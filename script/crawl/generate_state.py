#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Generate state.json for a dataset based on actual data.

This tool scans ndjson files and generates accurate state.json with:
- min/max timestamps from actual data
- cursors pointing to min timestamp (for backward crawling)
- statistics

Usage:
    python generate_state.py --datadir ../../raw_merged/data --outdir ../../raw_merged
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Dict, Optional

from utils import get_min_timestamp_from_ndjson, get_max_timestamp_from_ndjson


def slugify_to_assets(slug: str) -> str:
    """
    Convert slug back to assets format.
    e.g., "BTC.BTC__ETH.ETH" -> "BTC.BTC,ETH.ETH"
    """
    return slug.replace(".ndjson", "").replace("__", ",")


def count_lines(ndjson_path: Path) -> int:
    """Count total lines in ndjson file."""
    if not ndjson_path.exists():
        return 0
    count = 0
    with ndjson_path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                count += 1
    return count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate state.json from actual data"
    )
    parser.add_argument("--datadir", required=True, help="Data directory containing .ndjson files")
    parser.add_argument("--outdir", required=True, help="Output directory for state.json")
    parser.add_argument("--type", default="swap", help="Action type (default: swap)")
    parser.add_argument("--min-ts-global", type=int, default=None, help="Global min_ts boundary (optional)")

    args = parser.parse_args()

    datadir = Path(args.datadir)
    outdir = Path(args.outdir)

    if not datadir.exists():
        raise SystemExit(f"Error: {datadir} does not exist")

    outdir.mkdir(parents=True, exist_ok=True)
    state_path = outdir / "state.json"

    # Find all ndjson files
    ndjson_files = sorted(datadir.glob("*.ndjson"))

    if not ndjson_files:
        raise SystemExit(f"Error: No .ndjson files found in {datadir}")

    print(f"[INFO] Generating state.json from {len(ndjson_files)} files")
    print(f"[INFO] Data directory: {datadir.resolve()}")
    print(f"[INFO] Output: {state_path.resolve()}")
    print()

    assets_list = []
    cursors = {}
    total_records = 0
    global_min_ts: Optional[int] = None
    global_max_ts: Optional[int] = None

    for ndjson_file in ndjson_files:
        filename = ndjson_file.name
        assets = slugify_to_assets(filename)
        assets_list.append(assets)

        print(f"[INFO] Processing: {assets}")

        # Get min/max timestamps
        min_ts = get_min_timestamp_from_ndjson(ndjson_file)
        max_ts = get_max_timestamp_from_ndjson(ndjson_file)
        record_count = count_lines(ndjson_file)

        if min_ts is None or max_ts is None:
            print(f"  [WARN] No valid timestamps found, skipping")
            continue

        print(f"  Records: {record_count}")
        print(f"  Min timestamp: {min_ts} ({min_ts // 1_000_000_000} sec)")
        print(f"  Max timestamp: {max_ts} ({max_ts // 1_000_000_000} sec)")

        # Update global min/max
        if global_min_ts is None or min_ts < global_min_ts:
            global_min_ts = min_ts
        if global_max_ts is None or max_ts > global_max_ts:
            global_max_ts = max_ts

        # Create cursor pointing to min timestamp (for backward crawling)
        cursors[assets] = {
            "ts": min_ts,
            "offset": 0,
            "finished": True  # Mark as finished since we have complete data
        }

        total_records += record_count
        print()

    # Convert nanoseconds to seconds for config
    min_ts_sec = global_min_ts // 1_000_000_000 if global_min_ts else None
    max_ts_sec = global_max_ts // 1_000_000_000 if global_max_ts else None

    # Use provided min_ts_global if specified, otherwise use computed min
    if args.min_ts_global is not None:
        min_ts_sec = args.min_ts_global

    # Generate state
    state = {
        "version": 2,
        "cursors": cursors,
        "config": {
            "type": args.type,
            "assets": assets_list,
            "min_ts": min_ts_sec,
            "max_ts": max_ts_sec,
        },
        "stats": {
            "total_records": total_records,
            "total_requests": 0,  # Unknown from merged data
            "total_errors": 0,
            "total_appended": total_records,
            "updated_at_unix": int(time.time()),
        }
    }

    # Write state.json
    with state_path.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)

    print("[INFO] Summary:")
    print(f"  Total assets: {len(assets_list)}")
    print(f"  Total records: {total_records}")
    print(f"  Global time range (seconds):")
    print(f"    min_ts: {min_ts_sec}")
    print(f"    max_ts: {max_ts_sec}")
    print(f"\n[INFO] state.json written to {state_path.resolve()}")


if __name__ == "__main__":
    main()
