#!/usr/bin/env python3
"""
Filter records by height diff threshold.
Usage: python filter_height_diff.py [--threshold N] [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD]

Examples:
  python filter_height_diff.py --threshold 5000
  python filter_height_diff.py --threshold 2000 --start-date 2025-03-01 --end-date 2025-03-31
"""

import argparse
import json
from pathlib import Path
from datetime import datetime


DATA_DIR = Path(__file__).parent.parent.parent / "data" / "thorchain-2025"


def load_ndjson(filepath: Path) -> list[dict]:
    """Load records from an ndjson file."""
    records = []
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def get_height_diff(record: dict) -> int | None:
    """Calculate height diff: out[0].thorchainHeight - in[0].thorchainHeight"""
    in_list = record.get("in", [])
    out_list = record.get("out", [])
    if in_list and out_list:
        in_height = int(in_list[0].get("thorchainHeight", 0))
        out_height = int(out_list[0].get("thorchainHeight", 0))
        return out_height - in_height
    return None


def get_datetime(record: dict) -> datetime:
    """Convert timestamp (nanoseconds) to datetime."""
    ts_ns = int(record.get("timestamp", 0))
    return datetime.fromtimestamp(ts_ns / 1e9)


def filter_records(
    threshold: int,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
) -> list[dict]:
    """Filter records by height diff threshold and optional date range."""
    # Find all non-multi-* ndjson files
    ndjson_files = sorted(
        f for f in DATA_DIR.glob("*.ndjson") if not f.name.startswith("multi-")
    )

    results = []
    for filepath in ndjson_files:
        records = load_ndjson(filepath)
        pair = filepath.stem  # e.g., "BTC-DOGE"

        for record in records:
            height_diff = get_height_diff(record)
            if height_diff is None:
                continue

            dt = get_datetime(record)

            # Apply filters
            if height_diff < threshold:
                continue
            if start_date and dt < start_date:
                continue
            if end_date and dt > end_date:
                continue

            results.append(
                {
                    "id": record.get("id"),
                    "pair": pair,
                    "timestamp": dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "height_diff": height_diff,
                    "in_height": int(record["in"][0]["thorchainHeight"]),
                    "out_height": int(record["out"][0]["thorchainHeight"]),
                    "in_amount": int(record["in"][0]["amount"]),
                    "out_amount": int(record["out"][0]["amount"]),
                }
            )

    # Sort by height_diff descending
    results.sort(key=lambda x: x["height_diff"], reverse=True)
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Filter records by height diff threshold"
    )
    parser.add_argument(
        "--threshold",
        "-t",
        type=int,
        default=5000,
        help="Minimum height diff to filter (default: 5000)",
    )
    parser.add_argument(
        "--start-date",
        "-s",
        type=str,
        help="Start date filter (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        "-e",
        type=str,
        help="End date filter (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        help="Output file path (optional, prints to stdout if not specified)",
    )

    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d") if args.start_date else None
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59) if args.end_date else None

    print(f"Filtering with threshold >= {args.threshold}")
    if start_date:
        print(f"  Start date: {args.start_date}")
    if end_date:
        print(f"  End date: {args.end_date}")
    print()

    results = filter_records(args.threshold, start_date, end_date)

    print(f"Found {len(results)} records:\n")
    print("-" * 100)
    print(f"{'ID':<66} {'Pair':<10} {'Date':<20} {'HeightDiff':>10}")
    print("-" * 100)

    for r in results:
        print(f"{r['id']:<66} {r['pair']:<10} {r['timestamp']:<20} {r['height_diff']:>10}")

    print("-" * 100)

    # Print IDs only for easy copy
    print("\nIDs only:")
    for r in results:
        print(r["id"])

    # Output to file if specified
    if args.output:
        output_path = Path(args.output)
        with open(output_path, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\nFull results saved to: {output_path}")


if __name__ == "__main__":
    main()
