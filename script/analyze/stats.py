#!/usr/bin/env python3
"""
Statistics for THORChain swap data.
- Per-pair statistics: record count, in/out amounts, height diff, timestamps
- Timestamp distribution analysis
"""

import json
from pathlib import Path
from collections import defaultdict
import statistics


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


def compute_stats(values: list[int | float]) -> dict:
    """Compute basic statistics for a list of numeric values."""
    if not values:
        return {"count": 0, "min": None, "max": None, "mean": None, "median": None}
    return {
        "count": len(values),
        "min": min(values),
        "max": max(values),
        "mean": round(statistics.mean(values), 2),
        "median": round(statistics.median(values), 2),
    }


def analyze_pair(records: list[dict], filename: str) -> dict:
    """Analyze a single pair file."""
    in_amounts = []
    out_amounts = []
    timestamps = []
    height_diffs = []

    for record in records:
        # Timestamp
        ts = int(record.get("timestamp", 0))
        timestamps.append(ts)

        # In amounts and heights
        in_list = record.get("in", [])
        out_list = record.get("out", [])

        for inp in in_list:
            in_amounts.append(int(inp.get("amount", 0)))

        for out in out_list:
            out_amounts.append(int(out.get("amount", 0)))

        # Height diff: out[0].thorchainHeight - in[0].thorchainHeight
        if in_list and out_list:
            in_height = int(in_list[0].get("thorchainHeight", 0))
            out_height = int(out_list[0].get("thorchainHeight", 0))
            height_diffs.append(out_height - in_height)

    # Timestamp distribution: count how many entries share each timestamp
    ts_counts = defaultdict(int)
    for ts in timestamps:
        ts_counts[ts] += 1

    # Distribution of hit counts: {hit_count: number_of_timestamps_with_that_count}
    hit_distribution = defaultdict(int)
    for count in ts_counts.values():
        hit_distribution[count] += 1

    # Height diff coverage: percentage of records with height_diff <= threshold
    height_diff_thresholds = [10, 100, 300, 600, 1000, 6000, 14400]
    height_diff_coverage = {}
    total_diffs = len(height_diffs)
    if total_diffs > 0:
        for threshold in height_diff_thresholds:
            count_below = sum(1 for d in height_diffs if d <= threshold)
            height_diff_coverage[threshold] = round(count_below / total_diffs * 100, 2)

    return {
        "file": filename,
        "record_count": len(records),
        "in_amount_stats": compute_stats(in_amounts),
        "out_amount_stats": compute_stats(out_amounts),
        "height_diff_stats": compute_stats(height_diffs),
        "height_diff_coverage": height_diff_coverage,
        "timestamp_stats": {
            "min": min(timestamps) if timestamps else None,
            "max": max(timestamps) if timestamps else None,
            "unique_count": len(ts_counts),
        },
        "timestamp_hit_distribution": dict(sorted(hit_distribution.items())),
    }


def format_number(n: int | float | None) -> str:
    """Format large numbers for readability."""
    if n is None:
        return "N/A"
    if isinstance(n, float):
        return f"{n:,.2f}"
    return f"{n:,}"


def print_report(pair_analyses: list[dict]):
    """Print statistics report."""
    print("=" * 80)
    print("THORChain Swap Data Statistics Report")
    print("=" * 80)

    for analysis in pair_analyses:
        print(f"\n  [{analysis['file']}]")
        print(f"    Records: {analysis['record_count']}")

        in_stats = analysis["in_amount_stats"]
        print(f"\n    In Amount (satoshis/wei):")
        print(f"      Min:    {format_number(in_stats['min'])}")
        print(f"      Max:    {format_number(in_stats['max'])}")
        print(f"      Mean:   {format_number(in_stats['mean'])}")
        print(f"      Median: {format_number(in_stats['median'])}")

        out_stats = analysis["out_amount_stats"]
        print(f"\n    Out Amount (satoshis/wei):")
        print(f"      Min:    {format_number(out_stats['min'])}")
        print(f"      Max:    {format_number(out_stats['max'])}")
        print(f"      Mean:   {format_number(out_stats['mean'])}")
        print(f"      Median: {format_number(out_stats['median'])}")

        hd_stats = analysis["height_diff_stats"]
        print(f"\n    Height Diff (out - in blocks):")
        print(f"      Min:    {format_number(hd_stats['min'])}")
        print(f"      Max:    {format_number(hd_stats['max'])}")
        print(f"      Mean:   {format_number(hd_stats['mean'])}")
        print(f"      Median: {format_number(hd_stats['median'])}")

        hd_coverage = analysis["height_diff_coverage"]
        if hd_coverage:
            print(f"\n    Height Diff Coverage:")
            for threshold, pct in hd_coverage.items():
                print(f"      <= {threshold:>5}: {pct:>6.2f}%")

        ts_stats = analysis["timestamp_stats"]
        print(f"\n    Timestamp (nanoseconds):")
        print(f"      Min: {format_number(ts_stats['min'])}")
        print(f"      Max: {format_number(ts_stats['max'])}")
        print(f"      Unique timestamps: {ts_stats['unique_count']}")

        print(f"\n    Timestamp Hit Distribution:")
        print(f"      (entries per timestamp -> count of such timestamps)")
        for hits, count in sorted(analysis["timestamp_hit_distribution"].items()):
            print(f"        {hits} entry/entries: {count} timestamp(s)")

    print("\n" + "=" * 80)


def main():
    # Find all ndjson files
    ndjson_files = sorted(DATA_DIR.glob("*.ndjson"))

    if not ndjson_files:
        print(f"No .ndjson files found in {DATA_DIR}")
        return

    print(f"Found {len(ndjson_files)} data files in {DATA_DIR}\n")

    pair_analyses = []
    for filepath in ndjson_files:
        print(f"Processing {filepath.name}...")
        records = load_ndjson(filepath)
        analysis = analyze_pair(records, filepath.name)
        pair_analyses.append(analysis)

    print()
    print_report(pair_analyses)


if __name__ == "__main__":
    main()
