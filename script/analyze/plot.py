#!/usr/bin/env python3
"""
Plot THORChain swap data:
1. Amount vs Timestamp (for all non-multi-* files)
2. Height Diff vs Timestamp (out[0].thorchainHeight - in[0].thorchainHeight)

Each plot has 3 subplots grouped by reverse pairs.
"""

import json
from pathlib import Path
from collections import defaultdict
from datetime import datetime

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


DATA_DIR = Path(__file__).parent.parent.parent / "data"
OUTPUT_DIR = Path(__file__).parent.parent.parent / "png"

# Plot style config
SCATTER_ALPHA = 0.3  # Transparency for scatter points
GRID_ALPHA = 0.3     # Transparency for grid lines

# Pair groups: each group contains reverse pairs
# Format: [(pair1, pair2, title), ...]
PAIR_GROUPS = [
    ("BTC-ETH", "ETH-BTC", "BTC<>ETH"),
    ("BTC-DOGE", "DOGE-BTC", "BTC<>DOGE"),
    ("ETH-DOGE", "DOGE-ETH", "ETH<>DOGE"),
]

# Legend labels with direction arrows
PAIR_LABELS = {
    "BTC-ETH": "BTC→ETH",
    "ETH-BTC": "ETH→BTC",
    "BTC-DOGE": "BTC→DOGE",
    "DOGE-BTC": "DOGE→BTC",
    "ETH-DOGE": "ETH→DOGE",
    "DOGE-ETH": "DOGE→ETH",
}

# Style config: {pair_name: (color, marker)}
# Colors: orange for BTC-ETH, blue for BTC-DOGE, green for ETH-DOGE
PAIR_STYLES = {
    # BTC-ETH pair (orange tones)
    "BTC-ETH": ("#d95f02", "o"),   # dark orange, circle
    "ETH-BTC": ("#fc8d62", "x"),   # light orange, x
    # BTC-DOGE pair (blue tones)
    "BTC-DOGE": ("#1f78b4", "o"),  # dark blue, circle
    "DOGE-BTC": ("#6baed6", "x"),  # light blue, x
    # ETH-DOGE pair (green tones)
    "ETH-DOGE": ("#1b7837", "o"),  # dark green, circle
    "DOGE-ETH": ("#74c476", "x"),  # light green, x
}


def load_ndjson(filepath: Path) -> list[dict]:
    """Load records from an ndjson file."""
    records = []
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def extract_data(records: list[dict]) -> tuple[list, list, list, list]:
    """
    Extract timestamp, in_amount, out_amount, and height_diff from records.
    Returns: (timestamps_as_datetime, in_amounts, out_amounts, height_diffs)
    """
    timestamps = []
    in_amounts = []
    out_amounts = []
    height_diffs = []

    for record in records:
        ts_ns = int(record.get("timestamp", 0))
        # Convert nanoseconds to datetime
        ts_sec = ts_ns / 1e9
        dt = datetime.fromtimestamp(ts_sec)
        timestamps.append(dt)

        # In amount (first input)
        in_list = record.get("in", [])
        if in_list:
            in_amounts.append(int(in_list[0].get("amount", 0)))
        else:
            in_amounts.append(0)

        # Out amount (first output)
        out_list = record.get("out", [])
        if out_list:
            out_amounts.append(int(out_list[0].get("amount", 0)))
        else:
            out_amounts.append(0)

        # Height diff: out[0].thorchainHeight - in[0].thorchainHeight
        if in_list and out_list:
            in_height = int(in_list[0].get("thorchainHeight", 0))
            out_height = int(out_list[0].get("thorchainHeight", 0))
            height_diffs.append(out_height - in_height)
        else:
            height_diffs.append(0)

    return timestamps, in_amounts, out_amounts, height_diffs


def scatter_pair(ax, timestamps, values, pair_name):
    """Helper to scatter plot a single pair with its style."""
    color, marker = PAIR_STYLES.get(pair_name, ("#333333", "o"))
    label = PAIR_LABELS.get(pair_name, pair_name)
    scatter_kwargs = {"label": label, "alpha": SCATTER_ALPHA, "s": 20, "c": color, "marker": marker}
    if marker == "o":
        scatter_kwargs["edgecolors"] = "none"
    ax.scatter(timestamps, values, **scatter_kwargs)


def plot_amount_vs_timestamp(all_data: dict[str, tuple], output_path: Path):
    """
    Plot in_amount vs timestamp as 3 subplots grouped by reverse pairs.
    """
    fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)

    for idx, (pair1, pair2, title) in enumerate(PAIR_GROUPS):
        ax = axes[idx]

        # Plot pair1
        key1 = f"{pair1}.ndjson"
        if key1 in all_data:
            timestamps, in_amounts, _, _ = all_data[key1]
            scatter_pair(ax, timestamps, in_amounts, pair1)

        # Plot pair2
        key2 = f"{pair2}.ndjson"
        if key2 in all_data:
            timestamps, in_amounts, _, _ = all_data[key2]
            scatter_pair(ax, timestamps, in_amounts, pair2)

        ax.set_ylabel("In Amount", fontsize=10)
        ax.set_title(title, fontsize=11)
        ax.set_yscale("log")
        ax.legend(loc="upper right", fontsize=9)
        ax.grid(True, alpha=GRID_ALPHA)

    # Format x-axis on bottom subplot
    axes[-1].set_xlabel("Timestamp", fontsize=11)
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    axes[-1].xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.xticks(rotation=45, ha="right")

    fig.suptitle("Swap In Amount vs Timestamp (Grouped by Reverse Pairs)", fontsize=13)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"Saved: {output_path}")
    plt.close()


def plot_height_diff_vs_timestamp(all_data: dict[str, tuple], output_path: Path):
    """
    Plot height_diff vs timestamp as 3 subplots grouped by reverse pairs.
    """
    fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)

    for idx, (pair1, pair2, title) in enumerate(PAIR_GROUPS):
        ax = axes[idx]

        # Plot pair1
        key1 = f"{pair1}.ndjson"
        if key1 in all_data:
            timestamps, _, _, height_diffs = all_data[key1]
            scatter_pair(ax, timestamps, height_diffs, pair1)

        # Plot pair2
        key2 = f"{pair2}.ndjson"
        if key2 in all_data:
            timestamps, _, _, height_diffs = all_data[key2]
            scatter_pair(ax, timestamps, height_diffs, pair2)

        ax.set_ylabel("Height Diff", fontsize=10)
        ax.set_title(title, fontsize=11)
        ax.legend(loc="upper right", fontsize=9)
        ax.grid(True, alpha=GRID_ALPHA)

    # Format x-axis on bottom subplot
    axes[-1].set_xlabel("Timestamp", fontsize=11)
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    axes[-1].xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.xticks(rotation=45, ha="right")

    fig.suptitle("Block Height Difference vs Timestamp (Grouped by Reverse Pairs)", fontsize=13)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"Saved: {output_path}")
    plt.close()


def aggregate_daily(timestamps: list, in_amounts: list) -> tuple[list, list, list]:
    """
    Aggregate data by day.
    Returns: (dates, tx_counts, total_amounts)
    """
    daily_counts = defaultdict(int)
    daily_amounts = defaultdict(int)

    for ts, amount in zip(timestamps, in_amounts):
        day = ts.date()
        daily_counts[day] += 1
        daily_amounts[day] += amount

    # Sort by date
    sorted_days = sorted(daily_counts.keys())
    dates = [datetime.combine(d, datetime.min.time()) for d in sorted_days]
    tx_counts = [daily_counts[d] for d in sorted_days]
    total_amounts = [daily_amounts[d] for d in sorted_days]

    return dates, tx_counts, total_amounts


def plot_daily_tx_count(all_data: dict[str, tuple], output_path: Path):
    """
    Plot daily transaction count as 3 subplots grouped by reverse pairs.
    """
    fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)

    for idx, (pair1, pair2, title) in enumerate(PAIR_GROUPS):
        ax = axes[idx]

        # Plot pair1
        key1 = f"{pair1}.ndjson"
        if key1 in all_data:
            timestamps, in_amounts, _, _ = all_data[key1]
            dates, tx_counts, _ = aggregate_daily(timestamps, in_amounts)
            color, _ = PAIR_STYLES.get(pair1, ("#333333", "o"))
            label1 = PAIR_LABELS.get(pair1, pair1)
            ax.bar(dates, tx_counts, label=label1, alpha=0.7, color=color, width=0.4)

        # Plot pair2
        key2 = f"{pair2}.ndjson"
        if key2 in all_data:
            timestamps, in_amounts, _, _ = all_data[key2]
            dates, tx_counts, _ = aggregate_daily(timestamps, in_amounts)
            color, _ = PAIR_STYLES.get(pair2, ("#333333", "o"))
            label2 = PAIR_LABELS.get(pair2, pair2)
            # Offset bars slightly for visibility
            dates_offset = [d.timestamp() + 0.4 * 86400 for d in dates]
            dates_offset = [datetime.fromtimestamp(t) for t in dates_offset]
            ax.bar(dates_offset, tx_counts, label=label2, alpha=0.7, color=color, width=0.4)

        ax.set_ylabel("TX Count", fontsize=10)
        ax.set_title(title, fontsize=11)
        ax.legend(loc="upper right", fontsize=9)
        ax.grid(True, alpha=GRID_ALPHA)

    # Format x-axis on bottom subplot
    axes[-1].set_xlabel("Date", fontsize=11)
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    axes[-1].xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.xticks(rotation=45, ha="right")

    fig.suptitle("Daily Transaction Count (Grouped by Reverse Pairs)", fontsize=13)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"Saved: {output_path}")
    plt.close()


def plot_daily_amount(all_data: dict[str, tuple], output_path: Path):
    """
    Plot daily cumulative amount as 3 subplots grouped by reverse pairs.
    """
    fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)

    for idx, (pair1, pair2, title) in enumerate(PAIR_GROUPS):
        ax = axes[idx]

        # Plot pair1
        key1 = f"{pair1}.ndjson"
        if key1 in all_data:
            timestamps, in_amounts, _, _ = all_data[key1]
            dates, _, total_amounts = aggregate_daily(timestamps, in_amounts)
            color, _ = PAIR_STYLES.get(pair1, ("#333333", "o"))
            label1 = PAIR_LABELS.get(pair1, pair1)
            ax.bar(dates, total_amounts, label=label1, alpha=0.7, color=color, width=0.4)

        # Plot pair2
        key2 = f"{pair2}.ndjson"
        if key2 in all_data:
            timestamps, in_amounts, _, _ = all_data[key2]
            dates, _, total_amounts = aggregate_daily(timestamps, in_amounts)
            color, _ = PAIR_STYLES.get(pair2, ("#333333", "o"))
            label2 = PAIR_LABELS.get(pair2, pair2)
            # Offset bars slightly for visibility
            dates_offset = [d.timestamp() + 0.4 * 86400 for d in dates]
            dates_offset = [datetime.fromtimestamp(t) for t in dates_offset]
            ax.bar(dates_offset, total_amounts, label=label2, alpha=0.7, color=color, width=0.4)

        ax.set_ylabel("Total Amount", fontsize=10)
        ax.set_title(title, fontsize=11)
        ax.set_yscale("log")
        ax.legend(loc="upper right", fontsize=9)
        ax.grid(True, alpha=GRID_ALPHA)

    # Format x-axis on bottom subplot
    axes[-1].set_xlabel("Date", fontsize=11)
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    axes[-1].xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.xticks(rotation=45, ha="right")

    fig.suptitle("Daily Cumulative Amount (Grouped by Reverse Pairs)", fontsize=13)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"Saved: {output_path}")
    plt.close()


def plot_amount_distribution_cdf_single_pair(all_data: dict[str, tuple], output_dir: Path,
                                              pair1: str, pair2: str, title: str,
                                              min_amount: int = 1000, max_amount: int = 10**10, num_bins: int = 50):
    """
    Plot PDF (binned count) + CDF of in/out amounts for a single pair group.
    Creates one figure with 2 subplots (one per direction).

    Each subplot shows amounts for 2 assets (one IN, one OUT).
    Each asset has consistent color/style across both subplots.

    Style mapping:
    - Each asset gets unique color + fill pattern
    - Asset style is consistent whether it's IN or OUT
    """
    # Asset-specific styles: {asset: color}
    # Using the existing color scheme from PAIR_STYLES
    asset_styles = {
        "BTC": "#d95f02",   # Dark orange
        "ETH": "#1f78b4",   # Dark blue
        "DOGE": "#2ca02c",  # Green
        "LTC": "#9467bd",   # Purple
    }

    fig, axes = plt.subplots(2, 1, figsize=(12, 10), sharex=True)

    # Create logarithmic bins (shared for both subplots)
    log_bins = np.logspace(np.log10(min_amount), np.log10(max_amount), num_bins + 1)
    bin_widths = log_bins[1:] - log_bins[:-1]
    bin_centers = np.sqrt(log_bins[:-1] * log_bins[1:])  # Geometric mean for log scale

    for idx, pair_name in enumerate([pair1, pair2]):
        ax_left = axes[idx]
        ax_right = ax_left.twinx()  # Create right Y-axis for CDF

        key = f"{pair_name}.ndjson"
        if key not in all_data:
            continue

        _, in_amounts, out_amounts, _ = all_data[key]

        # Filter amounts to valid range
        in_amounts_valid = np.array([a for a in in_amounts if min_amount <= a <= max_amount])
        out_amounts_valid = np.array([a for a in out_amounts if min_amount <= a <= max_amount])

        if len(in_amounts_valid) == 0 and len(out_amounts_valid) == 0:
            continue

        # Parse pair name to get source and destination assets
        # e.g., "BTC-ETH" -> in_asset="BTC", out_asset="ETH"
        parts = pair_name.split("-")
        in_asset = parts[0]
        out_asset = parts[1]

        # Get colors for each asset
        in_color = asset_styles.get(in_asset, "#333333")
        out_color = asset_styles.get(out_asset, "#666666")

        # Process in amounts (source asset)
        if len(in_amounts_valid) > 0:
            # PDF: Bar chart with solid fill (no hatch)
            counts, _ = np.histogram(in_amounts_valid, bins=log_bins)
            ax_left.bar(
                bin_centers, counts, width=bin_widths,
                color=in_color, alpha=0.7, edgecolor='none',
                label=f"{in_asset} IN"
            )

            # CDF: Solid line for IN
            sorted_amounts = np.sort(in_amounts_valid)
            cdf = np.arange(1, len(sorted_amounts) + 1) / len(sorted_amounts) * 100
            plot_x = np.concatenate([[min_amount], sorted_amounts])
            plot_y = np.concatenate([[0], cdf])
            ax_right.plot(
                plot_x, plot_y,
                color=in_color, linewidth=2.5, linestyle='-', alpha=0.9
            )

        # Process out amounts (destination asset)
        if len(out_amounts_valid) > 0:
            # PDF: Bar chart with solid fill (no hatch)
            counts, _ = np.histogram(out_amounts_valid, bins=log_bins)
            ax_left.bar(
                bin_centers, counts, width=bin_widths,
                color=out_color, alpha=0.7, edgecolor='none',
                label=f"{out_asset} OUT"
            )

            # CDF: Dashed line for OUT
            sorted_amounts = np.sort(out_amounts_valid)
            cdf = np.arange(1, len(sorted_amounts) + 1) / len(sorted_amounts) * 100
            plot_x = np.concatenate([[min_amount], sorted_amounts])
            plot_y = np.concatenate([[0], cdf])
            ax_right.plot(
                plot_x, plot_y,
                color=out_color, linewidth=2.5, linestyle='--', alpha=0.9
            )

        # Left axis (PDF)
        ax_left.set_ylabel("Count", fontsize=11)
        ax_left.set_xscale("log")
        ax_left.set_xlim(min_amount, max_amount)
        ax_left.grid(True, alpha=0.2, which='major', axis='y')

        # Right axis (CDF)
        ax_right.set_ylabel("CDF %", fontsize=11)
        ax_right.set_ylim(0, 100)

        # Add horizontal reference lines for CDF
        for pct in [50, 90, 95, 99]:
            ax_right.axhline(y=pct, color="gray", linestyle=":", linewidth=0.5, alpha=0.3)

        # Subplot title
        label = PAIR_LABELS.get(pair_name, pair_name)
        ax_left.set_title(f"{label}", fontsize=12, pad=10)
        ax_left.legend(loc="upper left", fontsize=10, framealpha=0.9)

    # X-axis label only on bottom subplot
    axes[-1].set_xlabel("Amount (log scale)", fontsize=11)

    # Overall title
    fig.suptitle(f"{title} - Amount Distribution", fontsize=14, y=0.995)
    plt.tight_layout()

    # Save with pair-specific filename
    safe_title = title.replace("<>", "-").replace(">", "-").replace("<", "-")
    output_path = output_dir / f"amount_dist_{safe_title}.png"
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"Saved: {output_path}")
    plt.close()


def plot_amount_distribution_cdf(all_data: dict[str, tuple], output_dir: Path,
                                   min_amount: int = 1000, max_amount: int = 10**10, num_bins: int = 50):
    """
    Generate separate amount distribution plots for each pair group.
    Each pair group (e.g., BTC<>ETH) gets its own PNG file.
    """
    for pair1, pair2, title in PAIR_GROUPS:
        plot_amount_distribution_cdf_single_pair(
            all_data, output_dir, pair1, pair2, title,
            min_amount, max_amount, num_bins
        )


def plot_height_diff_cdf(all_data: dict[str, tuple], output_path: Path, max_x: int = 1000, bin_size: int = 5):
    """
    Plot PDF (binned count) + CDF of height diff with dual Y-axes.
    Left Y-axis: Count per bin (aggregated by bin_size)
    Right Y-axis: CDF (coverage percentage 0-100%)
    X-axis: height diff threshold (1 to max_x)
    """
    fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)

    # Offset for the two directions in each pair group
    OFFSET = bin_size * 0.15

    for idx, (pair1, pair2, title) in enumerate(PAIR_GROUPS):
        ax_left = axes[idx]
        ax_right = ax_left.twinx()  # Create right Y-axis for CDF

        for i, pair_name in enumerate([pair1, pair2]):
            key = f"{pair_name}.ndjson"
            if key not in all_data:
                continue

            _, _, _, height_diffs = all_data[key]
            height_diffs = np.array([h for h in height_diffs if 0 < h <= max_x])  # Filter to range
            if len(height_diffs) == 0:
                continue

            color, _ = PAIR_STYLES.get(pair_name, ("#333333", "o"))
            label = PAIR_LABELS.get(pair_name, pair_name)

            # Offset: first pair slightly left, second pair slightly right
            offset = -OFFSET if i == 0 else OFFSET

            # PDF: Aggregate by bin_size (no alpha for thin lines)
            bins = np.arange(0, max_x + bin_size, bin_size)
            counts, bin_edges = np.histogram(height_diffs, bins=bins)
            bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
            ax_left.vlines(
                bin_centers + offset, 0, counts,
                colors=color, alpha=0.8, linewidth=1.5, label=f"{label} (count)"
            )

            # CDF: Line on right Y-axis, ensure it starts from (0, 0)
            sorted_diffs = np.sort(height_diffs)
            cdf = np.arange(1, len(sorted_diffs) + 1) / len(sorted_diffs) * 100
            # Prepend (0, 0) and first data point at y=0 to ensure proper start
            plot_x = np.concatenate([[0, sorted_diffs[0]], sorted_diffs])
            plot_y = np.concatenate([[0, 0], cdf])
            ax_right.plot(
                plot_x, plot_y,
                color=color, linewidth=1.5, label=f"{label} (CDF)"
            )

        # Left axis (PDF)
        ax_left.set_ylabel("Count", fontsize=10)
        ax_left.set_xlim(0, max_x)
        # ax_left.set_ylim(bottom=0)

        # Right axis (CDF)
        ax_right.set_ylabel("Coverage % (CDF)", fontsize=10)
        ax_right.set_ylim(0, 100)

        # Add horizontal reference lines for CDF
        for pct in [90, 95, 99]:
            ax_right.axhline(y=pct, color="gray", linestyle="--", linewidth=0.5, alpha=0.5)

        ax_left.set_title(title, fontsize=11)
        # No grid to keep vlines clean

        # Combined legend
        lines1, labels1 = ax_left.get_legend_handles_labels()
        lines2, labels2 = ax_right.get_legend_handles_labels()
        ax_right.legend(lines1 + lines2, labels1 + labels2, loc="center right", fontsize=8)

    axes[-1].set_xlabel("Height Diff (blocks)", fontsize=11)

    fig.suptitle(f"Height Diff Distribution: Count (bin={bin_size}) + CDF", fontsize=13)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"Saved: {output_path}")
    plt.close()


def main():
    # Find all non-multi-* ndjson files
    ndjson_files = sorted(
        f for f in DATA_DIR.glob("*.ndjson") if not f.name.startswith("multi-")
    )

    if not ndjson_files:
        print(f"No non-multi-* .ndjson files found in {DATA_DIR}")
        return

    print(f"Found {len(ndjson_files)} data files (excluding multi-*):\n")
    for f in ndjson_files:
        print(f"  - {f.name}")
    print()

    # Load and extract data from all files
    all_data = {}
    for filepath in ndjson_files:
        print(f"Loading {filepath.name}...")
        records = load_ndjson(filepath)
        timestamps, in_amounts, out_amounts, height_diffs = extract_data(records)
        all_data[filepath.name] = (timestamps, in_amounts, out_amounts, height_diffs)
        print(f"  -> {len(records)} records loaded")

    print()

    # Generate plots
    amount_plot_path = OUTPUT_DIR / "amount_vs_timestamp.png"
    height_plot_path = OUTPUT_DIR / "height_diff_vs_timestamp.png"
    daily_tx_plot_path = OUTPUT_DIR / "daily_tx_count.png"
    daily_amount_plot_path = OUTPUT_DIR / "daily_amount.png"
    height_cdf_plot_path = OUTPUT_DIR / "height_diff_cdf.png"

    print("Generating plots...")
    plot_amount_vs_timestamp(all_data, amount_plot_path)
    plot_height_diff_vs_timestamp(all_data, height_plot_path)
    plot_daily_tx_count(all_data, daily_tx_plot_path)
    plot_daily_amount(all_data, daily_amount_plot_path)
    plot_height_diff_cdf(all_data, height_cdf_plot_path)
    # Generate separate amount distribution plots for each pair group
    plot_amount_distribution_cdf(all_data, OUTPUT_DIR)

    print("\nDone!")


if __name__ == "__main__":
    main()
