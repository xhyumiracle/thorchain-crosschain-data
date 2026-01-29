#!/usr/bin/env python3
"""
Plot THORChain swap data:
1. Amount vs Timestamp (for all non-multi-* files)
2. Height Diff vs Timestamp (out[0].thorchainHeight - in[0].thorchainHeight)
3. Time Diff vs Timestamp (optional, requires blockchain_txs/)
4. Time Diff CDF (optional, requires blockchain_txs/)

Each plot has 3 subplots grouped by reverse pairs.
"""

import json
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Add parent directory to path for utils import
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.blockchain import load_blockchain_txs, get_tx_timestamp

DATA_DIR = Path(__file__).parent.parent.parent / "data" / "thorchain-2025"
OUTPUT_DIR = Path(__file__).parent.parent.parent / "png"
BLOCKCHAIN_TXS_DIR = Path(__file__).parent.parent.parent / "blockchain_txs"

# Plot style config
SCATTER_ALPHA = 0.3  # Transparency for scatter points
GRID_ALPHA = 0.3     # Transparency for grid lines

# Pair groups: each group contains reverse pairs
# Format: [(pair1, pair2, title), ...]

# Main trading pairs (3 groups)
MAIN_PAIR_GROUPS = [
    ("BTC-ETH", "ETH-BTC", "BTC<>ETH"),
    ("BTC-DOGE", "DOGE-BTC", "BTC<>DOGE"),
    ("ETH-DOGE", "DOGE-ETH", "ETH<>DOGE"),
]

# LTC trading pairs (3 groups)
LTC_PAIR_GROUPS = [
    ("BTC-LTC", "LTC-BTC", "BTC<>LTC"),
    ("ETH-LTC", "LTC-ETH", "ETH<>LTC"),
    ("DOGE-LTC", "LTC-DOGE", "DOGE<>LTC"),
]

# All pairs (for amount_distribution_cdf which handles variable-length groups)
PAIR_GROUPS = MAIN_PAIR_GROUPS + LTC_PAIR_GROUPS

# Legend labels with direction arrows
PAIR_LABELS = {
    "BTC-ETH": "BTC→ETH",
    "ETH-BTC": "ETH→BTC",
    "BTC-DOGE": "BTC→DOGE",
    "DOGE-BTC": "DOGE→BTC",
    "ETH-DOGE": "ETH→DOGE",
    "DOGE-ETH": "DOGE→ETH",
    "BTC-LTC": "BTC→LTC",
    "LTC-BTC": "LTC→BTC",
    "ETH-LTC": "ETH→LTC",
    "LTC-ETH": "LTC→ETH",
    "DOGE-LTC": "DOGE→LTC",
    "LTC-DOGE": "LTC→DOGE",
}

# Unified asset colors (consistent across all plots, matching amount_distribution_cdf)
# Each trading direction uses the color of the source asset
ASSET_COLORS = {
    "BTC": "#d95f02",   # Orange
    "ETH": "#1f78b4",   # Blue
    "DOGE": "#2ca02c",  # Green
    "LTC": "#9467bd",   # Purple
}

def get_pair_style(pair_name: str) -> tuple[str, str]:
    """Get color and marker for a pair based on source asset.

    Args:
        pair_name: Trading pair like "BTC-ETH" (source-destination)

    Returns:
        tuple: (color, marker)
    """
    source_asset = pair_name.split("-")[0]
    color = ASSET_COLORS.get(source_asset, "#333333")
    marker = "o"  # Circle for all pairs
    return color, marker

# Legacy PAIR_STYLES for backward compatibility (now uses dynamic color assignment)
PAIR_STYLES = {pair: get_pair_style(pair) for pair in PAIR_LABELS.keys()}


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
    color, marker = get_pair_style(pair_name)
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

    for idx, (pair1, pair2, title) in enumerate(MAIN_PAIR_GROUPS):
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

    for idx, (pair1, pair2, title) in enumerate(MAIN_PAIR_GROUPS):
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

    for idx, (pair1, pair2, title) in enumerate(MAIN_PAIR_GROUPS):
        ax = axes[idx]

        # Plot pair1
        key1 = f"{pair1}.ndjson"
        if key1 in all_data:
            timestamps, in_amounts, _, _ = all_data[key1]
            dates, tx_counts, _ = aggregate_daily(timestamps, in_amounts)
            color, _ = get_pair_style(pair1)
            label1 = PAIR_LABELS.get(pair1, pair1)
            ax.bar(dates, tx_counts, label=label1, alpha=0.7, color=color, width=0.4)

        # Plot pair2
        key2 = f"{pair2}.ndjson"
        if key2 in all_data:
            timestamps, in_amounts, _, _ = all_data[key2]
            dates, tx_counts, _ = aggregate_daily(timestamps, in_amounts)
            color, _ = get_pair_style(pair2)
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

    for idx, (pair1, pair2, title) in enumerate(MAIN_PAIR_GROUPS):
        ax = axes[idx]

        # Plot pair1
        key1 = f"{pair1}.ndjson"
        if key1 in all_data:
            timestamps, in_amounts, _, _ = all_data[key1]
            dates, _, total_amounts = aggregate_daily(timestamps, in_amounts)
            color, _ = get_pair_style(pair1)
            label1 = PAIR_LABELS.get(pair1, pair1)
            ax.bar(dates, total_amounts, label=label1, alpha=0.7, color=color, width=0.4)

        # Plot pair2
        key2 = f"{pair2}.ndjson"
        if key2 in all_data:
            timestamps, in_amounts, _, _ = all_data[key2]
            dates, _, total_amounts = aggregate_daily(timestamps, in_amounts)
            color, _ = get_pair_style(pair2)
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
    # Use unified asset colors
    asset_styles = ASSET_COLORS

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

    for idx, (pair1, pair2, title) in enumerate(MAIN_PAIR_GROUPS):
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

            color, _ = get_pair_style(pair_name)
            label = PAIR_LABELS.get(pair_name, pair_name)

            # Offset: first pair slightly left, second pair slightly right
            offset = -OFFSET if i == 0 else OFFSET

            # PDF: Aggregate by bin_size (no alpha for thin lines)
            bins = np.arange(0, max_x + bin_size, bin_size)
            counts, bin_edges = np.histogram(height_diffs, bins=bins)
            bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
            ax_left.vlines(
                bin_centers + offset, 0, counts,
                colors=color, alpha=0.8, linewidth=1.5, label=label
            )

            # CDF: Line on right Y-axis, ensure it starts from (0, 0)
            sorted_diffs = np.sort(height_diffs)
            cdf = np.arange(1, len(sorted_diffs) + 1) / len(sorted_diffs) * 100
            # Prepend (0, 0) and first data point at y=0 to ensure proper start
            plot_x = np.concatenate([[0, sorted_diffs[0]], sorted_diffs])
            plot_y = np.concatenate([[0, 0], cdf])
            ax_right.plot(
                plot_x, plot_y,
                color=color, linewidth=1.5
            )

        # Left axis (PDF)
        ax_left.set_ylabel("Frequency (per bin)", fontsize=10)
        ax_left.set_xlim(0, max_x)
        # ax_left.set_ylim(bottom=0)

        # Right axis (CDF)
        ax_right.set_ylabel("Coverage % (CDF)", fontsize=10)
        ax_right.set_ylim(0, 100)

        # Add horizontal reference lines for CDF (label only the first one)
        for i, pct in enumerate([90, 95, 99]):
            label_text = "90/95/99% coverage" if i == 0 else None
            ax_right.axhline(y=pct, color="gray", linestyle="--", linewidth=0.5, alpha=0.5, label=label_text)

        ax_left.set_title(title, fontsize=11)
        # No grid to keep vlines clean

        # Combine legends from both axes
        lines1, labels1 = ax_left.get_legend_handles_labels()
        lines2, labels2 = ax_right.get_legend_handles_labels()
        if lines1 or lines2:
            ax_left.legend(lines1 + lines2, labels1 + labels2, loc="upper right", fontsize=9)

    axes[-1].set_xlabel("Height Diff (blocks)", fontsize=11)

    fig.suptitle(f"Height Diff Distribution: Frequency ({bin_size}-block bins) + CDF", fontsize=13)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"Saved: {output_path}")
    plt.close()




def extract_time_diffs(records: list[dict], blockchain_txs: dict, pair_name: str) -> tuple[list, list]:
    """
    Extract time_diff and timestamps from records using blockchain tx data.
    Returns: (timestamps_as_datetime, time_diffs_in_seconds)
    Missing tx hashes will print warnings and be skipped.
    """
    timestamps = []
    time_diffs = []
    missing_txs = set()

    for record in records:
        in_list = record.get('in', [])
        out_list = record.get('out', [])
        if not in_list or not out_list:
            continue

        in_entry = in_list[0]
        out_entry = out_list[0]
        in_chain = in_entry.get('chain', '')
        out_chain = out_entry.get('chain', '')
        in_txid = in_entry.get('txID', '').upper()
        out_txid = out_entry.get('txID', '').upper()

        in_tx = blockchain_txs.get(in_chain, {}).get(in_txid)
        out_tx = blockchain_txs.get(out_chain, {}).get(out_txid)

        if not in_tx:
            missing_txs.add(f"{in_chain}:{in_txid[:16]}...")
            continue
        if not out_tx:
            missing_txs.add(f"{out_chain}:{out_txid[:16]}...")
            continue

        in_ts = get_tx_timestamp(in_tx)
        out_ts = get_tx_timestamp(out_tx)
        if in_ts is None or out_ts is None:
            continue

        time_diff = out_ts - in_ts
        ts_ns = int(record.get("timestamp", 0))
        ts_sec = ts_ns / 1e9
        dt = datetime.fromtimestamp(ts_sec)
        timestamps.append(dt)
        time_diffs.append(time_diff)

    if missing_txs:
        print(f"  [WARN] {pair_name}: Missing {len(missing_txs)} blockchain tx(s):")
        for tx_ref in sorted(missing_txs)[:3]:
            print(f"    - {tx_ref}")
        if len(missing_txs) > 3:
            print(f"    ... and {len(missing_txs) - 3} more")

    return timestamps, time_diffs


def plot_time_diff_vs_timestamp(all_time_data: dict[str, tuple], output_path: Path):
    """Plot time_diff vs timestamp as 3 subplots grouped by reverse pairs."""
    fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)

    for idx, (pair1, pair2, title) in enumerate(MAIN_PAIR_GROUPS):
        ax = axes[idx]
        has_data = False

        for pair_name in [pair1, pair2]:
            key = f"{pair_name}.ndjson"
            if key in all_time_data:
                timestamps, time_diffs = all_time_data[key]
                if len(timestamps) > 0:
                    scatter_pair(ax, timestamps, time_diffs, pair_name)
                    has_data = True

        ax.set_ylabel("Time Diff (seconds)", fontsize=10)
        ax.set_title(title, fontsize=11)
        if has_data:
            ax.legend(loc="upper right", fontsize=9)
        ax.grid(True, alpha=GRID_ALPHA)
        ax.axhline(y=0, color='red', linestyle='--', linewidth=0.8, alpha=0.5)

    axes[-1].set_xlabel("Timestamp", fontsize=11)
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    axes[-1].xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.xticks(rotation=45, ha="right")

    fig.suptitle("Time Difference vs Timestamp (Grouped by Reverse Pairs)", fontsize=13)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"Saved: {output_path}")
    plt.close()


def plot_time_diff_cdf(all_time_data: dict[str, tuple], output_path: Path, max_x: int = 2000, bin_size: int = 30):
    """Plot PDF (binned count) + CDF of time diff with dual Y-axes."""
    fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)
    OFFSET = bin_size * 0.15

    # First pass: collect all data to determine global x-axis range
    all_min_vals = []
    all_max_vals = []
    for pair1, pair2, _ in MAIN_PAIR_GROUPS:
        for pair_name in [pair1, pair2]:
            key = f"{pair_name}.ndjson"
            if key in all_time_data:
                timestamps, time_diffs = all_time_data[key]
                if len(time_diffs) > 0:
                    time_diffs_arr = np.array([t for t in time_diffs if -max_x <= t <= max_x])
                    if len(time_diffs_arr) > 0:
                        all_min_vals.append(np.min(time_diffs_arr))
                        all_max_vals.append(np.max(time_diffs_arr))

    # Determine reasonable x-axis range with some padding
    if all_min_vals and all_max_vals:
        global_min = min(all_min_vals)
        global_max = max(all_max_vals)
        # Add 5% padding on each side
        padding = (global_max - global_min) * 0.05
        xlim_min = global_min - padding
        xlim_max = global_max + padding
    else:
        xlim_min, xlim_max = -max_x, max_x

    for idx, (pair1, pair2, title) in enumerate(MAIN_PAIR_GROUPS):
        ax_left = axes[idx]
        ax_right = ax_left.twinx()

        for i, pair_name in enumerate([pair1, pair2]):
            key = f"{pair_name}.ndjson"
            if key not in all_time_data:
                continue

            timestamps, time_diffs = all_time_data[key]
            if len(time_diffs) == 0:
                continue

            time_diffs_arr = np.array([t for t in time_diffs if -max_x <= t <= max_x])
            if len(time_diffs_arr) == 0:
                continue

            color, _ = get_pair_style(pair_name)
            label = PAIR_LABELS.get(pair_name, pair_name)
            offset = -OFFSET if i == 0 else OFFSET

            bins = np.arange(xlim_min, xlim_max + bin_size, bin_size)
            counts, bin_edges = np.histogram(time_diffs_arr, bins=bins)
            bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
            ax_left.vlines(bin_centers + offset, 0, counts, colors=color, alpha=0.8, linewidth=1.5, label=label)

            sorted_diffs = np.sort(time_diffs_arr)
            cdf = np.arange(1, len(sorted_diffs) + 1) / len(sorted_diffs) * 100
            plot_x = np.concatenate([[sorted_diffs[0]], sorted_diffs])
            plot_y = np.concatenate([[0], cdf])
            ax_right.plot(plot_x, plot_y, color=color, linewidth=1.5)

        ax_left.set_ylabel("Frequency (per bin)", fontsize=10)
        ax_left.set_xlim(xlim_min, xlim_max)
        ax_left.axvline(x=0, color='red', linestyle='--', linewidth=0.8, alpha=0.5, label="Zero line")
        ax_right.set_ylabel("Coverage % (CDF)", fontsize=10)
        ax_right.set_ylim(0, 100)

        for i, pct in enumerate([90, 95, 99]):
            label_text = "90/95/99% coverage" if i == 0 else None
            ax_right.axhline(y=pct, color="gray", linestyle="--", linewidth=0.5, alpha=0.5, label=label_text)

        ax_left.set_title(title, fontsize=11)

        # Combine legends from both axes
        lines1, labels1 = ax_left.get_legend_handles_labels()
        lines2, labels2 = ax_right.get_legend_handles_labels()
        if lines1 or lines2:
            ax_left.legend(lines1 + lines2, labels1 + labels2, loc="upper right", fontsize=9)

    axes[-1].set_xlabel("Time Diff (seconds)", fontsize=11)
    fig.suptitle(f"Time Diff Distribution: Frequency ({bin_size}s bins) + CDF", fontsize=13)
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

    # Check if blockchain_txs directory exists
    has_blockchain_txs = BLOCKCHAIN_TXS_DIR.exists()
    if has_blockchain_txs:
        print(f"Found blockchain_txs directory, loading transaction data...")
        blockchain_txs = load_blockchain_txs(BLOCKCHAIN_TXS_DIR, ['BTC', 'ETH', 'DOGE', 'LTC'])
        for chain, txs in blockchain_txs.items():
            print(f"  Loaded {len(txs)} {chain} transactions")

        if blockchain_txs:
            print(f"\nExtracting time_diff data...")
            all_time_data = {}
            for filepath in ndjson_files:
                pair_name = filepath.stem
                # Process all pairs that have blockchain data
                if any(chain in pair_name for chain in ['BTC', 'ETH', 'DOGE', 'LTC']):
                    print(f"  Processing {pair_name}...")
                    records = load_ndjson(filepath)
                    timestamps, time_diffs = extract_time_diffs(records, blockchain_txs, pair_name)
                    if len(timestamps) > 0:
                        all_time_data[filepath.name] = (timestamps, time_diffs)
                        print(f"    -> {len(time_diffs)} time_diff values extracted")
            print()
        else:
            print(f"  [WARN] No blockchain tx files found, skipping time_diff plots")
            all_time_data = None
    else:
        print(f"blockchain_txs directory not found, skipping time_diff plots\n")
        all_time_data = None

    # Generate plots for MAIN pairs
    print("Generating plots for main pairs...")
    plot_daily_tx_count(all_data, OUTPUT_DIR / "daily_tx_count.png")
    plot_daily_amount(all_data, OUTPUT_DIR / "daily_amount.png")
    plot_height_diff_cdf(all_data, OUTPUT_DIR / "height_diff_cdf.png")
    plot_height_diff_vs_timestamp(all_data, OUTPUT_DIR / "height_diff_vs_timestamp.png")
    plot_amount_vs_timestamp(all_data, OUTPUT_DIR / "amount_vs_timestamp.png")

    # Generate plots for LTC pairs
    print("\nGenerating plots for LTC pairs...")
    # Create helper functions that use LTC_PAIR_GROUPS
    def plot_daily_tx_count_ltc(all_data, output_path):
        fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)
        for idx, (pair1, pair2, title) in enumerate(LTC_PAIR_GROUPS):
            ax = axes[idx]
            key1 = f"{pair1}.ndjson"
            if key1 in all_data:
                timestamps, in_amounts, _, _ = all_data[key1]
                dates, tx_counts, _ = aggregate_daily(timestamps, in_amounts)
                color, _ = get_pair_style(pair1)
                ax.bar(dates, tx_counts, label=PAIR_LABELS.get(pair1), alpha=0.7, color=color, width=0.4)
            key2 = f"{pair2}.ndjson"
            if key2 in all_data:
                timestamps, in_amounts, _, _ = all_data[key2]
                dates, tx_counts, _ = aggregate_daily(timestamps, in_amounts)
                color, _ = get_pair_style(pair2)
                dates_offset = [datetime.fromtimestamp(d.timestamp() + 0.4 * 86400) for d in dates]
                ax.bar(dates_offset, tx_counts, label=PAIR_LABELS.get(pair2), alpha=0.7, color=color, width=0.4)
            ax.set_ylabel("TX Count", fontsize=10)
            ax.set_title(title, fontsize=11)
            ax.legend(loc="upper right", fontsize=9)
            ax.grid(True, alpha=GRID_ALPHA)
        axes[-1].set_xlabel("Date", fontsize=11)
        axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        plt.xticks(rotation=45, ha="right")
        fig.suptitle("Daily Transaction Count - LTC Pairs (Grouped by Reverse Pairs)", fontsize=13)
        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved: {output_path}")
        plt.close()

    def plot_daily_amount_ltc(all_data, output_path):
        fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)
        for idx, (pair1, pair2, title) in enumerate(LTC_PAIR_GROUPS):
            ax = axes[idx]
            key1 = f"{pair1}.ndjson"
            if key1 in all_data:
                timestamps, in_amounts, _, _ = all_data[key1]
                dates, _, total_amounts = aggregate_daily(timestamps, in_amounts)
                color, _ = get_pair_style(pair1)
                ax.bar(dates, total_amounts, label=PAIR_LABELS.get(pair1), alpha=0.7, color=color, width=0.4)
            key2 = f"{pair2}.ndjson"
            if key2 in all_data:
                timestamps, in_amounts, _, _ = all_data[key2]
                dates, _, total_amounts = aggregate_daily(timestamps, in_amounts)
                color, _ = get_pair_style(pair2)
                dates_offset = [datetime.fromtimestamp(d.timestamp() + 0.4 * 86400) for d in dates]
                ax.bar(dates_offset, total_amounts, label=PAIR_LABELS.get(pair2), alpha=0.7, color=color, width=0.4)
            ax.set_ylabel("Total Amount", fontsize=10)
            ax.set_title(title, fontsize=11)
            ax.set_yscale("log")
            ax.legend(loc="upper right", fontsize=9)
            ax.grid(True, alpha=GRID_ALPHA)
        axes[-1].set_xlabel("Date", fontsize=11)
        axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        plt.xticks(rotation=45, ha="right")
        fig.suptitle("Daily Cumulative Amount - LTC Pairs (Grouped by Reverse Pairs)", fontsize=13)
        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved: {output_path}")
        plt.close()

    def plot_height_diff_cdf_ltc(all_data, output_path):
        fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)
        OFFSET = 5 * 0.15
        for idx, (pair1, pair2, title) in enumerate(LTC_PAIR_GROUPS):
            ax_left = axes[idx]
            ax_right = ax_left.twinx()
            for i, pair_name in enumerate([pair1, pair2]):
                key = f"{pair_name}.ndjson"
                if key not in all_data:
                    continue
                _, _, _, height_diffs = all_data[key]
                height_diffs = np.array([h for h in height_diffs if 0 < h <= 1000])
                if len(height_diffs) == 0:
                    continue
                color, _ = get_pair_style(pair_name)
                offset = -OFFSET if i == 0 else OFFSET
                bins = np.arange(0, 1005, 5)
                counts, bin_edges = np.histogram(height_diffs, bins=bins)
                bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
                ax_left.vlines(bin_centers + offset, 0, counts, colors=color, alpha=0.8, linewidth=1.5, label=PAIR_LABELS.get(pair_name))
                sorted_diffs = np.sort(height_diffs)
                cdf = np.arange(1, len(sorted_diffs) + 1) / len(sorted_diffs) * 100
                ax_right.plot(np.concatenate([[0, sorted_diffs[0]], sorted_diffs]), np.concatenate([[0, 0], cdf]), color=color, linewidth=1.5)
            ax_left.set_ylabel("Frequency (per bin)", fontsize=10)
            ax_left.set_xlim(0, 1000)
            ax_right.set_ylabel("Coverage % (CDF)", fontsize=10)
            ax_right.set_ylim(0, 100)
            for i, pct in enumerate([90, 95, 99]):
                label_text = "90/95/99% coverage" if i == 0 else None
                ax_right.axhline(y=pct, color="gray", linestyle="--", linewidth=0.5, alpha=0.5, label=label_text)
            ax_left.set_title(title, fontsize=11)
            lines1, labels1 = ax_left.get_legend_handles_labels()
            lines2, labels2 = ax_right.get_legend_handles_labels()
            if lines1 or lines2:
                ax_left.legend(lines1 + lines2, labels1 + labels2, loc="upper right", fontsize=9)
        axes[-1].set_xlabel("Height Diff (blocks)", fontsize=11)
        fig.suptitle("Height Diff Distribution - LTC Pairs: Frequency (5-block bins) + CDF", fontsize=13)
        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved: {output_path}")
        plt.close()

    def plot_height_diff_vs_timestamp_ltc(all_data, output_path):
        fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)
        for idx, (pair1, pair2, title) in enumerate(LTC_PAIR_GROUPS):
            ax = axes[idx]
            for pair_name in [pair1, pair2]:
                key = f"{pair_name}.ndjson"
                if key in all_data:
                    timestamps, _, _, height_diffs = all_data[key]
                    scatter_pair(ax, timestamps, height_diffs, pair_name)
            ax.set_ylabel("Height Diff", fontsize=10)
            ax.set_title(title, fontsize=11)
            ax.legend(loc="upper right", fontsize=9)
            ax.grid(True, alpha=GRID_ALPHA)
        axes[-1].set_xlabel("Timestamp", fontsize=11)
        axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        plt.xticks(rotation=45, ha="right")
        fig.suptitle("Block Height Difference vs Timestamp - LTC Pairs (Grouped by Reverse Pairs)", fontsize=13)
        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved: {output_path}")
        plt.close()

    plot_daily_tx_count_ltc(all_data, OUTPUT_DIR / "daily_tx_count_ltc.png")
    plot_daily_amount_ltc(all_data, OUTPUT_DIR / "daily_amount_ltc.png")
    plot_height_diff_cdf_ltc(all_data, OUTPUT_DIR / "height_diff_cdf_ltc.png")
    plot_height_diff_vs_timestamp_ltc(all_data, OUTPUT_DIR / "height_diff_vs_timestamp_ltc.png")

    # Generate separate amount distribution plots for each pair group
    print("\nGenerating amount distribution plots...")
    plot_amount_distribution_cdf(all_data, OUTPUT_DIR)

    # Generate time_diff plots if data is available
    if all_time_data:
        print("\nGenerating time_diff plots...")
        # Main pairs (excluding LTC)
        main_time_data = {k: v for k, v in all_time_data.items() if 'LTC' not in k}
        if main_time_data:
            plot_time_diff_vs_timestamp(main_time_data, OUTPUT_DIR / "time_diff_vs_timestamp.png")
            plot_time_diff_cdf(main_time_data, OUTPUT_DIR / "time_diff_cdf.png")

        # LTC pairs
        ltc_time_data = {k: v for k, v in all_time_data.items() if 'LTC' in k}
        if ltc_time_data:
            # Similar helper functions for LTC time_diff plots
            def plot_time_diff_vs_timestamp_ltc(data, output_path):
                fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)
                for idx, (pair1, pair2, title) in enumerate(LTC_PAIR_GROUPS):
                    ax = axes[idx]
                    for pair_name in [pair1, pair2]:
                        key = f"{pair_name}.ndjson"
                        if key in data:
                            timestamps, time_diffs = data[key]
                            if len(timestamps) > 0:
                                scatter_pair(ax, timestamps, time_diffs, pair_name)
                    ax.set_ylabel("Time Diff (seconds)", fontsize=10)
                    ax.set_title(title, fontsize=11)
                    ax.legend(loc="upper right", fontsize=9)
                    ax.grid(True, alpha=GRID_ALPHA)
                    ax.axhline(y=0, color='red', linestyle='--', linewidth=0.8, alpha=0.5)
                axes[-1].set_xlabel("Timestamp", fontsize=11)
                axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
                plt.xticks(rotation=45, ha="right")
                fig.suptitle("Time Difference vs Timestamp - LTC Pairs (Grouped by Reverse Pairs)", fontsize=13)
                plt.tight_layout()
                plt.savefig(output_path, dpi=150, bbox_inches="tight")
                print(f"Saved: {output_path}")
                plt.close()

            def plot_time_diff_cdf_ltc(data, output_path):
                fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)
                OFFSET = 30 * 0.15
                all_min, all_max = [], []
                for pair1, pair2, _ in LTC_PAIR_GROUPS:
                    for pair_name in [pair1, pair2]:
                        key = f"{pair_name}.ndjson"
                        if key in data:
                            _, time_diffs = data[key]
                            if len(time_diffs) > 0:
                                arr = np.array([t for t in time_diffs if -2000 <= t <= 2000])
                                if len(arr) > 0:
                                    all_min.append(np.min(arr))
                                    all_max.append(np.max(arr))
                if all_min and all_max:
                    padding = (max(all_max) - min(all_min)) * 0.05
                    xlim_min, xlim_max = min(all_min) - padding, max(all_max) + padding
                else:
                    xlim_min, xlim_max = -2000, 2000
                for idx, (pair1, pair2, title) in enumerate(LTC_PAIR_GROUPS):
                    ax_left = axes[idx]
                    ax_right = ax_left.twinx()
                    for i, pair_name in enumerate([pair1, pair2]):
                        key = f"{pair_name}.ndjson"
                        if key not in data:
                            continue
                        _, time_diffs = data[key]
                        if len(time_diffs) == 0:
                            continue
                        arr = np.array([t for t in time_diffs if xlim_min <= t <= xlim_max])
                        if len(arr) == 0:
                            continue
                        color, _ = get_pair_style(pair_name)
                        offset = -OFFSET if i == 0 else OFFSET
                        bins = np.arange(xlim_min, xlim_max + 30, 30)
                        counts, bin_edges = np.histogram(arr, bins=bins)
                        bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
                        ax_left.vlines(bin_centers + offset, 0, counts, colors=color, alpha=0.8, linewidth=1.5, label=PAIR_LABELS.get(pair_name))
                        sorted_diffs = np.sort(arr)
                        cdf = np.arange(1, len(sorted_diffs) + 1) / len(sorted_diffs) * 100
                        ax_right.plot(np.concatenate([[sorted_diffs[0]], sorted_diffs]), np.concatenate([[0], cdf]), color=color, linewidth=1.5)
                    ax_left.set_ylabel("Frequency (per bin)", fontsize=10)
                    ax_left.set_xlim(xlim_min, xlim_max)
                    ax_left.axvline(x=0, color='red', linestyle='--', linewidth=0.8, alpha=0.5, label="Zero line")
                    ax_right.set_ylabel("Coverage % (CDF)", fontsize=10)
                    ax_right.set_ylim(0, 100)
                    for i, pct in enumerate([90, 95, 99]):
                        label_text = "90/95/99% coverage" if i == 0 else None
                        ax_right.axhline(y=pct, color="gray", linestyle="--", linewidth=0.5, alpha=0.5, label=label_text)
                    ax_left.set_title(title, fontsize=11)
                    lines1, labels1 = ax_left.get_legend_handles_labels()
                    lines2, labels2 = ax_right.get_legend_handles_labels()
                    if lines1 or lines2:
                        ax_left.legend(lines1 + lines2, labels1 + labels2, loc="upper right", fontsize=9)
                axes[-1].set_xlabel("Time Diff (seconds)", fontsize=11)
                fig.suptitle("Time Diff Distribution - LTC Pairs: Frequency (30s bins) + CDF", fontsize=13)
                plt.tight_layout()
                plt.savefig(output_path, dpi=150, bbox_inches="tight")
                print(f"Saved: {output_path}")
                plt.close()

            plot_time_diff_vs_timestamp_ltc(ltc_time_data, OUTPUT_DIR / "time_diff_vs_timestamp_ltc.png")
            plot_time_diff_cdf_ltc(ltc_time_data, OUTPUT_DIR / "time_diff_cdf_ltc.png")

    print("\nDone!")


if __name__ == "__main__":
    main()
