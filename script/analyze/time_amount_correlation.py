#!/usr/bin/env python3
"""
Analyze and plot the relationship between time_delta and amount for each pair.

This helps understand why fast transactions (≤30min) tend to have larger amounts.
"""

import json
import sys
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np

# Import shared utilities
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from script.utils.blockchain import load_blockchain_txs, get_tx_timestamp

SOURCE_DIR = Path(__file__).parent.parent.parent / "data" / "thorchain-2025"
BLOCKCHAIN_TXS_DIR = Path(__file__).parent.parent.parent / "blockchain_txs"
OUTPUT_DIR = Path(__file__).parent.parent.parent / "png"

# HF thresholds for reference lines
AMOUNT_THRESHOLDS = {
    "BTC": 10_000_000,   # 0.1 BTC
    "ETH": 200_000_000,  # 2.0 ETH
    "DOGE": 100_000_000_000,  # 1000 DOGE
}

TIME_THRESHOLD_MIN = 30


def get_amount(record: dict) -> tuple[str | None, int]:
    """Get asset and amount from record's first input."""
    in_list = record.get("in", [])
    if not in_list:
        return None, 0

    entry = in_list[0]
    asset = entry.get('asset', '')
    amount = int(entry.get('amount', 0))
    return asset, amount


def get_time_diff(record: dict, blockchain_txs: dict[str, dict]) -> int | None:
    """Calculate time diff in seconds."""
    in_list = record.get("in", [])
    out_list = record.get("out", [])

    if not in_list or not out_list:
        return None

    in_entry = in_list[0]
    out_entry = out_list[0]

    in_txid = in_entry.get('txID')
    in_asset = in_entry.get('asset')
    out_txid = out_entry.get('txID')
    out_asset = out_entry.get('asset')

    if not in_txid or not in_asset or not out_txid or not out_asset:
        return None

    in_chain_txs = blockchain_txs.get(in_asset, {})
    out_chain_txs = blockchain_txs.get(out_asset, {})

    in_tx_data = in_chain_txs.get(in_txid)
    out_tx_data = out_chain_txs.get(out_txid)

    if not in_tx_data or not out_tx_data:
        return None

    in_time = get_tx_timestamp(in_tx_data)
    out_time = get_tx_timestamp(out_tx_data)

    if in_time is None or out_time is None:
        return None

    return out_time - in_time


def collect_data(pair_file: Path, blockchain_txs: dict) -> dict:
    """Collect time_delta and amount data for a pair."""
    in_asset, out_asset = pair_file.stem.split('-')

    # Skip LTC pairs
    if in_asset == "LTC" or out_asset == "LTC":
        return None

    data_points = []

    with open(pair_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except:
                continue

            # Get amount
            asset, amount = get_amount(record)
            if asset != in_asset:
                continue

            # Get time diff
            time_diff_sec = get_time_diff(record, blockchain_txs)
            if time_diff_sec is None:
                continue

            data_points.append({
                'time_min': time_diff_sec / 60,
                'amount': amount / 1e8,  # Convert to human-readable
            })

    if not data_points:
        return None

    return {
        'pair': pair_file.stem,
        'in_asset': in_asset,
        'out_asset': out_asset,
        'data': data_points,
    }


def plot_pair(result: dict, output_dir: Path):
    """Plot time vs amount for a single pair."""
    pair = result['pair']
    in_asset = result['in_asset']
    data = result['data']

    # Extract x, y
    time_values = [d['time_min'] for d in data]
    amount_values = [d['amount'] for d in data]

    # Get threshold
    amount_threshold = AMOUNT_THRESHOLDS.get(in_asset, 0) / 1e8

    # Create figure
    fig, ax = plt.subplots(figsize=(10, 6))

    # Scatter plot with alpha for density
    ax.scatter(time_values, amount_values, alpha=0.3, s=10, c='blue', edgecolors='none')

    # Add threshold lines
    ax.axhline(y=amount_threshold, color='red', linestyle='--', linewidth=2,
               label=f'Amount threshold (HF): {amount_threshold:.2f} {in_asset}')
    ax.axvline(x=TIME_THRESHOLD_MIN, color='orange', linestyle='--', linewidth=2,
               label=f'Time threshold (HF): {TIME_THRESHOLD_MIN} min')

    # Add shaded HF region
    ax.axhspan(amount_threshold, ax.get_ylim()[1], alpha=0.1, color='red')
    ax.axvspan(0, TIME_THRESHOLD_MIN, alpha=0.1, color='orange')

    # Labels and title
    ax.set_xlabel('Time Delta (minutes)', fontsize=12)
    ax.set_ylabel(f'Amount ({in_asset})', fontsize=12)
    ax.set_title(f'{pair}: Time Delta vs Amount\n(Blue dots: all records, shaded: HF region)',
                 fontsize=14, fontweight='bold')

    # Log scale for better visualization
    ax.set_yscale('log')
    ax.set_xscale('log')

    # Grid
    ax.grid(True, alpha=0.3, which='both')

    # Legend
    ax.legend(loc='upper right', fontsize=10)

    # Statistics text
    total_count = len(data)
    hf_count = sum(1 for d in data if d['time_min'] <= TIME_THRESHOLD_MIN and d['amount'] >= amount_threshold)
    stats_text = f'Total: {total_count:,}\nHF (both filters): {hf_count:,} ({hf_count/total_count*100:.1f}%)'
    ax.text(0.02, 0.98, stats_text, transform=ax.transAxes,
            verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
            fontsize=10)

    plt.tight_layout()

    # Save
    output_file = output_dir / f'time_amount_{pair}.png'
    plt.savefig(output_file, dpi=150)
    plt.close()

    print(f"  ✓ {pair}: {total_count:,} records -> {output_file.name}")


def plot_combined(results: list, output_dir: Path):
    """Plot all pairs in a 2x3 grid."""
    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    axes = axes.flatten()

    for idx, result in enumerate(results):
        ax = axes[idx]
        pair = result['pair']
        in_asset = result['in_asset']
        data = result['data']

        # Extract x, y
        time_values = [d['time_min'] for d in data]
        amount_values = [d['amount'] for d in data]

        # Get threshold
        amount_threshold = AMOUNT_THRESHOLDS.get(in_asset, 0) / 1e8

        # Scatter plot
        ax.scatter(time_values, amount_values, alpha=0.3, s=5, c='blue', edgecolors='none')

        # Add threshold lines
        ax.axhline(y=amount_threshold, color='red', linestyle='--', linewidth=1.5)
        ax.axvline(x=TIME_THRESHOLD_MIN, color='orange', linestyle='--', linewidth=1.5)

        # Shaded regions
        ax.axhspan(amount_threshold, max(amount_values)*1.5, alpha=0.1, color='red')
        ax.axvspan(0, TIME_THRESHOLD_MIN, alpha=0.1, color='orange')

        # Labels
        ax.set_xlabel('Time (min)', fontsize=10)
        ax.set_ylabel(f'Amount ({in_asset})', fontsize=10)
        ax.set_title(pair, fontsize=12, fontweight='bold')

        # Log scale
        ax.set_yscale('log')
        ax.set_xscale('log')
        ax.grid(True, alpha=0.3, which='both')

        # Stats
        total_count = len(data)
        hf_count = sum(1 for d in data if d['time_min'] <= TIME_THRESHOLD_MIN and d['amount'] >= amount_threshold)
        ax.text(0.98, 0.02, f'{hf_count:,}/{total_count:,}\n({hf_count/total_count*100:.0f}%)',
                transform=ax.transAxes, ha='right', va='bottom',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.7),
                fontsize=8)

    plt.suptitle('Time Delta vs Amount - All Pairs (HF thresholds shown)',
                 fontsize=16, fontweight='bold', y=0.995)
    plt.tight_layout()

    # Save
    output_file = output_dir / 'time_amount_all_pairs.png'
    plt.savefig(output_file, dpi=150)
    plt.close()

    print(f"\n  ✓ Combined plot -> {output_file.name}")


def main():
    print(f"\n{'='*70}")
    print("Time Delta vs Amount Correlation Analysis")
    print(f"{'='*70}\n")

    # Load blockchain txs
    print("Loading blockchain data...")
    blockchain_txs = load_blockchain_txs(BLOCKCHAIN_TXS_DIR, ["BTC", "ETH", "DOGE"])
    print()

    # Collect data
    print("Collecting data...")
    pair_files = sorted(SOURCE_DIR.glob("*.jsonl"))
    results = []

    for pair_file in pair_files:
        result = collect_data(pair_file, blockchain_txs)
        if result:
            results.append(result)
            print(f"  {result['pair']}: {len(result['data']):,} records")

    if not results:
        print("No data collected!")
        return

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Plot individual pairs
    print(f"\nGenerating individual plots...")
    for result in results:
        plot_pair(result, OUTPUT_DIR)

    # Plot combined
    print(f"\nGenerating combined plot...")
    plot_combined(results, OUTPUT_DIR)

    print(f"\n{'='*70}")
    print(f"✓ All plots saved to: {OUTPUT_DIR}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
