#!/usr/bin/env python3
"""
Analyze amount distribution for records passing 30min time filter.

This script checks the min/max amounts in records that pass the time<=30min filter,
to see if amount threshold (level 10) has any filtering effect.
"""

import json
import sys
from pathlib import Path

# Import shared utilities
sys.path.insert(0, str(Path(__file__).parent.parent))
from script.utils.blockchain import load_blockchain_txs, get_tx_timestamp

TIME_THRESHOLD_MIN = 30

# Current thresholds (level 10)
AMOUNT_THRESHOLDS = {
    "BTC": 10_000_000,   # 0.1 BTC
    "ETH": 200_000_000,  # 2.0 ETH
    "DOGE": 100_000_000_000,  # 1000 DOGE
}

SOURCE_DIR = Path(__file__).parent.parent / "data" / "thorchain-2025"
BLOCKCHAIN_TXS_DIR = Path(__file__).parent.parent / "blockchain_txs"


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
    """Calculate time diff in seconds using real blockchain timestamps."""
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

    # Get blockchain tx data
    in_chain_txs = blockchain_txs.get(in_asset, {})
    out_chain_txs = blockchain_txs.get(out_asset, {})

    in_tx_data = in_chain_txs.get(in_txid)
    out_tx_data = out_chain_txs.get(out_txid)

    if not in_tx_data or not out_tx_data:
        return None

    # Get timestamps
    in_time = get_tx_timestamp(in_tx_data)
    out_time = get_tx_timestamp(out_tx_data)

    if in_time is None or out_time is None:
        return None

    return out_time - in_time


def analyze_pair(pair_file: Path, blockchain_txs: dict) -> dict:
    """
    Analyze amount distribution for records passing 30min time filter.

    Returns:
        dict with amount statistics
    """
    # Parse pair name
    in_asset, out_asset = pair_file.stem.split('-')

    # Skip LTC pairs (no time data)
    if in_asset == "LTC" or out_asset == "LTC":
        return None

    # Get amount threshold
    amount_threshold = AMOUNT_THRESHOLDS.get(in_asset, 0)

    # Collect amounts for records passing time filter
    amounts_time_filtered = []

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

            # Check time filter
            if time_diff_sec <= TIME_THRESHOLD_MIN * 60:
                amounts_time_filtered.append(amount)

    if not amounts_time_filtered:
        return None

    # Calculate statistics
    amounts_time_filtered.sort()

    min_amount = amounts_time_filtered[0]
    max_amount = amounts_time_filtered[-1]
    median_amount = amounts_time_filtered[len(amounts_time_filtered) // 2]

    # Count records below threshold
    below_threshold = sum(1 for a in amounts_time_filtered if a < amount_threshold)

    return {
        'pair': pair_file.stem,
        'in_asset': in_asset,
        'count': len(amounts_time_filtered),
        'threshold': amount_threshold,
        'min': min_amount,
        'max': max_amount,
        'median': median_amount,
        'below_threshold': below_threshold,
    }


def main():
    print(f"\n{'='*70}")
    print("Amount Distribution Analysis for Time≤30min Records")
    print(f"{'='*70}\n")

    # Load blockchain txs
    print("Loading blockchain transaction data...")
    blockchain_txs = load_blockchain_txs(BLOCKCHAIN_TXS_DIR, ["BTC", "ETH", "DOGE"])

    for asset in ["BTC", "ETH", "DOGE"]:
        if asset in blockchain_txs:
            print(f"  {asset}: {len(blockchain_txs[asset]):,} transactions ✓")

    print(f"\nAnalyzing records with time_diff <= {TIME_THRESHOLD_MIN} min...")
    print(f"{'='*70}\n")

    # Find all pair files
    pair_files = sorted(SOURCE_DIR.glob("*.ndjson"))

    results = []

    for pair_file in pair_files:
        result = analyze_pair(pair_file, blockchain_txs)
        if result is not None:
            results.append(result)

    # Print results table
    print(f"{'Pair':<15} {'Count':<8} {'Threshold':<12} {'Min':<12} {'Max':<12} {'Below':<8}")
    print(f"{'-'*70}")

    for r in results:
        # Format amounts in human-readable form
        threshold_str = f"{r['threshold']/1e8:.2f}"
        min_str = f"{r['min']/1e8:.4f}"
        max_str = f"{r['max']/1e8:.2f}"

        print(f"{r['pair']:<15} {r['count']:<8} {threshold_str:<12} {min_str:<12} {max_str:<12} {r['below_threshold']:<8}")

    # Detailed analysis
    print(f"\n{'='*70}")
    print("DETAILED ANALYSIS")
    print(f"{'='*70}\n")

    for r in results:
        print(f"{r['pair']} ({r['in_asset']}):")
        print(f"  Records with time≤30min: {r['count']}")
        print(f"  Amount threshold: {r['threshold']/1e8:.2f} {r['in_asset']}")
        print(f"  Amount range: {r['min']/1e8:.4f} ~ {r['max']/1e8:.2f} {r['in_asset']}")
        print(f"  Median: {r['median']/1e8:.4f} {r['in_asset']}")
        print(f"  Records below threshold: {r['below_threshold']} ({r['below_threshold']/r['count']*100:.1f}%)")

        if r['below_threshold'] == 0:
            print(f"  → ✓ ALL records pass amount filter (threshold has NO effect)")
        else:
            print(f"  → ⚠ {r['below_threshold']} records filtered out by amount threshold")

        print()

    # Summary
    print(f"{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}\n")

    no_effect_pairs = [r for r in results if r['below_threshold'] == 0]
    has_effect_pairs = [r for r in results if r['below_threshold'] > 0]

    print(f"Pairs where amount threshold (level 10) has NO filtering effect:")
    print(f"  Count: {len(no_effect_pairs)}/{len(results)}\n")

    if no_effect_pairs:
        for r in no_effect_pairs:
            print(f"  ✓ {r['pair']}: min={r['min']/1e8:.4f} >= threshold={r['threshold']/1e8:.2f}")

    print(f"\nPairs where amount threshold filters out records:")
    print(f"  Count: {len(has_effect_pairs)}/{len(results)}\n")

    if has_effect_pairs:
        for r in has_effect_pairs:
            print(f"  ⚠ {r['pair']}: {r['below_threshold']} records below threshold "
                  f"(min={r['min']/1e8:.4f} < {r['threshold']/1e8:.2f})")

    print(f"\n{'='*70}")
    print("✓ Analysis complete")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
