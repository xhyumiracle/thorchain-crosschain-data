#!/usr/bin/env python3
"""
Simple script to check min amounts in thorchain-2025:
1. Min amount without any filter
2. Min amount with time_diff <= 30min filter

No comparison to HF dataset, just pure statistics.
"""

import json
import sys
from pathlib import Path

# Import shared utilities
sys.path.insert(0, str(Path(__file__).parent.parent))
from script.utils.blockchain import load_blockchain_txs, get_tx_timestamp

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


def analyze_pair(pair_file: Path, blockchain_txs: dict):
    """Analyze min amounts for a pair."""
    in_asset, out_asset = pair_file.stem.split('-')

    # Skip LTC pairs
    if in_asset == "LTC" or out_asset == "LTC":
        return None

    all_amounts = []
    time_filtered_amounts = []

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

            all_amounts.append(amount)

            # Get time diff
            time_diff_sec = get_time_diff(record, blockchain_txs)
            if time_diff_sec is not None and time_diff_sec <= 30 * 60:
                time_filtered_amounts.append(amount)

    if not all_amounts:
        return None

    all_amounts.sort()
    time_filtered_amounts.sort()

    return {
        'pair': pair_file.stem,
        'in_asset': in_asset,
        'total_count': len(all_amounts),
        'min_all': all_amounts[0],
        'max_all': all_amounts[-1],
        'time_filtered_count': len(time_filtered_amounts),
        'min_time_filtered': time_filtered_amounts[0] if time_filtered_amounts else None,
        'max_time_filtered': time_filtered_amounts[-1] if time_filtered_amounts else None,
    }


def main():
    print(f"\n{'='*70}")
    print("Simple Min Amount Analysis - thorchain-2025 Only")
    print(f"{'='*70}\n")

    # Load blockchain txs
    print("Loading blockchain data...")
    blockchain_txs = load_blockchain_txs(BLOCKCHAIN_TXS_DIR, ["BTC", "ETH", "DOGE"])
    print()

    # Process each pair
    pair_files = sorted(SOURCE_DIR.glob("*.jsonl"))

    results = []
    for pair_file in pair_files:
        result = analyze_pair(pair_file, blockchain_txs)
        if result:
            results.append(result)

    # Print results
    print(f"{'Pair':<15} {'Total':<8} {'Min (All)':<15} {'With ≤30min':<12} {'Min (≤30min)':<15}")
    print(f"{'-'*70}")

    for r in results:
        min_all_str = f"{r['min_all']/1e8:.4f} {r['in_asset']}"
        min_filtered_str = f"{r['min_time_filtered']/1e8:.4f} {r['in_asset']}" if r['min_time_filtered'] else "N/A"

        print(f"{r['pair']:<15} {r['total_count']:<8} {min_all_str:<15} "
              f"{r['time_filtered_count']:<12} {min_filtered_str:<15}")

    print(f"\n{'='*70}\n")


if __name__ == "__main__":
    main()
