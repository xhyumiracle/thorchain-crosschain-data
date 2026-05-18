#!/usr/bin/env python3
"""
Find optimal time thresholds for all trading pairs
to reach exactly 100 records while maintaining amount >= level 10.

Automatically scans all *.ndjson files in the data directory.

This script analyzes data WITHOUT modifying source files.
"""

import json
import sys
from pathlib import Path

# Import shared utilities
sys.path.insert(0, str(Path(__file__).parent.parent))
from script.utils.blockchain import load_blockchain_txs, get_tx_timestamp

# Current thresholds (level 10)
AMOUNT_THRESHOLDS = {
    "BTC": 10_000_000,   # 0.1 BTC
    "ETH": 200_000_000,  # 2.0 ETH
    "DOGE": 100_000_000_000,  # 1000 DOGE
    "LTC": 400_000_000,  # 4.0 LTC
}

CURRENT_TIME_THRESHOLD_MIN = 30

DATA_DIR = Path(__file__).parent.parent / "data" / "thorchain-2025"
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


def analyze_pair(pair_file: Path, blockchain_txs: dict, target_count: int = 100):
    """
    Analyze a pair file to find optimal time threshold.

    Returns:
        dict with analysis results
    """
    print(f"\n{'='*70}")
    print(f"Analyzing {pair_file.name}")
    print(f"{'='*70}\n")

    # Parse pair name (e.g., ETH-DOGE -> in_asset=ETH, out_asset=DOGE)
    in_asset, out_asset = pair_file.stem.split('-')

    # Current amount threshold for this pair's in_asset
    amount_threshold = AMOUNT_THRESHOLDS.get(in_asset, 0)

    print(f"Amount filter: {in_asset} >= {amount_threshold/1e8:.2f}")
    print(f"Current time threshold: <= {CURRENT_TIME_THRESHOLD_MIN} min")
    print(f"Target: {target_count} records\n")

    # Collect all records with their metrics
    records_with_metrics = []

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

            # Apply amount filter
            if amount < amount_threshold:
                continue

            # Get time diff
            time_diff_sec = get_time_diff(record, blockchain_txs)
            if time_diff_sec is None:
                continue

            records_with_metrics.append({
                'amount': amount,
                'time_diff_sec': time_diff_sec,
                'time_diff_min': time_diff_sec / 60,
            })

    # Sort by time_diff (ascending - fastest first)
    records_with_metrics.sort(key=lambda x: x['time_diff_sec'])

    print(f"Total records (after amount filter): {len(records_with_metrics)}")
    print(f"Current time filter (<=30min): {sum(1 for r in records_with_metrics if r['time_diff_sec'] <= 30 * 60)}")

    if len(records_with_metrics) < target_count:
        print(f"\n⚠ WARNING: Not enough records even without time filter!")
        print(f"  Available: {len(records_with_metrics)}, Target: {target_count}")
        return None

    # Find the threshold that gives exactly target_count records
    if len(records_with_metrics) >= target_count:
        # The time at position target_count-1 (0-indexed) is the threshold
        optimal_record = records_with_metrics[target_count - 1]
        optimal_time_sec = optimal_record['time_diff_sec']
        optimal_time_min = optimal_time_sec / 60

        print(f"\n✓ Found optimal time threshold:")
        print(f"  time_diff <= {optimal_time_min:.1f} min ({optimal_time_sec} sec)")
        print(f"  This gives exactly {target_count} records")

        # Show some context
        print(f"\n  Records around threshold:")
        start_idx = max(0, target_count - 5)
        end_idx = min(len(records_with_metrics), target_count + 5)

        for i in range(start_idx, end_idx):
            r = records_with_metrics[i]
            marker = "  → " if i == target_count - 1 else "    "
            print(f"{marker}#{i+1:3d}: time={r['time_diff_min']:6.1f}min, amount={r['amount']/1e8:8.2f} {in_asset}")

        # Calculate increase
        increase_min = optimal_time_min - CURRENT_TIME_THRESHOLD_MIN
        increase_pct = increase_min / CURRENT_TIME_THRESHOLD_MIN * 100
        print(f"\n  Increase from current: +{increase_min:.1f} min (+{increase_pct:.1f}%)")
        print(f"  Current: {CURRENT_TIME_THRESHOLD_MIN}min → Optimal: {optimal_time_min:.1f}min")

        return {
            'pair': pair_file.stem,
            'in_asset': in_asset,
            'current_time_min': CURRENT_TIME_THRESHOLD_MIN,
            'optimal_time_min': optimal_time_min,
            'optimal_time_sec': optimal_time_sec,
            'increase_min': increase_min,
            'increase_pct': increase_pct,
            'records_at_optimal': target_count,
            'records_at_current': sum(1 for r in records_with_metrics if r['time_diff_sec'] <= CURRENT_TIME_THRESHOLD_MIN * 60),
        }

    return None


def main():
    print(f"\n{'='*70}")
    print("Time Threshold Optimization Test")
    print(f"{'='*70}\n")

    # Load blockchain txs
    print("Loading blockchain transaction data...")
    blockchain_txs = load_blockchain_txs(BLOCKCHAIN_TXS_DIR, ["BTC", "ETH", "DOGE", "LTC"])

    for asset in ["BTC", "ETH", "DOGE", "LTC"]:
        if asset in blockchain_txs:
            print(f"  {asset}: {len(blockchain_txs[asset]):,} transactions ✓")
        else:
            print(f"  {asset}: MISSING ✗")
            print(f"\nERROR: Please run fetch_blockchain_txs.py first!")
            return

    # Analyze all trading pairs
    print(f"\nScanning for trading pairs in {DATA_DIR}...")
    pair_files = sorted(DATA_DIR.glob("*.ndjson"))
    results = []

    for pair_file in pair_files:
        result = analyze_pair(pair_file, blockchain_txs, target_count=100)
        if result:
            results.append(result)

    # Summary
    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}\n")

    if results:
        print("Recommended time threshold adjustments:\n")

        max_time_min = max(r['optimal_time_min'] for r in results)

        for r in results:
            print(f"  {r['pair']}:")
            print(f"    Current: <= {r['current_time_min']}min")
            print(f"    Optimal: <= {r['optimal_time_min']:.1f}min")
            print(f"    Change:  +{r['increase_min']:.1f}min (+{r['increase_pct']:.1f}%)")
            print(f"    Result:  {r['records_at_current']} → {r['records_at_optimal']} records")
            print()

        print(f"Proposed unified time threshold:\n")
        print(f"  time_diff <= {max_time_min:.1f} min (rounded: {int(max_time_min + 0.5)}min)")
        print(f"  This ensures all analyzed pairs reach at least 100 records.")
    else:
        print("No adjustments needed.")

    print(f"\n{'='*70}")
    print("✓ Analysis complete (no source files modified)")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
