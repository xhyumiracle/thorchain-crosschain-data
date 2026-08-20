#!/usr/bin/env python3
"""
Find optimal amount thresholds to achieve ~100 records per pair with 30min time filter.

Strategy:
1. Load blockchain_txs data
2. For each pair, test different amount thresholds
3. Find the threshold that gives closest to 100 records (with time <= 30min)
"""

import json
import sys
from pathlib import Path
from collections import defaultdict

# Add utils to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.blockchain import load_blockchain_txs, get_tx_timestamp


def get_time_diff(record, blockchain_txs):
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


def test_threshold_for_pair(pair_file, blockchain_txs, threshold_in_base_units, asset):
    """Test a specific threshold for a pair file."""
    count = 0
    
    with open(pair_file, 'r') as f:
        for line in f:
            try:
                record = json.loads(line.strip())
            except:
                continue
            
            # Check amount threshold
            in_list = record.get("in", [])
            if not in_list:
                continue
            
            entry = in_list[0]
            rec_asset = entry.get('asset', '')
            amount = int(entry.get('amount', 0))
            
            if rec_asset != asset:
                continue
            
            if amount < threshold_in_base_units:
                continue
            
            # Check time filter
            time_diff = get_time_diff(record, blockchain_txs)
            if time_diff is None or time_diff > 1800:  # 30 min = 1800 sec
                continue
            
            count += 1
    
    return count


def find_threshold_for_pair(pair_file, blockchain_txs, asset, target=100):
    """Binary search to find threshold that gives closest to target records."""
    
    # Define search range based on asset (in human-readable units)
    if asset == "BTC":
        min_thresh, max_thresh = 0.001, 1.0  # 0.001 to 1.0 BTC
    elif asset == "ETH":
        min_thresh, max_thresh = 0.01, 20.0  # 0.01 to 20 ETH
    elif asset == "LTC":
        min_thresh, max_thresh = 0.1, 50.0  # 0.1 to 50 LTC
    elif asset == "DOGE":
        min_thresh, max_thresh = 10, 5000  # 10 to 5000 DOGE
    else:
        return None, None
    
    best_threshold = None
    best_count = 0
    best_diff = float('inf')
    
    # Test multiple thresholds
    num_tests = 20
    step = (max_thresh - min_thresh) / num_tests
    
    results = []
    
    for i in range(num_tests + 1):
        thresh_human = min_thresh + i * step
        thresh_base = int(thresh_human * 1e8)
        
        count = test_threshold_for_pair(pair_file, blockchain_txs, thresh_base, asset)
        results.append((thresh_human, count))
        
        diff = abs(count - target)
        if diff < best_diff:
            best_diff = diff
            best_threshold = thresh_human
            best_count = count
    
    return best_threshold, best_count, results


def main():
    script_dir = Path(__file__).parent.parent.parent
    data_dir = script_dir / "data" / "thorchain-2025"
    blockchain_dir = script_dir / "blockchain_txs"
    
    print("Loading blockchain transaction data...")
    blockchain_txs = load_blockchain_txs(blockchain_dir, ["BTC", "ETH", "DOGE", "LTC"])
    
    for asset in ["BTC", "ETH", "DOGE", "LTC"]:
        if asset in blockchain_txs:
            print(f"  {asset}: {len(blockchain_txs[asset]):,} transactions")
    print()
    
    # Test pairs
    pairs_to_test = [
        ("BTC-DOGE.jsonl", "BTC"),
        ("BTC-ETH.jsonl", "BTC"),
        ("BTC-LTC.jsonl", "BTC"),
        ("DOGE-BTC.jsonl", "DOGE"),
        ("DOGE-ETH.jsonl", "DOGE"),
        ("DOGE-LTC.jsonl", "DOGE"),
        ("ETH-BTC.jsonl", "ETH"),
        ("ETH-DOGE.jsonl", "ETH"),
        ("ETH-LTC.jsonl", "ETH"),
        ("LTC-BTC.jsonl", "LTC"),
        ("LTC-DOGE.jsonl", "LTC"),
        ("LTC-ETH.jsonl", "LTC"),
    ]
    
    print("Finding optimal thresholds for each pair (target: 100 records with time <= 30min)\n")
    print("=" * 80)
    
    results_summary = []
    
    for pair_file, asset in pairs_to_test:
        file_path = data_dir / pair_file
        if not file_path.exists():
            print(f"⚠ {pair_file}: File not found")
            continue
        
        print(f"\nAnalyzing {pair_file}...")
        threshold, count, all_results = find_threshold_for_pair(file_path, blockchain_txs, asset)
        
        if threshold is None:
            print(f"  ✗ Could not find threshold")
            continue
        
        # Show progression
        print(f"\n  Threshold scan results:")
        for t, c in all_results[::4]:  # Show every 4th result
            unit = "DOGE" if asset == "DOGE" else asset
            print(f"    {asset} >= {t:.4g} {unit}: {c:3d} records")
        
        print(f"\n  ✓ Optimal: {asset} >= {threshold:.4g} → {count} records")
        results_summary.append((pair_file, asset, threshold, count))
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY: Optimal thresholds for ~100 records per pair")
    print("=" * 80)
    
    by_asset = defaultdict(list)
    for pair, asset, thresh, count in results_summary:
        by_asset[asset].append((pair, thresh, count))
    
    for asset in ["BTC", "ETH", "LTC", "DOGE"]:
        if asset not in by_asset:
            continue
        
        print(f"\n{asset}:")
        max_thresh = max(thresh for _, thresh, _ in by_asset[asset])
        print(f"  Recommended threshold: {max_thresh:.4g} {asset} (max across all pairs)")
        
        for pair, thresh, count in sorted(by_asset[asset]):
            status = "✓" if count >= 100 else "⚠"
            print(f"    {status} {pair:20s}: {asset} >= {thresh:.4g} → {count:3d} records")


if __name__ == "__main__":
    main()
