#!/usr/bin/env python3
"""
Find balanced thresholds so ALL pairs are close to 100 records with 30min time filter.

Strategy: 
- For pairs with > 100 records: increase threshold to reduce
- For pairs with < 100 records: decrease threshold to increase
- Find thresholds where all pairs converge around 100
"""

import json
import sys
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.blockchain import load_blockchain_txs, get_tx_timestamp


def get_time_diff(record, blockchain_txs):
    """Calculate time diff in seconds."""
    in_list = record.get("in", [])
    out_list = record.get("out", [])
    
    if not in_list or not out_list:
        return None
    
    in_entry, out_entry = in_list[0], out_list[0]
    in_txid, in_asset = in_entry.get('txID'), in_entry.get('asset')
    out_txid, out_asset = out_entry.get('txID'), out_entry.get('asset')
    
    if not all([in_txid, in_asset, out_txid, out_asset]):
        return None
    
    in_tx = blockchain_txs.get(in_asset, {}).get(in_txid)
    out_tx = blockchain_txs.get(out_asset, {}).get(out_txid)
    
    if not in_tx or not out_tx:
        return None
    
    in_time = get_tx_timestamp(in_tx)
    out_time = get_tx_timestamp(out_tx)
    
    if in_time is None or out_time is None:
        return None
    
    return out_time - in_time


def test_thresholds(data_dir, blockchain_txs, thresholds):
    """Test a set of thresholds and return counts for all pairs."""
    # thresholds = {"BTC": 0.05, "ETH": 1.2, "LTC": 2.0, "DOGE": 950} in human-readable units
    
    thresholds_base = {asset: int(val * 1e8) for asset, val in thresholds.items()}
    
    pairs = [
        ("BTC-DOGE", "BTC"), ("BTC-ETH", "BTC"), ("BTC-LTC", "BTC"),
        ("DOGE-BTC", "DOGE"), ("DOGE-ETH", "DOGE"), ("DOGE-LTC", "DOGE"),
        ("ETH-BTC", "ETH"), ("ETH-DOGE", "ETH"), ("ETH-LTC", "ETH"),
        ("LTC-BTC", "LTC"), ("LTC-DOGE", "LTC"), ("LTC-ETH", "LTC"),
    ]
    
    results = {}
    
    for pair_name, asset in pairs:
        pair_file = data_dir / f"{pair_name}.ndjson"
        if not pair_file.exists():
            continue
        
        count = 0
        threshold_base = thresholds_base[asset]
        
        with open(pair_file, 'r') as f:
            for line in f:
                try:
                    record = json.loads(line.strip())
                except:
                    continue
                
                in_list = record.get("in", [])
                if not in_list:
                    continue
                
                entry = in_list[0]
                rec_asset = entry.get('asset', '')
                amount = int(entry.get('amount', 0))
                
                if rec_asset != asset or amount < threshold_base:
                    continue
                
                time_diff = get_time_diff(record, blockchain_txs)
                if time_diff is None or time_diff > 1800:
                    continue
                
                count += 1
        
        results[pair_name] = count
    
    return results


def main():
    script_dir = Path(__file__).parent.parent.parent
    data_dir = script_dir / "data" / "thorchain-2025"
    blockchain_dir = script_dir / "blockchain_txs"
    
    print("Loading blockchain transaction data...")
    blockchain_txs = load_blockchain_txs(blockchain_dir, ["BTC", "ETH", "DOGE", "LTC"])
    print()
    
    print("=" * 80)
    print("Finding balanced thresholds for ALL pairs to reach ~100 records")
    print("Constraint: amount filter + time <= 30min")
    print("=" * 80)
    
    # Test current HF thresholds first
    print("\n[Baseline] Current HF thresholds (level 10):")
    current_thresholds = {"BTC": 0.1, "ETH": 2.0, "LTC": 4.0, "DOGE": 1000}
    current_results = test_thresholds(data_dir, blockchain_txs, current_thresholds)
    
    for pair in sorted(current_results.keys()):
        count = current_results[pair]
        status = "✓" if count >= 100 else "✗"
        print(f"  {status} {pair:12s}: {count:3d} records")
    
    # Test different threshold combinations
    test_scenarios = [
        {
            "name": "Lower BTC/ETH, same LTC/DOGE",
            "thresholds": {"BTC": 0.05, "ETH": 1.0, "LTC": 4.0, "DOGE": 1000}
        },
        {
            "name": "Even lower BTC/ETH",
            "thresholds": {"BTC": 0.03, "ETH": 0.8, "LTC": 4.0, "DOGE": 1000}
        },
        {
            "name": "Minimal BTC/ETH",
            "thresholds": {"BTC": 0.02, "ETH": 0.5, "LTC": 4.0, "DOGE": 1000}
        },
        {
            "name": "Balanced reduction",
            "thresholds": {"BTC": 0.07, "ETH": 1.5, "LTC": 3.0, "DOGE": 800}
        },
    ]
    
    for scenario in test_scenarios:
        print(f"\n[Test] {scenario['name']}:")
        print(f"  Thresholds: BTC>={scenario['thresholds']['BTC']}, ETH>={scenario['thresholds']['ETH']}, LTC>={scenario['thresholds']['LTC']}, DOGE>={scenario['thresholds']['DOGE']}")
        
        results = test_thresholds(data_dir, blockchain_txs, scenario['thresholds'])
        
        below_100 = []
        around_100 = []
        above_100 = []
        
        for pair in sorted(results.keys()):
            count = results[pair]
            if count < 100:
                below_100.append((pair, count))
            elif count <= 110:
                around_100.append((pair, count))
            else:
                above_100.append((pair, count))
        
        # Show summary
        print(f"\n  Below 100: {len(below_100)} pairs")
        for pair, count in below_100:
            print(f"    ✗ {pair:12s}: {count:3d}")
        
        print(f"\n  Around 100 (100-110): {len(around_100)} pairs")
        for pair, count in around_100:
            print(f"    ✓ {pair:12s}: {count:3d}")
        
        print(f"\n  Above 110: {len(above_100)} pairs")
        for pair, count in above_100[:5]:  # Show first 5
            print(f"    ⚠ {pair:12s}: {count:3d}")
        if len(above_100) > 5:
            print(f"    ... and {len(above_100) - 5} more")
    
    print("\n" + "=" * 80)
    print("Analysis: Can all pairs reach 100 with adjusted thresholds?")
    print("=" * 80)


if __name__ == "__main__":
    main()
