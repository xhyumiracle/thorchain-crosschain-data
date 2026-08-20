#!/usr/bin/env python3
"""
Check for negative time diff in dataset.

Usage:
    # Check mini dataset (default)
    python script/analyze/check_negative_timediff.py

    # Check specific dataset
    python script/analyze/check_negative_timediff.py data/thorchain-2025-high-fast

    # Check specific pairs only
    python script/analyze/check_negative_timediff.py --pairs BTC-ETH ETH-BTC
"""

import json
import argparse
import sys
from pathlib import Path

# Load blockchain tx data (from filter_data.py enrichment)
BLOCKCHAIN_TXS_DIR = Path("blockchain_txs")
DEFAULT_DATA_DIR = Path("data/thorchain-2025-high-fast-mini")
SUPPORTED_CHAINS = ["BTC", "ETH", "DOGE", "LTC"]

def load_blockchain_txs(asset: str) -> dict:
    """Load blockchain tx data for given asset."""
    file = BLOCKCHAIN_TXS_DIR / f"{asset.lower()}.jsonl"
    if not file.exists():
        return {}

    txs = {}
    with open(file) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    record = json.loads(line)
                    # Blockchair format: txid in transaction.hash
                    if 'transaction' in record and 'hash' in record['transaction']:
                        txid = record['transaction']['hash']
                        # Normalize: remove 0x prefix and convert to uppercase
                        # (ETH has 0x prefix, others don't. THORChain stores all as uppercase without 0x)
                        if txid.startswith('0x'):
                            txid = txid[2:]
                        txs[txid.upper()] = record
                except:
                    pass
    return txs

def get_tx_timestamp(tx_data: dict) -> int | None:
    """Extract timestamp from blockchain tx data (Blockchair format)."""
    if 'transaction' in tx_data and 'time' in tx_data['transaction']:
        # time is ISO format string "2025-06-07 22:43:40"
        time_str = tx_data['transaction']['time']
        from datetime import datetime
        dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        return int(dt.timestamp())
    return None

def get_time_diff(record: dict, blockchain_txs: dict) -> int | None:
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

    in_tx_data = in_chain_txs.get(in_txid.upper())
    out_tx_data = out_chain_txs.get(out_txid.upper())

    if not in_tx_data or not out_tx_data:
        return None

    in_ts = get_tx_timestamp(in_tx_data)
    out_ts = get_tx_timestamp(out_tx_data)

    if in_ts is None or out_ts is None:
        return None

    return out_ts - in_ts

def main():
    parser = argparse.ArgumentParser(description="Check for negative time diff in dataset")
    parser.add_argument("data_dir", nargs="?", default=str(DEFAULT_DATA_DIR), help="Dataset directory to check")
    parser.add_argument("--pairs", nargs="+", help="Specific pairs to check (e.g., BTC-ETH ETH-BTC)")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        print(f"Error: Directory not found: {data_dir}")
        sys.exit(1)

    # Load blockchain tx data for all supported chains
    print("Loading blockchain transaction data...")
    blockchain_txs = {}
    loaded_chains = []
    for chain in SUPPORTED_CHAINS:
        txs = load_blockchain_txs(chain)
        if txs:
            blockchain_txs[chain] = txs
            loaded_chains.append(chain)
            print(f"  Loaded {len(txs):,} {chain} txs")

    if not blockchain_txs:
        print("Error: No blockchain tx data found.")
        print(f"Expected directory: {BLOCKCHAIN_TXS_DIR}/")
        sys.exit(1)

    print()

    # Find files to check
    if args.pairs:
        files = []
        for pair in args.pairs:
            filename = f"{pair}.jsonl"
            filepath = data_dir / filename
            if filepath.exists():
                files.append(filepath)
            else:
                print(f"Warning: {filename} not found in {data_dir}")
    else:
        files = sorted(data_dir.glob("*.jsonl"))

    if not files:
        print(f"No .jsonl files found in {data_dir}")
        sys.exit(1)

    # Check each file
    total_negative = 0
    total_checked = 0

    for file in files:
        print("=" * 60)
        print(f"Checking {file.name}")
        print("=" * 60)

        records = []
        with open(file) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except:
                        pass

        negative_count = 0
        no_data_count = 0

        for record in records:
            time_diff = get_time_diff(record, blockchain_txs)

            if time_diff is None:
                no_data_count += 1
            elif time_diff < 0:
                negative_count += 1
                print(f"  ID: {record.get('id')}")
                print(f"  Time diff: {time_diff} seconds ({time_diff/60:.1f} minutes)")
                print(f"  In TX:  {record['in'][0].get('txID') if record.get('in') else 'N/A'}")
                print(f"  Out TX: {record['out'][0].get('txID') if record.get('out') else 'N/A'}")
                print()

        total_negative += negative_count
        total_checked += len(records)

        print(f"Summary: {negative_count}/{len(records)} records with negative time diff")
        if no_data_count > 0:
            print(f"         {no_data_count}/{len(records)} records with no blockchain data")
        print()

    print("=" * 60)
    print(f"TOTAL: {total_negative}/{total_checked} records with negative time diff across all files")
    print("=" * 60)


if __name__ == "__main__":
    main()
