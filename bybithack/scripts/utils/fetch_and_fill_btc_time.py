#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fetch BTC transaction data for Bybit hack thor_crosschain_links and fill timestamps.

This script:
1. Reads thor_crosschain_links from eth_partial_a5a023_populated.json
2. Collects all BTC out txids
3. Fetches blockchain tx data via Blockchair API
4. Saves to blockchain_txs/btc_bybithack.jsonl
5. Updates out.timestamp and out.time in the JSON

Usage:
    uv run python fetch_and_fill_btc_time.py
"""

import json
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).resolve().parents[5]  # up to project root
sys.path.insert(0, str(project_root))

from src.tools.blockchair import BlockchairClient

# Paths
SCRIPT_DIR = Path(__file__).parent  # scripts/utils/
BYBITHACK_DIR = SCRIPT_DIR.parent.parent  # bybithack/
DATA_DIR = BYBITHACK_DIR / "data"
BLOCKCHAIN_TXS_DIR = BYBITHACK_DIR.parent / "blockchain_txs"

JSON_FILE = DATA_DIR / "partial_a5a023_eth_raw.json"
BTC_OUTPUT_FILE = BLOCKCHAIN_TXS_DIR / "btc_bybithack.jsonl"
FINAL_OUTPUT_FILE = DATA_DIR / "partial_a5a023_eth_raw.json"


def normalize_txid(txid: str) -> str:
    """Normalize BTC transaction ID (lowercase)."""
    return txid.lower()


def collect_btc_txids(json_file: Path) -> list[str]:
    """Collect all BTC out txids from thor_crosschain_links."""
    print(f"[INFO] Loading {json_file.name}...")

    with open(json_file, 'r') as f:
        data = json.load(f)

    links = data.get("thor_crosschain_links", [])
    print(f"[INFO] Found {len(links)} thor_crosschain_links")

    btc_txids = []
    for link in links:
        out_data = link.get("out", {})
        asset = out_data.get("asset", "")
        txid = out_data.get("txid", "")

        if asset == "BTC.BTC" and txid:
            btc_txids.append(txid)

    print(f"[INFO] Collected {len(btc_txids)} BTC out txids")
    return btc_txids


def fetch_btc_transactions(txids: list[str], output_file: Path) -> dict[str, dict]:
    """
    Fetch BTC transaction data and save to jsonl file.

    Returns dict mapping txid (uppercase) -> tx_data
    """
    print(f"\n{'='*70}")
    print(f"Fetching BTC transactions")
    print(f"Total txids: {len(txids)}")
    print(f"Output: {output_file}")
    print(f"{'='*70}\n")

    # Create output directory
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Initialize Blockchair client
    client = BlockchairClient()

    batch_size = 10
    total = len(txids)

    # Store results
    tx_data_map = {}

    # Open output file
    with open(output_file, 'w') as f:
        for i in range(0, total, batch_size):
            batch = txids[i:i + batch_size]

            print(f"Fetching batch {i//batch_size + 1}/{(total + batch_size - 1)//batch_size} ({len(batch)} txids)...")

            try:
                # Normalize txids for API call
                normalized_batch = [normalize_txid(txid) for txid in batch]

                # Fetch transactions - pass "BTC" as asset
                batch_data = client.get_transactions_batch("BTC", normalized_batch)

                # Write and store results
                success_count = 0
                for orig_txid in batch:
                    normalized_txid = normalize_txid(orig_txid)
                    tx_data = batch_data.get(normalized_txid)

                    if not tx_data:
                        print(f"  ✗ Failed to fetch: {orig_txid}")
                        continue

                    # Add original txid
                    tx_data['_original_txid'] = orig_txid

                    # Save to file
                    json_line = json.dumps(tx_data)
                    f.write(json_line + '\n')

                    # Store in map (use uppercase for lookup)
                    tx_data_map[orig_txid.upper()] = tx_data
                    success_count += 1

                print(f"  ✓ Fetched {success_count}/{len(batch)} transactions")

            except Exception as e:
                print(f"  ✗ Error fetching batch: {e}")
                continue

    print(f"\n✓ Saved to {output_file}")
    return tx_data_map


def load_blockchain_txs(blockchain_txs_file: Path) -> dict[str, dict]:
    """Load blockchain transactions from jsonl file."""
    tx_map = {}

    if not blockchain_txs_file.exists():
        return tx_map

    with open(blockchain_txs_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                tx_data = json.loads(line)
                orig_txid = tx_data.get('_original_txid', '')
                if orig_txid:
                    # Use uppercase for lookup
                    tx_map[orig_txid.upper()] = tx_data
            except json.JSONDecodeError:
                continue

    return tx_map


def get_tx_timestamp(tx_data: dict) -> str:
    """
    Extract timestamp from Blockchair transaction data.

    Returns:
        utc_time_string (format: "YYYY-MM-DD HH:MM:SS")
    """
    # Blockchair BTC transaction structure:
    # tx_data['transaction']['time']
    transaction = tx_data.get('transaction', {})

    # Get time field (format: "YYYY-MM-DD HH:MM:SS")
    time_str = transaction.get('time', '')

    return time_str if time_str else None


def fill_timestamps(json_file: Path, tx_data_map: dict[str, dict], output_file: Path):
    """Fill out.time and calculate time_delta in thor_crosschain_links."""
    print(f"\n{'='*70}")
    print("Filling timestamps in thor_crosschain_links")
    print(f"{'='*70}\n")

    # Load JSON
    with open(json_file, 'r') as f:
        data = json.load(f)

    links = data.get("thor_crosschain_links", [])

    updated_count = 0
    missing_count = 0

    for link in links:
        in_data = link.get("in", {})
        out_data = link.get("out", {})
        txid = out_data.get("txid", "")

        if not txid:
            continue

        # Lookup tx data (use uppercase)
        tx_data = tx_data_map.get(txid.upper())

        if not tx_data:
            print(f"  ✗ No blockchain data for: {txid}")
            missing_count += 1
            continue

        # Get timestamp
        time_str = get_tx_timestamp(tx_data)

        if time_str is None:
            print(f"  ✗ Failed to parse timestamp for: {txid}")
            missing_count += 1
            continue

        # Update out.time
        out_data["time"] = time_str

        # Calculate time_delta (out.time - in.time) in seconds
        try:
            in_time_str = in_data.get("time", "")
            if in_time_str and time_str:
                in_dt = datetime.strptime(in_time_str, "%Y-%m-%d %H:%M:%S")
                out_dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                time_delta_seconds = int((out_dt - in_dt).total_seconds())
                link["time_delta"] = time_delta_seconds

                updated_count += 1
                print(f"  ✓ Updated {txid}: {time_str} (Δ{time_delta_seconds}s)")
            else:
                updated_count += 1
                print(f"  ✓ Updated {txid}: {time_str}")
        except ValueError as e:
            print(f"  ⚠ Updated time but failed to calculate delta: {e}")
            updated_count += 1

    # Reorder fields in links: move time_delta to front, remove timestamp and thorchain_height_diff
    for link in links:
        # Create new ordered dict with desired field order
        reordered = {}
        reordered["thor25-idx"] = link.get("thor25-idx")
        # Try to get id field from both possible names
        reordered["thor25-id"] = link.get("thor25-id") or link.get("id")
        if "time_delta" in link:
            reordered["time_delta"] = link["time_delta"]
        reordered["type"] = link.get("type")
        reordered["status"] = link.get("status")
        reordered["in"] = link.get("in")
        reordered["out"] = link.get("out")

        # Replace with reordered version
        link.clear()
        link.update(reordered)

    # Save updated JSON
    print(f"\n[INFO] Writing output to {output_file.name}...")
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"\n{'='*70}")
    print(f"✓ Updated {updated_count} timestamps")
    if missing_count > 0:
        print(f"✗ Failed {missing_count} transactions")
    print(f"{'='*70}")


def main():
    print("=" * 70)
    print("Fetch BTC Transactions and Fill Timestamps")
    print("=" * 70)
    print()

    # Step 1: Collect BTC txids
    print("Step 1: Collecting BTC transaction IDs...")
    btc_txids = collect_btc_txids(JSON_FILE)

    if not btc_txids:
        print("[ERROR] No BTC txids found!")
        return

    # Step 2: Fetch blockchain data
    print("\nStep 2: Fetching blockchain transaction data...")

    # Check if already fetched - auto-use existing data
    if BTC_OUTPUT_FILE.exists():
        print(f"[INFO] Found existing blockchain data at {BTC_OUTPUT_FILE}")
        print("[INFO] Loading existing blockchain data...")
        tx_data_map = load_blockchain_txs(BTC_OUTPUT_FILE)
        print(f"[INFO] Loaded {len(tx_data_map)} transactions")
    else:
        tx_data_map = fetch_btc_transactions(btc_txids, BTC_OUTPUT_FILE)

    # Step 3: Fill timestamps
    print("\nStep 3: Filling timestamps...")
    fill_timestamps(JSON_FILE, tx_data_map, FINAL_OUTPUT_FILE)

    print("\n" + "=" * 70)
    print("✓ All done!")
    print(f"Output: {FINAL_OUTPUT_FILE}")
    print("=" * 70)


if __name__ == "__main__":
    main()
