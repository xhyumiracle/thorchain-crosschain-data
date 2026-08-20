"""
Incremental fetch of blockchain transaction data from Blockchair API.

This script:
1. Reads from thorchain-2025 full dataset with custom amount thresholds
2. Loads existing blockchain_txs data to skip already-fetched txs
3. Only fetches NEW transactions (incremental update)
4. Supports all chains including LTC

Custom thresholds:
- BTC >= 0.05
- ETH >= 1.2
- LTC >= 2.0
- DOGE >= 950

Usage:
    uv run python script/enrich/fetch_blockchain_txs_incremental.py
"""

import json
import sys
import argparse
from pathlib import Path
from collections import defaultdict

# Add project root to path
project_root = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(project_root))

from src.tools.blockchair import BlockchairClient

# Custom amount thresholds (in 1e8 base units)
AMOUNT_THRESHOLDS = {
    "BTC": 5_000_000,        # 0.05 BTC
    "ETH": 120_000_000,      # 1.2 ETH
    "LTC": 200_000_000,      # 2.0 LTC
    "DOGE": 95_000_000_000,  # 950 DOGE
}

# Chain mapping
CHAIN_MAP = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "DOGE": "dogecoin",
    "LTC": "litecoin"
}

# Directories
SOURCE_DIR = Path(__file__).parent.parent.parent / "data" / "thorchain-2025"
OUTPUT_DIR = Path(__file__).parent.parent.parent / "blockchain_txs"
CACHE_DIR = Path(__file__).parent.parent.parent / ".cache"


def normalize_txid(chain: str, txid: str) -> str:
    """Normalize transaction ID for Blockchair API calls."""
    if chain == "ETH" and not txid.startswith("0x"):
        return f"0x{txid.lower()}"
    return txid.lower()


def load_existing_txids(output_dir: Path) -> dict[str, set[str]]:
    """Load already-fetched transaction IDs from existing jsonl files."""
    existing = defaultdict(set)

    if not output_dir.exists():
        return existing

    for asset in CHAIN_MAP.keys():
        output_file = output_dir / f"{asset.lower()}.jsonl"
        if not output_file.exists():
            continue

        print(f"  Loading existing {asset} txids from {output_file.name}...")
        with open(output_file, 'r') as f:
            for line in f:
                try:
                    data = json.loads(line.strip())
                    # Try both normalized and original txid
                    if '_original_txid' in data:
                        existing[asset].add(data['_original_txid'].lower())
                    # Also check the transaction field
                    if 'transaction' in data and 'hash' in data['transaction']:
                        existing[asset].add(data['transaction']['hash'].lower())
                except:
                    continue

        print(f"    Found {len(existing[asset])} existing {asset} transactions")

    return existing


def collect_filtered_txids(source_dir: Path) -> dict[str, set[str]]:
    """Collect txids that pass custom amount thresholds."""
    txids_by_chain = defaultdict(set)

    jsonl_files = list(source_dir.glob("*.jsonl"))
    if not jsonl_files:
        raise FileNotFoundError(f"No jsonl files found in {source_dir}")

    print(f"\nCollecting txids with custom thresholds:")
    for asset, threshold in AMOUNT_THRESHOLDS.items():
        print(f"  {asset}: >= {threshold/1e8:.2f}")
    print()

    for file_path in jsonl_files:
        print(f"  Processing {file_path.name}...", end=" ", flush=True)

        count = 0
        with open(file_path, 'r') as f:
            for line in f:
                try:
                    record = json.loads(line.strip())
                except:
                    continue

                # Check in entries
                for entry in record.get('in', []):
                    asset = entry.get('asset')
                    txid = entry.get('txID')
                    amount = int(entry.get('amount', 0))

                    if asset and txid and asset in AMOUNT_THRESHOLDS:
                        threshold = AMOUNT_THRESHOLDS[asset]
                        if amount >= threshold:
                            txids_by_chain[asset].add(txid)
                            count += 1

                # Check out entries
                for entry in record.get('out', []):
                    asset = entry.get('asset')
                    txid = entry.get('txID')
                    amount = int(entry.get('amount', 0))

                    if asset and txid and asset in AMOUNT_THRESHOLDS:
                        threshold = AMOUNT_THRESHOLDS[asset]
                        if amount >= threshold:
                            txids_by_chain[asset].add(txid)
                            count += 1

        print(f"collected {count} txids")

    return txids_by_chain


def fetch_and_append_blockchain_txs(
    new_txids_by_chain: dict[str, set[str]],
    existing_txids: dict[str, set[str]]
):
    """Fetch new blockchain tx data and append to existing files."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for asset, all_txids in new_txids_by_chain.items():
        chain_name = CHAIN_MAP[asset]
        output_file = OUTPUT_DIR / f"{asset.lower()}.jsonl"

        # Filter out existing txids
        existing = existing_txids.get(asset, set())
        new_txids = [txid for txid in all_txids if txid.lower() not in existing]

        print(f"\n{'='*60}")
        print(f"Processing {asset} ({chain_name})")
        print(f"Total txids passing threshold: {len(all_txids)}")
        print(f"Already fetched: {len(existing)}")
        print(f"New txids to fetch: {len(new_txids)}")
        print(f"API calls needed (batch 10): {(len(new_txids) + 9) // 10}")
        print(f"Output: {output_file}")
        print(f"{'='*60}\n")

        if not new_txids:
            print(f"✓ No new transactions to fetch for {asset}")
            continue

        # Initialize Blockchair client
        client = BlockchairClient()

        total = len(new_txids)
        batch_size = 10
        fetched_count = 0

        # Open output file in append mode
        with open(output_file, 'a') as f:
            for i in range(0, total, batch_size):
                batch = new_txids[i:i + batch_size]

                print(f"Fetching batch {i//batch_size + 1}/{(total + batch_size - 1)//batch_size} ({len(batch)} txids)...")

                try:
                    # Normalize txids for API call
                    normalized_batch = [normalize_txid(asset, txid) for txid in batch]

                    # Fetch transactions
                    batch_data = client.get_transactions_batch(asset, normalized_batch)

                    # Write raw Blockchair API response for each transaction
                    success_count = 0
                    for orig_txid in batch:
                        normalized_txid = normalize_txid(asset, orig_txid)
                        tx_data = batch_data.get(normalized_txid.lower())
                        if not tx_data:
                            continue

                        # Add original txid to response for easier lookup later
                        tx_data['_original_txid'] = orig_txid

                        # Save raw Blockchair response
                        json_line = json.dumps(tx_data)
                        f.write(json_line + '\n')
                        success_count += 1
                        fetched_count += 1

                    print(f"  ✓ Fetched {success_count}/{len(batch)} transactions")

                except Exception as e:
                    print(f"  ✗ Error fetching batch: {e}")
                    continue

        print(f"\n✓ Completed {asset}: fetched {fetched_count} new transactions, appended to {output_file}")


def main():
    """Main execution."""
    print(f"\n{'='*70}")
    print("THORChain Blockchain Transaction Incremental Fetcher")
    print(f"{'='*70}")
    print(f"\nCustom Amount Thresholds:")
    for asset, threshold in AMOUNT_THRESHOLDS.items():
        print(f"  {asset}: >= {threshold/1e8:.2f}")
    print(f"\nSource: {SOURCE_DIR}")
    print(f"Output: {OUTPUT_DIR}")
    print(f"Cache:  {CACHE_DIR}")
    print(f"{'='*70}\n")

    # Step 1: Load existing txids
    print("Step 1: Loading existing blockchain transaction data...")
    existing_txids = load_existing_txids(OUTPUT_DIR)

    total_existing = sum(len(txids) for txids in existing_txids.values())
    print(f"\nTotal existing transactions: {total_existing:,}")

    # Step 2: Collect txids that pass thresholds
    print(f"\n{'='*70}")
    print("Step 2: Collecting transaction IDs from source data...")
    print(f"{'='*70}")

    all_txids = collect_filtered_txids(SOURCE_DIR)

    print("\nSummary:")
    total_txids = 0
    new_txids_count = 0
    for asset in sorted(all_txids.keys()):
        count = len(all_txids[asset])
        existing_count = len(existing_txids.get(asset, set()))
        new_count = len([t for t in all_txids[asset] if t.lower() not in existing_txids.get(asset, set())])

        total_txids += count
        new_txids_count += new_count

        print(f"  {asset}: {count:,} total, {existing_count:,} existing, {new_count:,} new")

    print(f"\n  Grand total: {total_txids:,} txids")
    print(f"  Already fetched: {total_existing:,}")
    print(f"  New to fetch: {new_txids_count:,}")
    print(f"  Total API calls needed: {sum((len([t for t in txids if t.lower() not in existing_txids.get(asset, set())]) + 9) // 10 for asset, txids in all_txids.items()):,}")

    if new_txids_count == 0:
        print(f"\n{'='*70}")
        print("✓ All transactions already fetched! No new data to fetch.")
        print(f"{'='*70}\n")
        return

    # Confirm
    print(f"\n{'='*70}")
    response = input(f"Proceed with fetching {new_txids_count:,} new transactions? [y/N]: ").strip().lower()
    if response != 'y':
        print("Aborted.")
        return

    # Step 3: Fetch and append new data
    print(f"\n{'='*70}")
    print("Step 3: Fetching new blockchain transaction data...")
    print(f"{'='*70}")

    fetch_and_append_blockchain_txs(all_txids, existing_txids)

    print(f"\n{'='*70}")
    print("✓ All done! Incremental update completed.")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
