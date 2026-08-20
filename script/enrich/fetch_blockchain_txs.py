"""
Fetch blockchain transaction data from Blockchair API for amount-filtered THORChain swaps.

This script:
1. Reads from amount-filtered dataset (default: thorchain-2025-amtgte10/)
2. Collects unique transaction IDs per chain
3. Batch fetches blockchain tx data via Blockchair API
4. Saves to blockchain_txs/{chain}.jsonl

Usage:
    # Default (reads from thorchain-2025-amtgte10)
    uv run python script/enrich/fetch_blockchain_txs.py

    # Custom input directory
    uv run python script/enrich/fetch_blockchain_txs.py --input-dir thorchain-2025-amtgte10
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

# Chain mapping
CHAIN_MAP = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "DOGE": "dogecoin",
    "LTC": "litecoin"
}

# Default directories
CACHE_DIR = Path(__file__).parent.parent.parent / ".cache"
OUTPUT_DIR = Path(__file__).parent.parent.parent / "blockchain_txs"


def normalize_txid(chain: str, txid: str) -> str:
    """Normalize transaction ID for Blockchair API calls."""
    if chain == "ETH" and not txid.startswith("0x"):
        return f"0x{txid.lower()}"
    return txid.lower()


def collect_all_txids(input_dir: Path) -> dict[str, set[str]]:
    """Collect all unique txids per chain from all jsonl files.

    Groups txids by their actual asset (from the 'asset' field in each entry),
    not by filename pattern.
    """
    txids_by_chain = defaultdict(set)

    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    jsonl_files = list(input_dir.glob("*.jsonl"))
    if not jsonl_files:
        raise FileNotFoundError(f"No jsonl files found in {input_dir}")

    print(f"Collecting txids from {len(jsonl_files)} files...")

    for file_path in jsonl_files:
        print(f"  Processing {file_path.name}...")

        with open(file_path, 'r') as f:
            for line in f:
                record = json.loads(line.strip())

                # Group txids by their actual asset
                for entry in record.get('in', []):
                    asset = entry.get('asset')
                    txid = entry.get('txID')
                    if asset and txid and asset in CHAIN_MAP:
                        txids_by_chain[asset].add(txid)

                for entry in record.get('out', []):
                    asset = entry.get('asset')
                    txid = entry.get('txID')
                    if asset and txid and asset in CHAIN_MAP:
                        txids_by_chain[asset].add(txid)

    return txids_by_chain


def load_existing_txids(output_dir: Path) -> dict[str, set[str]]:
    """Load already-fetched transaction IDs from existing jsonl files."""
    existing = defaultdict(set)

    if not output_dir.exists():
        return existing

    for asset in CHAIN_MAP.keys():
        output_file = output_dir / f"{asset.lower()}.jsonl"
        if not output_file.exists():
            continue

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

    return existing


def fetch_and_save_blockchain_txs(txids_by_chain: dict[str, set[str]], existing_txids: dict[str, set[str]]):
    """Fetch blockchain tx data and append to existing jsonl files (incremental)."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for asset, all_txids in txids_by_chain.items():
        chain_name = CHAIN_MAP[asset]
        output_file = OUTPUT_DIR / f"{asset.lower()}.jsonl"

        # Filter out existing txids (incremental)
        existing = existing_txids.get(asset, set())
        new_txids = [txid for txid in all_txids if txid.lower() not in existing]

        print(f"\n{'='*60}")
        print(f"Processing {asset} ({chain_name})")
        print(f"Total unique txids: {len(all_txids)}")
        print(f"Already fetched: {len(existing)}")
        print(f"New txids to fetch: {len(new_txids)}")
        print(f"API calls needed (batch 10): {(len(new_txids) + 9) // 10}")
        print(f"Output: {output_file}")
        print(f"{'='*60}\n")

        if not new_txids:
            print(f"✓ No new transactions to fetch for {asset}\n")
            continue

        # Initialize Blockchair client
        client = BlockchairClient()

        total = len(new_txids)
        batch_size = 10

        # Open output file in append mode (incremental)
        with open(output_file, 'a') as f:
            for i in range(0, total, batch_size):
                batch = new_txids[i:i + batch_size]

                print(f"Fetching batch {i//batch_size + 1}/{(total + batch_size - 1)//batch_size} ({len(batch)} txids)...")

                try:
                    # Normalize txids for API call (ETH needs 0x prefix)
                    normalized_batch = [normalize_txid(asset, txid) for txid in batch]

                    # Fetch transactions - returns dict {txid: raw_blockchair_data}
                    # Pass asset (e.g., "DOGE") not chain_name (e.g., "dogecoin")
                    # API client will handle chain name mapping internally
                    batch_data = client.get_transactions_batch(asset, normalized_batch)

                    # Write raw Blockchair API response for each transaction
                    # Use normalized txid for lookup, but store original txid as key
                    success_count = 0
                    for orig_txid in batch:
                        normalized_txid = normalize_txid(asset, orig_txid)
                        tx_data = batch_data.get(normalized_txid.lower())
                        if not tx_data:
                            continue

                        # Add original txid to response for easier lookup later
                        tx_data['_original_txid'] = orig_txid

                        # Save raw Blockchair response (no model conversion needed)
                        json_line = json.dumps(tx_data)
                        f.write(json_line + '\n')
                        success_count += 1

                    print(f"  ✓ Fetched {success_count}/{len(batch)} transactions")

                except Exception as e:
                    print(f"  ✗ Error fetching batch: {e}")
                    continue

        print(f"\n✓ Completed {asset}: appended {total} new transactions to {output_file}")


def main():
    """Main execution."""
    parser = argparse.ArgumentParser(description="Fetch blockchain transaction data")
    parser.add_argument("--input-dir", type=str, required=True,
                        help="Input directory containing amount-filtered jsonl files")
    parser.add_argument("--append", action="store_true",
                        help="Append to existing blockchain_txs data (skip confirmation)")

    args = parser.parse_args()

    # Get script directory
    script_dir = Path(__file__).parent.parent.parent

    # Determine input directory (support both absolute and relative paths)
    if Path(args.input_dir).is_absolute():
        input_dir = Path(args.input_dir)
    else:
        input_dir = script_dir / args.input_dir

    # Check input directory
    if not input_dir.exists():
        print(f"\n{'='*70}")
        print("ERROR: Input directory not found!")
        print(f"{'='*70}")
        print(f"\nExpected: {input_dir}")
        print(f"\nPlease create the amount-filtered dataset first:")
        print(f"  uv run python script/process/filter_data.py --amount-level-gte 10")
        print(f"{'='*70}\n")
        exit(1)

    # Check if output directory exists and has data
    if OUTPUT_DIR.exists():
        existing_files = list(OUTPUT_DIR.glob("*.jsonl"))
        if existing_files and not args.append:
            print(f"\n{'='*70}")
            print("WARNING: Output directory already contains data!")
            print(f"{'='*70}")
            print(f"\nOutput directory: {OUTPUT_DIR}")
            print(f"Existing files:")
            for f in existing_files:
                print(f"  - {f.name}")
            print(f"\nThis script will APPEND to existing files.")
            print(f"To start fresh, please:")
            print(f"  rm -rf {OUTPUT_DIR}")
            print(f"\nOr use --append flag to skip this confirmation.")
            print(f"{'='*70}\n")

            response = input("Continue anyway? [y/N]: ").strip().lower()
            if response != 'y':
                print("Aborted.")
                exit(0)
            print()

    print("THORChain Blockchain Transaction Fetcher")
    print("=" * 60)
    print(f"Input: {input_dir}")
    print(f"Output: {OUTPUT_DIR}")
    print(f"Cache: {CACHE_DIR}")
    print("=" * 60)

    # Step 1: Load existing txids
    print("\nStep 1: Loading existing blockchain transaction data...")
    existing_txids = load_existing_txids(OUTPUT_DIR)

    total_existing = sum(len(txids) for txids in existing_txids.values())
    if total_existing > 0:
        print(f"\nExisting transactions:")
        for asset in sorted(existing_txids.keys()):
            count = len(existing_txids[asset])
            print(f"  {asset}: {count:,} transactions")
        print(f"  Total: {total_existing:,} transactions")
    else:
        print("  No existing data found.")

    # Step 2: Collect all txids
    print("\nStep 2: Collecting transaction IDs from input...")
    txids_by_chain = collect_all_txids(input_dir)

    print("\nSummary:")
    total_txids = 0
    total_new = 0
    for asset, txids in sorted(txids_by_chain.items()):
        count = len(txids)
        existing_count = len(existing_txids.get(asset, set()))
        new_count = len([t for t in txids if t.lower() not in existing_txids.get(asset, set())])
        total_txids += count
        total_new += new_count
        print(f"  {asset}: {count:,} total, {existing_count:,} existing, {new_count:,} new")
    print(f"\n  Grand total: {total_txids:,} txids")
    print(f"  Already fetched: {total_existing:,}")
    print(f"  New to fetch: {total_new:,}")
    print(f"  Total API calls needed: {sum((len([t for t in txids if t.lower() not in existing_txids.get(asset, set())]) + 9) // 10 for asset, txids in txids_by_chain.items()):,}")

    if total_new == 0:
        print(f"\n{'='*60}")
        print("✓ All transactions already fetched! No new data to fetch.")
        print(f"{'='*60}\n")
        return

    # Step 3: Fetch and save
    print("\nStep 3: Fetching new blockchain transaction data...")
    fetch_and_save_blockchain_txs(txids_by_chain, existing_txids)

    print("\n" + "=" * 60)
    print("✓ All done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
