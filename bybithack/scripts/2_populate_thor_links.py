#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Populate thor_crosschain_links with matched records from thorchain-2025 dataset.

For the 35 Thor router transactions found in thorchain-2025, this script:
1. Reads the matched txids from thor_txids.txt
2. Searches thorchain-2025 dataset for these txids in the "in" section (ETH side)
3. Extracts complete records and populates thor_crosschain_links in the JSON

Field mapping:
- thor25-idx: from thorchain idx field
- in.time: preserved from original JSON
- in.eth_value: preserved from original JSON
- in.amount: from thorchain (1e8 units)
- in.txid: thorchain format (uppercase, no 0x)
- out.time: placeholder ".."
- out.amount: from thorchain (1e8 units)
- out.txid: thorchain format (uppercase, no 0x)
- out.receiver: from thorchain address field

Usage:
    python populate_thor_links.py
"""

import json
from pathlib import Path
from typing import Dict, List, Optional


SCRIPT_DIR = Path(__file__).parent  # scripts/
BYBITHACK_DIR = SCRIPT_DIR.parent  # bybithack/
DATA_DIR = BYBITHACK_DIR / "data"

THOR_TXIDS_FILE = BYBITHACK_DIR / "thor_txids.txt"
JSON_FILE = BYBITHACK_DIR / "eth_partial_a5a023.json"  # Input: initial JSON
THORCHAIN_DATA_DIR = BYBITHACK_DIR.parent / "data" / "thorchain-2025"
OUTPUT_FILE = DATA_DIR / "partial_a5a023_eth_raw.json"  # Output: raw data with thor links


def load_thor_txids(txids_file: Path) -> set:
    """Load Thor router txids from file (lowercase, no 0x prefix)."""
    with open(txids_file, 'r') as f:
        return set(line.strip().lower() for line in f if line.strip())


def search_thorchain_data(txids: set, data_dir: Path) -> Dict[str, Dict]:
    """
    Search thorchain-2025 dataset for txids in the "in" section (ETH transactions).

    Returns dict mapping txid (lowercase, no prefix) -> complete record
    """
    matched_records = {}

    # Only search ETH-BTC.ndjson since we're looking for ETH->BTC swaps
    ndjson_file = data_dir / "ETH-BTC.ndjson"

    if not ndjson_file.exists():
        print(f"[ERROR] File not found: {ndjson_file}")
        return matched_records

    print(f"[INFO] Searching {ndjson_file.name}...")

    with open(ndjson_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            # Check if "in" entry has a txID matching our list
            in_list = record.get("in", [])
            for in_entry in in_list:
                in_txid = in_entry.get("txID", "").lower()

                if in_txid in txids:
                    matched_records[in_txid] = record
                    print(f"  ✓ Found: {in_txid}")
                    break

    return matched_records


def format_thor_record_for_json(
    record: Dict,
    matched_txid: str,
    original_endpoint: Dict
) -> Dict:
    """
    Format a thorchain record for insertion into thor_crosschain_links.

    Args:
        record: Complete thorchain record
        matched_txid: The txid that matched (lowercase, no prefix)
        original_endpoint: Original endpoint data from thor_paths_endpoints

    Returns:
        Formatted dict with all thorchain fields
    """
    in_list = record.get("in", [])
    out_list = record.get("out", [])

    if len(in_list) != 1 or len(out_list) != 1:
        return None

    in_entry = in_list[0]
    out_entry = out_list[0]

    # Convert amounts from 1e8 units to human-friendly values
    in_amount = in_entry.get("amount", 0)
    out_amount = out_entry.get("amount", 0)

    # Convert to int if string
    if isinstance(in_amount, str):
        in_amount = int(in_amount)
    if isinstance(out_amount, str):
        out_amount = int(out_amount)

    in_value = in_amount / 1e8
    out_value = out_amount / 1e8

    # Create formatted record
    formatted = {
        "thor25-idx": record.get("idx"),
        "thor25-id": record.get("id", ""),
        "type": record.get("type", ""),
        "status": record.get("status", ""),
        "in": {
            "asset": f"{in_entry.get('chain', '')}.{in_entry.get('asset', '')}",
            "sender": in_entry.get("address", ""),
            "txid": in_entry.get("txID", ""),  # Thorchain format (uppercase, no 0x)
            "value": in_value,  # Human-friendly value (converted from 1e8)
            "time": original_endpoint.get("time", "")  # Preserved from original JSON
        },
        "out": {
            "asset": f"{out_entry.get('chain', '')}.{out_entry.get('asset', '')}",
            "receiver": out_entry.get("address", ""),  # Changed from sender to receiver
            "txid": out_entry.get("txID", ""),  # Thorchain format (uppercase, no 0x)
            "value": out_value,  # Human-friendly value (converted from 1e8)
            "time": ".."  # Placeholder for now
        }
    }

    return formatted


def populate_json(
    json_file: Path,
    matched_records: Dict[str, Dict],
    output_file: Path
):
    """
    Read JSON file, populate thor_crosschain_links, and write output.
    """
    print(f"\n[INFO] Loading {json_file.name}...")
    with open(json_file, 'r') as f:
        data = json.load(f)

    # Get thor_paths_endpoints to match against
    endpoints = data.get("thor_paths_endpoints", [])

    print(f"[INFO] Found {len(endpoints)} thor_paths_endpoints")
    print(f"[INFO] Matched {len(matched_records)} records from thorchain-2025")

    # Build thor_crosschain_links
    links = []
    matched_count = 0

    for endpoint in endpoints:
        endpoint_txid = endpoint.get("txid", "").lower().replace("0x", "")

        if endpoint_txid in matched_records:
            record = matched_records[endpoint_txid]
            formatted = format_thor_record_for_json(
                record,
                endpoint_txid,
                endpoint
            )

            if formatted:
                links.append(formatted)
                matched_count += 1

    # Update data
    data["thor_crosschain_links_found"] = matched_count
    data["thor_crosschain_links"] = links

    # Write output
    print(f"\n[INFO] Writing output to {output_file.name}...")
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"[INFO] ✓ Populated {matched_count} thor_crosschain_links")
    print(f"[INFO] Output saved to: {output_file}")


def main():
    print("=" * 70)
    print("Populating thor_crosschain_links from thorchain-2025 data")
    print("=" * 70)
    print()

    # Load Thor router txids
    print(f"[INFO] Loading Thor router txids from {THOR_TXIDS_FILE.name}...")
    txids = load_thor_txids(THOR_TXIDS_FILE)
    print(f"[INFO] Loaded {len(txids)} txids")
    print()

    # Search thorchain data
    print(f"[INFO] Searching thorchain-2025 dataset...")
    matched_records = search_thorchain_data(txids, THORCHAIN_DATA_DIR)
    print()
    print(f"[INFO] Found {len(matched_records)} matched records")
    print()

    # Populate JSON
    populate_json(JSON_FILE, matched_records, OUTPUT_FILE)

    print()
    print("=" * 70)
    print("Done!")
    print("=" * 70)


if __name__ == "__main__":
    main()
