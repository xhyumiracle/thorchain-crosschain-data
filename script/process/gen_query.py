#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Generate YAML batch query files from cleaned ndjson data.

Usage:
    # Generate from a single ndjson file
    python gen_query.py --input ../../data/BTC-DOGE.ndjson --output ../../queries/BTC-DOGE.yaml

    # Generate from all ndjson files (batch mode)
    python gen_query.py --batch --input-dir ../../data --output-dir ../../queries
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


QUERY_TEMPLATE = (
    "What is the source transaction for this cross-chain {out_asset} output "
    "to {out_address} in tx {out_txid} on {out_chain}, "
    "given that it originates from {in_asset} on {in_chain}?"
)


def load_blockchain_txs(blockchain_tx_dir: Path) -> Dict[str, Dict[str, Any]]:
    """Load blockchain transaction data from ndjson files.

    Returns dict mapping (chain, txid) -> tx_data
    """
    txs = {}

    if not blockchain_tx_dir.exists():
        return txs

    for ndjson_file in blockchain_tx_dir.glob("*.ndjson"):
        chain = ndjson_file.stem.upper()  # btc.ndjson -> BTC

        with open(ndjson_file, 'r') as f:
            for line in f:
                try:
                    tx_data = json.loads(line.strip())
                    original_txid = tx_data.get('_original_txid', '').upper()
                    if original_txid:
                        txs[(chain, original_txid)] = tx_data
                except json.JSONDecodeError:
                    continue

    return txs


def get_tx_timestamp(tx_data: Dict[str, Any]) -> Optional[int]:
    """Extract Unix timestamp from Blockchair API response."""
    tx_info = tx_data.get('transaction', {})
    time_val = tx_info.get('time')

    if time_val is None:
        return None

    # If already int, return directly
    if isinstance(time_val, int):
        return time_val

    # If string, parse ISO format
    if isinstance(time_val, str):
        from datetime import datetime
        try:
            dt = datetime.fromisoformat(time_val.replace("Z", "+00:00"))
            return int(dt.timestamp())
        except (ValueError, AttributeError):
            return None

    return None


def generate_query_from_record(
    record: Dict[str, Any],
    blockchain_txs: Optional[Dict[str, Dict[str, Any]]] = None
) -> Dict[str, Any] | None:
    """
    Generate a query dict from a single ndjson record.

    Returns None if record is invalid or should be skipped.
    """
    in_list = record.get("in", [])
    out_list = record.get("out", [])

    # Validate: must have exactly 1 in and 1 out
    if len(in_list) != 1 or len(out_list) != 1:
        return None

    in_entry = in_list[0]
    out_entry = out_list[0]

    # Extract fields
    in_chain = in_entry.get("chain", "")
    in_asset = in_entry.get("asset", "")
    in_txid = in_entry.get("txID", "")
    in_amount = int(in_entry.get("amount", 0))
    in_height = in_entry.get("thorchainHeight", 0)

    out_chain = out_entry.get("chain", "")
    out_asset = out_entry.get("asset", "")
    out_txid = out_entry.get("txID", "")
    out_address = out_entry.get("address", "")
    out_amount = int(out_entry.get("amount", 0))
    out_height = out_entry.get("thorchainHeight", 0)

    # Validate required fields
    if not all([in_chain, in_asset, in_txid, out_chain, out_asset, out_txid, out_address]):
        return None

    # Calculate height diff
    height_diff = out_height - in_height if (out_height and in_height) else 0

    # Generate query from template
    query = QUERY_TEMPLATE.format(
        out_asset=out_asset,
        out_address=out_address,
        out_txid=out_txid,
        out_chain=out_chain,
        in_asset=in_asset,
        in_chain=in_chain,
    )

    # Build query item with metadata
    metadata = {
        "query_id": record.get("id", ""),
        "thorchain_height_diff": height_diff,
        "src_amount": in_amount,
        "dst_amount": out_amount,
    }

    # Add timestamp_delta if blockchain_txs data is available
    if blockchain_txs:
        in_tx_data = blockchain_txs.get((in_chain, in_txid.upper()))
        out_tx_data = blockchain_txs.get((out_chain, out_txid.upper()))

        if in_tx_data and out_tx_data:
            in_ts = get_tx_timestamp(in_tx_data)
            out_ts = get_tx_timestamp(out_tx_data)

            if in_ts is not None and out_ts is not None:
                # timestamp_delta in seconds (out - in)
                metadata["timestamp_delta"] = out_ts - in_ts

    query_item = {
        "query": query,
        "groundtruth": in_txid,
        "metadata": metadata
    }

    return query_item


def process_ndjson_file(
    ndjson_path: Path,
    blockchain_txs: Optional[Dict[str, Dict[str, Any]]] = None
) -> List[Dict[str, Any]]:
    """
    Process a single ndjson file and generate query items.

    Returns list of query items.
    """
    queries = []

    with open(ndjson_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"[WARN] Failed to parse line {line_num} in {ndjson_path.name}: {e}")
                continue

            query_item = generate_query_from_record(record, blockchain_txs)
            if query_item is not None:
                queries.append(query_item)

    return queries


def write_yaml_file(queries: List[Dict[str, Any]], output_path: Path) -> None:
    """Write queries to YAML file."""
    output_data = {
        "queries": queries
    }

    with open(output_path, "w", encoding="utf-8") as f:
        # Write header comment
        f.write("# Batch Query File for BlockchainMAS\n")
        f.write("# Auto-generated from THORChain ndjson data\n")
        f.write("# Format: Each query has 'query', 'groundtruth', and 'metadata'\n\n")

        # Write YAML
        yaml.safe_dump(
            output_data,
            f,
            allow_unicode=True,
            default_flow_style=False,
            sort_keys=False,
            width=120,
        )


def process_single_file(
    input_path: Path,
    output_path: Path,
    blockchain_txs: Optional[Dict[str, Dict[str, Any]]] = None
) -> None:
    """Process a single ndjson file and generate YAML."""
    print(f"[INFO] Processing {input_path.name}...")

    queries = process_ndjson_file(input_path, blockchain_txs)

    if not queries:
        print(f"[WARN] No valid queries generated from {input_path.name}")
        return

    write_yaml_file(queries, output_path)
    print(f"[INFO] Generated {len(queries)} queries -> {output_path}")


def process_batch(
    input_dir: Path,
    output_dir: Path,
    blockchain_txs: Optional[Dict[str, Dict[str, Any]]] = None
) -> None:
    """Process all ndjson files in input_dir and generate YAML files."""
    # Find all ndjson files
    ndjson_files = list(input_dir.glob("*.ndjson"))

    if not ndjson_files:
        print(f"[WARN] No ndjson files found in {input_dir}")
        return

    # Filter out multi-* files
    valid_files = []
    skipped_files = []

    for ndjson_path in ndjson_files:
        if ndjson_path.stem.startswith("multi-"):
            skipped_files.append(ndjson_path.name)
        else:
            valid_files.append(ndjson_path)

    if skipped_files:
        print(f"[INFO] Skipping {len(skipped_files)} multi-* files: {', '.join(skipped_files)}")

    if not valid_files:
        print(f"[WARN] No valid ndjson files to process (all are multi-* files)")
        return

    print(f"[INFO] Found {len(valid_files)} valid files to process")

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Process each file
    total_queries = 0
    for ndjson_path in valid_files:
        output_path = output_dir / f"{ndjson_path.stem}.yaml"

        queries = process_ndjson_file(ndjson_path, blockchain_txs)

        if queries:
            write_yaml_file(queries, output_path)
            print(f"[INFO] {ndjson_path.name} -> {output_path.name} ({len(queries)} queries)")
            total_queries += len(queries)
        else:
            print(f"[WARN] No valid queries from {ndjson_path.name}")

    print(f"\n[INFO] Done. Total queries generated: {total_queries}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate YAML batch query files from THORChain ndjson data"
    )

    # Single file mode
    parser.add_argument(
        "--input",
        type=str,
        help="Input ndjson file path"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output YAML file path"
    )

    # Batch mode
    parser.add_argument(
        "--batch",
        action="store_true",
        help="Batch mode: process all ndjson files in input-dir"
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default="../../data",
        help="Input directory containing ndjson files (batch mode)"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="../../queries",
        help="Output directory for YAML files (batch mode)"
    )

    # Optional blockchain transaction data
    parser.add_argument(
        "--blockchain-txs-dir",
        type=str,
        default=None,
        help="Optional: Directory containing blockchain transaction ndjson files (for timestamp_delta enrichment)"
    )

    args = parser.parse_args()

    # Load blockchain transaction data if provided
    blockchain_txs = None
    if args.blockchain_txs_dir:
        blockchain_tx_dir = Path(args.blockchain_txs_dir)
        if blockchain_tx_dir.exists():
            print(f"[INFO] Loading blockchain transaction data from {blockchain_tx_dir}...")
            blockchain_txs = load_blockchain_txs(blockchain_tx_dir)
            print(f"[INFO] Loaded {len(blockchain_txs)} blockchain transactions")

    if args.batch:
        # Batch mode
        input_dir = Path(args.input_dir)
        output_dir = Path(args.output_dir)

        if not input_dir.exists():
            raise SystemExit(f"Input directory does not exist: {input_dir}")

        process_batch(input_dir, output_dir, blockchain_txs)

    else:
        # Single file mode
        if not args.input or not args.output:
            raise SystemExit("Error: --input and --output are required for single file mode")

        input_path = Path(args.input)
        output_path = Path(args.output)

        if not input_path.exists():
            raise SystemExit(f"Input file does not exist: {input_path}")

        # Create output directory if needed
        output_path.parent.mkdir(parents=True, exist_ok=True)

        process_single_file(input_path, output_path, blockchain_txs)


if __name__ == "__main__":
    main()
