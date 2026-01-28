#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Generate query YAML files from common ancestor groups.

This script:
1. Reads common_ancestor_depth_N.ndjson files from data/
2. Converts each group to a query item
3. Outputs to queries/common_ancestor_hop_N.yaml

Usage:
    uv run python gen_query.py
"""

import json
import yaml
from pathlib import Path
from typing import List, Dict


SCRIPT_DIR = Path(__file__).parent  # scripts/
BYBITHACK_DIR = SCRIPT_DIR.parent  # bybithack/
DATA_DIR = BYBITHACK_DIR / "data" / "partial_a5a023"
QUERIES_DIR = BYBITHACK_DIR / "queries" / "partial_a5a023"


def generate_query_text(group: Dict) -> str:
    """
    Generate query text listing all BTC transactions.

    Args:
        group: Group data with paths and crosschain_links

    Returns:
        Query text string
    """
    # Collect all BTC out transactions
    btc_txs = []
    for path in group['paths']:
        cclink = path['crosschain_link']
        out_txid = cclink['out']['txid']
        out_receiver = cclink['out']['receiver']
        btc_txs.append(f"{out_txid} (to {out_receiver})")

    # Build query text
    query_prefix = "I suspect these BTC transactions are cross-chain bridged from the same entity on ETH, can you help me to find it? "
    query_text = query_prefix + ", ".join(btc_txs)

    return query_text


def calculate_metadata(group: Dict) -> Dict:
    """
    Calculate metadata from group data.

    Args:
        group: Group data with paths and crosschain_links

    Returns:
        Metadata dict with time_delta_min, time_delta_max, total_value
    """
    time_deltas = []
    total_value = 0.0

    for path in group['paths']:
        cclink = path['crosschain_link']

        # Collect time_delta
        if 'time_delta' in cclink:
            time_deltas.append(cclink['time_delta'])

        # Sum in.value
        total_value += cclink['in']['value']

    # Calculate min/max time_delta
    time_delta_min = min(time_deltas) if time_deltas else None
    time_delta_max = max(time_deltas) if time_deltas else None

    metadata = {
        'group_id': group['group_id'],
        'idx': group['idx'],
        'endpoints_count': group['endpoints_count'],
        'common_hops': group['common_hops'],
        'time_delta_min': time_delta_min,
        'time_delta_max': time_delta_max,
        'total_value': round(total_value, 4)
    }

    return metadata


def convert_group_to_query_item(group: Dict) -> Dict:
    """
    Convert a group to a query item.

    Args:
        group: Group data from ndjson

    Returns:
        Query item dict
    """
    query_text = generate_query_text(group)
    metadata = calculate_metadata(group)

    query_item = {
        'query': query_text,
        'groundtruth': group['common_ancestor'],
        'metadata': metadata
    }

    return query_item


def process_ndjson_file(input_file: Path, output_file: Path):
    """
    Process a single ndjson file and generate corresponding yaml.

    Args:
        input_file: Path to input ndjson file
        output_file: Path to output yaml file
    """
    print(f"[INFO] Processing {input_file.name}...")

    query_items = []

    with open(input_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                group = json.loads(line)
                query_item = convert_group_to_query_item(group)
                query_items.append(query_item)
            except json.JSONDecodeError as e:
                print(f"  ✗ Error parsing line: {e}")
                continue

    # Write to yaml
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w') as f:
        yaml.dump(query_items, f, allow_unicode=True, default_flow_style=False, sort_keys=False)

    print(f"  ✓ Generated {len(query_items)} query items")
    print(f"  -> {output_file}")


def main():
    print("=" * 70)
    print("Generate Query YAML Files from Common Ancestor Groups")
    print("=" * 70)
    print()

    # Find all depth ndjson files in data/
    ndjson_files = sorted(DATA_DIR.glob("common_ancestor_depth_*.ndjson"))

    if not ndjson_files:
        print("[ERROR] No common_ancestor_depth_*.ndjson files found in data/")
        return

    print(f"[INFO] Found {len(ndjson_files)} ndjson files")
    print()

    total_queries = 0

    for ndjson_file in ndjson_files:
        # Extract depth number from filename
        depth = int(ndjson_file.stem.split('_')[-1])

        # Output to queries/common_ancestor_hop_N.yaml
        output_file = QUERIES_DIR / f"common_ancestor_hop_{depth}.yaml"

        process_ndjson_file(ndjson_file, output_file)

        # Count queries in this file
        with open(output_file, 'r') as f:
            queries = yaml.safe_load(f)
            total_queries += len(queries)

        print()

    print("=" * 70)
    print(f"✓ Generated {total_queries} total query items across {len(ndjson_files)} files")
    print("=" * 70)


if __name__ == "__main__":
    main()
