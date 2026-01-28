#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Split thor_crosschain_links by common ancestors at different depths.

This script:
1. Reads partial_a5a023_eth_raw.json
2. For each crosschain link, traces back through the corresponding path
3. Groups endpoints by common ancestors at different depths
4. Outputs to common_ancestor_depth_N.ndjson files

Usage:
    uv run python split_by_ancestor.py [--max-depth N] [--amount-threshold X]
"""

import json
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from collections import defaultdict


SCRIPT_DIR = Path(__file__).parent  # scripts/
BYBITHACK_DIR = SCRIPT_DIR.parent  # bybithack/
DATA_DIR = BYBITHACK_DIR / "data"

JSON_FILE = DATA_DIR / "partial_a5a023_eth_raw.json"
OUTPUT_DIR = DATA_DIR / "partial_a5a023"


def normalize_address(addr: str) -> str:
    """Normalize Ethereum address to lowercase."""
    return addr.lower()


def normalize_txid(txid: str) -> str:
    """Normalize transaction ID to lowercase without 0x prefix."""
    return txid.lower().replace('0x', '')


def get_ancestors(
    start_address: str,
    path: List[Dict],
    max_depth: int,
    amount_threshold: float
) -> List[Tuple[str, str, int]]:
    """
    Trace backwards from start_address through path to find ancestors.

    Args:
        start_address: The address to start from (sender to THORChain router)
        path: List of transactions in the path (already ordered from origin to destination)
        max_depth: Maximum depth to trace back
        amount_threshold: Minimum value for transactions to consider

    Returns:
        List of (txid, address, depth) tuples representing ancestors

    Note:
        - depth=1 means the immediate predecessor (1 hop back)
        - depth=2 means 2 hops back, etc.
    """
    ancestors = []

    # Normalize start address
    start_addr_norm = normalize_address(start_address)

    # Find the position of start_address in the path (should be the last 'from')
    # The path is ordered: [hop1, hop2, ..., hopN] where hopN.to = thorchain_router
    # We need to find where start_address appears as 'from'

    start_idx = -1
    for i in range(len(path) - 1, -1, -1):
        if normalize_address(path[i]['from']) == start_addr_norm:
            start_idx = i
            break

    if start_idx == -1:
        # start_address not found in path
        return ancestors

    # Trace backwards from start_idx
    current_depth = 0

    for i in range(start_idx, -1, -1):
        hop = path[i]
        value = hop.get('value', 0)

        # Check amount threshold
        if value < amount_threshold:
            continue

        current_depth += 1

        if current_depth > max_depth:
            break

        # Record this ancestor
        ancestors.append((
            hop['txid'],
            hop['from'],
            current_depth
        ))

    return ancestors


def build_path_structure(path: List[Dict], cc_sender: str, target_hop: int) -> List[Dict]:
    """
    Build the path structure for output, going back target_hop hops from cc_sender.

    Note: The path EXCLUDES the final hop from cc_sender to thor router, as that info
    is already in crosschain_link. Path goes from ancestor to cc_sender only.

    Args:
        path: List of transaction hops
        cc_sender: The crosschain sender address
        target_hop: How many hops back from cc_sender to include

    Returns:
        List of path items with depth, hop, txid, sender, receiver, value, asset, time
        - depth: counted forward from ancestor (depth=1 is ancestor, depth=2 is next step, etc.)
        - hop: counted backwards from cc_sender (hop=2 means 1 hop before cc_sender, hop=3 means 2 hops before, etc.)
    """
    # Find where cc_sender appears in the path
    cc_sender_norm = normalize_address(cc_sender)

    # First, find the cc_sender transaction (the one to THORChain router)
    cc_idx = -1
    for i in range(len(path) - 1, -1, -1):
        if normalize_address(path[i]['from']) == cc_sender_norm:
            cc_idx = i
            break

    if cc_idx == -1:
        return []

    # Build path items going back target_hop-1 hops from BEFORE cc_idx
    # We exclude cc_idx itself (the tx from cc_sender to router)
    path_items = []

    # Start from before cc_idx and go back target_hop-1 transactions
    # target_hop includes the cc_sender->router hop, so we need target_hop-1 hops before that
    start_idx = max(0, cc_idx - target_hop + 1)

    for i in range(start_idx, cc_idx):  # Note: range goes up to but NOT including cc_idx
        hop_data = path[i]
        hop = cc_idx - i  # hop: backwards from cc_sender (1, 2, 3, ...). No +1 since we're excluding the cc_sender->router hop
        depth = target_hop - hop  # depth: forward from ancestor (1, 2, 3, ...)

        path_items.append({
            "depth": depth,
            "hop": hop,
            "txid": hop_data['txid'],
            "sender": hop_data['from'],
            "receiver": hop_data['to'],
            "value": hop_data['value'],
            "asset": "ETH.ETH",
            "time": hop_data['time']
        })

    # Sort by depth (ascending: ancestor first)
    path_items.sort(key=lambda x: x['depth'])

    return path_items


def find_matching_path(link: Dict, all_paths: List[Dict]) -> Optional[Dict]:
    """
    Find the thor_path that matches this crosschain link.

    Args:
        link: A thor_crosschain_link entry
        all_paths: List of all thor_paths

    Returns:
        Matching path dict or None
    """
    link_txid_norm = normalize_txid(link['in']['txid'])

    for path in all_paths:
        # Check if the last hop's txid matches
        last_hop_txid = normalize_txid(path['path'][-1]['txid'])
        if last_hop_txid == link_txid_norm:
            return path

    return None


def group_by_common_ancestor(
    links: List[Dict],
    paths: List[Dict],
    max_depth: int,
    amount_threshold: float
) -> Dict[int, List[Dict]]:
    """
    Group crosschain links by common ancestor at different depths.

    Args:
        links: List of thor_crosschain_links
        paths: List of thor_paths
        max_depth: Maximum depth to search
        amount_threshold: Minimum value threshold

    Returns:
        Dict mapping depth -> list of group objects
    """
    # Build mapping from cc_sender+txid to (link, path)
    endpoint_map = {}

    for link in links:
        cc_sender = link['in']['sender']
        cc_txid = link['in']['txid']

        # Find matching path
        matching_path = find_matching_path(link, paths)

        if matching_path:
            key = f"{normalize_address(cc_sender)}:{normalize_txid(cc_txid)}"
            endpoint_map[key] = {
                'link': link,
                'path': matching_path,
                'cc_sender': cc_sender,
                'cc_txid': cc_txid
            }

    print(f"[INFO] Matched {len(endpoint_map)} crosschain links to paths")

    # For each depth level, find common ancestors
    depth_groups = defaultdict(list)

    for depth in range(1, max_depth + 1):
        # Map ancestor -> list of endpoints
        ancestor_to_endpoints = defaultdict(list)

        for key, endpoint_data in endpoint_map.items():
            link = endpoint_data['link']
            path = endpoint_data['path']
            cc_sender = endpoint_data['cc_sender']
            cc_txid = endpoint_data['cc_txid']

            # Get ancestors at this depth
            ancestors = get_ancestors(
                cc_sender,
                path['path'],
                depth,
                amount_threshold
            )

            if ancestors:
                # The ancestor at exactly this depth is the last one (deepest)
                if len(ancestors) >= depth:
                    # Find the ancestor at exactly this depth
                    ancestor_at_depth = None
                    for txid, addr, d in ancestors:
                        if d == depth:
                            ancestor_at_depth = normalize_address(addr)
                            break

                    if ancestor_at_depth:
                        ancestor_to_endpoints[ancestor_at_depth].append({
                            'cc_sender': cc_sender,
                            'cc_txid': cc_txid,
                            'hop': depth - 1,  # Adjusted: subtract 1 because we exclude cc_sender->router hop
                            'link': link,
                            'path': path
                        })

        # Filter: only keep ancestors with >= 2 endpoints
        for ancestor, endpoints in ancestor_to_endpoints.items():
            if len(endpoints) >= 2:
                # Calculate adjusted depth (common_hops)
                adjusted_depth = depth - 1

                # Generate group_id: first 8 chars of ancestor address + common_hops + endpoints_count
                ancestor_prefix = ancestor.replace('0x', '')[:8]
                group_id = f"{ancestor_prefix}_{adjusted_depth}_{len(endpoints)}"

                # Build group object
                group = {
                    'group_id': group_id,
                    'common_ancestor': ancestor,
                    'common_hops': adjusted_depth,
                    'endpoints_count': len(endpoints),
                    'endpoints': [
                        {
                            'cc_sender': ep['cc_sender'],
                            'cc_txid': ep['cc_txid'],
                            'hop': ep['hop']
                        }
                        for ep in endpoints
                    ],
                    'paths': [
                        {
                            'cc_sender': ep['cc_sender'],
                            'path': build_path_structure(ep['path']['path'], ep['cc_sender'], depth),
                            'crosschain_link': ep['link']
                        }
                        for ep in endpoints
                    ]
                }

                # Store in depth_groups using adjusted depth
                depth_groups[adjusted_depth].append(group)

    return depth_groups


def main():
    parser = argparse.ArgumentParser(description='Split thor crosschain links by common ancestor')
    parser.add_argument('--max-depth', type=int, default=5,
                       help='Maximum depth to search (default: 5)')
    parser.add_argument('--amount-threshold', type=float, default=0.0,
                       help='Minimum transaction value to consider (default: 0.0)')

    args = parser.parse_args()

    print("=" * 70)
    print("Split Thor Crosschain Links by Common Ancestor")
    print("=" * 70)
    print()
    print(f"Max depth:        {args.max_depth}")
    print(f"Amount threshold: {args.amount_threshold} ETH")
    print()

    # Load data
    print(f"[INFO] Loading {JSON_FILE.name}...")
    with open(JSON_FILE, 'r') as f:
        data = json.load(f)

    links = data.get('thor_crosschain_links', [])
    paths = data.get('thor_paths', [])

    print(f"[INFO] Found {len(links)} crosschain links")
    print(f"[INFO] Found {len(paths)} paths")
    print()

    # Group by common ancestor
    print("[INFO] Grouping by common ancestor...")
    depth_groups = group_by_common_ancestor(
        links,
        paths,
        args.max_depth,
        args.amount_threshold
    )

    # Write output files
    print()
    print("=" * 70)
    print("Writing output files...")
    print("=" * 70)
    print()

    total_groups = 0
    total_endpoints = 0
    group_id_counter = 0

    for depth in sorted(depth_groups.keys()):
        groups = depth_groups[depth]

        if not groups:
            continue

        output_file = OUTPUT_DIR / f"common_ancestor_depth_{depth}.ndjson"

        with open(output_file, 'w') as f:
            for group in groups:
                # Add idx field
                group_with_idx = {"idx": group_id_counter}
                group_with_idx.update(group)
                f.write(json.dumps(group_with_idx, ensure_ascii=False) + '\n')
                group_id_counter += 1

        num_endpoints = sum(len(g['endpoints']) for g in groups)

        print(f"Depth {depth}: {len(groups):2d} groups, {num_endpoints:3d} endpoints")
        print(f"  -> {output_file.name}")

        total_groups += len(groups)
        total_endpoints += num_endpoints

    print()
    print("=" * 70)
    print(f"✓ Total: {total_groups} groups, {total_endpoints} endpoints")
    print("=" * 70)


if __name__ == "__main__":
    main()
