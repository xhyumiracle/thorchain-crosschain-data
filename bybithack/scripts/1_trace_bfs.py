#!/usr/bin/env python3
"""
BFS-based ETH flow tracer to find paths to THORChain router.
Optimized for finding limited samples with finite resources.
"""

import sys
import os
import logging
import json
from datetime import datetime
from typing import Dict, List, Set, Tuple
from collections import deque

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from config import get_asset_unit

# Constants
ETH_UNIT = get_asset_unit("ETH")
THOR_ROUTER = "0xD37BbE5744D730a1d98d8DC97c42F0Ca46aD7146"
MIN_TRANSFER = 10.0  # ETH
MAX_DEPTH = 5  # Full run: up to 5 hops
TARGET_THOR_BRANCHES = 10  # Stop after finding this many different branches to Thor
MIN_TOOL_CALLS_AFTER_TARGET = 500  # After reaching target, continue until this many API calls
MAX_TRANSFERS_PER_ADDR = 90  # If addr has >90 transfers, treat as DeFi protocol and skip

# Default starting point (can be overridden via --start-address CLI arg)
# See ../start_addresses.txt for list of candidate addresses
START_ADDRESS = None  # Must be provided via CLI or will use first address from start_addresses.txt

# Blacklist: high-frequency addresses (DeFi protocols, aggregators)
BLACKLIST = {
    "0xd3f64baa732061f8b3626ee44bab354f854877ac",  # Unizen
    "0xfc99f58a8974a4bc36e60e2d490bb8d72899ee9f",  # OKX Web3 Proxy
}

# Output
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "../results")
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "eth_bfs_trace.json")  # Fixed filename

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)


class BFSTracer:
    def __init__(self, resume_file: str = None, start_address: str = None, max_depth: int = None, output_dir: str = None):
        self.visited_pairs: Set[Tuple[str, str]] = set()  # (from, to) pairs
        self.visited_addrs: Set[str] = set()  # All addresses queried
        self.transfer_cache: Dict[str, List[Dict]] = {}  # Cache API results
        self.thor_paths: List[Dict] = []  # Found paths to Thor
        self.thor_branches: Set[str] = set()  # Unique first-hop addresses that led to Thor

        # Override defaults if provided
        self.start_address = start_address or START_ADDRESS
        self.max_depth = max_depth if max_depth is not None else MAX_DEPTH
        self.output_dir = output_dir or OUTPUT_DIR
        os.makedirs(self.output_dir, exist_ok=True)
        self.output_file = os.path.join(self.output_dir, "eth_bfs_trace.json")

        # Resume from previous run if specified
        if resume_file and os.path.exists(resume_file):
            self.load_state(resume_file)

    def norm(self, addr: str) -> str:
        """Normalize address to lowercase."""
        return addr.lower() if addr else ""

    def is_blacklisted(self, addr: str) -> bool:
        """Check if address is in blacklist."""
        return self.norm(addr) in BLACKLIST

    def load_state(self, resume_file: str):
        """Load previous state from JSON file."""
        logger.info(f"[RESUME] Loading state from {resume_file}")
        with open(resume_file, 'r') as f:
            data = json.load(f)

        # Restore thor paths and branches
        self.thor_paths = data.get('thor_paths', [])
        self.thor_branches = set(data.get('thor_branches', []))

        # Restore visited states (need to rebuild from data)
        # Mark all addresses in cache as visited
        for path in self.thor_paths:
            for step in path['path']:
                self.visited_addrs.add(step['from'])
                self.visited_addrs.add(step['to'])
                self.visited_pairs.add((step['from'], step['to']))

        logger.info(f"  Resumed: {len(self.thor_paths)} Thor paths, {len(self.thor_branches)} branches")
        logger.info(f"  Visited: {len(self.visited_addrs)} addrs, {len(self.visited_pairs)} pairs")

    def get_transfers_out(self, address: str) -> List[Dict]:
        """Get all transfers OUT from address using Blockchair search API."""
        addr_norm = self.norm(address)

        # Check cache first
        if addr_norm in self.transfer_cache:
            logger.info(f"[CACHE] {address[:10]}... ({len(self.transfer_cache[addr_norm])} transfers)")
            return self.transfer_cache[addr_norm]

        logger.info(f"[QUERY] {address[:10]}... (value >= {MIN_TRANSFER} ETH)")

        from src.tools.blockchair import BlockchairClient
        from src.clients.base import with_retry

        # Use standard client with @with_retry decorator for automatic retry handling
        # Retry logic includes: timeout, SSL errors, 429/5xx, etc.
        # 432 (quota exhausted) will raise QuotaExhaustedError and stop immediately
        client = BlockchairClient()

        @with_retry()
        def _fetch_data():
            url = f"https://api.blockchair.com/ethereum/transactions?q=sender({addr_norm}),time(2025-02-21..)&limit=100&s=time(desc)"
            if client.api_key:
                url += f"&key={client.api_key}"
            response = client.client.get(url)
            return client._handle_response(response)

        data = _fetch_data()
        txs_raw = data.get('data', [])

        # Convert and filter
        transfers = []
        for tx in txs_raw:
            if tx.get('failed', False):
                continue

            value_eth = int(tx.get('value', 0)) / ETH_UNIT
            if value_eth >= MIN_TRANSFER:
                transfers.append({
                    'txid': tx.get('hash'),
                    'from': self.norm(tx.get('sender')),
                    'to': self.norm(tx.get('recipient')),
                    'value': value_eth,
                    'time': tx.get('time'),
                })

        logger.info(f"  Found {len(transfers)} transfers >= {MIN_TRANSFER} ETH")

        # Check if this looks like a DeFi protocol (too many transfers)
        if len(transfers) > MAX_TRANSFERS_PER_ADDR:
            logger.info(f"  ⚠️  WARNING: >90 transfers, likely DeFi protocol - will skip in BFS")

        self.transfer_cache[addr_norm] = transfers
        return transfers

    def save_progress(self, queue: deque, reason: str = "checkpoint"):
        """Save current progress to JSON file."""
        output_file = self.output_file  # Use instance output file

        # Convert queue to serializable format
        queue_snapshot = [
            {
                'address': item[0],
                'depth': item[1],
                'path': item[2]
            }
            for item in queue
        ]

        result = {
            'stop_reason': reason,
            'thor_paths': self.thor_paths,
            'thor_branches_count': len(self.thor_branches),
            'thor_branches': list(self.thor_branches),
            'stats': {
                'unique_addrs_queried': len(self.transfer_cache),
                'visited_pairs': len(self.visited_pairs),
                'thor_paths_found': len(self.thor_paths),
            },
            'search_state': {
                'queue_size': len(queue_snapshot),
                'queue_snapshot': queue_snapshot[:100],  # Save first 100 items
            },
            'config': {
                'start_address': START_ADDRESS,
                'thor_router': THOR_ROUTER,
                'min_transfer': MIN_TRANSFER,
                'max_depth': MAX_DEPTH,
                'target_branches': TARGET_THOR_BRANCHES,
                'blacklist': list(BLACKLIST),
            }
        }

        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2)

        logger.info(f"\n{'='*60}")
        logger.info(f"[SAVED] {output_file}")
        logger.info(f"Thor paths: {len(self.thor_paths)} | Branches: {len(self.thor_branches)}/{TARGET_THOR_BRANCHES}")
        logger.info(f"API queries: {len(self.transfer_cache)} | Visited pairs: {len(self.visited_pairs)}")
        logger.info(f"{'='*60}\n")

        return output_file

    def trace_bfs(self, resume_data: Dict = None):
        """BFS traversal from start address."""
        logger.info(f"ETH BFS Tracer")
        logger.info(f"Start: {self.start_address}")
        logger.info(f"Thor: {THOR_ROUTER}")
        logger.info(f"Min: {MIN_TRANSFER} ETH, Max depth: {self.max_depth}")
        logger.info(f"Target: {TARGET_THOR_BRANCHES} different branches to Thor")
        logger.info(f"Blacklist: {len(BLACKLIST)} addresses\n")

        # BFS queue: (address, depth, path)
        if resume_data and 'search_state' in resume_data:
            # Resume from saved queue
            queue_data = resume_data['search_state']['queue_snapshot']
            queue = deque([
                (item['address'], item['depth'], item['path'])
                for item in queue_data
            ])
            logger.info(f"[RESUME] Loaded {len(queue)} items from queue\n")
        else:
            # Start fresh
            queue = deque([(self.start_address, 0, [])])

        while queue:
            # Check stop condition: (10 branches AND 500+ API calls) OR queue empty
            if len(self.thor_branches) >= TARGET_THOR_BRANCHES and len(self.transfer_cache) >= MIN_TOOL_CALLS_AFTER_TARGET:
                logger.info(f"\n✓ TARGET REACHED: {len(self.thor_branches)} branches + {len(self.transfer_cache)} API calls!")
                self.save_progress(queue, reason="target_reached")
                break

            address, depth, path = queue.popleft()
            addr_norm = self.norm(address)

            logger.info(f"\n[BFS] Depth {depth} | Queue size: {len(queue)} | Address: {address[:10]}...")

            # Depth limit
            if depth > self.max_depth:
                logger.info(f"  Skip: max depth reached")
                continue

            # Blacklist check
            if self.is_blacklisted(address):
                logger.info(f"  Skip: blacklisted address")
                continue

            # Skip if already queried
            if addr_norm in self.visited_addrs:
                logger.info(f"  Skip: already visited")
                continue

            self.visited_addrs.add(addr_norm)

            # Get transfers
            transfers = self.get_transfers_out(addr_norm)

            if not transfers:
                logger.info(f"  No large transfers (>={MIN_TRANSFER} ETH)")
                continue

            # Skip if too many transfers (likely DeFi protocol)
            if len(transfers) > MAX_TRANSFERS_PER_ADDR:
                logger.info(f"  Skip: >90 transfers, likely DeFi protocol")
                continue

            # Process each transfer
            for tx in transfers:
                recipient = tx['to']
                recipient_norm = self.norm(recipient)

                # Global deduplication: skip if (from, to) pair already seen
                pair = (addr_norm, recipient_norm)
                if pair in self.visited_pairs:
                    continue

                self.visited_pairs.add(pair)

                step_info = {
                    'from': addr_norm,
                    'to': recipient_norm,
                    'txid': tx['txid'],
                    'value': round(tx['value'], 4),
                    'time': tx['time']
                }

                new_path = path + [step_info]

                # Check if reached Thor
                if recipient_norm == self.norm(THOR_ROUTER):
                    # Record the first hop address (branch identifier)
                    if len(new_path) > 0:
                        first_hop = new_path[0]['to']
                        self.thor_branches.add(first_hop)
                        branch_num = len(self.thor_branches)
                    else:
                        branch_num = 1

                    logger.info(f"  ✓ THOR! Branch #{branch_num} | {tx['value']:.2f} ETH | Depth {depth+1}")

                    self.thor_paths.append({
                        'path': new_path,
                        'depth': depth + 1,
                        'total_value': sum(s['value'] for s in new_path),
                    })

                    # Save immediately after finding a Thor path
                    self.save_progress(queue, reason="thor_found")
                    continue

                # Add to queue for further exploration
                if depth + 1 <= self.max_depth:
                    queue.append((recipient, depth + 1, new_path))

        # Final save
        if queue:
            logger.info(f"\nQueue exhausted or stopped")

        self.save_progress(queue, reason="completed")

        logger.info(f"\n{'='*60}")
        logger.info(f"FINAL RESULTS")
        logger.info(f"{'='*60}")
        logger.info(f"Thor paths found: {len(self.thor_paths)}")
        logger.info(f"Unique branches: {len(self.thor_branches)}")
        logger.info(f"API queries: {len(self.transfer_cache)}")
        logger.info(f"Visited pairs: {len(self.visited_pairs)}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description='BFS ETH flow tracer')
    parser.add_argument('--resume', type=str, help='Resume from previous JSON file')
    parser.add_argument('--start-address', type=str, help='Starting address to trace from')
    parser.add_argument('--max-depth', type=int, help='Maximum depth (hops) to explore')
    parser.add_argument('--output-dir', type=str, help='Output directory for results')
    args = parser.parse_args()

    # Load resume data if specified
    resume_data = None
    if args.resume:
        with open(args.resume, 'r') as f:
            resume_data = json.load(f)

    tracer = BFSTracer(
        resume_file=args.resume,
        start_address=args.start_address,
        max_depth=args.max_depth,
        output_dir=args.output_dir
    )
    tracer.trace_bfs(resume_data=resume_data)


if __name__ == "__main__":
    main()
