#!/usr/bin/env python3
"""
Shared utilities for loading and processing blockchain transaction data.

Used by multiple scripts in analyze/ and process/ directories.
"""

import json
from pathlib import Path
from datetime import datetime


def load_blockchain_txs(blockchain_tx_dir: Path, chains: list[str] | None = None) -> dict[str, dict]:
    """
    Load blockchain transaction data from ndjson files.

    Args:
        blockchain_tx_dir: Directory containing blockchain tx files
        chains: List of chain names (e.g., ['BTC', 'ETH', 'DOGE']).
                If None, loads all *.ndjson files in the directory.

    Returns:
        Dict mapping chain -> (txid -> tx_data)
        Example: {'BTC': {'TXID1': {...}, 'TXID2': {...}}, 'ETH': {...}}
        Returns empty dict if directory doesn't exist (no error)
    """
    if not blockchain_tx_dir.exists():
        return {}

    blockchain_txs = {}

    # If chains not specified, find all ndjson files
    if chains is None:
        chains = [f.stem.upper() for f in blockchain_tx_dir.glob("*.ndjson")]

    for chain in chains:
        tx_file = blockchain_tx_dir / f"{chain.lower()}.ndjson"
        if not tx_file.exists():
            continue

        txs = {}
        with open(tx_file, 'r') as f:
            for line in f:
                tx_data = json.loads(line.strip())
                txid = tx_data.get('_original_txid', '').upper()
                if txid:
                    txs[txid] = tx_data

        if txs:
            blockchain_txs[chain] = txs

    return blockchain_txs


def get_tx_timestamp(tx_data: dict) -> int | None:
    """
    Extract Unix timestamp from blockchain transaction data.

    Handles both UTXO chains (int timestamp) and account chains (string timestamp).

    Args:
        tx_data: Transaction data from Blockchair API

    Returns:
        Unix timestamp (seconds) or None if not found
    """
    tx_info = tx_data.get('transaction', {})
    time_val = tx_info.get('time')

    if isinstance(time_val, int):
        return time_val
    elif isinstance(time_val, str):
        # Account chains format: "2025-12-31 20:10:59"
        dt = datetime.strptime(time_val, "%Y-%m-%d %H:%M:%S")
        return int(dt.timestamp())
    return None


def compute_time_diff(record: dict, blockchain_txs: dict[str, dict], warn_missing: bool = False) -> int | None:
    """
    Compute time difference (seconds) between in and out transactions.

    Args:
        record: THORChain swap record
        blockchain_txs: Dict mapping chain -> (txid -> tx_data)
        warn_missing: If True, print warning for missing tx hashes

    Returns:
        Time diff in seconds (out_ts - in_ts) or None if missing data
    """
    in_list = record.get('in', [])
    out_list = record.get('out', [])

    if not in_list or not out_list:
        return None

    in_entry = in_list[0]
    out_entry = out_list[0]

    in_chain = in_entry.get('chain', '')
    out_chain = out_entry.get('chain', '')
    in_txid = in_entry.get('txID', '').upper()
    out_txid = out_entry.get('txID', '').upper()

    # Get blockchain tx data
    in_tx = blockchain_txs.get(in_chain, {}).get(in_txid)
    out_tx = blockchain_txs.get(out_chain, {}).get(out_txid)

    if not in_tx:
        if warn_missing:
            print(f"[WARN] Missing blockchain tx: {in_chain}:{in_txid[:16]}...")
        return None
    if not out_tx:
        if warn_missing:
            print(f"[WARN] Missing blockchain tx: {out_chain}:{out_txid[:16]}...")
        return None

    in_ts = get_tx_timestamp(in_tx)
    out_ts = get_tx_timestamp(out_tx)

    if in_ts is None or out_ts is None:
        return None

    return out_ts - in_ts
