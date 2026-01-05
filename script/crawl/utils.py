#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Shared utilities for THORChain data processing.

Functions:
- canonical_action_key: Generate unique key for action deduplication
- load_seen_keys: Load existing keys from ndjson file
- get_min_timestamp_from_ndjson: Find minimum timestamp in ndjson file
- get_max_timestamp_from_ndjson: Find maximum timestamp in ndjson file
- append_ndjson: Append records to ndjson with deduplication
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional


def canonical_action_key(action: Dict[str, Any]) -> str:
    """
    Generate canonical key for action deduplication.

    Key format: {date}|{height}|{type}|{status}|{memo}|in:{txids}|out:{txids}

    Args:
        action: Action record from Midgard API

    Returns:
        Canonical key string
    """
    date = action.get("date", "")
    height = action.get("height", "")
    typ = action.get("type", "")
    status = action.get("status", "")
    memo = action.get("memo", "")

    def collect_txids(side: str) -> List[str]:
        txids: List[str] = []
        for item in action.get(side, []) or []:
            txid = (item or {}).get("txID", "")
            if txid:
                txids.append(txid)
        return sorted(set(txids))

    in_tx = ",".join(collect_txids("in"))
    out_tx = ",".join(collect_txids("out"))
    return f"{date}|{height}|{typ}|{status}|{memo}|in:{in_tx}|out:{out_tx}"


def load_seen_keys(ndjson_path: Path, cap_lines: int = 2_000_000, log_func=None) -> set:
    """
    Load canonical keys from existing ndjson file for deduplication.

    Args:
        ndjson_path: Path to ndjson file
        cap_lines: Maximum lines to read (default: 2M)
        log_func: Optional logging function

    Returns:
        Set of canonical keys
    """
    if not ndjson_path.exists():
        return set()
    keys = set()
    with ndjson_path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i >= cap_lines:
                if log_func:
                    log_func(f"[WARN] dedup key load capped at {cap_lines} lines for {ndjson_path.name}")
                break
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                keys.add(canonical_action_key(obj))
            except Exception:
                continue
    return keys


def get_min_timestamp_from_ndjson(ndjson_path: Path) -> Optional[int]:
    """
    Scan ndjson file to find the minimum timestamp (in nanoseconds).

    Args:
        ndjson_path: Path to ndjson file

    Returns:
        Minimum timestamp in nanoseconds, or None if file doesn't exist or is empty
    """
    if not ndjson_path.exists():
        return None

    min_ts: Optional[int] = None
    with ndjson_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                date = int(obj.get("date", "0"))
                if date > 0:
                    if min_ts is None or date < min_ts:
                        min_ts = date
            except Exception:
                continue
    return min_ts


def get_max_timestamp_from_ndjson(ndjson_path: Path) -> Optional[int]:
    """
    Scan ndjson file to find the maximum timestamp (in nanoseconds).

    Args:
        ndjson_path: Path to ndjson file

    Returns:
        Maximum timestamp in nanoseconds, or None if file doesn't exist or is empty
    """
    if not ndjson_path.exists():
        return None

    max_ts: Optional[int] = None
    with ndjson_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                date = int(obj.get("date", "0"))
                if date > 0:
                    if max_ts is None or date > max_ts:
                        max_ts = date
            except Exception:
                continue
    return max_ts


def append_ndjson(path: Path, records: List[Dict[str, Any]], seen: set) -> int:
    """
    Append records to ndjson file with deduplication.

    Args:
        path: Path to ndjson file
        records: List of action records to append
        seen: Set of canonical keys already seen (will be updated)

    Returns:
        Number of records actually appended (after deduplication)
    """
    appended = 0
    with path.open("a", encoding="utf-8") as f:
        for r in records:
            k = canonical_action_key(r)
            if k in seen:
                continue
            seen.add(k)
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
            appended += 1
    return appended
