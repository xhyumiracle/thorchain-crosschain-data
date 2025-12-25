#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Thorchain data washer: transforms raw ndjson/json files into cleaned format.

Usage:
    python thorchain_wash.py --indir data/thorchain/raw --outdir data/thorchain/data
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set


def parse_asset_string(asset_str: str) -> tuple[str, str]:
    """
    Parse "CHAIN.ASSET" format.
    e.g. "DOGE.DOGE" -> ("DOGE", "DOGE")
         "ETH.USDC" -> ("ETH", "USDC")
    """
    asset_str = asset_str.strip().upper()
    if "." in asset_str:
        parts = asset_str.split(".", 1)
        return parts[0], parts[1]
    return asset_str, asset_str


def compute_record_id(record: Dict[str, Any]) -> str:
    """
    Compute deterministic ID using SHA-256.

    Canonical string format:
    1) For each in/out entry: "{direction}|{chain}|{asset}|{address}|{txID}"
    2) Deduplicate and sort
    3) Join with newline + "{type}|{status}"
    """
    entries: Set[str] = set()

    typ = str(record.get("type", "")).strip().lower()
    status = str(record.get("status", "")).strip().lower()

    for item in record.get("in", []) or []:
        address = str(item.get("address", "")).strip()
        txid = str(item.get("txID", "")).strip()
        for coin in item.get("coins", []) or []:
            asset_raw = str(coin.get("asset", "")).strip()
            chain, asset = parse_asset_string(asset_raw)
            entry = f"in|{chain}|{asset}|{address}|{txid}"
            entries.add(entry)

    for item in record.get("out", []) or []:
        address = str(item.get("address", "")).strip()
        txid = str(item.get("txID", "")).strip()
        for coin in item.get("coins", []) or []:
            asset_raw = str(coin.get("asset", "")).strip()
            chain, asset = parse_asset_string(asset_raw)
            entry = f"out|{chain}|{asset}|{address}|{txid}"
            entries.add(entry)

    sorted_entries = sorted(entries)
    canonical = "\n".join(sorted_entries) + f"\n{typ}|{status}"

    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def transform_record(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Transform a raw record to cleaned format.
    Returns None if record should be filtered out.
    """
    # Filter: only success + swap
    status = str(raw.get("status", "")).strip().lower()
    typ = str(raw.get("type", "")).strip().lower()

    if status != "success" or typ != "swap":
        return None

    # Outer height applies to all in entries
    outer_height = int(raw.get("height", 0))

    # Build in list
    in_list: List[Dict[str, Any]] = []
    for item in raw.get("in", []) or []:
        address = str(item.get("address", "")).strip()
        txid = str(item.get("txID", "")).strip()
        for coin in item.get("coins", []) or []:
            asset_raw = str(coin.get("asset", "")).strip()
            chain, asset = parse_asset_string(asset_raw)
            amount = str(coin.get("amount", "")).strip()
            in_list.append({
                "chain": chain,
                "asset": asset,
                "txID": txid,
                "address": address,
                "amount": amount,
                "thorchainHeight": outer_height,
            })

    # Build out list, filter out THOR.* assets
    out_list_raw: List[Dict[str, Any]] = []
    for item in raw.get("out", []) or []:
        address = str(item.get("address", "")).strip()
        txid = str(item.get("txID", "")).strip()
        item_height = int(item.get("height", 0))
        for coin in item.get("coins", []) or []:
            asset_raw = str(coin.get("asset", "")).strip()
            # Filter out THOR.* assets
            if asset_raw.upper().startswith("THOR."):
                continue
            chain, asset = parse_asset_string(asset_raw)
            amount = str(coin.get("amount", "")).strip()
            out_list_raw.append({
                "chain": chain,
                "asset": asset,
                "txID": txid,
                "address": address,
                "amount": amount,
                "thorchainHeight": item_height,
            })

    result = {
        "id": compute_record_id(raw),
        "timestamp": raw.get("date", ""),
        "type": typ,
        "status": status,
        "in": in_list,
        "out": out_list_raw,
    }

    return result


def get_output_filename(record: Dict[str, Any]) -> Optional[str]:
    """
    Determine output filename based on in/out chains.
    Format: {in_chain}-{out_chain}.ndjson

    Special cases (in priority order):
    - multi-in.ndjson: multiple in entries
    - multi-out.ndjson: multiple out entries
    - multi-in-out.ndjson: both multiple in and multiple out
    - multi-coins-in.ndjson: single in entry but with >1 coins (same txID)
    - multi-coins-out.ndjson: single out entry but with >1 coins (same txID)
    - multi-coins-in-out.ndjson: both in and out have >1 coins per entry
    """
    in_list = record.get("in", []) or []
    out_list = record.get("out", []) or []

    in_chains: Set[str] = set()
    out_chains: Set[str] = set()

    for item in in_list:
        in_chains.add(item.get("chain", ""))

    for item in out_list:
        out_chains.add(item.get("chain", ""))

    if not in_chains or not out_chains:
        return None

    multi_in = len(in_list) > 1
    multi_out = len(out_list) > 1
    record_id = record.get("id", "unknown")

    # Priority 1: multi-in / multi-out (multiple entries)
    if multi_in and multi_out:
        print(f"[WARN] Multi-in AND multi-out: id={record_id}")
        return "multi-in-out.ndjson"
    elif multi_in:
        print(f"[WARN] Multi-in: id={record_id}")
        return "multi-in.ndjson"
    elif multi_out:
        print(f"[WARN] Multi-out: id={record_id}")
        return "multi-out.ndjson"

    # Priority 2: multi-coins (single entry but with >1 coins, detected by same txID)
    # Count entries per txID for in list
    in_txid_counts: Dict[str, int] = {}
    for item in in_list:
        txid = item.get("txID", "")
        in_txid_counts[txid] = in_txid_counts.get(txid, 0) + 1
    multi_coins_in = any(c > 1 for c in in_txid_counts.values())

    # Count entries per txID for out list
    out_txid_counts: Dict[str, int] = {}
    for item in out_list:
        txid = item.get("txID", "")
        out_txid_counts[txid] = out_txid_counts.get(txid, 0) + 1
    multi_coins_out = any(c > 1 for c in out_txid_counts.values())

    if multi_coins_in and multi_coins_out:
        print(f"[WARN] Multi-coins-in AND multi-coins-out: id={record_id}")
        return "multi-coins-in-out.ndjson"
    elif multi_coins_in:
        print(f"[WARN] Multi-coins-in: id={record_id}")
        return "multi-coins-in.ndjson"
    elif multi_coins_out:
        print(f"[WARN] Multi-coins-out: id={record_id}")
        return "multi-coins-out.ndjson"

    # Normal case: single in, single out
    in_chain = sorted(in_chains)[0]
    out_chain = sorted(out_chains)[0]

    return f"{in_chain}-{out_chain}.ndjson"


def process_file(filepath: Path) -> List[tuple[str, Dict[str, Any]]]:
    """
    Process a single json/ndjson file.
    Returns list of (output_filename, transformed_record) tuples.
    """
    results: List[tuple[str, Dict[str, Any]]] = []

    # Skip state files
    if filepath.name == "state.json":
        print(f"[INFO] Skipping state file: {filepath}")
        return results

    content = filepath.read_text(encoding="utf-8").strip()
    if not content:
        return results

    records: List[Dict[str, Any]] = []

    try:
        # Try to detect format
        if content.startswith("["):
            # JSON array
            records = json.loads(content)
        elif content.startswith("{"):
            # Could be single JSON object or ndjson
            # First try parsing as single object
            try:
                obj = json.loads(content)
                records = [obj]
            except json.JSONDecodeError:
                # Try ndjson (each line is a JSON object)
                for line in content.split("\n"):
                    line = line.strip()
                    if line:
                        records.append(json.loads(line))
        else:
            # Assume ndjson
            for line in content.split("\n"):
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    except json.JSONDecodeError as e:
        print(f"[WARN] Failed to parse {filepath}: {e}")
        return results

    for raw in records:
        transformed = transform_record(raw)
        if transformed is None:
            continue

        output_name = get_output_filename(transformed)
        if output_name is None:
            continue

        results.append((output_name, transformed))

    return results


def main() -> None:
    ap = argparse.ArgumentParser(description="Transform raw Thorchain data to cleaned format")
    ap.add_argument("--indir", type=str, required=True, help="Input directory with raw ndjson/json files")
    ap.add_argument("--outdir", type=str, default="data/thorchain/data", help="Output directory")
    ap.add_argument("--dry-run", action="store_true", help="Print output without writing files")
    args = ap.parse_args()

    indir = Path(args.indir)
    outdir = Path(args.outdir)

    if not indir.exists():
        raise SystemExit(f"Input directory does not exist: {indir}")

    if not args.dry_run:
        outdir.mkdir(parents=True, exist_ok=True)

    # Collect all json/ndjson files
    files = list(indir.glob("**/*.json")) + list(indir.glob("**/*.ndjson"))

    if not files:
        print(f"[WARN] No json/ndjson files found in {indir}")
        return

    print(f"[INFO] Found {len(files)} files to process")

    # Group results by output filename
    output_data: Dict[str, List[Dict[str, Any]]] = {}

    for filepath in files:
        print(f"[INFO] Processing {filepath}")
        results = process_file(filepath)
        for output_name, record in results:
            if output_name not in output_data:
                output_data[output_name] = []
            output_data[output_name].append(record)

    # Write outputs
    total_records = 0
    for output_name, records in output_data.items():
        total_records += len(records)
        output_path = outdir / output_name

        if args.dry_run:
            print(f"\n[DRY-RUN] Would write {len(records)} records to {output_path}")
            for r in records:
                print(json.dumps(r, indent=2, ensure_ascii=False))
        else:
            with output_path.open("w", encoding="utf-8") as f:
                for r in records:
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")
            print(f"[INFO] Wrote {len(records)} records to {output_path}")

    print(f"\n[INFO] Done. Total records processed: {total_records}")


if __name__ == "__main__":
    main()
