#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extract THORChain artifacts for each group.

This script:
1. Reads common_ancestor_depth_N.ndjson files from data/partial_a5a023/
2. For each group, extracts all cc_txids from endpoints
3. Finds matching records in thorchain-2025/ETH-BTC.ndjson
4. Saves each group's THORChain records to thor-artifacts/{group_id}.ndjson

Usage:
    uv run python extract_thor_artifacts.py
"""

import json
from pathlib import Path
from typing import Dict, Set


SCRIPT_DIR = Path(__file__).parent  # scripts/utils/
BYBITHACK_DIR = SCRIPT_DIR.parent.parent  # bybithack/
DATA_DIR = BYBITHACK_DIR / "data" / "partial_a5a023"
THOR_ARTIFACTS_DIR = BYBITHACK_DIR / ".local" / "thor-artifacts"
THORCHAIN_FILE = BYBITHACK_DIR.parent / "data" / "thorchain-2025" / "ETH-BTC.ndjson"


def normalize_txid(txid: str) -> str:
    """Normalize transaction ID (lowercase, no 0x prefix)."""
    return txid.lower().replace('0x', '')


def load_thorchain_data() -> Dict[str, Dict]:
    """
    Load all thorchain records and index by in.txID.

    Returns:
        Dict mapping normalized txid -> thorchain record
    """
    print(f"[INFO] Loading THORChain data from {THORCHAIN_FILE.name}...")

    thor_records = {}

    if not THORCHAIN_FILE.exists():
        print(f"[ERROR] THORChain file not found: {THORCHAIN_FILE}")
        return thor_records

    with open(THORCHAIN_FILE, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
                in_list = record.get('in', [])

                # Index by in.txID (ETH transaction)
                for in_entry in in_list:
                    txid = in_entry.get('txID', '')
                    if txid:
                        normalized_txid = normalize_txid(txid)
                        thor_records[normalized_txid] = record

            except json.JSONDecodeError:
                continue

    print(f"[INFO] Loaded {len(thor_records)} THORChain records")
    return thor_records


def extract_group_artifacts(group: Dict, thor_records: Dict[str, Dict]) -> list:
    """
    Extract THORChain artifacts for a group.

    Args:
        group: Group data with endpoints
        thor_records: Dict of thorchain records indexed by txid

    Returns:
        List of thorchain records for this group
    """
    artifacts = []
    found_txids = set()

    # Collect all cc_txids from endpoints
    for endpoint in group['endpoints']:
        cc_txid = endpoint['cc_txid']
        normalized_txid = normalize_txid(cc_txid)

        # Find in thorchain records
        if normalized_txid in thor_records:
            if normalized_txid not in found_txids:
                artifacts.append(thor_records[normalized_txid])
                found_txids.add(normalized_txid)

    return artifacts


def process_ndjson_file(input_file: Path, thor_records: Dict[str, Dict]):
    """
    Process a single ndjson file and extract artifacts for each group.

    Args:
        input_file: Path to input ndjson file
        thor_records: Dict of thorchain records
    """
    print(f"\n[INFO] Processing {input_file.name}...")

    with open(input_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                group = json.loads(line)
                group_id = group['group_id']
                endpoints_count = group['endpoints_count']

                # Extract artifacts
                artifacts = extract_group_artifacts(group, thor_records)

                # Save to file
                output_file = THOR_ARTIFACTS_DIR / f"{group_id}.ndjson"
                output_file.parent.mkdir(parents=True, exist_ok=True)

                with open(output_file, 'w') as out_f:
                    for artifact in artifacts:
                        out_f.write(json.dumps(artifact, ensure_ascii=False) + '\n')

                print(f"  ✓ {group_id}: {len(artifacts)}/{endpoints_count} artifacts -> {output_file.name}")

            except json.JSONDecodeError as e:
                print(f"  ✗ Error parsing line: {e}")
                continue


def main():
    print("=" * 70)
    print("Extract THORChain Artifacts for Groups")
    print("=" * 70)
    print()

    # Load thorchain data
    thor_records = load_thorchain_data()

    if not thor_records:
        print("[ERROR] No THORChain records loaded!")
        return

    # Find all depth ndjson files
    ndjson_files = sorted(DATA_DIR.glob("common_ancestor_depth_*.ndjson"))

    if not ndjson_files:
        print(f"[ERROR] No common_ancestor_depth_*.ndjson files found in {DATA_DIR}")
        return

    print(f"[INFO] Found {len(ndjson_files)} ndjson files")

    # Process each file
    total_groups = 0
    total_artifacts = 0

    for ndjson_file in ndjson_files:
        process_ndjson_file(ndjson_file, thor_records)

        # Count processed groups and artifacts
        with open(ndjson_file, 'r') as f:
            for line in f:
                if line.strip():
                    total_groups += 1

    # Count total artifacts
    artifact_files = list(THOR_ARTIFACTS_DIR.glob("*.ndjson"))
    for artifact_file in artifact_files:
        with open(artifact_file, 'r') as f:
            for line in f:
                if line.strip():
                    total_artifacts += 1

    print()
    print("=" * 70)
    print(f"✓ Processed {total_groups} groups")
    print(f"✓ Extracted {total_artifacts} THORChain artifacts")
    print(f"✓ Output: {THOR_ARTIFACTS_DIR}")
    print("=" * 70)


if __name__ == "__main__":
    main()
