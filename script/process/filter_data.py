#!/usr/bin/env python3
"""
Filter THORChain swap data with flexible filtering options.

Supports filtering by:
- Amount (using level = 1/fee_rate)
- Height diff (THORChain blocks)
- Time diff (real blockchain time, requires blockchain_txs/)
- Date range

Usage:
    # Amount only (high value)
    python script/process/filter_data.py --amount-level-gte 10
    # Output: thorchain-2025-amtgte10

    # Amount + time diff (most common)
    python script/process/filter_data.py --amount-level-gte 10 --time-diff-lte 30
    # Output: thorchain-2025-amtgte10-dtlt30

    # Identify slow swaps
    python script/process/filter_data.py --height-diff-gte 5000 --start-date 2025-03-01 --end-date 2025-03-31
    # Output: thorchain-2025-dhgt5000
"""

import json
import argparse
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Amount level to threshold mapping (level = 1/fee_rate)
AMOUNT_LEVEL_TO_THRESHOLDS = {
    10: {"BTC": 10_000_000, "ETH": 200_000_000, "DOGE": 100_000_000_000},           # 0.1 BTC, 2.0 ETH, 1k DOGE
    20: {"BTC": 20_000_000, "ETH": 400_000_000, "DOGE": 200_000_000_000},           # 0.2 BTC, 4.0 ETH, 2k DOGE
    50: {"BTC": 50_000_000, "ETH": 1_000_000_000, "DOGE": 500_000_000_000},         # 0.5 BTC, 10.0 ETH, 5k DOGE
    100: {"BTC": 100_000_000, "ETH": 2_000_000_000, "DOGE": 1_000_000_000_000},     # 1.0 BTC, 20.0 ETH, 10k DOGE
}


def load_blockchain_txs(blockchain_tx_dir: Path, chain: str) -> dict:
    """Load blockchain transaction data from ndjson file.

    Each line contains raw Blockchair API response with structure:
    {
        "transaction": {...},
        "inputs": [...],  # UTXO chains only
        "outputs": [...],  # UTXO chains only
        "_original_txid": "..."  # Original txid from THORChain data
    }
    """
    tx_file = blockchain_tx_dir / f"{chain.lower()}.ndjson"

    if not tx_file.exists():
        return {}

    txs = {}
    with open(tx_file, 'r') as f:
        for line in f:
            tx_data = json.loads(line.strip())

            # Use original txid as key (without 0x prefix for consistency)
            txid = tx_data.get('_original_txid')
            if not txid:
                # Fallback: extract from transaction object
                tx_info = tx_data.get('transaction', {})
                txid = tx_info.get('hash', '').replace('0x', '').upper()

            if txid:
                txs[txid.upper()] = tx_data

    return txs


def get_tx_timestamp(tx_data: dict) -> int | None:
    """Extract Unix timestamp from Blockchair API response.

    Blockchair returns time as:
    - UTXO chains: "time": 1234567890 (Unix timestamp int)
    - Account chains: "time": "2025-12-31 20:10:59" (UTC string)
    """
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


def get_record_datetime(record: dict) -> datetime | None:
    """Convert record timestamp (nanoseconds) to datetime."""
    ts_ns = record.get("timestamp")
    if ts_ns is None:
        return None
    return datetime.fromtimestamp(int(ts_ns) / 1e9)


def get_amount(record: dict) -> tuple[str | None, int]:
    """Get asset and amount from record's first input."""
    in_list = record.get("in", [])
    if not in_list:
        return None, 0

    entry = in_list[0]
    asset = entry.get('asset', '')
    amount = int(entry.get('amount', 0))
    return asset, amount


def get_height_diff(record: dict) -> int | None:
    """Calculate height diff: out[0].thorchainHeight - in[0].thorchainHeight."""
    in_list = record.get("in", [])
    out_list = record.get("out", [])

    if not in_list or not out_list:
        return None

    in_height = int(in_list[0].get("thorchainHeight", 0))
    out_height = int(out_list[0].get("thorchainHeight", 0))

    if not in_height or not out_height:
        return None

    return out_height - in_height


def get_time_diff(record: dict, blockchain_txs: dict[str, dict]) -> int | None:
    """Calculate time diff in seconds using real blockchain timestamps."""
    in_list = record.get("in", [])
    out_list = record.get("out", [])

    if not in_list or not out_list:
        return None

    in_entry = in_list[0]
    out_entry = out_list[0]

    in_txid = in_entry.get('txID')
    in_asset = in_entry.get('asset')
    out_txid = out_entry.get('txID')
    out_asset = out_entry.get('asset')

    if not in_txid or not in_asset or not out_txid or not out_asset:
        return None

    # Get blockchain tx data
    in_chain_txs = blockchain_txs.get(in_asset, {})
    out_chain_txs = blockchain_txs.get(out_asset, {})

    in_tx_data = in_chain_txs.get(in_txid)
    out_tx_data = out_chain_txs.get(out_txid)

    if not in_tx_data or not out_tx_data:
        return None

    # Get timestamps
    in_time = get_tx_timestamp(in_tx_data)
    out_time = get_tx_timestamp(out_tx_data)

    if in_time is None or out_time is None:
        return None

    return out_time - in_time


def passes_filters(
    record: dict,
    amount_thresholds: dict | None,
    amount_gte: bool,
    height_diff_threshold: int | None,
    height_diff_gte: bool,
    time_diff_threshold_sec: int | None,
    time_diff_gte: bool,
    start_date: datetime | None,
    end_date: datetime | None,
    blockchain_txs: dict[str, dict] | None,
) -> tuple[bool, dict]:
    """
    Check if record passes all filters.

    Returns:
        (passes: bool, stats: dict)
    """
    stats = {}

    # Filter: Amount
    if amount_thresholds is not None:
        asset, amount = get_amount(record)
        stats['amount'] = amount
        stats['asset'] = asset

        if not asset:
            return False, stats

        threshold = amount_thresholds.get(asset, 0)

        if amount_gte:
            if amount < threshold:
                return False, stats
        else:  # lte
            if amount > threshold:
                return False, stats

    # Filter: Height diff
    if height_diff_threshold is not None:
        height_diff = get_height_diff(record)
        stats['height_diff'] = height_diff

        if height_diff is None:
            return False, stats

        if height_diff_gte:
            if height_diff < height_diff_threshold:
                return False, stats
        else:  # lte
            if height_diff > height_diff_threshold:
                return False, stats

    # Filter: Time diff
    if time_diff_threshold_sec is not None:
        if blockchain_txs is None:
            raise ValueError("Time diff filter requires blockchain_txs to be loaded")

        time_diff_sec = get_time_diff(record, blockchain_txs)
        stats['time_diff'] = time_diff_sec

        if time_diff_sec is None:
            return False, stats

        if time_diff_gte:
            if time_diff_sec < time_diff_threshold_sec:
                return False, stats
        else:  # lte
            if time_diff_sec > time_diff_threshold_sec:
                return False, stats

    # Filter: Date range
    if start_date or end_date:
        dt = get_record_datetime(record)

        if dt is None:
            return False, stats

        if start_date and dt < start_date:
            return False, stats

        if end_date and dt > end_date:
            return False, stats

    return True, stats


def filter_file(
    input_file: Path,
    output_file: Path,
    amount_thresholds: dict | None,
    amount_gte: bool,
    height_diff_threshold: int | None,
    height_diff_gte: bool,
    time_diff_threshold_sec: int | None,
    time_diff_gte: bool,
    start_date: datetime | None,
    end_date: datetime | None,
    blockchain_txs: dict[str, dict] | None,
) -> dict:
    """Filter a single ndjson file and return statistics."""
    counts = {'total': 0, 'kept': 0}
    metric_values = []  # For storing height_diff or time_diff values

    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(input_file, 'r') as f_in, open(output_file, 'w') as f_out:
        for line in f_in:
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except:
                continue

            counts['total'] += 1

            passes, stats = passes_filters(
                record,
                amount_thresholds,
                amount_gte,
                height_diff_threshold,
                height_diff_gte,
                time_diff_threshold_sec,
                time_diff_gte,
                start_date,
                end_date,
                blockchain_txs,
            )

            if passes:
                counts['kept'] += 1
                f_out.write(line + '\n')

                # Collect metric values
                if 'height_diff' in stats and stats['height_diff'] is not None:
                    metric_values.append(stats['height_diff'])
                elif 'time_diff' in stats and stats['time_diff'] is not None:
                    metric_values.append(stats['time_diff'])

    counts['metric_values'] = metric_values
    return counts


def build_output_dir_name(
    base_name: str,
    amount_level: int | None,
    amount_gte: bool,
    height_diff_threshold: int | None,
    height_diff_gte: bool,
    time_diff_threshold_min: int | None,
    time_diff_gte: bool,
) -> str:
    """Build output directory name based on filters."""
    parts = [base_name]

    if amount_level is not None:
        if amount_gte:
            parts.append(f"amtgte{amount_level}")
        else:
            parts.append(f"amtlte{amount_level}")

    if height_diff_threshold is not None:
        if height_diff_gte:
            parts.append(f"dhgte{height_diff_threshold}")
        else:
            parts.append(f"dhlte{height_diff_threshold}")

    if time_diff_threshold_min is not None:
        if time_diff_gte:
            parts.append(f"dtgte{time_diff_threshold_min}")
        else:
            parts.append(f"dtlte{time_diff_threshold_min}")

    return "-".join(parts)


def main():
    parser = argparse.ArgumentParser(description="Filter THORChain swap data")

    # Amount filters
    parser.add_argument("--amount-level-gte", type=int, choices=[10, 20, 50, 100],
                        help="Amount >= level threshold (10, 20, 50, 100)")
    parser.add_argument("--amount-level-lte", type=int, choices=[10, 20, 50, 100],
                        help="Amount <= level threshold (10, 20, 50, 100)")

    # Height diff filters
    parser.add_argument("--height-diff-lte", type=int,
                        help="Height diff <= threshold (blocks)")
    parser.add_argument("--height-diff-gte", type=int,
                        help="Height diff >= threshold (blocks)")

    # Time diff filters (in minutes)
    parser.add_argument("--time-diff-lte", type=int,
                        help="Time diff <= threshold (minutes)")
    parser.add_argument("--time-diff-gte", type=int,
                        help="Time diff >= threshold (minutes)")

    # Date range
    parser.add_argument("--start-date", type=str,
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str,
                        help="End date (YYYY-MM-DD)")

    # Input/output
    parser.add_argument("--input-dir", type=str, default=None,
                        help="Input directory (default: thorchain-2025)")
    parser.add_argument("--output-dir", type=str, default=None,
                        help="Output directory (default: auto-generated)")

    args = parser.parse_args()

    # Validate: Cannot have both gte and lte for the same filter
    if args.amount_level_gte and args.amount_level_lte:
        parser.error("Cannot specify both --amount-level-gte and --amount-level-lte")
    if args.height_diff_gte and args.height_diff_lte:
        parser.error("Cannot specify both --height-diff-gte and --height-diff-lte")
    if args.time_diff_gte and args.time_diff_lte:
        parser.error("Cannot specify both --time-diff-gte and --time-diff-lte")

    # Validate: At least one filter must be specified
    if not any([
        args.amount_level_gte, args.amount_level_lte,
        args.height_diff_gte, args.height_diff_lte,
        args.time_diff_gte, args.time_diff_lte,
    ]):
        parser.error("At least one filter must be specified")

    # Parse dates
    start_date = None
    end_date = None
    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)

    # Get script directory
    script_dir = Path(__file__).parent.parent.parent

    # Determine input directory
    if args.input_dir:
        input_dir = Path(args.input_dir)
    else:
        input_dir = script_dir / "thorchain-2025"

    # Prepare filter parameters
    amount_level = args.amount_level_gte or args.amount_level_lte
    amount_thresholds = AMOUNT_LEVEL_TO_THRESHOLDS.get(amount_level) if amount_level else None
    amount_gte = args.amount_level_gte is not None

    height_diff_threshold = args.height_diff_gte or args.height_diff_lte
    height_diff_gte = args.height_diff_gte is not None

    time_diff_threshold_min = args.time_diff_gte or args.time_diff_lte
    time_diff_threshold_sec = time_diff_threshold_min * 60 if time_diff_threshold_min else None
    time_diff_gte = args.time_diff_gte is not None

    # Determine output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir_name = build_output_dir_name(
            "thorchain-2025",
            amount_level,
            amount_gte,
            height_diff_threshold,
            height_diff_gte,
            time_diff_threshold_min,
            time_diff_gte,
        )
        output_dir = script_dir / output_dir_name

    # Load blockchain txs if needed
    blockchain_txs = None
    if time_diff_threshold_sec is not None:
        blockchain_tx_dir = script_dir / "blockchain_txs"

        # Check if blockchain_txs directory exists
        if not blockchain_tx_dir.exists():
            print(f"\n{'='*70}")
            print("ERROR: blockchain_txs/ directory not found!")
            print(f"{'='*70}")
            print(f"\nTime diff filter requires blockchain transaction data.")
            print(f"Expected directory: {blockchain_tx_dir}")
            print(f"\nPlease run the following steps:")
            print(f"1. Filter by amount first to create intermediate dataset:")
            print(f"   uv run python script/process/filter_data.py --amount-level-gte 10 --output-dir thorchain-2025-amtgte10")
            print(f"2. Fetch blockchain transactions:")
            print(f"   uv run python script/enrich/fetch_blockchain_txs.py")
            print(f"3. Then run this command again with --time-diff-lte")
            print(f"{'='*70}\n")
            exit(1)

        print("Loading blockchain transaction data...")
        blockchain_txs = {}
        missing_data = []

        for asset in ["BTC", "ETH", "DOGE"]:
            print(f"  Loading {asset}...", end=" ", flush=True)
            txs = load_blockchain_txs(blockchain_tx_dir, asset)
            blockchain_txs[asset] = txs

            if not txs:
                missing_data.append(asset)
                print(f"⚠ MISSING")
            else:
                print(f"✓ {len(txs):,} transactions")

        if missing_data:
            print(f"\n{'='*70}")
            print(f"ERROR: Missing blockchain transaction data!")
            print(f"{'='*70}")
            print(f"\nMissing data for: {', '.join(missing_data)}")
            print(f"\nExpected files:")
            for asset in missing_data:
                print(f"  {blockchain_tx_dir / f'{asset.lower()}.ndjson'}")
            print(f"\nPlease fetch blockchain transactions first:")
            print(f"  uv run python script/enrich/fetch_blockchain_txs.py")
            print(f"{'='*70}\n")
            exit(1)

        print()

    # Print configuration
    print(f"\n{'='*70}")
    print("THORChain Data Filter")
    print(f"{'='*70}")
    print(f"Input:  {input_dir}")
    print(f"Output: {output_dir}")
    print(f"\nFilters:")

    if amount_thresholds:
        op = ">=" if amount_gte else "<="
        print(f"  Amount {op} level {amount_level}:")
        print(f"    BTC {op} {amount_thresholds['BTC']/1e8:.1f}")
        print(f"    ETH {op} {amount_thresholds['ETH']/1e8:.1f}")
        print(f"    DOGE {op} {amount_thresholds['DOGE']/1e8:.0f}")

    if height_diff_threshold:
        op = ">=" if height_diff_gte else "<="
        print(f"  Height diff {op} {height_diff_threshold} blocks")

    if time_diff_threshold_sec:
        op = ">=" if time_diff_gte else "<="
        print(f"  Time diff {op} {time_diff_threshold_min} min ({time_diff_threshold_sec}s)")

    if start_date:
        print(f"  Start date: {args.start_date}")
    if end_date:
        print(f"  End date: {args.end_date}")

    print(f"{'='*70}\n")

    # Find input files
    files = list(input_dir.glob("*.ndjson"))
    if not files:
        print(f"No ndjson files found in {input_dir}")
        return

    # Filter files
    total_counts = defaultdict(int)
    all_metric_values = []

    for file in sorted(files):
        output_file = output_dir / file.name

        print(f"Processing {file.name}...", end=" ", flush=True)

        counts = filter_file(
            file,
            output_file,
            amount_thresholds,
            amount_gte,
            height_diff_threshold,
            height_diff_gte,
            time_diff_threshold_sec,
            time_diff_gte,
            start_date,
            end_date,
            blockchain_txs,
        )

        total_counts['total'] += counts['total']
        total_counts['kept'] += counts['kept']
        all_metric_values.extend(counts['metric_values'])

        pct = counts['kept'] / counts['total'] * 100 if counts['total'] > 0 else 0
        print(f"{counts['total']:6,} -> {counts['kept']:6,} ({pct:5.1f}%)")

    # Summary
    pct = total_counts['kept'] / total_counts['total'] * 100 if total_counts['total'] > 0 else 0
    print(f"{'-'*70}")
    print(f"{'Total':20} {total_counts['total']:6,} -> {total_counts['kept']:6,} ({pct:5.1f}%)")

    # Metric statistics
    if all_metric_values:
        all_metric_values.sort()
        metric_name = "Height diff" if height_diff_threshold else "Time diff"
        unit = "blocks" if height_diff_threshold else "seconds"

        print(f"\n{metric_name} statistics (kept records):")
        print(f"  Min: {all_metric_values[0]} {unit}")
        print(f"  Median: {all_metric_values[len(all_metric_values)//2]} {unit}")
        print(f"  Max: {all_metric_values[-1]} {unit}")

        if not height_diff_threshold:  # time diff
            print(f"  (Min: {all_metric_values[0]/60:.1f} min, "
                  f"Median: {all_metric_values[len(all_metric_values)//2]/60:.1f} min, "
                  f"Max: {all_metric_values[-1]/60:.1f} min)")

    print(f"\n{'='*70}")
    print(f"✓ Output saved to: {output_dir}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
