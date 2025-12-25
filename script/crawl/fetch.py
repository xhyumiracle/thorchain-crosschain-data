#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Midgard v2 /actions crawler using per-asset timestamp cursor (backwards).

New design (v2):
- Each asset pair has its own cursor_ts and offset
- Cursor moves backward by tracking min timestamp in each batch
- Stops when all assets reach min_ts boundary
- Supports seamless resume from existing ndjson files

Safety:
- If data exists and neither --resume nor --fresh is provided, exit with a clear message.

Output:
- outdir/state.json
- outdir/data/<slug>.ndjson for each assets entry
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TextIO

import requests


# Global log file handle
_log_file: Optional[TextIO] = None


def local_time_str() -> str:
    """Get current local time as formatted string."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    """Print message with local timestamp to both stdout and log file."""
    timestamped = f"[{local_time_str()}] {msg}"
    print(timestamped)
    if _log_file is not None:
        _log_file.write(timestamped + "\n")
        _log_file.flush()


DEFAULT_BASE_URLS = [
    "https://midgard.thorchain.liquify.com",
    # "https://midgard.ninerealms.com",  # Often rate-limited more aggressively
]

DEFAULT_ASSETS = [
    "BTC.BTC,DOGE.DOGE",
    "BTC.BTC,ETH.ETH",
    "ETH.ETH,DOGE.DOGE",
]


@dataclass
class AssetCursor:
    """Per-asset crawl state."""
    ts: int  # current timestamp cursor (nanoseconds)
    offset: int = 0  # offset for same timestamp
    finished: bool = False  # reached min_ts boundary
    cooldown_until: float = 0.0  # unix timestamp when this asset can be requested again


def now_ns() -> int:
    """Current time in nanoseconds."""
    return int(time.time() * 1_000_000_000)


def now_ts() -> int:
    """Current time in seconds."""
    return int(time.time())


def ns_to_sec(ns: int) -> int:
    """Convert nanoseconds to seconds."""
    if ns > 10_000_000_000:
        return ns // 1_000_000_000
    return ns


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def load_json(path: Path) -> Dict[str, Any]:
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_json_atomic(path: Path, obj: Dict[str, Any]) -> None:
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True)
    tmp.replace(path)


def choose_base_url(urls: List[str], idx: int) -> str:
    return urls[idx % len(urls)].rstrip("/")


def slugify_assets(assets_param: str) -> str:
    s = assets_param.strip().replace(",", "__")
    s = re.sub(r"[^A-Za-z0-9._~-]+", "_", s)
    return s


def single_request(
    session: requests.Session,
    url: str,
    params: Dict[str, Any],
    timeout: int,
    base_sleep: float,
    max_sleep: float,
    attempt: int,
    base_urls: Optional[List[str]] = None,
    base_url_idx_ref: Optional[List[int]] = None,
) -> Tuple[Optional[requests.Response], Optional[str], float]:
    """
    Make a single request attempt.
    Returns: (response, error, cooldown_seconds)
    - If successful: (response, None, 0)
    - If retryable error: (None, None, cooldown_seconds)
    - If fatal error: (None, error_message, 0)
    """
    try:
        resp = session.get(url, params=params, timeout=timeout)
    except requests.RequestException as e:
        wait = min(max_sleep, base_sleep * (2 ** attempt) + random.uniform(0, 0.6))
        log(f"[WARN] net error: {e}; cooldown {wait:.2f}s (attempt {attempt+1})")
        return None, None, wait

    if resp.status_code == 200:
        return resp, None, 0

    if resp.status_code in (403, 429, 500, 502, 503, 504):
        retry_after = resp.headers.get("Retry-After")
        if retry_after is not None:
            try:
                wait = float(retry_after)
            except ValueError:
                wait = base_sleep * (2 ** attempt)
        else:
            # 403 (Cloudflare) needs longer cooldown
            if resp.status_code == 403:
                wait = max(30.0, base_sleep * (2 ** attempt))
            else:
                wait = base_sleep * (2 ** attempt)

        wait = min(max_sleep, wait + random.uniform(0, 5.0))
        body_preview = resp.text[:200] if resp.text else "(empty)"
        log(f"[WARN] HTTP {resp.status_code}; cooldown {wait:.2f}s (attempt {attempt+1})")
        log(f"[WARN] response body: {body_preview}")

        # On 403, switch to another base URL if multiple available
        # Note: switch takes effect on next request (after cooldown)
        if resp.status_code == 403 and base_urls and base_url_idx_ref is not None and len(base_urls) > 1:
            base_url_idx_ref[0] += 1
            new_base = base_urls[base_url_idx_ref[0] % len(base_urls)]
            log(f"[INFO] will use {new_base} after cooldown")

        return None, None, wait

    return None, f"HTTP {resp.status_code}: {resp.text[:400]}", 0


def try_fetch_actions_page(
    session: requests.Session,
    base_url: str,
    typ: str,
    assets: str,
    cursor_ts: int,
    offset: int,
    limit: int,
    timeout: int,
    base_sleep: float,
    max_sleep: float,
    attempt: int,
    base_urls: Optional[List[str]] = None,
    base_url_idx_ref: Optional[List[int]] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], float]:
    """
    Try to fetch one page. Returns (data, error, cooldown_seconds).
    - Success: (data, None, 0)
    - Retryable: (None, None, cooldown_seconds)
    - Fatal: (None, error, 0)
    """
    endpoint = f"{base_url.rstrip('/')}/v2/actions"
    params = {
        "type": typ,
        "asset": assets,
        "timestamp": cursor_ts,
        "offset": offset,
        "limit": limit,
    }
    resp, err, cooldown = single_request(
        session=session,
        url=endpoint,
        params=params,
        timeout=timeout,
        base_sleep=base_sleep,
        max_sleep=max_sleep,
        attempt=attempt,
        base_urls=base_urls,
        base_url_idx_ref=base_url_idx_ref,
    )
    if err is not None:
        return None, err, 0
    if cooldown > 0:
        return None, None, cooldown
    try:
        return resp.json(), None, 0
    except Exception as e:
        return None, f"json decode error: {e}", 0


def canonical_action_key(action: Dict[str, Any]) -> str:
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


def load_seen_keys(ndjson_path: Path, cap_lines: int = 2_000_000) -> set:
    if not ndjson_path.exists():
        return set()
    keys = set()
    with ndjson_path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i >= cap_lines:
                log(f"[WARN] dedup key load capped at {cap_lines} lines for {ndjson_path.name}")
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
    Returns None if file doesn't exist or is empty.
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


def append_ndjson(path: Path, records: List[Dict[str, Any]], seen: set) -> int:
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


def main() -> None:
    global _log_file

    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", type=str, default="midgard_dataset_ts", help="Output directory")
    ap.add_argument("--log-file", type=str, default=None, help="Log file path (default: outdir/crawl.log)")
    ap.add_argument("--type", type=str, default="swap", help="actions type (default: swap)")
    ap.add_argument(
        "--assets",
        action="append",
        default=[],
        help='Repeatable. Each is passed as the "asset" query param, e.g. "BTC.BTC,DOGE.DOGE". '
             "If omitted, defaults to BTC/DOGE, BTC/ETH, ETH/DOGE."
    )

    ap.add_argument("--limit", type=int, default=50, help="page size (Midgard typically max=50)")
    ap.add_argument("--sleep-between-requests", type=float, default=0.3, help="baseline throttle (seconds)")
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--max-retries", type=int, default=10)
    ap.add_argument("--base-sleep", type=float, default=2.5)
    ap.add_argument("--max-sleep", type=float, default=240.0)
    ap.add_argument("--base-urls", type=str, default=",".join(DEFAULT_BASE_URLS),
                    help="comma-separated base urls for rotation")

    ap.add_argument("--resume", action="store_true", help="Resume from existing data (reads min timestamp from ndjson files)")
    ap.add_argument(
        "--fresh",
        action="store_true",
        help="Start a fresh crawl (requires clearing existing data first)."
    )

    ap.add_argument("--min-ts", type=int, default=None, help="Lower bound unix timestamp (seconds), inclusive. Stop when reaching this.")
    ap.add_argument("--max-ts", type=int, default=None, help="Upper bound unix timestamp (seconds), inclusive. Start from here.")

    ap.add_argument("--no-dedup", action="store_true", help="Disable dedup (faster, reruns may duplicate)")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    ensure_dir(outdir)
    state_path = outdir / "state.json"
    data_dir = outdir / "data"

    # Initialize log file
    log_path = Path(args.log_file) if args.log_file else outdir / "crawl.log"
    _log_file = log_path.open("a", encoding="utf-8")
    log(f"=== Log session started ===")

    # Check for existing data
    has_data = data_dir.exists() and any(data_dir.iterdir())
    has_state = state_path.exists()

    # Safety gate: no flag provided but data exists
    if not args.resume and not args.fresh and (has_data or has_state):
        msg_parts = ["Found existing data in output directory:"]
        if has_state:
            msg_parts.append(f"  - {state_path.as_posix()}")
        if has_data:
            msg_parts.append(f"  - {data_dir.as_posix()}/")
        msg_parts.append("")
        msg_parts.append("Choose one:")
        msg_parts.append("  --resume  : continue from last checkpoint")
        msg_parts.append("  --fresh   : start over (requires clearing data first)")
        raise SystemExit("\n".join(msg_parts))

    # Safety gate for --fresh with existing data
    if args.fresh and (has_data or has_state):
        msg_parts = ["--fresh requested but found existing data:"]
        if has_state:
            msg_parts.append(f"  - {state_path.as_posix()}")
        if has_data:
            msg_parts.append(f"  - {data_dir.as_posix()}/")
        msg_parts.append("")
        msg_parts.append("To avoid accidental data loss, please clear them first:")
        msg_parts.append(f"  rm -rf {state_path.as_posix()} {data_dir.as_posix()}")
        msg_parts.append("")
        msg_parts.append("Then re-run with --fresh.")
        raise SystemExit("\n".join(msg_parts))

    base_urls = [u.strip().rstrip("/") for u in args.base_urls.split(",") if u.strip()]
    if not base_urls:
        raise SystemExit("No base URLs provided")

    if args.limit <= 0 or args.limit > 50:
        log("[WARN] Midgard usually caps limit at 50; limit>50 may be ignored by server.")

    assets_list = args.assets if args.assets else DEFAULT_ASSETS
    assets_list = [a.strip() for a in assets_list if a.strip()]
    if not assets_list:
        raise SystemExit("No assets configured (use --assets or rely on defaults).")

    # Convert min_ts to nanoseconds for comparison with action dates
    min_ts_ns: Optional[int] = None
    if args.min_ts is not None:
        min_ts_ns = args.min_ts * 1_000_000_000

    # Prepare output files and dedup sets
    ensure_dir(data_dir)
    files: Dict[str, Path] = {}
    seen: Dict[str, set] = {}
    cursors: Dict[str, AssetCursor] = {}

    for assets in assets_list:
        slug = slugify_assets(assets)
        ndjson_path = data_dir / f"{slug}.ndjson"
        files[assets] = ndjson_path

        if args.no_dedup:
            seen[assets] = set()
        else:
            log(f"[INFO] loading dedup keys for assets={assets} ...")
            seen[assets] = load_seen_keys(ndjson_path)

        # Initialize cursor for this asset
        if args.resume and ndjson_path.exists():
            # Resume: start from min timestamp found in existing data
            existing_min_ts = get_min_timestamp_from_ndjson(ndjson_path)
            if existing_min_ts is not None:
                log(f"[INFO] {assets}: resuming from min_ts={existing_min_ts} ({ns_to_sec(existing_min_ts)} sec)")
                cursors[assets] = AssetCursor(ts=existing_min_ts, offset=0)
            else:
                # No data yet, start from now or max_ts
                start_ts = now_ns()
                if args.max_ts is not None:
                    start_ts = min(start_ts, args.max_ts * 1_000_000_000)
                cursors[assets] = AssetCursor(ts=start_ts, offset=0)
        else:
            # Fresh start: from now or max_ts
            start_ts = now_ns()
            if args.max_ts is not None:
                start_ts = min(start_ts, args.max_ts * 1_000_000_000)
            cursors[assets] = AssetCursor(ts=start_ts, offset=0)

    session = requests.Session()
    session.headers.update({
        "User-Agent": "midgard-ts-crawler/0.5 (+research; slow-crawl; respect-rate-limit)",
        "Accept": "application/json",
    })

    base_url_idx_ref = [0]  # Use list for mutable reference in nested function
    total_requests = 0
    total_errors = 0
    total_appended = 0

    def checkpoint() -> None:
        state = {
            "version": 2,
            "cursors": {
                assets: {"ts": c.ts, "offset": c.offset, "finished": c.finished}
                for assets, c in cursors.items()
            },
            "config": {
                "type": args.type,
                "assets": assets_list,
                "min_ts": args.min_ts,
                "max_ts": args.max_ts,
            },
            "stats": {
                "total_requests": total_requests,
                "total_errors": total_errors,
                "total_appended": total_appended,
                "updated_at_unix": now_ts(),
            }
        }
        save_json_atomic(state_path, state)

    log("[INFO] starting per-asset timestamp-based crawl (v2)")
    log(f"[INFO] outdir={outdir.resolve().as_posix()}")
    log(f"[INFO] type={args.type}")
    log(f"[INFO] assets_list={assets_list}")
    log(f"[INFO] base_urls={base_urls}")
    log(f"[INFO] min_ts={args.min_ts} ({min_ts_ns} ns)" if args.min_ts else "[INFO] min_ts=None")
    for assets, cursor in cursors.items():
        log(f"[INFO] {assets}: start cursor_ts={cursor.ts} ({ns_to_sec(cursor.ts)} sec)")

    checkpoint()

    # Per-asset retry attempt counter
    attempts: Dict[str, int] = {a: 0 for a in assets_list}

    # Main loop: round-robin through assets until all finished
    while True:
        # Check if all assets are finished
        active_assets = [a for a in assets_list if not cursors[a].finished]
        if not active_assets:
            log("[INFO] all assets reached min_ts boundary; stopping.")
            break

        # Find assets that are ready (not in cooldown)
        now = time.time()
        ready_assets = [a for a in active_assets if cursors[a].cooldown_until <= now]

        if not ready_assets:
            # All active assets are in cooldown, wait for the earliest one
            earliest = min(cursors[a].cooldown_until for a in active_assets)
            wait_time = max(0.1, earliest - now)
            log(f"[INFO] all assets in cooldown; sleeping {wait_time:.1f}s")
            time.sleep(wait_time)
            continue

        for assets in ready_assets:
            cursor = cursors[assets]

            # Check min_ts boundary
            if min_ts_ns is not None and cursor.ts < min_ts_ns:
                log(f"[INFO] {assets}: reached min_ts boundary; marking finished.")
                cursor.finished = True
                checkpoint()
                continue

            base_url = choose_base_url(base_urls, base_url_idx_ref[0])
            # Only increment if multiple URLs (for round-robin)
            if len(base_urls) > 1:
                base_url_idx_ref[0] += 1

            # Use seconds for API call (Midgard expects seconds)
            cursor_ts_sec = ns_to_sec(cursor.ts)

            log(f"\n[INFO] {assets}: cursor_ts={cursor.ts} ({cursor_ts_sec} sec) offset={cursor.offset}")

            data, err, cooldown = try_fetch_actions_page(
                session=session,
                base_url=base_url,
                typ=args.type,
                assets=assets,
                cursor_ts=cursor_ts_sec,
                offset=cursor.offset,
                limit=args.limit,
                timeout=args.timeout,
                base_sleep=args.base_sleep,
                max_sleep=args.max_sleep,
                attempt=attempts[assets],
                base_urls=base_urls,
                base_url_idx_ref=base_url_idx_ref,
            )
            total_requests += 1

            # Handle cooldown (retryable error)
            if cooldown > 0:
                cursor.cooldown_until = time.time() + cooldown
                attempts[assets] += 1
                if attempts[assets] > args.max_retries:
                    total_errors += 1
                    log(f"[ERROR] {assets}: exceeded max_retries={args.max_retries}")
                    attempts[assets] = 0  # Reset for next cursor position
                checkpoint()
                continue

            # Handle fatal error
            if err is not None:
                total_errors += 1
                log(f"[ERROR] {assets}: {err}")
                attempts[assets] = 0
                checkpoint()
                cursor.cooldown_until = time.time() + args.sleep_between_requests
                continue

            # Success - reset attempt counter
            attempts[assets] = 0

            actions = (data or {}).get("actions", []) or []
            log(f"[INFO] {assets}: got {len(actions)} actions from {base_url}")

            if not actions:
                # No more data at this cursor, mark as finished
                log(f"[INFO] {assets}: no more data; marking finished.")
                cursor.finished = True
                checkpoint()
                continue

            # Filter actions that are before min_ts
            filtered_actions = []
            found_boundary = False
            for a in actions:
                try:
                    date = int(a.get("date", "0"))
                    if min_ts_ns is not None and date < min_ts_ns:
                        found_boundary = True
                        continue  # Skip this action
                    filtered_actions.append(a)
                except Exception:
                    filtered_actions.append(a)

            if found_boundary:
                log(f"[INFO] {assets}: found records before min_ts, filtering...")

            # Find min timestamp in this batch and count records at that timestamp
            min_date_in_batch: Optional[int] = None
            for a in actions:
                try:
                    date = int(a.get("date", "0"))
                    if date > 0:
                        if min_date_in_batch is None or date < min_date_in_batch:
                            min_date_in_batch = date
                except Exception:
                    pass

            # Inject API metadata into each action for reproducibility
            for a in filtered_actions:
                a["_api_ts"] = cursor_ts_sec
                a["_api_offset"] = cursor.offset

            appended = append_ndjson(files[assets], filtered_actions, seen[assets])
            total_appended += appended
            if appended:
                log(f"[INFO] {assets}: appended {appended} new actions -> {files[assets].as_posix()}")

            # Update cursor for next iteration
            if min_date_in_batch is not None:
                # Count how many records have the min timestamp
                count_at_min = sum(1 for a in actions if int(a.get("date", "0")) == min_date_in_batch)

                if min_date_in_batch == cursor.ts:
                    # Same timestamp, just increase offset
                    cursor.offset += count_at_min
                else:
                    # Move to new timestamp, set offset to count at that timestamp
                    cursor.ts = min_date_in_batch
                    cursor.offset = count_at_min

                log(f"[INFO] {assets}: next cursor_ts={cursor.ts} ({ns_to_sec(cursor.ts)} sec) offset={cursor.offset}")

            # Check if we've crossed min_ts boundary
            if found_boundary and not filtered_actions:
                log(f"[INFO] {assets}: all remaining records before min_ts; marking finished.")
                cursor.finished = True

            checkpoint()
            # Normal throttling: sleep immediately instead of using cooldown
            time.sleep(args.sleep_between_requests)

    log("\n[INFO] finished")
    log(f"[INFO] total_requests={total_requests} total_errors={total_errors} total_appended={total_appended}")
    log(f"[INFO] state saved at {state_path.resolve().as_posix()}")


if __name__ == "__main__":
    main()
