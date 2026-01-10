# THORChain Crosschain Data

## About THORChain

[THORChain](https://thorchain.org/) is a decentralized cross-chain liquidity protocol built as an independent Layer 1 blockchain using Cosmos SDK. It enables native asset swaps across different blockchains without wrapped tokens or centralized custodians. The THORChain network produces blocks approximately every 6 seconds ([docs](https://docs.thorchain.org/)).

## Data Selection

THORChain supports cross-chain swaps across many blockchains (BTC, ETH, BSC, AVAX, DOGE, LTC, BCH, etc.) and various assets (native coins, ERC-20 tokens, BEP-20 tokens, etc.).

**This dataset focuses on:**
- **Chains**: BTC, ETH, DOGE (3 common chains)
- **Assets**: Native assets only (BTC, ETH, DOGE)
- **Filter**: `type=swap` and `status=success` records only

## Dataset Naming Convention

**Format**: `thorchain-2025-<condition1>-<condition2>` (Short: `Thor25<C1><C2>`)

**Amount**: `L` (Low) | `H` (High) | `X` (eXtra)
**Time**: `F` (Fast) | `S` (Slow)

**Current datasets**:
- `thorchain-2025` (Thor25): Full 2025 dataset (151,461 standard 1-in-1-out swaps)
- `thorchain-2025-high-fast` (Thor25HF): High amount (0.09 BTC / 1.9 ETH / 1k DOGE) + Fast completion (≤30min, 13,857 records)
- `thorchain-2025-high-fast-mini` (Thor25HF-mini): Mini test set sampled from HF (1,200 records, 100 per pair)
- `thorchain-2025-multi` (Thor25M): Multi-output swaps (156 records, currently not used for queries)

## Quick Start

Query files are not included in the repository (they're generated from the source data). Generate them locally:

```bash
# Generate queries for mini test set (1,200 queries, recommended for first try)
uv run python script/process/gen_query.py --batch --input-dir data/thorchain-2025-high-fast-mini --output-dir queries/thorchain-2025-high-fast-mini

# Generate queries for high-fast dataset (~13.9k queries)
uv run python script/process/gen_query.py --batch --input-dir data/thorchain-2025-high-fast --output-dir queries/thorchain-2025-high-fast

# Generate queries for full dataset (~151k queries)
uv run python script/process/gen_query.py --batch --input-dir data/thorchain-2025 --output-dir queries/thorchain-2025
```

Then use with BlockchainMAS:
```bash
cd /path/to/BlockchainMAS
python -m src.main --batch data/thorchain/queries/thorchain-2025-high-fast-mini/BTC-ETH.yaml
```

## Data Characteristics

### Overview
- **Time Range**: 2025-01-01 00:00:00 UTC ~ 2025-12-31 23:59:59 UTC (full year)
- **Total Records**: 151,461 successful swaps across 12 pair files
- **Pairs**: BTC<>ETH, BTC<>DOGE, BTC<>LTC, ETH<>DOGE, ETH<>LTC, DOGE<>LTC (both directions)

### Amount Unit Normalization
THORChain Midgard API normalizes all asset amounts to **1e8 base units** (similar to Bitcoin's satoshi), regardless of the native blockchain's decimal precision ([Midgard docs](https://docs.thorchain.org/technical-documentation/technology/midgard)):
- **BTC** (native 1e8 satoshi) → preserved as **1e8**
- **ETH** (native 1e18 wei) → shortened to **1e8**
- **DOGE** (native 1e8) → preserved as **1e8**
- **LTC** (native 1e8 litoshi) → preserved as **1e8**

This means: `1 BTC = 1 ETH = 1 DOGE = 1 LTC = 100,000,000 units` in the data.

### Timestamp Format
All timestamps in Midgard API are **Unix timestamps in UTC timezone**, with the `date` field in nanoseconds ([API spec](https://midgard.ninerealms.com/v2/swagger.json)).

### Record Indexing
Each record has two identifiers: `idx` (dataset-local sequential index starting from 0) and `id` (hash-based stable identifier across all datasets).

### Transaction Count by Direction

**Full Dataset (151,461 records):**
- **BTC→ETH**: 50,597 records (33.4%)
- **ETH→BTC**: 37,087 records (24.5%)
- **LTC→BTC**: 18,090 records (11.9%)
- **BTC→LTC**: 11,256 records (7.4%)
- **LTC→ETH**: 9,503 records (6.3%)
- **ETH→LTC**: 7,819 records (5.2%)
- **DOGE→BTC**: 5,149 records (3.4%)
- **DOGE→ETH**: 3,782 records (2.5%)
- **BTC→DOGE**: 3,482 records (2.3%)
- **ETH→DOGE**: 2,560 records (1.7%)
- **DOGE→LTC**: 1,016 records (0.7%)
- **LTC→DOGE**: 1,120 records (0.7%)

**High-Fast Dataset (13,857 records):**
- **BTC→ETH**: 6,511 records (47.0%) [time-filtered ≤30min]
- **ETH→BTC**: 4,928 records (35.6%) [time-filtered ≤30min]
- **DOGE→BTC**: 1,469 records (10.6%) [time-filtered ≤30min]
- **DOGE→ETH**: 739 records (5.3%) [time-filtered ≤30min]
- **BTC→DOGE**: 107 records (0.8%) [time-filtered ≤30min]
- **ETH→DOGE**: 103 records (0.7%) [time-filtered ≤30min]
- **LTC→BTC**: 0 records [amount-only, no time filter]
- **LTC→ETH**: 0 records [amount-only, no time filter]
- **BTC→LTC**: 0 records [amount-only, no time filter]
- **ETH→LTC**: 0 records [amount-only, no time filter]
- **DOGE→LTC**: 0 records [amount-only, no time filter]
- **LTC→DOGE**: 0 records [amount-only, no time filter]

**Note**: LTC pairs currently have 0 records because blockchain transaction data is not available for time-based filtering. BTC/ETH/DOGE pairs use both amount (≥0.09 BTC / ≥1.9 ETH / ≥1000 DOGE) and time (≤30min) filtering.

### Daily Distribution
![Daily TX Count](png/daily_tx_count.png)
![Daily Amount](png/daily_amount.png)

### Amount Distribution
- Amount ranges span multiple orders of magnitude (10³ to 10¹⁰)
- Each asset shows distinct distribution patterns
- IN (solid) vs OUT (dashed) amounts show swap behavior

| Pair      | Distribution Plot                                             |
| --------- | ------------------------------------------------------------- |
| BTC<>ETH  | ![BTC-ETH Amount Distribution](png/amount_dist_BTC-ETH.png)   |
| BTC<>DOGE | ![BTC-DOGE Amount Distribution](png/amount_dist_BTC-DOGE.png) |
| ETH<>DOGE | ![ETH-DOGE Amount Distribution](png/amount_dist_ETH-DOGE.png) |

### Height Diff (Swap Completion Time)
- Most swaps complete quickly: median 6-26 thorchain blocks depending on pair
- ~80-90% complete within 100 blocks
- ~99%+ complete within 1000 blocks
- Outliers exist up to 24k blocks (ETH→BTC)

![Height Diff CDF](png/height_diff_cdf.png)

### Traffic Spikes
- 2025-02-22~03-03: Major spike in ETH→BTC (~10 days, tx count surged from ~100/day to 1000-2000/day, daily amount jumped from ~100 ETH to 20,000-85,000 ETH), related to **Bybit hack** fund flows
- 2025-03-14~15: Abnormal spike in BTC<>ETH (1800+ tx/day, height diff up to 5000+ blocks)
- 2025-06: Another spike (~1100 tx/day, height diff up to 3000+ blocks)

![Height Diff vs Timestamp](png/height_diff_vs_timestamp.png)


## Scripts

### crawl/ - Fetching Raw Data and Reprocessing

#### fetch_swaps.py
Midgard API crawler (backwards by timestamp) for THORChain swap actions.

- `--min-ts`: Lower bound Unix timestamp, stop when reaching this
- `--max-ts`: Upper bound Unix timestamp, start from here
- `--fresh`: Start a new crawl from scratch
- `--resume`: Continue from last saved state

```bash
# Fresh crawl
uv run python script/crawl/fetch_swaps.py --outdir raw --min-ts 1735689600 --fresh

# Resume
uv run python script/crawl/fetch_swaps.py --outdir raw --min-ts 1735689600 --resume
```

#### wash.py
Transform raw data to cleaned format. Filters `status != 'success'` records and removes `THOR.*` assets (e.g., THOR.RUNE affiliate fees) from outputs to keep only the actual swap assets.

**Record ID**: `id = SHA-256("\n".join(sorted(entries)) + "\n{type}|{status}")` where each entry = `"{direction}|{chain}|{asset}|{address}|{txID}"` (auto-deduplicated). This derived ID appears as `query_id` in generated query files.

```bash
uv run python script/crawl/wash.py --indir raw/data --outdir data/thorchain-2025
```

### process/ - Data Processing Pipeline

#### filter_data.py
Filter swap data by amount and time thresholds to create high-quality datasets.

```bash
# Edit thresholds in script, then run:
uv run python script/process/filter_data.py
```

Configuration presets: 0.01%, 0.02%, 0.05%, 0.1% fee rates. See `FILTERING_THRESHOLDS.md` for details.

#### sample_mini.py
Sample mini dataset from high-fast data for testing.

```bash
uv run python script/process/sample_mini.py
```

#### gen_query.py
Generate YAML batch query files from ndjson data.

```bash
# Generate from a single ndjson file
uv run python script/process/gen_query.py --input ../../data/BTC-DOGE.ndjson --output ../../queries/BTC-DOGE.yaml

# Generate from all ndjson files (batch mode)
uv run python script/process/gen_query.py --batch --input-dir ../../data --output-dir ../../queries

# # Optional: Add timestamp_delta metadata (requires blockchain_txs/ directory from enrich/)
# uv run python script/process/gen_query.py --batch --input-dir ../../data --output-dir ../../queries --blockchain-txs-dir ../../blockchain_txs
```

The generated YAML files can be used with BlockchainMAS:
```bash
cd /path/to/BlockchainMAS
python -m src.main --batch data/thorchain/queries/BTC-DOGE.yaml
```

### analyze/ - Tools for Validate and Analyzing Data

#### validate.py
Check for duplicate records by ID.

```bash
uv run python script/analyze/validate.py
```

#### stats.py
Compute per-pair statistics (amounts, height diff, timestamps).

```bash
uv run python script/analyze/stats.py
```

#### plot.py
Plot amount & height diff vs timestamp.

```bash
uv run python script/analyze/plot.py
```

Output: `png/*.png`

#### identify_slow_swaps.py
Identify swaps with abnormally long completion times (for debugging/analysis).

- `-t`: Height diff threshold (records with diff > threshold)
- `-s`: Start date (YYYY-MM-DD)
- `-e`: End date (YYYY-MM-DD)

```bash
# Basic
uv run python script/analyze/identify_slow_swaps.py -t 5000

# With date range
uv run python script/analyze/identify_slow_swaps.py -t 2000 -s 2025-03-01 -e 2025-03-31

# Export JSON
uv run python script/analyze/identify_slow_swaps.py -t 5000 -o results.json
```
