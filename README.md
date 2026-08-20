---
license: cc-by-4.0
language:
- en
pretty_name: Thor25
tags:
- tabular
- datasets
- blockchain
- blockchain-forensics
- cryptocurrency
- thorchain
- cross-chain
- transaction-tracing
- digital-forensics
- financial-crime
- graph-reasoning
- agent
- benchmark
- agentic-ai
- agentic-benchmark
- llm-agents
- tool-use
- environment-interaction
size_categories:
- 100K<n<1M
configs:
- config_name: thor25
  data_files:
  - split: train
    path: data/thorchain-2025/*.jsonl
- config_name: thor25hf
  data_files:
  - split: train
    path: data/thorchain-2025-high-fast/*.jsonl
- config_name: thor25hf-mini
  data_files:
  - split: test
    path: data/thorchain-2025-high-fast-mini/*.jsonl
- config_name: thor25m
  data_files:
  - split: train
    path: data/thorchain-2025-multi/*.jsonl
---

# Thor25: THORChain Cross-Chain Data

This repository contains the Thor25 dataset released with *LOCARD: An Agentic Framework for Blockchain Forensics*, published at IEEE ICBC 2026, Brisbane, Australia, June 1-5, 2026.

Thor25 supports research on agentic blockchain forensics, cross-chain transaction tracing, and evidence-grounded tool use by LLM agents. It does not map cleanly to conventional NLP task categories such as question answering or text generation: solving each benchmark case requires an agent to iteratively interact with blockchain data and investigative tools in a real blockchain environment.

- Paper DOI: https://doi.org/10.1109/ICBC67748.2026.11575479
- Hugging Face paper page: https://huggingface.co/papers/2604.04211
- arXiv: https://arxiv.org/abs/2604.04211
- Hugging Face dataset: https://huggingface.co/datasets/xhyumiracle/thor25
- LOCARD code: https://github.com/xhyumiracle/locard

## About THORChain

[THORChain](https://thorchain.org/) is a decentralized cross-chain liquidity protocol built as an independent Layer 1 blockchain using Cosmos SDK. It enables native asset swaps across different blockchains without wrapped tokens or centralized custodians. The THORChain network produces blocks approximately every 6 seconds ([docs](https://docs.thorchain.org/)).

## Data Selection

THORChain supports cross-chain swaps across many blockchains (BTC, ETH, BSC, AVAX, DOGE, LTC, BCH, etc.) and various assets (native coins, ERC-20 tokens, BEP-20 tokens, etc.).

**This dataset focuses on:**
- **Chains**: BTC, ETH, DOGE, LTC (4 common chains)
- **Assets**: Native assets only (BTC, ETH, DOGE, LTC)
- **Filter**: `type=swap` and `status=success` records only

## Dataset Naming Convention

**Format**: `thorchain-2025-<condition1>-<condition2>` (Short: `Thor25<C1><C2>`)

**Amount**: `L` (Low) | `H` (High) | `X` (eXtra)
**Time**: `F` (Fast) | `S` (Slow)

**Current datasets**:
- `thorchain-2025` (Thor25): Full 2025 dataset (151,461 standard 1-in-1-out swaps)
- `thorchain-2025-high-fast` (Thor25HF): High amount (0.09 BTC / 1.9 ETH / 2.5 LTC / 1k DOGE) + Fast completion (≤30min, 20,235 records)
- `thorchain-2025-high-fast-mini` (Thor25HF-mini): Mini test set sampled from Thor25HF (1,200 records, 100 per pair)
- `thorchain-2025-multi` (Thor25M): Multi-output swaps (166 records, currently not used for queries)

## Hugging Face Usage

```python
from datasets import load_dataset

dataset = load_dataset("xhyumiracle/thor25", "thor25hf-mini")
print(dataset["test"][0])
```

Load the full dataset:

```python
from datasets import load_dataset

dataset = load_dataset("xhyumiracle/thor25", "thor25")
```

## Quick Start

Query files are not included in the repository (they're generated from the source data). Generate them locally:

```bash
# Generate queries for mini test set (1,200 queries, recommended for first try)
uv run python script/process/gen_query.py --batch --input-dir data/thorchain-2025-high-fast-mini --output-dir queries/thorchain-2025-high-fast-mini

# Generate queries for high-fast dataset (~20.2k queries)
uv run python script/process/gen_query.py --batch --input-dir data/thorchain-2025-high-fast --output-dir queries/thorchain-2025-high-fast

# Generate queries for full dataset (~151k queries)
uv run python script/process/gen_query.py --batch --input-dir data/thorchain-2025 --output-dir queries/thorchain-2025
```

## Data Fields

Each record contains:

- `idx`: dataset-local sequential index.
- `id`: hash-based stable identifier derived from transaction entries.
- `timestamp`: THORChain action timestamp in Unix nanoseconds, UTC.
- `type`: THORChain action type. This release keeps `swap` records.
- `status`: THORChain action status. This release keeps `success` records.
- `in`: list of source transaction entries.
- `out`: list of destination transaction entries.

Each entry in `in` and `out` includes:

- `chain`: blockchain name, one of `BTC`, `ETH`, `DOGE`, or `LTC`.
- `asset`: native asset name.
- `txID`: blockchain transaction hash.
- `address`: sender or receiver address.
- `amount`: asset amount in THORChain Midgard base units.
- `thorchainHeight`: THORChain block height.

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

**High-Fast Dataset (20,235 records):**
- **BTC→ETH**: 6,511 records (32.2%)
- **ETH→BTC**: 4,928 records (24.4%)
- **LTC→BTC**: 3,234 records (16.0%)
- **LTC→ETH**: 2,038 records (10.1%)
- **DOGE→BTC**: 1,469 records (7.3%)
- **DOGE→ETH**: 739 records (3.7%)
- **BTC→LTC**: 639 records (3.2%)
- **ETH→LTC**: 246 records (1.2%)
- **DOGE→LTC**: 117 records (0.6%)
- **BTC→DOGE**: 107 records (0.5%)
- **LTC→DOGE**: 104 records (0.5%)
- **ETH→DOGE**: 103 records (0.5%)

All pairs filtered with: source tx amount thresholds (≥0.09 BTC / ≥1.9 ETH / ≥2.5 LTC / ≥1000 DOGE) + time constraint (≤30min).

### Daily Distribution
![Daily TX Count - Main Pairs](png/daily_tx_count.png)
![Daily Amount - Main Pairs](png/daily_amount.png)

<details>
<summary>📊 LTC Pairs (Click to expand)</summary>

![Daily TX Count - LTC Pairs](png/daily_tx_count_ltc.png)
![Daily Amount - LTC Pairs](png/daily_amount_ltc.png)

</details>

### Amount Distribution
- Amount ranges span multiple orders of magnitude (10³ to 10¹⁰)
- Each asset shows distinct distribution patterns
- IN (solid line in CDF) vs OUT (dashed line in CDF) amounts show swap behavior

![Amount Distribution - Main Pairs](png/amount_distribution_cdf.png)

<details>
<summary>📊 LTC Pairs (Click to expand)</summary>

![Amount Distribution - LTC Pairs](png/amount_distribution_cdf_ltc.png)

</details>

### Height Diff (Swap Completion Time)
- Most swaps complete quickly: median 6-26 thorchain blocks depending on pair
- ~80-90% complete within 100 blocks
- ~99%+ complete within 1000 blocks
- Outliers exist up to 24k blocks (ETH→BTC)

![Height Diff CDF - Main Pairs](png/height_diff_cdf.png)
![Height Diff vs Timestamp - Main Pairs](png/height_diff_vs_timestamp.png)

<details>
<summary>📊 LTC Pairs (Click to expand)</summary>

![Height Diff CDF - LTC Pairs](png/height_diff_cdf_ltc.png)
![Height Diff vs Timestamp - LTC Pairs](png/height_diff_vs_timestamp_ltc.png)

</details>

### Time Diff (Blockchain Timestamps)
- Time difference calculated from actual blockchain transaction timestamps
- Provides real-world completion time in seconds (vs THORChain blocks)
- Shows directional asymmetry: BTC→DOGE (~177s) vs DOGE→BTC (~762s)
- Most swaps complete within 500 seconds

![Time Diff CDF - Main Pairs](png/time_diff_cdf.png)
![Time Diff vs Timestamp - Main Pairs](png/time_diff_vs_timestamp.png)

<details>
<summary>📊 LTC Pairs (Click to expand)</summary>

![Time Diff CDF - LTC Pairs](png/time_diff_cdf_ltc.png)
![Time Diff vs Timestamp - LTC Pairs](png/time_diff_vs_timestamp_ltc.png)

</details>

### Time-Amount Relationship
- Visualization of time difference vs transaction amount across all pairs
- High-Fast (HF) dataset thresholds shown as reference lines
- Helps understand the characteristics of filtered datasets

![Time-Amount All Pairs](png/time_amount_all_pairs.png)

### Traffic Spikes
- 2025-02-22~03-03: Major spike in ETH→BTC (~10 days, tx count surged from ~100/day to 1000-2000/day, daily amount jumped from ~100 ETH to 20,000-85,000 ETH), related to **Bybit hack** fund flows
- 2025-03-14~15: Abnormal spike in BTC<>ETH (1800+ tx/day, height diff up to 5000+ blocks)
- 2025-06: Another spike (~1100 tx/day, height diff up to 3000+ blocks)


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
# Custom amount thresholds (see script header for examples)
uv run python script/process/filter_data.py --src-amount-gte-BTC 0.09 --src-amount-gte-ETH 1.9 --src-amount-gte-LTC 2.5 --src-amount-gte-DOGE 1000
```

#### sample_mini.py
Sample mini dataset from high-fast data for testing.

```bash
uv run python script/process/sample_mini.py
```

#### gen_query.py
Generate YAML batch query files from jsonl data.

```bash
# Generate from a single jsonl file
uv run python script/process/gen_query.py --input ../../data/BTC-DOGE.jsonl --output ../../queries/BTC-DOGE.yaml

# Generate from all jsonl files (batch mode)
uv run python script/process/gen_query.py --batch --input-dir ../../data --output-dir ../../queries

# # Optional: Add timestamp_delta metadata (requires blockchain_txs/ directory from enrich/)
# uv run python script/process/gen_query.py --batch --input-dir ../../data --output-dir ../../queries --blockchain-txs-dir ../../blockchain_txs
```

The generated YAML files can be used with the system:
```bash
cd <project_root>
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

## Intended Uses

This dataset is intended for research on blockchain forensics and cross-chain transaction tracing, evaluation of agentic AI systems that perform evidence-grounded investigation, benchmarking retrieval/candidate-generation/ranking/forensic-reasoning workflows, and reproducibility of LOCARD experiments.

## Limitations

Thor25 is not a complete dataset of all THORChain activity. It focuses on selected native assets, successful swaps, and benchmark-friendly record types. Blockchain attribution is inherently uncertain, and benchmark results should not be interpreted as legal, compliance, or financial conclusions.

The dataset contains public blockchain addresses and transaction hashes. Users should avoid attempting to deanonymize individuals or use the dataset for harmful surveillance.

## Citation

```bibtex
@inproceedings{YuK26,
  title     = {LOCARD: An Agentic Framework for Blockchain Forensics},
  author    = {Xiaohang Yu and William Knottenbelt},
  booktitle = {2026 IEEE International Conference on Blockchain and Cryptocurrency (ICBC)},
  pages     = {1--9},
  publisher = {IEEE},
  year      = {2026},
  doi       = {10.1109/ICBC67748.2026.11575479},
  url       = {https://doi.org/10.1109/ICBC67748.2026.11575479}
}
```

## License

Thor25 is released under the Creative Commons Attribution 4.0 International license (`cc-by-4.0`).
