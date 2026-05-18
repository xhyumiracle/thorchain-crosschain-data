# Bybit Hack THORChain Dataset

## Overview

Partial money flow trace from Bybit hack (Feb 21, 2025) exploiter address `0xa5a023...` with 35 ETH→BTC THORChain crosschain swaps (Mar 1-3, 2025). ~83% overlap with thor2025 dataset.

**Thor Router**: `0xD37BbE5744D730a1d98d8DC97c42F0Ca46aD7146`

## Data Generation Pipeline

```
scripts/start_addresses.txt
  → scripts/1_trace_bfs.py (BFS ETH tracing to Thor Router)
  → data/partial_a5a023_eth_raw.json (42 paths)
  → scripts/2_populate_thor_links.py (match thor2025 dataset)
  → scripts/3_split_by_ancestor.py (group by common ancestor)
  → data/partial_a5a023/common_ancestor_depth_[1-3].ndjson (final data)
```

## Data Files

### Final Dataset (Used in Experiments)
- **data/partial_a5a023/common_ancestor_depth_1.ndjson**: 6 groups (depth-1 ancestors)
- **data/partial_a5a023/common_ancestor_depth_2.ndjson**: 2 groups (depth-2 ancestors)
- **data/partial_a5a023/common_ancestor_depth_3.ndjson**: 1 group (32 endpoints, root exploiter `0xa5a023...`)

### Intermediate Data
- **data/partial_a5a023_eth_raw.json**: Raw BFS trace with 42 Thor paths (35 matched)
- **time_delta_analysis.png**: Time delta visualization

### Private Data (Not in Git)
- **.local/thor-artifacts/**: Raw THORChain records (regenerate via `scripts/utils/extract_thor_artifacts.py`)
- **.local/results_batch/**: Batch tracing results
- **.local/.cache/**: API cache
- **.local/ANCESTOR_ANALYSIS.md**: Detailed analysis (Chinese)

## Scripts

All scripts in `scripts/` directory.

### Main Pipeline

1. **1_trace_bfs.py**: BFS ETH flow tracer to Thor Router
   ```bash
   cd scripts && uv run python 1_trace_bfs.py --start-address 0x... --max-depth 5 --output-dir ../data/
   ```

2. **2_populate_thor_links.py**: Match ETH traces with thor2025 crosschain records

3. **3_split_by_ancestor.py**: Group addresses by common ancestor depth
   ```bash
   cd scripts && uv run python 3_split_by_ancestor.py --max-depth 5 --amount-threshold 10.0
   ```

### Batch & Query Tools

- **batch_trace.sh**: Batch trace multiple addresses from `start_addresses.txt`
  ```bash
  cd scripts && ./batch_trace.sh  # outputs to ../.local/results_batch/
  ```
- **gen_query.py**: Generate query configs for data extraction

### Utils (`scripts/utils/`)

- **analyze_time_delta.py**: Generate time delta visualization
- **check_thor_coverage.sh**: Verify thor2025 dataset coverage
- **extract_thor_artifacts.py**: Extract raw THORChain records for each group
- **fetch_and_fill_btc_time.py**: Fill BTC transaction timestamps

## Data Format

### Common Ancestor Groups

Each ndjson line = 1 group:

```json
{
  "group_id": "<ancestor>_<depth>_<count>",
  "common_ancestor": "0x...",
  "common_hops": 1,
  "endpoints_count": 5,
  "endpoints": [
    {"cc_sender": "0x...", "cc_txid": "...", "hop": 1}
  ],
  "paths": [
    {
      "cc_sender": "0x...",
      "path": [
        {"depth": 1, "hop": 1, "txid": "0x...",
         "sender": "0x...", "receiver": "0x...",
         "value": 35.27, "time": "..."}
      ],
      "crosschain_link": {
        "thor25-idx": 22499,
        "in": {"asset": "ETH.ETH", "txid": "...", "value": 35.27, ...},
        "out": {"asset": "BTC.BTC", "txid": "...", "value": 0.89, ...}
      }
    }
  ]
}
```

**Path depth**: `1` = direct tx to Thor Router, higher = earlier in the chain

## Key Findings

- **Main source**: `0xa5a023...` is common ancestor for 32/35 crosschain swaps (91%)
- **Depth-2 distribution**: 6 intermediate addresses (19 endpoints total)
- **Coverage**: 35/42 paths matched thor2025 dataset (83%)
