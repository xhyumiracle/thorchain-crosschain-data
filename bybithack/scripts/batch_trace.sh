#!/bin/bash
# Batch trace multiple start addresses to find one meeting criteria

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/../../.."
RESULTS_BASE="$SCRIPT_DIR/../.local/results_batch"
SUMMARY_FILE="$RESULTS_BASE/summary.txt"

# Criteria (can be overridden via environment variables)
TARGET_PATHS=${TARGET_PATHS:-10}
TARGET_HIGHFAST=${TARGET_HIGHFAST:-10}
MAX_DEPTH=${MAX_DEPTH:-3}

# Read start addresses from file (skip comments and empty lines) - compatible with older bash
START_ADDRESSES=()
while IFS= read -r line; do
    START_ADDRESSES+=("$line")
done < <(grep -v '^#' "$SCRIPT_DIR/start_addresses.txt" | grep -v '^$')

mkdir -p "$RESULTS_BASE"

# Write header
cat > "$SUMMARY_FILE" << HEADER
Batch Trace Summary
Generated: $(date)
Target: ≥$TARGET_PATHS paths, ≥$TARGET_HIGHFAST high-fast matches
Max depth: $MAX_DEPTH hops
================================================

HEADER

success_addr=""

for addr in "${START_ADDRESSES[@]}"; do
    echo ""
    echo "========================================"
    echo "Processing: $addr"
    echo "========================================"
    
    # Normalize address
    addr_lower=$(echo "$addr" | tr '[:upper:]' '[:lower:]')
    addr_short="${addr_lower:0:10}"
    OUTPUT_DIR="$RESULTS_BASE/results_${addr_short}"
    mkdir -p "$OUTPUT_DIR"
    
    OUTPUT_JSON="$OUTPUT_DIR/eth_bfs_trace.json"
    
    # Run tracer with CLI arguments
    echo "Running tracer (max $MAX_DEPTH hops)..."
    cd "$PROJECT_ROOT"
    if ! timeout 600 uv run python "$SCRIPT_DIR/1_trace_bfs.py" \
        --start-address "$addr" \
        --max-depth "$MAX_DEPTH" \
        --output-dir "$OUTPUT_DIR" \
        > "$OUTPUT_DIR/trace.log" 2>&1; then
        echo "  WARNING: Script timeout or error"
        echo "$addr: TIMEOUT/ERROR" >> "$SUMMARY_FILE"
        echo "" >> "$SUMMARY_FILE"
        continue
    fi

    # Check output
    if [ ! -f "$OUTPUT_JSON" ]; then
        echo "  ERROR: No output generated"
        echo "$addr: NO OUTPUT" >> "$SUMMARY_FILE"
        echo "" >> "$SUMMARY_FILE"
        continue
    fi

    # Extract stats
    num_paths=$(python -m json.tool "$OUTPUT_JSON" | grep '"thor_paths_found"' | grep -o '[0-9]*')
    unique_addrs=$(python -m json.tool "$OUTPUT_JSON" | grep '"unique_addrs_queried"' | grep -o '[0-9]*')
    
    # Extract Thor txids (remove 0x, lowercase)
    python -m json.tool "$OUTPUT_JSON" | \
        grep -B2 '"to": "0xd37bbe5744d730a1d98d8dc97c42f0ca46ad7146"' | \
        grep '"txid"' | \
        sed 's/.*"txid": "\([^"]*\)".*/\1/' | \
        sed 's/^0x//' | \
        tr '[:upper:]' '[:lower:]' | \
        sort -u > "$OUTPUT_DIR/thor_txids.txt"
    
    total_thor_txs=$(wc -l < "$OUTPUT_DIR/thor_txids.txt" | tr -d ' ')
    
    # Count high-fast matches
    highfast_count=0
    while IFS= read -r txid; do
        if grep -qi "$txid" "$PROJECT_ROOT/data/thorchain/data/thorchain-2025-high-fast"/*.jsonl 2>/dev/null; then
            highfast_count=$((highfast_count + 1))
            echo "$txid" >> "$OUTPUT_DIR/highfast_matches.txt"
        fi
    done < "$OUTPUT_DIR/thor_txids.txt"
    
    # Count regular 2025 matches
    reg_count=0
    while IFS= read -r txid; do
        if grep -qi "$txid" "$PROJECT_ROOT/data/thorchain/data/thorchain-2025"/*.jsonl 2>/dev/null; then
            reg_count=$((reg_count + 1))
        fi
    done < "$OUTPUT_DIR/thor_txids.txt"
    
    # Display results
    echo ""
    echo "Results:"
    echo "  Thor paths: $num_paths"
    echo "  Thor txs: $total_thor_txs"
    echo "  High-fast matches: $highfast_count"
    echo "  Regular 2025 matches: $reg_count"
    echo "  API queries: $unique_addrs"
    
    # Append to summary
    cat >> "$SUMMARY_FILE" << RESULT
$addr:
  Thor paths: $num_paths
  Thor txs: $total_thor_txs
  High-fast: $highfast_count / $total_thor_txs
  Regular: $reg_count / $total_thor_txs
  API queries: $unique_addrs
RESULT
    
    # Check criteria
    if [ "$num_paths" -ge "$TARGET_PATHS" ] && [ "$highfast_count" -ge "$TARGET_HIGHFAST" ]; then
        echo "  ✓ SUCCESS" >> "$SUMMARY_FILE"
        echo ""
        echo "========================================"
        echo "✓ FOUND TARGET!"
        echo "  Address: $addr"
        echo "  Paths: $num_paths ≥ $TARGET_PATHS ✓"
        echo "  High-fast: $highfast_count ≥ $TARGET_HIGHFAST ✓"
        echo "========================================"
        success_addr="$addr"
        break
    else
        echo "  ✗ Insufficient" >> "$SUMMARY_FILE"
    fi
    
    echo "" >> "$SUMMARY_FILE"
done

echo ""
echo "========================================"
echo "Batch processing complete"
echo "Summary: $SUMMARY_FILE"
if [ -n "$success_addr" ]; then
    echo "SUCCESS: $success_addr meets criteria"
else
    echo "No address met criteria"
fi
echo "========================================"
