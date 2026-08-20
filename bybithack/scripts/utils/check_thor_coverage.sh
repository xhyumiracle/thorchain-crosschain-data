#!/bin/bash
# Check how many thor_paths_endpoints txids appear in thorchain datasets

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
THORCHAIN_DATA="$SCRIPT_DIR/../data"
JSON_FILE="$SCRIPT_DIR/eth_partial_a5a023.json"
OUTPUT_FILE="$SCRIPT_DIR/thor_coverage_report.txt"

echo "================================================"
echo "Thor Coverage Analysis"
echo "================================================"
echo ""

# Extract thor router txids
echo "[INFO] Extracting Thor router txids from JSON..."
python -m json.tool "$JSON_FILE" | \
    grep -A2 '"to": "0xd37bbe5744d730a1d98d8dc97c42f0ca46ad7146"' | \
    grep '"txid"' | \
    sed 's/.*"txid": "\([^"]*\)".*/\1/' | \
    sed 's/^0x//' | \
    tr '[:upper:]' '[:lower:]' | \
    sort -u > "$SCRIPT_DIR/thor_txids.txt"

total_txs=$(wc -l < "$SCRIPT_DIR/thor_txids.txt" | tr -d ' ')
echo "[INFO] Found $total_txs unique Thor router txids"
echo ""

# Write header
cat > "$OUTPUT_FILE" << HEADER
Thor Coverage Analysis Report
Generated: $(date)
Input: eth_partial_a5a023.json
Total Thor txids: $total_txs
================================================

HEADER

# Check against each dataset
for dataset_dir in "$THORCHAIN_DATA"/thorchain-2025*; do
    if [ ! -d "$dataset_dir" ]; then
        continue
    fi

    dataset_name=$(basename "$dataset_dir")
    echo "[INFO] Checking against $dataset_name..."

    # Count matches and collect matched txids
    match_count=0
    matched_txids=()

    while IFS= read -r txid; do
        if grep -qi "$txid" "$dataset_dir"/*.jsonl 2>/dev/null; then
            match_count=$((match_count + 1))
            matched_txids+=("$txid")
        fi
    done < "$SCRIPT_DIR/thor_txids.txt"

    # Calculate percentage
    if [ "$total_txs" -gt 0 ]; then
        percentage=$(awk "BEGIN {printf \"%.1f\", ($match_count/$total_txs)*100}")
    else
        percentage="0.0"
    fi

    # Write to report
    cat >> "$OUTPUT_FILE" << RESULT

Dataset: $dataset_name
  Matched: $match_count / $total_txs ($percentage%)
RESULT

    # Show matched txids (first 10)
    if [ "$match_count" -gt 0 ]; then
        echo "  Sample matches:" >> "$OUTPUT_FILE"
        for ((i=0; i<10 && i<match_count; i++)); do
            echo "    ${matched_txids[$i]}" >> "$OUTPUT_FILE"
        done
        if [ "$match_count" -gt 10 ]; then
            echo "    ... and $((match_count - 10)) more" >> "$OUTPUT_FILE"
        fi
    fi

    echo "  $dataset_name: $match_count / $total_txs ($percentage%)"
done

echo ""
echo "================================================"
echo "Report saved to: $OUTPUT_FILE"
echo "================================================"
