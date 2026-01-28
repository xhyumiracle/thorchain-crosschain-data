#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Analyze time_delta statistics from thor_crosschain_links.
"""

import json
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

BYBITHACK_DIR = Path(__file__).parent.parent.parent  # bybithack/
DATA_DIR = BYBITHACK_DIR / "data"
JSON_FILE = DATA_DIR / "partial_a5a023_eth_raw.json"

def analyze_time_delta():
    # Load data
    with open(JSON_FILE, 'r') as f:
        data = json.load(f)
    
    links = data.get("thor_crosschain_links", [])
    time_deltas = [link.get("time_delta") for link in links if "time_delta" in link]
    
    print("=" * 70)
    print("Time Delta Analysis (ETH→BTC Swaps)")
    print("=" * 70)
    print()
    
    # Statistics
    time_deltas_arr = np.array(time_deltas)
    print(f"Total swaps: {len(time_deltas)}")
    print()
    print("Time Delta Statistics (seconds):")
    print(f"  Min:    {np.min(time_deltas_arr):6d}s  ({np.min(time_deltas_arr)/60:.1f} min)")
    print(f"  Max:    {np.max(time_deltas_arr):6d}s  ({np.max(time_deltas_arr)/60:.1f} min)")
    print(f"  Mean:   {np.mean(time_deltas_arr):6.0f}s  ({np.mean(time_deltas_arr)/60:.1f} min)")
    print(f"  Median: {np.median(time_deltas_arr):6.0f}s  ({np.median(time_deltas_arr)/60:.1f} min)")
    print(f"  Std:    {np.std(time_deltas_arr):6.0f}s  ({np.std(time_deltas_arr)/60:.1f} min)")
    print()
    
    # Percentiles
    print("Percentiles:")
    for p in [25, 50, 75, 90, 95, 99]:
        val = np.percentile(time_deltas_arr, p)
        print(f"  P{p:2d}:    {val:6.0f}s  ({val/60:.1f} min)")
    print()
    
    # Distribution by time ranges
    print("Distribution by time ranges:")
    ranges = [
        (0, 3000, "< 50 min"),
        (3000, 3600, "50-60 min"),
        (3600, 4200, "60-70 min"),
        (4200, 5400, "70-90 min"),
        (5400, float('inf'), "> 90 min"),
    ]
    
    for min_val, max_val, label in ranges:
        count = sum(1 for t in time_deltas if min_val <= t < max_val)
        pct = count / len(time_deltas) * 100
        print(f"  {label:12s}: {count:2d} swaps ({pct:5.1f}%)")
    
    print()
    print("=" * 70)
    
    # Create visualization
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('THORChain ETH→BTC Swap Time Analysis (Bybit Hack)', fontsize=14, fontweight='bold')
    
    # 1. Histogram
    ax1 = axes[0, 0]
    ax1.hist(time_deltas_arr / 60, bins=20, color='steelblue', edgecolor='black', alpha=0.7)
    ax1.axvline(np.mean(time_deltas_arr) / 60, color='red', linestyle='--', linewidth=2, label=f'Mean: {np.mean(time_deltas_arr)/60:.1f} min')
    ax1.axvline(np.median(time_deltas_arr) / 60, color='green', linestyle='--', linewidth=2, label=f'Median: {np.median(time_deltas_arr)/60:.1f} min')
    ax1.set_xlabel('Time Delta (minutes)')
    ax1.set_ylabel('Frequency')
    ax1.set_title('Distribution of Swap Times')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # 2. Box plot
    ax2 = axes[0, 1]
    bp = ax2.boxplot([time_deltas_arr / 60], vert=True, patch_artist=True, widths=0.5)
    bp['boxes'][0].set_facecolor('lightblue')
    bp['medians'][0].set_color('red')
    bp['medians'][0].set_linewidth(2)
    ax2.set_ylabel('Time Delta (minutes)')
    ax2.set_title('Box Plot of Swap Times')
    ax2.set_xticklabels(['All Swaps'])
    ax2.grid(True, alpha=0.3, axis='y')
    
    # 3. Time series
    ax3 = axes[1, 0]
    indices = list(range(len(time_deltas)))
    ax3.scatter(indices, time_deltas_arr / 60, alpha=0.6, color='steelblue', s=50)
    ax3.plot(indices, time_deltas_arr / 60, alpha=0.3, color='gray', linestyle='-')
    ax3.axhline(np.mean(time_deltas_arr) / 60, color='red', linestyle='--', linewidth=1, label='Mean')
    ax3.set_xlabel('Swap Index')
    ax3.set_ylabel('Time Delta (minutes)')
    ax3.set_title('Time Delta by Swap Order')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # 4. Cumulative distribution
    ax4 = axes[1, 1]
    sorted_deltas = np.sort(time_deltas_arr / 60)
    cumulative = np.arange(1, len(sorted_deltas) + 1) / len(sorted_deltas) * 100
    ax4.plot(sorted_deltas, cumulative, linewidth=2, color='steelblue')
    ax4.axhline(50, color='red', linestyle='--', alpha=0.5, label='50th percentile')
    ax4.axhline(90, color='orange', linestyle='--', alpha=0.5, label='90th percentile')
    ax4.set_xlabel('Time Delta (minutes)')
    ax4.set_ylabel('Cumulative Percentage (%)')
    ax4.set_title('Cumulative Distribution')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Save plot
    output_file = BYBITHACK_DIR / "time_delta_analysis.png"
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"Plot saved to: {output_file}")
    print("=" * 70)

if __name__ == "__main__":
    analyze_time_delta()
