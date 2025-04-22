#!/usr/bin/env python3
"""
Skew Detection Utility for MapReduce Workflows

This script analyzes the distribution of keys in a dataset to detect potential skew,
helping optimize join operations in large-scale analytics tasks.

Usage:
    python3 skew_detection.py < input_data.txt > skew_analysis.json

Input:  Lines of tab-separated key-value pairs.
Output: A JSON object containing skew analysis statistics.
"""

import sys
import json
import numpy as np
from collections import Counter

def analyze_key_distribution(lines, skew_threshold_factor=0.01):
    """
    Analyze key distribution to identify skewed keys.

    Args:
        lines (iterable): Lines containing tab-separated key-value pairs.
        skew_threshold_factor (float): Fraction of total records considered for skew detection.

    Returns:
        dict: Skew analysis results including skewed keys, distribution stats, etc.
    """
    key_counter = Counter()
    total_records = 0

    for line in lines:
        total_records += 1
        try:
            key = line.strip().split('\t')[0]
            if ',' in key:  # Handle composite keys
                key = key.split(',')[0]
            key_counter[key] += 1
        except Exception as e:
            sys.stderr.write(f"Error processing line: {line.strip()} | Error: {e}\n")

    if not key_counter:
        return {
            "total_records": 0,
            "unique_keys": 0,
            "skew_threshold": 0,
            "skewed_keys": [],
            "top_keys": []
        }

    unique_keys = len(key_counter)
    avg_records_per_key = total_records / unique_keys
    skew_threshold = max(skew_threshold_factor * total_records, avg_records_per_key * 5)

    skewed_keys = [key for key, count in key_counter.items() if count > skew_threshold]
    top_keys = key_counter.most_common(10)
    counts = np.array(list(key_counter.values()))

    return {
        "total_records": total_records,
        "unique_keys": unique_keys,
        "average_records_per_key": avg_records_per_key,
        "skew_threshold": skew_threshold,
        "skewed_keys": skewed_keys,
        "skewed_keys_count": len(skewed_keys),
        "top_keys": top_keys,
        "distribution_stats": {
            "min": int(np.min(counts)),
            "max": int(np.max(counts)),
            "median": float(np.median(counts)),
            "mean": float(np.mean(counts)),
            "std_dev": float(np.std(counts)),
            "p90": float(np.percentile(counts, 90)),
            "p95": float(np.percentile(counts, 95)),
            "p99": float(np.percentile(counts, 99))
        }
    }

def main():
    """Entry point for the skew detection utility."""
    try:
        lines = sys.stdin.readlines()
        analysis = analyze_key_distribution(lines)
        print(json.dumps(analysis, indent=2))
    except Exception as e:
        error_report = {
            "error": str(e),
            "skewed_keys": []
        }
        print(json.dumps(error_report, indent=2))
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
