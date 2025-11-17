#!/usr/bin/env python3
"""
Quick-Start Demo: Data Sanitizer End-to-End

This script demonstrates the complete data cleaning pipeline:
1. Generate a small dirty dataset
2. Run Pass 1 (sampling & index building)
3. Run Pass 2 (cleaning & deduplication)
4. Display results

Usage:
    python demo_quickstart.py
"""

import os
import sys
import json
import tempfile
from pathlib import Path

import pandas as pd

# Add current directory to path
sys.path.insert(0, os.path.dirname(__file__))

from benchmark_generator import BenchmarkDataGenerator, generate_csv_dataset
from worker_pass1 import Pass1Worker
from worker_pass2 import Pass2Worker


def print_section(title):
    """Print a section header."""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def main():
    """Run the demo."""
    print_section("DATA SANITIZER: Quick-Start Demo")
    
    # Create temporary directory for demo
    with tempfile.TemporaryDirectory() as tmpdir:
        # ====================================================================
        # STEP 1: Generate Dirty Dataset
        # ====================================================================
        print_section("Step 1: Generate Dirty Dataset")
        
        input_file = os.path.join(tmpdir, "dirty_data.csv")
        print(f"Generating benchmark dataset: {input_file}")
        
        # Use the module-level function to generate CSV dataset
        generate_csv_dataset(
            num_rows=10_000,  # Small dataset for quick demo
            output_path=input_file,
            chunksize=5_000
        )
        
        # Show sample of dirty data
        df_dirty = pd.read_csv(input_file, nrows=10)
        print(f"\nGenerated {10_000:,} rows of test data")
        print("\nSample of dirty data (first 10 rows):")
        print(df_dirty.to_string())
        
        # ====================================================================
        # STEP 2: Pass 1 - Sampling & Index Building
        # ====================================================================
        print_section("Step 2: Pass 1 - Sampling & Index Building")
        
        pass1_worker = Pass1Worker(storage_backend=None)
        print("Running Pass 1 worker...")
        pass1_stats = pass1_worker.process_file(
            input_path=input_file,
            chunksize=1000,
            sample_size=500
        )
        
        print(f"\nPass 1 Complete:")
        print(f"  Total rows processed: {pass1_stats['total_rows']:,}")
        print(f"  Chunks processed: {pass1_stats['total_chunks']}")
        print(f"  Columns: {', '.join(pass1_stats['columns_processed'])}")
        print(f"  MinHash samples: {pass1_stats['minhash_samples']}")
        
        if pass1_stats.get("imputation_stats"):
            print(f"\n  Imputation Stats:")
            stats = pass1_stats["imputation_stats"]
            if stats.get("medians"):
                print(f"    Medians: {stats['medians']}")
            if stats.get("modes"):
                print(f"    Modes: {stats['modes']}")
        
        if pass1_stats.get("errors"):
            print(f"  Errors: {pass1_stats['errors']}")
            return 1
        
        # ====================================================================
        # STEP 3: Pass 2 - Cleaning & Deduplication
        # ====================================================================
        print_section("Step 3: Pass 2 - Cleaning & Deduplication")
        
        output_file = os.path.join(tmpdir, "cleaned_data.csv")
        print(f"Running Pass 2 worker...")
        print(f"Output: {output_file}")
        
        pass2_worker = Pass2Worker(storage_backend=None)
        pass2_stats = pass2_worker.process_file(
            input_path=input_file,
            output_path=output_file,
            chunksize=1000
        )
        
        print(f"\nPass 2 Complete:")
        print(f"  Total rows processed: {pass2_stats['total_rows']:,}")
        print(f"  Rows kept: {pass2_stats['rows_kept']:,}")
        print(f"  Rows dropped (duplicates): {pass2_stats['rows_dropped']:,}")
        print(f"  Deduplication rate: {pass2_stats['deduplication_rate']:.1%}")
        print(f"  Duplicates found: {pass2_stats['duplicates_found']}")
        print(f"  Near-duplicates found: {pass2_stats['near_duplicates_found']}")
        print(f"  Imputations applied: {pass2_stats['imputations_applied']}")
        print(f"  Normalizations applied: {pass2_stats['normalizations_applied']}")
        print(f"  Outliers detected: {pass2_stats['outliers_detected']}")
        print(f"  Average confidence score: {pass2_stats['confidence_score_avg']:.2f}")
        
        if pass2_stats.get("errors"):
            print(f"  Errors: {pass2_stats['errors']}")
            return 1
        
        # ====================================================================
        # STEP 4: Compare Results
        # ====================================================================
        print_section("Step 4: Results Comparison")
        
        df_cleaned = pd.read_csv(output_file)
        
        print(f"Original dataset: {len(df_dirty):,} rows (first 10 from full 10,000)")
        print(f"Cleaned dataset: {len(df_cleaned):,} rows")
        print(f"Reduction: {len(df_dirty) - len(df_cleaned):,} rows ({(1 - len(df_cleaned)/len(df_dirty)):.1%})")
        
        print(f"\nCleaned data (first 10 rows):")
        df_show = pd.read_csv(output_file, nrows=10)
        print(df_show.to_string())
        
        # ====================================================================
        # STEP 5: Quality Metrics
        # ====================================================================
        print_section("Step 5: Data Quality Metrics")
        
        print(f"Cleanliness Score:")
        print(f"  Deduplication Rate: {pass2_stats['deduplication_rate']:.1%}")
        print(f"  Confidence Score: {pass2_stats['confidence_score_avg']:.2f}/1.0")
        print(f"  Data Completeness: {(1 - pass2_stats['imputations_applied']/pass2_stats['total_rows']):.1%}")
        
        # ====================================================================
        # SUMMARY
        # ====================================================================
        print_section("Summary")
        
        print(f"✓ Pass 1: Generated {pass1_stats['minhash_samples']} LSH samples")
        print(f"✓ Pass 2: Processed {pass2_stats['total_rows']:,} rows")
        print(f"✓ Result: Removed {pass2_stats['rows_dropped']:,} duplicates")
        print(f"✓ Output: {output_file}")
        print(f"✓ Quality: {pass2_stats['confidence_score_avg']:.1%} average confidence")
        
        print(f"\n{'='*60}")
        print(f"Demo Complete! Data Sanitizer is working correctly.")
        print(f"{'='*60}\n")
        
        return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
