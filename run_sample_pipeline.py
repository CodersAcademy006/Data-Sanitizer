"""
Run a single file through Pass1 and Pass2 pipeline (local, no DB).
- Converts Excel (.xls/.xlsx/.csv.xls) to CSV if necessary
- Runs Pass1Worker to compute sampling & imputation stats
- Runs Pass2Worker to clean and dedupe, outputs cleaned CSV

Usage:
    python run_sample_pipeline.py --input <path-to-file> [--chunksize N] [--sample-size M]

Output:
  - cleaned file at ./output/cleaned_{original_basename}.csv
  - Pass1/Pass2 stats printed to stdout and saved as JSON alongside output
"""
import argparse
import os
import sys
import json
import tempfile
from pathlib import Path

import pandas as pd

# Ensure project root in path
sys.path.insert(0, os.path.dirname(__file__))

from worker_pass1 import Pass1Worker
from worker_pass2 import Pass2Worker


def convert_excel_to_csv(input_path: str, csv_out: str):
    df = pd.read_excel(input_path, engine='xlrd' if input_path.lower().endswith('.xls') else None)
    df.to_csv(csv_out, index=False)
    return csv_out


def main():
    parser = argparse.ArgumentParser(description='Run sample file through Data Sanitizer pipeline')
    parser.add_argument('--input', '-i', required=True, help='Path to input file (.csv, .jsonl, .parquet, .xls, .xlsx)')
    parser.add_argument('--chunksize', type=int, default=5000)
    parser.add_argument('--sample-size', type=int, default=2000)
    parser.add_argument('--output-dir', default='output')
    parser.add_argument('--job-id', default=None)
    args = parser.parse_args()

    input_path = os.path.abspath(args.input)
    if not os.path.exists(input_path):
        print(f"Input file not found: {input_path}")
        sys.exit(2)

    os.makedirs(args.output_dir, exist_ok=True)
    base = Path(input_path).stem
    cleaned_base = f"cleaned_{base}.csv"
    cleaned_path = os.path.join(args.output_dir, cleaned_base)
    stats_path = os.path.join(args.output_dir, f"stats_{base}.json")

    # If file is Excel (.xls or .xlsx) or has double extension like .csv.xls, convert
    lower = input_path.lower()
    needs_convert = lower.endswith('.xls') or lower.endswith('.xlsx') or lower.endswith('.csv.xls')

    to_process = input_path
    temp_csv = None
    if needs_convert:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        tmp.close()
        temp_csv = tmp.name
        print(f"Converting Excel to CSV: {input_path} -> {temp_csv}")
        try:
            convert_excel_to_csv(input_path, temp_csv)
            to_process = temp_csv
        except Exception as e:
            print(f"Failed to convert Excel to CSV: {e}")
            if os.path.exists(temp_csv):
                os.remove(temp_csv)
            sys.exit(3)

    # Run Pass 1
    print(f"Running Pass 1 on {to_process} (chunksize={args.chunksize}, sample_size={args.sample_size})...")
    p1 = Pass1Worker(storage_backend=None, job_id=args.job_id)
    p1_stats = p1.process_file(input_path=to_process, chunksize=args.chunksize, sample_size=args.sample_size)
    print("Pass 1 stats:\n", json.dumps(p1_stats, indent=2, default=str))

    # Save imputation stats if present into a small file for Pass2 to optionally use
    imputation_file = None
    if p1_stats.get('imputation_stats'):
        imputation_file = os.path.join(args.output_dir, f"imputation_{base}.json")
        with open(imputation_file, 'w') as f:
            json.dump(p1_stats['imputation_stats'], f, default=str)
        print(f"Saved imputation stats to {imputation_file}")

    # Run Pass 2
    print(f"Running Pass 2 to produce cleaned file at {cleaned_path}...")
    p2 = Pass2Worker(storage_backend=None, job_id=args.job_id)
    # If Pass2 supports reading imputation stats via schema_config, pass them
    schema_config = {}
    if imputation_file:
        schema_config = p1_stats.get('imputation_stats', {})

    p2_stats = p2.process_file(input_path=to_process, output_path=cleaned_path, chunksize=args.chunksize, schema_config=schema_config)
    print("Pass 2 stats:\n", json.dumps(p2_stats, indent=2, default=str))

    # Save combined stats
    combined = {'pass1': p1_stats, 'pass2': p2_stats}
    with open(stats_path, 'w') as f:
        json.dump(combined, f, indent=2, default=str)
    print(f"Saved combined stats to {stats_path}")

    # Clean up temp csv
    if temp_csv and os.path.exists(temp_csv):
        os.remove(temp_csv)

    print('\nPipeline complete.')
    print(f'Cleaned file: {cleaned_path}')
    print(f'Stats file: {stats_path}')

if __name__ == '__main__':
    main()