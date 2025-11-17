"""
Benchmark Dataset Generator

Generates realistic test datasets of varying sizes (1M, 10M, 100M rows)
with intentional dirty data patterns:
- Exact duplicates (5-10%)
- Near-duplicates with typos (2-5%)
- Missing values (5-15%)
- Outliers (1-2%)
- Schema drift (in JSONL/JSON formats)

Usage:
    python benchmark_generator.py --size 1m --format csv
    python benchmark_generator.py --size 10m --format parquet
    python benchmark_generator.py --size 100m --format jsonl
"""

import os
import json
import logging
import random
import argparse
from typing import Generator, List
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

try:
    import pyarrow.parquet as pq
    import pyarrow as pa
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ============================================================================
# REALISTIC DATA GENERATORS
# ============================================================================

class BenchmarkDataGenerator:
    """Generate realistic dirty datasets with configurable patterns."""
    
    FIRST_NAMES = [
        "John", "Jane", "Michael", "Sarah", "David", "Emma", "Robert", "Lisa",
        "James", "Mary", "Richard", "Patricia", "Joseph", "Jennifer", "Thomas", "Linda",
        "Charles", "Barbara", "Christopher", "Elizabeth", "Donald", "Susan", "Matthew", "Jessica"
    ]
    
    LAST_NAMES = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
        "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas",
        "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White"
    ]
    
    CITIES = [
        "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
        "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville",
        "Denver", "Boston", "Seattle", "Miami", "Atlanta", "Portland"
    ]
    
    STATES = ["CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI", "NJ", "VA"]
    
    INDUSTRIES = [
        "Technology", "Finance", "Healthcare", "Retail", "Manufacturing",
        "Education", "Hospitality", "Transportation", "Energy", "Telecommunications"
    ]
    
    def __init__(self, seed: int = 42, dirty_rate: float = 0.2):
        """
        Initialize generator.
        
        Args:
            seed: Random seed for reproducibility
            dirty_rate: Proportion of rows with dirty data (duplicates, nulls, outliers)
        """
        self.seed = seed
        self.dirty_rate = dirty_rate
        random.seed(seed)
        np.random.seed(seed)
    
    def generate_customer_record(self, row_id: int) -> dict:
        """Generate a single customer record."""
        return {
            "customer_id": f"CUST-{row_id:09d}",
            "first_name": random.choice(self.FIRST_NAMES),
            "last_name": random.choice(self.LAST_NAMES),
            "email": f"{random.choice(self.FIRST_NAMES).lower()}.{random.choice(self.LAST_NAMES).lower()}@example.com",
            "phone": f"+1-{random.randint(200, 999)}-{random.randint(2000, 9999)}-{random.randint(1000, 9999)}",
            "city": random.choice(self.CITIES),
            "state": random.choice(self.STATES),
            "zip_code": f"{random.randint(10000, 99999)}",
            "industry": random.choice(self.INDUSTRIES),
            "signup_date": (datetime.now() - timedelta(days=random.randint(1, 1000))).strftime("%Y-%m-%d"),
            "total_spend": round(random.uniform(100, 50000), 2),
            "account_status": random.choice(["active", "inactive", "suspended", "pending"])
        }
    
    def introduce_noise(self, record: dict, row_id: int) -> dict:
        """Introduce realistic dirty data patterns."""
        record = record.copy()
        noise_type = random.random()
        
        # 50% chance of exact duplicate (copy from earlier row)
        if noise_type < 0.05:
            # Exact duplicate (done at batch level)
            return record
        
        # 20% chance of missing values
        elif noise_type < 0.15:
            nullable_fields = ["email", "phone", "city", "industry"]
            field_to_null = random.choice(nullable_fields)
            record[field_to_null] = None
        
        # 15% chance of typos (near-duplicate)
        elif noise_type < 0.25:
            name_field = random.choice(["first_name", "last_name"])
            name = str(record[name_field])
            if len(name) > 2:
                idx = random.randint(0, len(name) - 1)
                # Swap adjacent characters or replace
                if random.random() < 0.5:
                    name = name[:idx] + random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + name[idx+1:]
                else:
                    name = name[:idx] + name[idx+1:idx+2] + name[idx] + name[idx+2:]
                record[name_field] = name
        
        # 5% chance of outlier (e.g., extreme spend)
        elif noise_type < 0.30:
            record["total_spend"] = round(random.uniform(50000, 1000000), 2)
        
        # 5% chance of extra whitespace or case issues
        elif noise_type < 0.35:
            field = random.choice(["first_name", "last_name", "city"])
            record[field] = "  " + str(record[field]) + "  "
        
        return record
    
    def stream_csv_records(self, num_rows: int, chunksize: int = 10000) -> Generator[pd.DataFrame, None, None]:
        """Stream CSV data as DataFrames."""
        for chunk_start in range(0, num_rows, chunksize):
            chunk_end = min(chunk_start + chunksize, num_rows)
            records = []
            
            for row_id in range(chunk_start, chunk_end):
                record = self.generate_customer_record(row_id)
                
                # Add noise
                if random.random() < self.dirty_rate:
                    record = self.introduce_noise(record, row_id)
                
                # Add exact duplicates
                if random.random() < 0.05 and row_id > 0:
                    dup_idx = random.randint(chunk_start, row_id - 1)
                    record = self.generate_customer_record(dup_idx)
                
                records.append(record)
            
            yield pd.DataFrame(records)
            logger.info(f"Generated {chunk_end}/{num_rows} records")
    
    def stream_jsonl_records(self, num_rows: int, chunksize: int = 10000) -> Generator[List[str], None, None]:
        """Stream JSONL data (includes schema drift)."""
        for chunk_start in range(0, num_rows, chunksize):
            chunk_end = min(chunk_start + chunksize, num_rows)
            lines = []
            
            for row_id in range(chunk_start, chunk_end):
                record = self.generate_customer_record(row_id)
                
                # Schema drift: add random extra fields in some records
                if random.random() < 0.1:
                    record["extra_field"] = f"extra_{random.randint(1, 100)}"
                
                if random.random() < 0.05:
                    record["nested"] = {
                        "level1": {
                            "level2": f"value_{random.randint(1, 1000)}"
                        }
                    }
                
                # Add noise
                if random.random() < self.dirty_rate:
                    record = self.introduce_noise(record, row_id)
                
                lines.append(json.dumps(record))
            
            yield lines
            logger.info(f"Generated {chunk_end}/{num_rows} JSONL records")

# ============================================================================
# DATASET GENERATION FUNCTIONS
# ============================================================================

def generate_csv_dataset(num_rows: int, output_path: str, chunksize: int = 50000):
    """Generate CSV dataset and write to file."""
    gen = BenchmarkDataGenerator(dirty_rate=0.2)
    
    logger.info(f"Generating {num_rows:,} row CSV dataset to {output_path}")
    
    first_write = True
    for chunk_df in gen.stream_csv_records(num_rows, chunksize=chunksize):
        mode = "w" if first_write else "a"
        chunk_df.to_csv(output_path, index=False, header=first_write, mode=mode)
        first_write = False
    
    logger.info(f"CSV dataset complete: {output_path}")

def generate_jsonl_dataset(num_rows: int, output_path: str, chunksize: int = 50000):
    """Generate JSONL dataset and write to file."""
    gen = BenchmarkDataGenerator(dirty_rate=0.2)
    
    logger.info(f"Generating {num_rows:,} row JSONL dataset to {output_path}")
    
    with open(output_path, "w") as f:
        for lines in gen.stream_jsonl_records(num_rows, chunksize=chunksize):
            for line in lines:
                f.write(line + "\n")
    
    logger.info(f"JSONL dataset complete: {output_path}")

def generate_parquet_dataset(num_rows: int, output_path: str, chunksize: int = 50000):
    """Generate Parquet dataset and write to file."""
    if not HAS_PYARROW:
        raise RuntimeError("pyarrow required for Parquet generation")
    
    gen = BenchmarkDataGenerator(dirty_rate=0.2)
    
    logger.info(f"Generating {num_rows:,} row Parquet dataset to {output_path}")
    
    # Write in chunks using Parquet's streaming capability
    writer = None
    for chunk_df in gen.stream_csv_records(num_rows, chunksize=chunksize):
        table = pa.Table.from_pandas(chunk_df)
        if writer is None:
            writer = pq.ParquetWriter(output_path, table.schema)
        writer.write_table(table)
    
    if writer:
        writer.close()
    
    logger.info(f"Parquet dataset complete: {output_path}")

# ============================================================================
# BENCHMARK HARNESS
# ============================================================================

def run_benchmarks(dataset_size: int, output_dir: str = "./benchmark_datasets"):
    """
    Run comprehensive benchmarks on generated datasets.
    Measures:
    - File size
    - Ingestion time (pass 1)
    - Cleaning time (pass 2)
    - Throughput (rows/sec)
    """
    os.makedirs(output_dir, exist_ok=True)
    
    logger.info("=" * 60)
    logger.info(f"BENCHMARK: {dataset_size:,} rows")
    logger.info("=" * 60)
    
    # Generate CSV dataset
    csv_path = os.path.join(output_dir, f"benchmark_{dataset_size}_rows.csv")
    if not os.path.exists(csv_path):
        generate_csv_dataset(dataset_size, csv_path)
    
    csv_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
    logger.info(f"CSV file size: {csv_size_mb:.2f} MB")
    
    # Generate JSONL dataset
    jsonl_path = os.path.join(output_dir, f"benchmark_{dataset_size}_rows.jsonl")
    if not os.path.exists(jsonl_path):
        generate_jsonl_dataset(dataset_size, jsonl_path)
    
    jsonl_size_mb = os.path.getsize(jsonl_path) / (1024 * 1024)
    logger.info(f"JSONL file size: {jsonl_size_mb:.2f} MB")
    
    # Generate Parquet dataset
    parquet_path = os.path.join(output_dir, f"benchmark_{dataset_size}_rows.parquet")
    if not os.path.exists(parquet_path):
        try:
            generate_parquet_dataset(dataset_size, parquet_path)
            parquet_size_mb = os.path.getsize(parquet_path) / (1024 * 1024)
            logger.info(f"Parquet file size: {parquet_size_mb:.2f} MB")
        except Exception as e:
            logger.warning(f"Parquet generation skipped: {e}")
    
    logger.info(f"\nBenchmark datasets ready in {output_dir}/")
    logger.info("Next: Run data cleaning pipeline on these datasets and measure performance")

# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Generate benchmark datasets for Data Sanitizer")
    parser.add_argument(
        "--size",
        choices=["1m", "10m", "100m"],
        default="1m",
        help="Dataset size (1 million, 10 million, or 100 million rows)"
    )
    parser.add_argument(
        "--output-dir",
        default="./benchmark_datasets",
        help="Output directory for datasets"
    )
    
    args = parser.parse_args()
    
    size_map = {"1m": 1_000_000, "10m": 10_000_000, "100m": 100_000_000}
    num_rows = size_map[args.size]
    
    run_benchmarks(num_rows, args.output_dir)

if __name__ == "__main__":
    main()
