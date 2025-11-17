"""
Pass 1 Worker: Sampling & Index Building

Responsibilities:
1. Stream input file from S3/local storage
2. Compute deterministic reservoirs per column
3. Insert MinHash/LSH samples into Milvus
4. Insert imputation stats (medians, modes) into Postgres
5. Return summary statistics

This is a stateless, horizontally-scalable worker.
"""

import logging
import json
import uuid
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import os

import pandas as pd
import numpy as np

# Import from our modules
from data_cleaning import (
    DeterministicReservoir,
    compute_minhash_signature,
    flatten_json,
    _shingles
)

logger = logging.getLogger(__name__)


class Pass1Worker:
    """
    Pass 1 Worker: Sampling and index building.
    """
    
    def __init__(self, storage_backend=None, job_id: str = None):
        """
        Args:
            storage_backend: StorageBackend instance (Postgres + Milvus)
            job_id: UUID of the job
        """
        self.storage = storage_backend
        self.job_id = job_id or str(uuid.uuid4())
        self.logger = logging.getLogger(f"{__name__}.{self.job_id}")
    
    def process_file(
        self,
        input_path: str,
        chunksize: int = 50_000,
        sample_size: int = 10_000,
        schema_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Process input file in Pass 1.
        
        Args:
            input_path: Path to input file (CSV, JSONL, etc.)
            chunksize: Rows per chunk
            sample_size: Reservoir size per column
            schema_config: Schema and column configuration
        
        Returns:
            Summary statistics dict
        """
        self.logger.info(f"Pass 1 starting: {input_path}")
        
        schema_config = schema_config or {}
        stats = {
            "job_id": self.job_id,
            "total_rows": 0,
            "total_chunks": 0,
            "columns_processed": [],
            "reservoirs": {},
            "imputation_stats": {},
            "minhash_samples": 0,
            "errors": [],
            "started_at": datetime.utcnow().isoformat(),
            "completed_at": None
        }
        
        try:
            # Initialize reservoirs for sampling (per column)
            reservoirs: Dict[str, DeterministicReservoir] = {}
            all_column_names = None
            
            # Stream chunks
            for chunk_idx, chunk in enumerate(self._stream_chunks(input_path, chunksize)):
                self.logger.info(f"Processing chunk {chunk_idx + 1}")
                stats["total_chunks"] += 1
                
                # Get column names from first chunk
                if all_column_names is None:
                    all_column_names = list(chunk.columns)
                    stats["columns_processed"] = all_column_names
                    # Initialize reservoirs
                    for col in all_column_names:
                        reservoirs[col] = DeterministicReservoir(
                            capacity=sample_size,
                            salt=f"{self.job_id}:{col}"
                        )
                
                # Process each row: flatten JSON, update reservoirs, compute MinHash
                for row_idx, row in chunk.iterrows():
                    absolute_row_id = stats["total_rows"]
                    stats["total_rows"] += 1
                    
                    # Flatten JSON if needed
                    flattened_row = flatten_json(row.to_dict())
                    
                    # Add to reservoirs
                    for col in all_column_names:
                        value = flattened_row.get(col, None)
                        if pd.notna(value):
                            reservoirs[col].add(absolute_row_id, str(value))
                    
                    # Compute MinHash for this row (for LSH)
                    if self.storage and chunk_idx == 0 and row_idx < 100:  # Sample first rows
                        try:
                            minhash = compute_minhash_signature(flattened_row, num_perm=64)
                            self._insert_lsh_sample(absolute_row_id, minhash)
                            stats["minhash_samples"] += 1
                        except Exception as e:
                            self.logger.warning(f"Failed to compute MinHash: {e}")
                
                # Update progress in storage
                if self.storage:
                    progress = (chunk_idx + 1) / max(1, stats["total_chunks"])
                    self._update_progress(progress)
            
            # Compute imputation stats (medians, modes)
            imputation_stats = self._compute_imputation_stats(chunk, reservoirs)
            stats["imputation_stats"] = imputation_stats
            
            # Store imputation stats to Postgres
            if self.storage:
                self.storage.store_imputation_stats(
                    self.job_id,
                    medians=imputation_stats.get("medians", {}),
                    modes=imputation_stats.get("modes", {})
                )
            
            # Log audit event
            if self.storage:
                self.storage.insert_audit_log(
                    self.job_id,
                    action="pass1_completed",
                    details={
                        "total_rows": stats["total_rows"],
                        "minhash_samples": stats["minhash_samples"],
                        "columns": all_column_names
                    }
                )
            
            stats["completed_at"] = datetime.utcnow().isoformat()
            self.logger.info(f"Pass 1 completed: {stats['total_rows']} rows processed")
            return stats
        
        except Exception as e:
            self.logger.error(f"Pass 1 failed: {e}", exc_info=True)
            stats["errors"].append(str(e))
            stats["completed_at"] = datetime.utcnow().isoformat()
            
            if self.storage:
                self.storage.update_job_status(
                    self.job_id,
                    status="failed",
                    error=str(e)
                )
            
            return stats
    
    def _stream_chunks(self, input_path: str, chunksize: int):
        """Stream chunks from input file."""
        if input_path.endswith(".csv"):
            return pd.read_csv(input_path, chunksize=chunksize)
        elif input_path.endswith(".jsonl"):
            return self._stream_jsonl(input_path, chunksize)
        elif input_path.endswith(".parquet"):
            return self._stream_parquet(input_path, chunksize)
        else:
            raise ValueError(f"Unsupported format: {input_path}")
    
    def _stream_jsonl(self, path: str, chunksize: int):
        """Stream JSONL file in chunks."""
        chunk = []
        with open(path, 'r') as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    chunk.append(obj)
                    if len(chunk) >= chunksize:
                        yield pd.DataFrame(chunk)
                        chunk = []
                except json.JSONDecodeError as e:
                    self.logger.warning(f"Invalid JSON line: {e}")
            if chunk:
                yield pd.DataFrame(chunk)
    
    def _stream_parquet(self, path: str, chunksize: int):
        """Stream Parquet file in chunks."""
        try:
            import pyarrow.parquet as pq
            parquet_file = pq.read_table(path)
            df = parquet_file.to_pandas()
            for i in range(0, len(df), chunksize):
                yield df.iloc[i:i+chunksize]
        except ImportError:
            raise RuntimeError("pyarrow required for Parquet support")
    
    def _insert_lsh_sample(self, row_id: int, minhash: List[int]):
        """Insert LSH sample into Milvus."""
        if not self.storage:
            return
        
        try:
            bucket_key = f"bucket_{minhash[0] % 100}"
            self.storage.batch_insert_lsh_samples([{
                "job_id": self.job_id,
                "bucket_key": bucket_key,
                "sampled_row_id": row_id,
                "snippet": f"row_{row_id}",
                "minhash_vector": [float(x) for x in minhash[:64]]
            }])
        except Exception as e:
            self.logger.warning(f"Failed to insert LSH sample: {e}")
    
    def _update_progress(self, progress: float):
        """Update job progress in Redis."""
        if not self.storage or not self.storage.redis_client:
            return
        
        try:
            self.storage.redis_client.set(
                f"job:{self.job_id}:pass1_progress",
                progress,
                ex=3600  # 1 hour TTL
            )
        except Exception as e:
            self.logger.warning(f"Failed to update progress: {e}")
    
    def _compute_imputation_stats(self, last_chunk: pd.DataFrame, reservoirs: Dict) -> Dict[str, Any]:
        """Compute medians and modes from sample data."""
        stats = {"medians": {}, "modes": {}}
        
        for col, reservoir in reservoirs.items():
            samples = reservoir.get_values()
            
            if not samples:
                continue
            
            # Try to compute median (numeric columns)
            try:
                numeric_samples = pd.to_numeric(samples, errors='coerce')
                numeric_samples = numeric_samples.dropna()
                if len(numeric_samples) > 0:
                    stats["medians"][col] = float(numeric_samples.median())
            except Exception:
                pass
            
            # Compute mode (most common value)
            try:
                mode_value = pd.Series(samples).mode()
                if len(mode_value) > 0:
                    stats["modes"][col] = str(mode_value[0])
            except Exception:
                pass
        
        return stats


def main():
    """CLI entry point for Pass 1 worker."""
    import argparse
    import sys
    
    logging.basicConfig(level=logging.INFO)
    
    parser = argparse.ArgumentParser(description="Pass 1 Worker: Sampling & Index Building")
    parser.add_argument("--job-id", default=str(uuid.uuid4()), help="Job UUID")
    parser.add_argument("--input-file", required=True, help="Path to input file (CSV, JSONL, Parquet)")
    parser.add_argument("--chunksize", type=int, default=50_000, help="Rows per chunk")
    parser.add_argument("--sample-size", type=int, default=10_000, help="Reservoir sample size")
    args = parser.parse_args()
    
    worker = Pass1Worker(job_id=args.job_id)
    stats = worker.process_file(
        input_path=args.input_file,
        chunksize=args.chunksize,
        sample_size=args.sample_size
    )
    
    print(json.dumps(stats, indent=2, default=str))
    return 0 if not stats.get("errors") else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
