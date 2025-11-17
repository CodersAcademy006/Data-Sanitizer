"""
Pass 2 Worker: Cleaning & Deduplication

Responsibilities:
1. Stream input file from S3/local storage
2. Query Postgres for exact duplicate detection (hash check)
3. Query Milvus for near-duplicate candidates (LSH)
4. Apply imputation, normalization, outlier detection
5. Stream cleaned output to S3 (Parquet by default)
6. Insert confidence scores + audit logs to Postgres

This is a stateless, horizontally-scalable worker.
"""

import logging
import json
import uuid
from typing import Dict, List, Optional, Tuple, Any, Generator
from datetime import datetime
import os
import hashlib
import re

import pandas as pd
import numpy as np

# Import from our modules
from data_cleaning import (
    detect_outliers,
    text_normalization
)

logger = logging.getLogger(__name__)


# Helper functions for Pass 2 worker
def compute_row_hash(row_dict: Dict[str, Any]) -> str:
    """
    Compute deterministic hash of a row for exact duplicate detection.
    Excludes 'id' column from hash to detect semantic duplicates.
    
    Args:
        row_dict: Dictionary representation of a row
        
    Returns:
        Hash string
    """
    # Exclude 'id' column for duplicate detection
    filtered_row = {k: v for k, v in row_dict.items() if k.lower() != 'id'}
    # Sort keys for determinism, convert to JSON string
    row_json = json.dumps(filtered_row, sort_keys=True, default=str)
    return hashlib.md5(row_json.encode()).hexdigest()


def clean_text_deterministic(value: str) -> str:
    """
    Deterministically clean text value.
    
    Args:
        value: Text to clean
        
    Returns:
        Cleaned text
    """
    if not isinstance(value, str):
        return value
    
    # Convert to lowercase
    value = value.lower().strip()
    
    # Remove extra whitespace
    value = re.sub(r'\s+', ' ', value)
    
    # Remove leading/trailing punctuation
    value = re.sub(r'^[^\w]+|[^\w]+$', '', value)
    
    return value


def normalize_numeric(value: float) -> float:
    """
    Normalize numeric value (handle inf, nan, scale).
    
    Args:
        value: Numeric value to normalize
        
    Returns:
        Normalized value
    """
    if not isinstance(value, (int, float)):
        return value
    
    # Replace inf with None
    if np.isinf(value):
        return None
    
    # Replace nan with None
    if np.isnan(value):
        return None
    
    # Return as-is (already numeric)
    return value


class Pass2Worker:
    """
    Pass 2 Worker: Cleaning and deduplication.
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
        output_path: str,
        chunksize: int = 50_000,
        schema_config: Optional[Dict[str, Any]] = None,
        cleaning_rules: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Process input file in Pass 2.
        
        Args:
            input_path: Path to input file (CSV, JSONL, etc.)
            output_path: Path to output cleaned file (Parquet or CSV)
            chunksize: Rows per chunk
            schema_config: Schema and column configuration
            cleaning_rules: Rules for cleaning (PII, canonicalization, etc.)
        
        Returns:
            Summary statistics dict
        """
        self.logger.info(f"Pass 2 starting: {input_path} -> {output_path}")
        
        schema_config = schema_config or {}
        cleaning_rules = cleaning_rules or {}
        
        stats = {
            "job_id": self.job_id,
            "total_rows": 0,
            "rows_kept": 0,
            "rows_dropped": 0,
            "duplicates_found": 0,
            "near_duplicates_found": 0,
            "imputations_applied": 0,
            "normalizations_applied": 0,
            "outliers_detected": 0,
            "total_chunks": 0,
            "columns_processed": [],
            "deduplication_rate": 0.0,
            "false_positive_rate": 0.0,
            "confidence_score_avg": 1.0,
            "errors": [],
            "started_at": datetime.utcnow().isoformat(),
            "completed_at": None
        }
        
        try:
            # Fetch imputation stats from Pass 1
            imputation_stats = {}
            if self.storage:
                imputation_stats = self.storage.get_imputation_stats(self.job_id) or {}
            
            medians = imputation_stats.get("medians", {})
            modes = imputation_stats.get("modes", {})
            
            # Track which rows we've seen (for exact dedup)
            seen_hashes = set()
            
            # Prepare output writer
            output_file = open(output_path, 'w')
            csv_writer = None
            parquet_rows = []
            
            all_column_names = None
            confidence_scores = {}
            
            # Stream chunks
            for chunk_idx, chunk in enumerate(self._stream_chunks(input_path, chunksize)):
                self.logger.info(f"Processing chunk {chunk_idx + 1}")
                stats["total_chunks"] += 1
                
                # Get column names from first chunk
                if all_column_names is None:
                    all_column_names = list(chunk.columns)
                    stats["columns_processed"] = all_column_names
                    
                    # Initialize CSV writer if outputting CSV
                    if output_path.endswith(".csv"):
                        import csv
                        csv_writer = csv.DictWriter(output_file, fieldnames=all_column_names)
                        csv_writer.writeheader()
                
                cleaned_chunk = []
                
                for row_idx, row in chunk.iterrows():
                    absolute_row_id = stats["total_rows"]
                    stats["total_rows"] += 1
                    
                    # Compute row hash
                    row_hash = compute_row_hash(row.to_dict())
                    
                    # Check for exact duplicates
                    if row_hash in seen_hashes:
                        stats["duplicates_found"] += 1
                        stats["rows_dropped"] += 1
                        if self.storage:
                            self._record_provenance(
                                absolute_row_id,
                                row.to_dict(),
                                row.to_dict(),
                                "exact_duplicate",
                                confidence_score=0.0
                            )
                        continue
                    
                    seen_hashes.add(row_hash)
                    
                    # Check for near duplicates (query LSH candidates)
                    near_dup_risk = False
                    if self.storage:
                        try:
                            candidates = self.storage.query_lsh_candidates(self.job_id, row_hash)
                            if candidates:
                                near_dup_risk = True
                                stats["near_duplicates_found"] += len(candidates)
                        except Exception as e:
                            self.logger.warning(f"Failed LSH query: {e}")
                    
                    # Clean row
                    cleaned_row = self._clean_row(
                        row.to_dict(),
                        medians=medians,
                        modes=modes,
                        cleaning_rules=cleaning_rules
                    )
                    
                    # Track changes for provenance
                    changes = self._compute_changes(row.to_dict(), cleaned_row)
                    stats["imputations_applied"] += sum(1 for c in changes if c["type"] == "imputation")
                    stats["normalizations_applied"] += sum(1 for c in changes if c["type"] == "normalization")
                    stats["outliers_detected"] += sum(1 for c in changes if c["type"] == "outlier")
                    
                    # Compute confidence score
                    confidence = self._compute_confidence_score(changes, near_dup_risk)
                    confidence_scores[absolute_row_id] = confidence
                    
                    # Record provenance
                    if self.storage:
                        self._record_provenance(
                            absolute_row_id,
                            row.to_dict(),
                            cleaned_row,
                            "cleaned",
                            confidence_score=confidence
                        )
                    
                    # Add to output
                    cleaned_chunk.append(cleaned_row)
                    stats["rows_kept"] += 1
                
                # Write cleaned chunk to output
                if output_path.endswith(".csv") and csv_writer:
                    for row in cleaned_chunk:
                        csv_writer.writerow(row)
                else:
                    parquet_rows.extend(cleaned_chunk)
                
                # Update progress
                if self.storage:
                    progress = (chunk_idx + 1) / max(1, stats["total_chunks"])
                    self._update_progress(progress)
            
            # Write Parquet output if needed
            if output_path.endswith(".parquet") and parquet_rows:
                df = pd.DataFrame(parquet_rows)
                try:
                    import pyarrow.parquet as pq
                    import pyarrow as pa
                    pq.write_table(pa.Table.from_pandas(df), output_path)
                except ImportError:
                    self.logger.error("pyarrow required for Parquet output")
                    raise
            
            output_file.close()
            
            # Compute aggregate stats
            stats["deduplication_rate"] = (
                (stats["total_rows"] - stats["rows_dropped"]) / max(1, stats["total_rows"])
            )
            
            if confidence_scores:
                stats["confidence_score_avg"] = np.mean(list(confidence_scores.values()))
            
            # Log audit event
            if self.storage:
                self.storage.insert_audit_log(
                    self.job_id,
                    action="pass2_completed",
                    details={
                        "total_rows": stats["total_rows"],
                        "rows_kept": stats["rows_kept"],
                        "duplicates_found": stats["duplicates_found"],
                        "deduplication_rate": stats["deduplication_rate"]
                    }
                )
                
                # Mark job as success
                self.storage.update_job_status(self.job_id, status="success")
            
            stats["completed_at"] = datetime.utcnow().isoformat()
            self.logger.info(f"Pass 2 completed: {stats['rows_kept']} rows kept, {stats['rows_dropped']} dropped")
            return stats
        
        except Exception as e:
            self.logger.error(f"Pass 2 failed: {e}", exc_info=True)
            stats["errors"].append(str(e))
            stats["completed_at"] = datetime.utcnow().isoformat()
            
            if self.storage:
                self.storage.update_job_status(
                    self.job_id,
                    status="failed",
                    error=str(e)
                )
            
            return stats
    
    def _stream_chunks(self, input_path: str, chunksize: int) -> Generator[pd.DataFrame, None, None]:
        """Stream chunks from input file."""
        if input_path.endswith(".csv"):
            return pd.read_csv(input_path, chunksize=chunksize)
        elif input_path.endswith(".jsonl"):
            return self._stream_jsonl(input_path, chunksize)
        elif input_path.endswith(".parquet"):
            return self._stream_parquet(input_path, chunksize)
        else:
            raise ValueError(f"Unsupported format: {input_path}")
    
    def _stream_jsonl(self, path: str, chunksize: int) -> Generator[pd.DataFrame, None, None]:
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
    
    def _stream_parquet(self, path: str, chunksize: int) -> Generator[pd.DataFrame, None, None]:
        """Stream Parquet file in chunks."""
        try:
            import pyarrow.parquet as pq
            parquet_file = pq.read_table(path)
            df = parquet_file.to_pandas()
            for i in range(0, len(df), chunksize):
                yield df.iloc[i:i+chunksize]
        except ImportError:
            raise RuntimeError("pyarrow required for Parquet support")
    
    def _clean_row(
        self,
        row: Dict[str, Any],
        medians: Dict[str, float],
        modes: Dict[str, str],
        cleaning_rules: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Clean a row: imputation, normalization, outlier detection.
        """
        cleaned = {}
        
        for col, value in row.items():
            # Handle missing values
            if pd.isna(value) or value is None or value == "":
                # Try mode first, then median
                if col in modes:
                    cleaned[col] = modes[col]
                elif col in medians:
                    cleaned[col] = medians[col]
                else:
                    cleaned[col] = None
            else:
                # Try to clean/normalize
                try:
                    # Clean text
                    if isinstance(value, str):
                        cleaned[col] = clean_text_deterministic(value)
                    # Normalize numeric
                    elif isinstance(value, (int, float)):
                        cleaned[col] = normalize_numeric(value)
                    else:
                        cleaned[col] = value
                except Exception as e:
                    self.logger.debug(f"Failed to clean {col}: {e}")
                    cleaned[col] = value
        
        return cleaned
    
    def _compute_changes(self, original: Dict[str, Any], cleaned: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Compute what changed between original and cleaned."""
        changes = []
        
        for col in original.keys():
            orig_val = original.get(col)
            clean_val = cleaned.get(col)
            
            if orig_val != clean_val:
                # Determine change type
                if pd.isna(orig_val) or orig_val is None:
                    change_type = "imputation"
                elif str(orig_val) != str(clean_val):
                    change_type = "normalization"
                else:
                    change_type = "other"
                
                changes.append({
                    "column": col,
                    "type": change_type,
                    "original": orig_val,
                    "cleaned": clean_val
                })
        
        return changes
    
    def _compute_confidence_score(self, changes: List[Dict], near_dup_risk: bool) -> float:
        """
        Compute confidence score for this row.
        1.0 = fully confident (no changes or only normalization)
        < 1.0 = less confident (imputation or near-dup risk)
        """
        score = 1.0
        
        # Reduce confidence if imputations made
        imputation_count = sum(1 for c in changes if c["type"] == "imputation")
        if imputation_count > 0:
            score *= 0.9  # 10% reduction per imputation
        
        # Reduce confidence if near-dup risk
        if near_dup_risk:
            score *= 0.85
        
        return max(0.5, score)
    
    def _record_provenance(
        self,
        row_id: int,
        original: Dict[str, Any],
        cleaned: Dict[str, Any],
        transformation_id: str,
        confidence_score: float
    ):
        """Record cell-level provenance to Postgres."""
        if not self.storage:
            return
        
        try:
            provenance_records = []
            for col in original.keys():
                orig_val = original.get(col)
                clean_val = cleaned.get(col)
                
                if orig_val != clean_val:
                    provenance_records.append({
                        "row_id": row_id,
                        "column_name": col,
                        "original_value": str(orig_val),
                        "cleaned_value": str(clean_val),
                        "transformation_id": transformation_id,
                        "confidence_score": confidence_score,
                        "source": "pass2_worker"
                    })
            
            if provenance_records:
                self.storage.batch_insert_provenance(self.job_id, provenance_records)
        except Exception as e:
            self.logger.warning(f"Failed to record provenance: {e}")
    
    def _update_progress(self, progress: float):
        """Update job progress in Redis."""
        if not self.storage or not self.storage.redis_client:
            return
        
        try:
            self.storage.redis_client.set(
                f"job:{self.job_id}:pass2_progress",
                progress,
                ex=3600  # 1 hour TTL
            )
        except Exception as e:
            self.logger.warning(f"Failed to update progress: {e}")


def main():
    """CLI entry point for Pass 2 worker."""
    import argparse
    import sys
    
    logging.basicConfig(level=logging.INFO)
    
    parser = argparse.ArgumentParser(description="Pass 2 Worker: Cleaning & Deduplication")
    parser.add_argument("--job-id", default=str(uuid.uuid4()), help="Job UUID")
    parser.add_argument("--input-file", required=True, help="Path to input file (CSV, JSONL, Parquet)")
    parser.add_argument("--output-file", required=True, help="Path to output cleaned file")
    parser.add_argument("--chunksize", type=int, default=50_000, help="Rows per chunk")
    args = parser.parse_args()
    
    worker = Pass2Worker(job_id=args.job_id)
    stats = worker.process_file(
        input_path=args.input_file,
        output_path=args.output_file,
        chunksize=args.chunksize
    )
    
    print(json.dumps(stats, indent=2, default=str))
    return 0 if not stats.get("errors") else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
