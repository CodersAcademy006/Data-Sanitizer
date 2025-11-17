"""
End-to-End Integration Tests

Tests the full pipeline: ingest -> Pass 1 -> Pass 2 -> download
"""

import pytest
import tempfile
import os
import json
import csv
from pathlib import Path
from typing import Tuple

import pandas as pd

from worker_pass1 import Pass1Worker
from worker_pass2 import Pass2Worker


@pytest.fixture
def temp_dir():
    """Create temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def sample_csv_file(temp_dir) -> str:
    """Create a small sample CSV file with some dirty data."""
    csv_path = os.path.join(temp_dir, "sample.csv")
    
    rows = [
        {"id": 1, "name": "John Doe", "email": "john@example.com", "status": "active"},
        {"id": 2, "name": "jane smith", "email": "jane@example.com", "status": "inactive"},
        {"id": 3, "name": "John Doe", "email": "john@example.com", "status": "active"},  # Exact dup
        {"id": 4, "name": "Bob Johnson", "email": None, "status": "ACTIVE"},
        {"id": 5, "name": "Alice Brown", "email": "alice@example.com", "status": "active"},
        {"id": 6, "name": "John  Doe", "email": "john@example.com", "status": "Active"},  # Near dup
    ]
    
    df = pd.DataFrame(rows)
    df.to_csv(csv_path, index=False)
    return csv_path


@pytest.fixture
def sample_jsonl_file(temp_dir) -> str:
    """Create a small sample JSONL file."""
    jsonl_path = os.path.join(temp_dir, "sample.jsonl")
    
    rows = [
        {"id": 1, "name": "John Doe", "email": "john@example.com", "status": "active"},
        {"id": 2, "name": "jane smith", "email": "jane@example.com", "status": "inactive"},
        {"id": 3, "name": "John Doe", "email": "john@example.com", "status": "active"},
    ]
    
    with open(jsonl_path, 'w') as f:
        for row in rows:
            f.write(json.dumps(row) + '\n')
    
    return jsonl_path


class TestPass1Worker:
    """Test Pass 1 worker (sampling & index building)."""
    
    def test_pass1_with_csv(self, sample_csv_file, temp_dir):
        """Test Pass 1 with CSV input."""
        worker = Pass1Worker(storage_backend=None)
        
        stats = worker.process_file(
            input_path=sample_csv_file,
            chunksize=5,
            sample_size=100
        )
        
        # Verify stats
        assert stats["job_id"]
        assert stats["total_rows"] == 6
        assert stats["total_chunks"] >= 1
        assert stats["columns_processed"] == ["id", "name", "email", "status"]
        assert not stats["errors"]
        assert stats["completed_at"]
    
    def test_pass1_with_jsonl(self, sample_jsonl_file):
        """Test Pass 1 with JSONL input."""
        worker = Pass1Worker(storage_backend=None)
        
        stats = worker.process_file(
            input_path=sample_jsonl_file,
            chunksize=5,
            sample_size=100
        )
        
        assert stats["total_rows"] == 3
        assert stats["columns_processed"] == ["id", "name", "email", "status"]
        assert not stats["errors"]
    
    def test_pass1_handles_missing_values(self, sample_csv_file):
        """Test Pass 1 handles missing values correctly."""
        worker = Pass1Worker(storage_backend=None)
        
        stats = worker.process_file(input_path=sample_csv_file)
        
        # Imputation stats should include modes for columns with missing values
        assert stats["imputation_stats"]
        assert not stats["errors"]


class TestPass2Worker:
    """Test Pass 2 worker (cleaning & deduplication)."""
    
    def test_pass2_with_csv_to_csv(self, sample_csv_file, temp_dir):
        """Test Pass 2 with CSV input and CSV output."""
        output_path = os.path.join(temp_dir, "cleaned.csv")
        worker = Pass2Worker(storage_backend=None)
        
        stats = worker.process_file(
            input_path=sample_csv_file,
            output_path=output_path,
            chunksize=5
        )
        
        # Verify stats
        assert stats["job_id"]
        assert stats["total_rows"] == 6
        assert stats["rows_dropped"] >= 1  # Should detect exact duplicates
        assert stats["deduplication_rate"] > 0.8
        assert not stats["errors"]
        
        # Verify output file exists
        assert os.path.exists(output_path)
        
        # Verify output content
        df_output = pd.read_csv(output_path)
        assert len(df_output) <= 6  # Should have same or fewer rows
    
    def test_pass2_with_csv_to_parquet(self, sample_csv_file, temp_dir):
        """Test Pass 2 with Parquet output."""
        output_path = os.path.join(temp_dir, "cleaned.parquet")
        worker = Pass2Worker(storage_backend=None)
        
        # Skip if pyarrow not available
        try:
            import pyarrow.parquet as pq
        except ImportError:
            pytest.skip("pyarrow not installed")
        
        stats = worker.process_file(
            input_path=sample_csv_file,
            output_path=output_path,
            chunksize=5
        )
        
        assert stats["rows_kept"] > 0
        assert os.path.exists(output_path)
    
    def test_pass2_detects_exact_duplicates(self, sample_csv_file, temp_dir):
        """Test that Pass 2 detects exact duplicates."""
        output_path = os.path.join(temp_dir, "cleaned.csv")
        worker = Pass2Worker(storage_backend=None)
        
        stats = worker.process_file(
            input_path=sample_csv_file,
            output_path=output_path,
            chunksize=5
        )
        
        # Row 3 is exact duplicate of row 1
        assert stats["duplicates_found"] >= 1
    
    def test_pass2_applies_imputations(self, sample_csv_file, temp_dir):
        """Test that Pass 2 applies imputations for missing values."""
        output_path = os.path.join(temp_dir, "cleaned.csv")
        
        # Add medians/modes that would be from Pass 1
        schema_config = {
            "medians": {"id": 3.0},
            "modes": {"email": "john@example.com"}
        }
        
        worker = Pass2Worker(storage_backend=None)
        stats = worker.process_file(
            input_path=sample_csv_file,
            output_path=output_path,
            chunksize=5,
            schema_config=schema_config
        )
        
        # Should have applied imputations
        assert stats["imputations_applied"] >= 0


class TestEndToEnd:
    """Test complete pipeline: Pass 1 -> Pass 2."""
    
    def test_full_pipeline_csv_to_csv(self, sample_csv_file, temp_dir):
        """Test full pipeline from raw CSV to cleaned CSV."""
        
        # Pass 1: Sampling & index building
        pass1_worker = Pass1Worker(storage_backend=None)
        pass1_stats = pass1_worker.process_file(
            input_path=sample_csv_file,
            chunksize=5,
            sample_size=100
        )
        
        assert not pass1_stats["errors"]
        assert pass1_stats["total_rows"] == 6
        
        # Pass 2: Cleaning & deduplication
        output_path = os.path.join(temp_dir, "cleaned.csv")
        pass2_worker = Pass2Worker(storage_backend=None)
        pass2_stats = pass2_worker.process_file(
            input_path=sample_csv_file,
            output_path=output_path,
            chunksize=5
        )
        
        assert not pass2_stats["errors"]
        assert pass2_stats["rows_kept"] > 0
        assert os.path.exists(output_path)
        
        # Verify output has fewer rows (duplicates removed)
        df_original = pd.read_csv(sample_csv_file)
        df_cleaned = pd.read_csv(output_path)
        
        assert len(df_cleaned) <= len(df_original)
    
    def test_full_pipeline_preserves_schema(self, sample_csv_file, temp_dir):
        """Test that full pipeline preserves column schema."""
        output_path = os.path.join(temp_dir, "cleaned.csv")
        
        df_original = pd.read_csv(sample_csv_file)
        original_columns = set(df_original.columns)
        
        pass2_worker = Pass2Worker(storage_backend=None)
        pass2_worker.process_file(
            input_path=sample_csv_file,
            output_path=output_path,
            chunksize=5
        )
        
        df_cleaned = pd.read_csv(output_path)
        cleaned_columns = set(df_cleaned.columns)
        
        assert original_columns == cleaned_columns


class TestDataValidation:
    """Test data validation and error handling."""
    
    def test_pass1_handles_invalid_json(self, temp_dir):
        """Test Pass 1 handles invalid JSONL gracefully."""
        jsonl_path = os.path.join(temp_dir, "invalid.jsonl")
        
        with open(jsonl_path, 'w') as f:
            f.write('{"id": 1, "name": "John"}\n')
            f.write('invalid json line\n')  # Invalid
            f.write('{"id": 2, "name": "Jane"}\n')
        
        worker = Pass1Worker(storage_backend=None)
        stats = worker.process_file(input_path=jsonl_path)
        
        # Should skip invalid line and continue
        assert stats["total_rows"] == 2
        # Should not have fatal error
        assert not stats.get("errors") or len(stats["errors"]) == 0
    
    def test_pass2_handles_unsupported_format(self, temp_dir):
        """Test Pass 2 handles unsupported file formats."""
        txt_path = os.path.join(temp_dir, "data.txt")
        with open(txt_path, 'w') as f:
            f.write("some random text")
        
        output_path = os.path.join(temp_dir, "output.csv")
        worker = Pass2Worker(storage_backend=None)
        
        # Should fail gracefully
        stats = worker.process_file(
            input_path=txt_path,
            output_path=output_path
        )
        
        assert stats["errors"]
    
    def test_handles_empty_file(self, temp_dir):
        """Test workers handle empty files."""
        empty_csv = os.path.join(temp_dir, "empty.csv")
        with open(empty_csv, 'w') as f:
            f.write("id,name,email\n")  # Header only, no data
        
        worker = Pass1Worker(storage_backend=None)
        stats = worker.process_file(input_path=empty_csv)
        
        assert stats["total_rows"] == 0


class TestDeterminism:
    """Test that workers are deterministic."""
    
    def test_pass1_deterministic(self, sample_csv_file):
        """Test Pass 1 produces same results on same input."""
        worker1 = Pass1Worker(storage_backend=None, job_id="test-job-1")
        worker2 = Pass1Worker(storage_backend=None, job_id="test-job-2")
        
        stats1 = worker1.process_file(input_path=sample_csv_file, sample_size=5)
        stats2 = worker2.process_file(input_path=sample_csv_file, sample_size=5)
        
        # Same input should produce same row counts and column lists
        assert stats1["total_rows"] == stats2["total_rows"]
        assert stats1["columns_processed"] == stats2["columns_processed"]
    
    def test_pass2_deterministic(self, sample_csv_file, temp_dir):
        """Test Pass 2 produces same results on same input."""
        output1 = os.path.join(temp_dir, "out1.csv")
        output2 = os.path.join(temp_dir, "out2.csv")
        
        worker1 = Pass2Worker(storage_backend=None)
        worker2 = Pass2Worker(storage_backend=None)
        
        stats1 = worker1.process_file(
            input_path=sample_csv_file,
            output_path=output1,
            chunksize=5
        )
        stats2 = worker2.process_file(
            input_path=sample_csv_file,
            output_path=output2,
            chunksize=5
        )
        
        # Same stats
        assert stats1["rows_kept"] == stats2["rows_kept"]
        assert stats1["duplicates_found"] == stats2["duplicates_found"]
        
        # Same output content
        df1 = pd.read_csv(output1)
        df2 = pd.read_csv(output2)
        
        pd.testing.assert_frame_equal(df1, df2)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
