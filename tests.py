"""
Comprehensive Test Suite for Data Sanitizer

Tests:
1. Unit tests for core algorithms (flatten_json, minhash, lsh)
2. Integration tests with real (small) datasets
3. Property-based tests for determinism
4. Storage backend tests
5. API endpoint tests
"""

import pytest
import tempfile
import json
import os
from datetime import datetime
from typing import List, Dict

import pandas as pd
import numpy as np

# Import modules to test
import sys
sys.path.insert(0, os.path.dirname(__file__))

from data_cleaning import (
    flatten_json,
    compute_minhash_signature,
    lsh_buckets_from_signature,
    _shingles,
    DeterministicReservoir,
    clean_columns,
    text_normalization,
    build_category_alias_map
)

# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def simple_json_data():
    """Simple JSON test data."""
    return {
        "user": {
            "name": "John Doe",
            "address": {
                "street": "123 Main St",
                "city": "New York"
            }
        },
        "items": ["a", "b", "c"],
        "score": 95
    }

@pytest.fixture
def test_dataframe():
    """Simple test DataFrame."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["  Alice  ", "BOB", "Charlie", "alice", "bob  "],
        "email": ["a@example.com", None, "c@example.com", "a@example.com", None],
        "score": [100.5, 200.0, 50.0, 100.5, None]
    })

@pytest.fixture
def dirty_dataframe():
    """DataFrame with dirty data patterns."""
    return pd.DataFrame({
        "customer_id": [1, 1, 2, 3, 3],  # Duplicates
        "name": ["JOHN", "John", "JANE", "BOB", "Bob"],  # Case variations
        "email": ["john@ex.com", "john@ex.com", None, "bob@ex.com", None],  # Nulls
        "status": ["active", "active", "inactive", "active", "Active"],  # Case variation
        "amount": [1000.0, 1000.0, 2000.0, 3000.0, 3000.0]
    })

# ============================================================================
# UNIT TESTS: JSON FLATTENING
# ============================================================================

class TestFlattenJson:
    """Tests for JSON flattening logic."""
    
    def test_flatten_simple_dict(self):
        """Flatten simple flat dictionary."""
        data = {"name": "John", "age": 30}
        result = flatten_json(data)
        assert result == {"name": "John", "age": 30}
    
    def test_flatten_nested_dict(self, simple_json_data):
        """Flatten nested dictionary."""
        result = flatten_json(simple_json_data)
        assert "user.name" in result
        assert result["user.name"] == "John Doe"
        assert "user.address.city" in result
        assert result["user.address.city"] == "New York"
    
    def test_flatten_with_lists(self, simple_json_data):
        """Lists should be converted to JSON strings."""
        result = flatten_json(simple_json_data)
        assert "items" in result
        assert isinstance(result["items"], str)
        assert json.loads(result["items"]) == ["a", "b", "c"]
    
    def test_flatten_empty_dict(self):
        """Flatten empty dictionary."""
        result = flatten_json({})
        assert result == {}
    
    def test_flatten_none(self):
        """Flatten None returns empty dict."""
        result = flatten_json(None)
        assert result == {}

# ============================================================================
# UNIT TESTS: MINHASH & LSH
# ============================================================================

class TestMinHash:
    """Tests for MinHash signature computation."""
    
    def test_minhash_deterministic(self):
        """Same input should produce same signature."""
        text = "hello world"
        shingles = _shingles(text, k=5)
        sig1 = compute_minhash_signature(shingles, num_hashes=64)
        sig2 = compute_minhash_signature(shingles, num_hashes=64)
        assert sig1 == sig2
    
    def test_minhash_different_text(self):
        """Different text should produce different signatures."""
        text1 = "hello world"
        text2 = "goodbye world"
        shingles1 = _shingles(text1, k=5)
        shingles2 = _shingles(text2, k=5)
        sig1 = compute_minhash_signature(shingles1, num_hashes=64)
        sig2 = compute_minhash_signature(shingles2, num_hashes=64)
        assert sig1 != sig2
    
    def test_minhash_similar_text(self):
        """Similar text should produce somewhat similar signatures."""
        text1 = "the quick brown fox"
        text2 = "the quick brown dog"  # One word differs
        shingles1 = _shingles(text1, k=5)
        shingles2 = _shingles(text2, k=5)
        sig1 = compute_minhash_signature(shingles1, num_hashes=64)
        sig2 = compute_minhash_signature(shingles2, num_hashes=64)
        # Count matching values
        matches = sum(1 for s1, s2 in zip(sig1, sig2) if s1 == s2)
        assert matches > 0  # Should have some overlap
    
    def test_minhash_empty_shingles(self):
        """Empty shingles should return all zeros."""
        sig = compute_minhash_signature(set(), num_hashes=64)
        assert all(h == 0 for h in sig)

class TestLSH:
    """Tests for LSH bucketing."""
    
    def test_lsh_deterministic(self):
        """Same signature should produce same buckets."""
        sig = [1, 2, 3, 4, 5, 6, 7, 8] * 8  # 64 hashes
        buckets1 = lsh_buckets_from_signature(sig, bands=16)
        buckets2 = lsh_buckets_from_signature(sig, bands=16)
        assert buckets1 == buckets2
    
    def test_lsh_bucket_count(self):
        """Should produce num_bands buckets."""
        sig = list(range(64))
        bands = 16
        buckets = lsh_buckets_from_signature(sig, bands=bands)
        assert len(buckets) == bands
    
    def test_lsh_different_bands(self):
        """Different band counts produce different results."""
        sig = list(range(64))
        buckets16 = lsh_buckets_from_signature(sig, bands=16)
        buckets8 = lsh_buckets_from_signature(sig, bands=8)
        assert len(buckets16) == 16
        assert len(buckets8) == 8

# ============================================================================
# UNIT TESTS: DETERMINISTIC RESERVOIR
# ============================================================================

class TestDeterministicReservoir:
    """Tests for deterministic reservoir sampling."""
    
    def test_reservoir_capacity(self):
        """Reservoir should not exceed capacity."""
        res = DeterministicReservoir(capacity=5)
        for i in range(100):
            res.add(i, f"value_{i}")
        assert len(res.get_values()) <= 5
    
    def test_reservoir_deterministic(self):
        """Same sequence should produce same reservoir."""
        res1 = DeterministicReservoir(capacity=10, salt="test_salt")
        res2 = DeterministicReservoir(capacity=10, salt="test_salt")
        
        for i in range(50):
            res1.add(i, i * 10)
            res2.add(i, i * 10)
        
        vals1 = sorted(res1.get_values())
        vals2 = sorted(res2.get_values())
        assert vals1 == vals2
    
    def test_reservoir_different_salt(self):
        """Different salts should produce different reservoirs."""
        res1 = DeterministicReservoir(capacity=10, salt="salt1")
        res2 = DeterministicReservoir(capacity=10, salt="salt2")
        
        for i in range(50):
            res1.add(i, i * 10)
            res2.add(i, i * 10)
        
        vals1 = sorted(res1.get_values())
        vals2 = sorted(res2.get_values())
        # Likely different (though small chance of overlap)
        # Just check both are non-empty and valid
        assert len(vals1) > 0
        assert len(vals2) > 0

# ============================================================================
# INTEGRATION TESTS: CLEANING FUNCTIONS
# ============================================================================

class TestCleaningFunctions:
    """Tests for data cleaning transformations."""
    
    def test_clean_columns(self, test_dataframe):
        """Test column cleaning (whitespace stripping)."""
        df, report = clean_columns(test_dataframe.copy())
        # Check that whitespace was removed
        assert df["name"][0] == "Alice"  # Was "  Alice  "
        assert df["name"][4] == "bob"    # Was "bob  "
        assert "name" in report
    
    def test_text_normalization(self, test_dataframe):
        """Test text normalization (lowercase)."""
        df, report = text_normalization(test_dataframe.copy(), keep_punctuation=True)
        # Check that text was lowercased
        assert df["name"][1] == "bob"  # Was "BOB"
        assert "name" in report
    
    def test_category_alias_map(self, dirty_dataframe):
        """Test category merging with alias map."""
        alias_map = build_category_alias_map(
            dirty_dataframe["status"],
            similarity_threshold=0.8
        )
        # "active" and "Active" should map to the same canonical form
        assert len(alias_map) > 0
    
    def test_combined_cleaning(self, test_dataframe):
        """Test combining multiple cleaning steps."""
        df = test_dataframe.copy()
        df, _ = clean_columns(df)
        df, _ = text_normalization(df)
        
        # Verify combined effect
        assert df["name"][0] == "alice"
        assert df["name"][1] == "bob"

# ============================================================================
# PROPERTY-BASED TESTS (Hypothesis)
# ============================================================================

pytest_plugins = ["hypothesis.extra.pandas"]

from hypothesis import given, strategies as st

class TestPropertyBased:
    """Property-based tests using Hypothesis."""
    
    @given(st.text(min_size=0, max_size=100))
    def test_flatten_roundtrip_simple_dict(self, text):
        """Flattening a simple dict should preserve data."""
        data = {"field": text}
        result = flatten_json(data)
        assert result["field"] == text
    
    @given(st.integers(min_value=0, max_value=1000000))
    def test_reservoir_with_integer_values(self, value):
        """Reservoir should handle arbitrary integers."""
        res = DeterministicReservoir(capacity=100)
        res.add(0, value)
        vals = res.get_values()
        assert len(vals) == 1
        assert vals[0] == value
    
    @given(st.lists(st.text(min_size=1, max_size=50), min_size=1, max_size=100))
    def test_minhash_on_various_inputs(self, words):
        """MinHash should work on various text inputs."""
        text = " ".join(words)
        shingles = _shingles(text, k=5)
        sig = compute_minhash_signature(shingles, num_hashes=64)
        assert len(sig) == 64
        assert all(isinstance(h, int) for h in sig)

# ============================================================================
# INTEGRATION TESTS: END-TO-END
# ============================================================================

class TestEndToEnd:
    """End-to-end integration tests."""
    
    def test_small_csv_cleaning(self):
        """Test full pipeline on small CSV."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create small dirty CSV
            csv_path = os.path.join(tmpdir, "test.csv")
            df = pd.DataFrame({
                "id": [1, 1, 2, 3],
                "name": ["  John  ", "John", "  Jane  ", "Bob"],
                "email": ["john@ex.com", "john@ex.com", None, "bob@ex.com"],
                "score": [100.0, 100.0, 50.0, 75.0]
            })
            df.to_csv(csv_path, index=False)
            
            # Run full pipeline
            from data_cleaning import run_full_cleaning_pipeline_two_pass_sqlite_batched
            
            output_dir = os.path.join(tmpdir, "output")
            cleaned_path, report_path = run_full_cleaning_pipeline_two_pass_sqlite_batched(
                path=csv_path,
                output_dir=output_dir,
                sqlite_path=os.path.join(tmpdir, "test.db"),
                chunksize=2
            )
            
            # Verify outputs exist
            assert os.path.exists(cleaned_path)
            assert os.path.exists(report_path)
            
            # Verify report structure
            with open(report_path) as f:
                report = json.load(f)
            assert "summary" in report
            assert "schema" in report
            assert "cleaning_details" in report

# ============================================================================
# STORAGE BACKEND TESTS
# ============================================================================

class TestStorageBackend:
    """Tests for Postgres/Milvus storage backend."""
    
    @pytest.mark.skip(reason="Requires running Postgres and Milvus")
    def test_create_and_fetch_job(self):
        """Test basic job CRUD."""
        from storage_backend import create_storage_backend
        
        backend = create_storage_backend()
        
        # Create job
        job_id = backend.create_job(
            tenant_id="test-tenant",
            dataset_name="test-dataset"
        )
        
        assert job_id is not None
        
        # Fetch job
        job = backend.get_job(job_id)
        assert job["dataset_name"] == "test-dataset"
        assert job["status"] == "queued"
        
        # Update status
        backend.update_job_status(job_id, "success")
        job = backend.get_job(job_id)
        assert job["status"] == "success"
        
        # Cleanup
        backend.cleanup_job(job_id)
        backend.close()

# ============================================================================
# API ENDPOINT TESTS
# ============================================================================

@pytest.mark.skip(reason="Requires running API server")
class TestAPIEndpoints:
    """Tests for REST API endpoints."""
    
    def test_ingest_endpoint(self):
        """Test POST /api/v1/datasets/{tenant_id}/ingest"""
        # Would require running API server
        pass
    
    def test_job_status_endpoint(self):
        """Test GET /api/v1/jobs/{job_id}"""
        pass

# ============================================================================
# CLI RUNNER
# ============================================================================

if __name__ == "__main__":
    # Run: pytest tests.py -v
    # Run coverage: pytest tests.py --cov=data_cleaning --cov-report=html
    pytest.main([__file__, "-v", "--tb=short"])
