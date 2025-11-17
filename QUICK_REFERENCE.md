# Quick Reference: Data Sanitizer API

## Setup (60 seconds)

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Generate test data (10K rows)
python benchmark_generator.py --size 10k --output-dir ./test_data

# 3. Run demo (end-to-end)
python demo_quickstart.py

# 4. Run tests
pytest test_integration.py -v
```

## Command-Line Usage

### Generate Benchmark Data
```bash
# 1M rows, CSV format
python benchmark_generator.py --size 1m --format csv

# 10M rows, Parquet format
python benchmark_generator.py --size 10m --format parquet

# 100M rows, JSONL format (large, takes ~10 min)
python benchmark_generator.py --size 100m --format jsonl
```

### Run Pass 1 Worker (Sampling)
```bash
python worker_pass1.py \
  --job-id my-job-123 \
  --input-file data.csv \
  --chunksize 50000 \
  --sample-size 10000
```

### Run Pass 2 Worker (Cleaning)
```bash
python worker_pass2.py \
  --job-id my-job-123 \
  --input-file data.csv \
  --output-file cleaned.csv \
  --chunksize 50000
```

### Run API Server
```bash
# Requires Postgres, Milvus, Redis running
uvicorn api_server:app --reload --host 0.0.0.0 --port 8000
```

## Python API Usage

### Full Pipeline (No Database)
```python
from worker_pass1 import Pass1Worker
from worker_pass2 import Pass2Worker

# Pass 1: Sampling & index building
pass1 = Pass1Worker()
stats1 = pass1.process_file("data.csv")
print(f"Pass 1: Processed {stats1['total_rows']} rows")

# Pass 2: Cleaning & deduplication
pass2 = Pass2Worker()
stats2 = pass2.process_file("data.csv", "cleaned.csv")
print(f"Pass 2: Kept {stats2['rows_kept']} rows, removed {stats2['rows_dropped']} duplicates")
```

### With Storage Backend (Postgres + Milvus)
```python
from storage_backend import StorageBackend, PostgresConfig, MilvusConfig
from worker_pass1 import Pass1Worker
from worker_pass2 import Pass2Worker

# Initialize storage
pg_config = PostgresConfig(host="localhost", database="data_sanitizer")
milvus_config = MilvusConfig(host="localhost")
storage = StorageBackend(pg_config, milvus_config)

# Create job record
job_id = storage.create_job(tenant_id="acme-corp", dataset_name="customers")

# Pass 1 with storage
pass1 = Pass1Worker(storage_backend=storage, job_id=job_id)
stats1 = pass1.process_file("data.csv")

# Pass 2 with storage (uses medians/modes from Pass 1)
pass2 = Pass2Worker(storage_backend=storage, job_id=job_id)
stats2 = pass2.process_file("data.csv", "cleaned.csv")

# Retrieve results
job = storage.get_job(job_id)
provenance = storage.get_cell_provenance(job_id)
audit_logs = storage.get_audit_logs(job_id)
```

## HTTP API Usage

### Start API Server
```bash
# Required: Postgres, Milvus, Redis running (via docker-compose)
docker-compose up -d
sleep 5

# Start API
uvicorn api_server:app --host 0.0.0.0 --port 8000
```

### Ingest Dataset
```bash
TENANT_ID="acme-corp"
API_KEY="acme-corp:secret-key-123"

curl -X POST "http://localhost:8000/api/v1/datasets/${TENANT_ID}/ingest" \
  -H "X-API-Key: ${API_KEY}" \
  -F "file=@customers.csv" \
  -F "dataset_name=customers"

# Returns:
# {
#   "job_id": "550e8400-e29b-41d4-a716-446655440000",
#   "status": "queued",
#   "created_at": "2025-11-16T10:00:00Z"
# }
```

### Check Job Status
```bash
JOB_ID="550e8400-e29b-41d4-a716-446655440000"

curl "http://localhost:8000/api/v1/jobs/${JOB_ID}" \
  -H "X-API-Key: acme-corp:secret-key-123"

# Returns:
# {
#   "job_id": "550e8400-e29b-41d4-a716-446655440000",
#   "status": "success",
#   "created_at": "2025-11-16T10:00:00Z",
#   "progress": {"pass1_progress": 1.0, "pass2_progress": 1.0},
#   "metrics": {
#     "rows_processed": 100000,
#     "duplicates_found": 15000,
#     "imputations_applied": 2000
#   }
# }
```

### Get Cleaning Report
```bash
curl "http://localhost:8000/api/v1/jobs/${JOB_ID}/report" \
  -H "X-API-Key: acme-corp:secret-key-123"

# Returns:
# {
#   "summary": {
#     "original_row_count": 100000,
#     "cleaned_row_count": 85000,
#     "rows_dropped": 15000,
#     "deduplication_rate": 0.85
#   },
#   "quality_metrics": {
#     "duplicates_detected": 15000,
#     "false_positive_rate": 0.02,
#     "imputation_rate": 0.02,
#     "confidence_score_avg": 0.92
#   },
#   "transformations_applied": [...]
# }
```

### Download Cleaned Data
```bash
# As Parquet
curl "http://localhost:8000/api/v1/jobs/${JOB_ID}/download?format=parquet" \
  -H "X-API-Key: acme-corp:secret-key-123" \
  -o cleaned_customers.parquet

# As CSV
curl "http://localhost:8000/api/v1/jobs/${JOB_ID}/download?format=csv" \
  -H "X-API-Key: acme-corp:secret-key-123" \
  -o cleaned_customers.csv
```

### Search Audit Logs
```bash
curl -X POST "http://localhost:8000/api/v1/jobs/${JOB_ID}/audit-log" \
  -H "X-API-Key: acme-corp:secret-key-123" \
  -H "Content-Type: application/json" \
  -d '{
    "action": "pass2_completed",
    "limit": 100
  }'

# Returns:
# {
#   "records": [
#     {
#       "action": "pass2_completed",
#       "details": {
#         "rows_kept": 85000,
#         "duplicates_found": 15000
#       },
#       "created_at": "2025-11-16T10:05:30Z"
#     }
#   ]
# }
```

### Get Cell-Level Confidence
```bash
curl -X POST "http://localhost:8000/api/v1/jobs/${JOB_ID}/confidence-scores" \
  -H "X-API-Key: acme-corp:secret-key-123" \
  -H "Content-Type: application/json" \
  -d '{
    "column": "email",
    "min_confidence": 0.8,
    "limit": 100
  }'

# Returns:
# {
#   "records": [
#     {
#       "row_id": 42,
#       "column": "email",
#       "original": "john@example.com",
#       "cleaned": "john@example.com",
#       "confidence_score": 0.95
#     }
#   ]
# }
```

### Health Check
```bash
curl http://localhost:8000/api/v1/health

# Returns: {"status": "healthy"}
```

## Testing

### Run All Tests
```bash
# Unit + integration tests
pytest test_integration.py -v

# With coverage report
pytest test_integration.py --cov=. --cov-report=html
open htmlcov/index.html

# Specific test class
pytest test_integration.py::TestPass2Worker -v

# Specific test
pytest test_integration.py::TestPass2Worker::test_pass2_detects_exact_duplicates -v
```

## Performance Benchmarking

### Generate Baseline (1M rows)
```bash
python benchmark_generator.py --size 1m --format csv

time python worker_pass1.py --input-file benchmark_1000000_rows.csv
time python worker_pass2.py --input-file benchmark_1000000_rows.csv \
  --output-file cleaned_1000000_rows.csv
```

### Expected Results
- **Pass 1**: 8-15 seconds for 1M rows (~70K rows/sec)
- **Pass 2**: 12-20 seconds for 1M rows (~50K rows/sec)
- **Total**: 20-35 seconds for 1M rows
- **Memory**: 200-400 MB peak

### Profile a Worker
```bash
python -m cProfile -s cumulative worker_pass1.py --input-file data.csv | head -20
```

## Docker Deployment

### Build Images
```bash
docker build -f Dockerfile.api -t data-sanitizer-api:latest .
docker build -f Dockerfile.worker-pass1 -t data-sanitizer-pass1:latest .
docker build -f Dockerfile.worker-pass2 -t data-sanitizer-pass2:latest .
```

### Run Local Stack
```bash
# Start all services (Postgres, Milvus, Redis, API, Workers)
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f api

# Stop all
docker-compose down
```

## Common Tasks

### Find Slow Rows (Low Confidence)
```python
from storage_backend import StorageBackend

storage = StorageBackend(pg_config)
low_conf = storage.query(
    "SELECT * FROM cell_provenance WHERE confidence_score < 0.7 ORDER BY created_at DESC LIMIT 100"
)
for row in low_conf:
    print(f"Row {row['row_id']}, Col {row['column_name']}: {row['original_value']} → {row['cleaned_value']} (conf: {row['confidence_score']})")
```

### Reprocess Failed Job
```bash
JOB_ID="550e8400-e29b-41d4-a716-446655440000"

# Get original input file from S3
aws s3 cp s3://my-bucket/uploads/${JOB_ID}/data.csv ./data.csv

# Re-run Pass 1
python worker_pass1.py --job-id ${JOB_ID} --input-file data.csv

# Re-run Pass 2
python worker_pass2.py --job-id ${JOB_ID} --input-file data.csv --output-file cleaned.csv
```

### Export Data to Parquet
```python
import pandas as pd

# Read cleaned CSV
df = pd.read_csv("cleaned.csv")

# Export to Parquet
df.to_parquet("cleaned.parquet", compression="snappy")
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `ImportError: No module named 'data_cleaning'` | Run from project root directory |
| `FileNotFoundError: data.csv` | Check file path is correct |
| `psycopg2.OperationalError` | Check Postgres is running and accessible |
| `pymilvus.exceptions.ConnectionFailure` | Check Milvus is running or disable it (optional) |
| `Out of memory` | Reduce `chunksize` parameter |
| API returns 401 Unauthorized | Check `X-API-Key` header is set correctly |
| Worker hangs indefinitely | Check for deadlock in Postgres; restart database |

## Configuration Tuning

```python
# Higher throughput (more memory)
worker.process_file(
    input_path="data.csv",
    chunksize=100_000,  # Larger chunks
    sample_size=50_000   # Larger samples
)

# Lower memory footprint (slower)
worker.process_file(
    input_path="data.csv",
    chunksize=10_000,    # Smaller chunks
    sample_size=1_000    # Smaller samples
)
```

---

**For detailed documentation, see**:
- `IMPLEMENTATION_GUIDE.md` - Complete implementation guide
- `docs/ARCHITECTURE.md` - System design
- `docs/DEPLOYMENT.md` - Production deployment
- `docs/30DAY_ROADMAP.md` - Week-by-week execution plan
