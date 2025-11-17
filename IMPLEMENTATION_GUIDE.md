# Data Sanitizer: Implementation Guide

## Overview

This guide walks you through the complete Data Sanitizer platform implementation, which transforms your Colab prototype into a production-ready, scalable data cleaning platform.

## Project Structure

```
Data Sanitizer/
├── Core Application
│   ├── data_cleaning.py              (Core algorithms - existing)
│   ├── storage_backend.py            (Postgres + Milvus + Redis)
│   ├── cloud_storage.py              (S3/GCS file readers, Parquet writer)
│   ├── api_server.py                 (FastAPI REST API with 8 endpoints)
│   ├── worker_pass1.py               (Sampling & index building)
│   ├── worker_pass2.py               (Cleaning & deduplication)
│   └── requirements.txt              (All dependencies)
│
├── Testing & Benchmarking
│   ├── test_integration.py           (End-to-end integration tests)
│   ├── tests.py                      (Unit tests for core algorithms)
│   ├── benchmark_generator.py        (Generate realistic dirty datasets)
│   └── demo_quickstart.py            (Quick-start demo)
│
├── Deployment
│   ├── docker-compose.yml            (Local development environment)
│   ├── Dockerfile.api                (API server container)
│   ├── Dockerfile.worker-pass1       (Pass 1 worker container)
│   ├── Dockerfile.worker-pass2       (Pass 2 worker container)
│   └── k8s/ (TBD)                    (Kubernetes manifests)
│
└── Documentation
    ├── docs/ARCHITECTURE.md          (System design & data models)
    ├── docs/DEPLOYMENT.md            (Infrastructure as Code)
    ├── docs/30DAY_ROADMAP.md         (Week-by-week execution plan)
    ├── README.md                     (Project overview)
    └── IMPLEMENTATION_GUIDE.md       (This file)
```

## Core Modules

### 1. storage_backend.py
**Purpose**: Unified interface to Postgres, Milvus, and Redis.

**Key Classes**:
- `PostgresConfig`, `MilvusConfig`, `RedisConfig` - Configuration dataclasses
- `StorageBackend` - Main interface class

**Key Methods**:
```python
# Job management
create_job(tenant_id, dataset_name, metadata) -> job_id
get_job(job_id) -> Job record
update_job_status(job_id, status, error=None)

# Row hashes (exact duplicate detection)
batch_insert_row_hashes(job_id, hashes)
check_existing_hashes(job_id, hashes) -> existing hashes

# Imputation stats (from Pass 1)
store_imputation_stats(job_id, medians, modes)
get_imputation_stats(job_id) -> stats dict

# Cell-level provenance (from Pass 2)
batch_insert_provenance(job_id, records)

# Audit logging
insert_audit_log(job_id, action, details)

# Milvus LSH
batch_insert_lsh_samples(samples)
query_lsh_candidates(job_id, hash_value) -> candidates

# Redis caching
set_job_progress(job_id, progress)
get_job_progress(job_id) -> progress
```

### 2. cloud_storage.py
**Purpose**: Stream files from S3/GCS and write cleaned data to Parquet/CSV.

**Key Classes**:
- `S3FileReader` - Streams CSV, JSON, JSONL, Parquet, Excel from S3
- `GCSFileReader` - Stub for Google Cloud Storage
- `ParquetStreamWriter` - Efficient columnar output
- `CSVStreamWriter` - CSV fallback

**Key Methods**:
```python
# S3 file reader
reader = S3FileReader(bucket, key, chunksize=50_000)
for chunk in reader.stream_chunks():
    # Process DataFrame chunk
    pass

# Parquet writer
writer = ParquetStreamWriter(output_path)
writer.write_chunk(df_chunk)
writer.close()
```

### 3. api_server.py
**Purpose**: FastAPI REST API for job orchestration and status tracking.

**Endpoints**:
```
POST   /api/v1/datasets/{tenant_id}/ingest
       → Upload dataset, return job_id

GET    /api/v1/jobs/{job_id}
       → Get job status with progress

GET    /api/v1/jobs/{job_id}/report
       → Get cleaning report JSON

GET    /api/v1/jobs/{job_id}/download?format=parquet|csv
       → Download cleaned data

POST   /api/v1/jobs/{job_id}/audit-log
       → Search audit trail

POST   /api/v1/jobs/{job_id}/confidence-scores
       → Get confidence data per cell

GET    /api/v1/health
       → Health check

GET    /api/v1/metrics
       → Prometheus metrics
```

**Features**:
- API key authentication (`X-API-Key` header)
- Per-tenant rate limiting
- Background job processing
- Auto-generated OpenAPI docs at `/api/docs`

### 4. worker_pass1.py
**Purpose**: First pass of cleaning pipeline - sampling and index building.

**Key Class**: `Pass1Worker`

**Key Method**:
```python
worker = Pass1Worker(storage_backend=None, job_id="...")
stats = worker.process_file(
    input_path="data.csv",
    chunksize=50_000,
    sample_size=10_000,
    schema_config={}
)
```

**What it does**:
1. Streams input file in chunks
2. Computes deterministic reservoirs per column (for sampling)
3. Computes MinHash signatures (for deduplication)
4. Inserts LSH samples into Milvus
5. Stores imputation stats (medians, modes) to Postgres

**Output**:
- Stats dict with row count, columns, imputation stats, errors

### 5. worker_pass2.py
**Purpose**: Second pass of cleaning pipeline - cleaning and deduplication.

**Key Class**: `Pass2Worker`

**Key Method**:
```python
worker = Pass2Worker(storage_backend=None, job_id="...")
stats = worker.process_file(
    input_path="data.csv",
    output_path="cleaned.csv",
    chunksize=50_000,
    schema_config={},
    cleaning_rules={}
)
```

**What it does**:
1. Streams input file in chunks
2. Queries Postgres for exact duplicate detection
3. Queries Milvus for near-duplicate candidates
4. Applies imputation (fill missing values with medians/modes)
5. Applies normalization (clean text, normalize numbers)
6. Detects outliers
7. Streams cleaned output to S3/local file (Parquet or CSV)
8. Records cell-level provenance and confidence scores

**Output**:
- Stats dict with rows kept/dropped, deduplication rate, confidence score
- Cleaned data file (CSV or Parquet)

## Data Models

### Jobs Table
```sql
CREATE TABLE jobs (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  dataset_name TEXT,
  status TEXT,  -- queued, pass1_running, pass2_running, success, failed
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  error_message TEXT
);
```

### Row Hashes Table (for exact deduplication)
```sql
CREATE TABLE row_hashes (
  id BIGSERIAL,
  job_id UUID,
  hash TEXT NOT NULL,
  first_seen_row BIGINT,
  created_at TIMESTAMP,
  UNIQUE(job_id, hash)
);
```

### Imputation Stats Table (from Pass 1)
```sql
CREATE TABLE imputation_stats (
  id UUID PRIMARY KEY,
  job_id UUID UNIQUE,
  medians JSONB,  -- {column_name: numeric_value}
  modes JSONB     -- {column_name: most_common_value}
);
```

### Cell Provenance Table (from Pass 2)
```sql
CREATE TABLE cell_provenance (
  id UUID PRIMARY KEY,
  job_id UUID,
  row_id BIGINT,
  column_name TEXT,
  original_value TEXT,
  cleaned_value TEXT,
  transformation_id TEXT,
  confidence_score FLOAT,
  source TEXT,  -- "pass2_worker", "llm_suggest", etc.
  UNIQUE(job_id, row_id, column_name)
);
```

### Audit Logs Table
```sql
CREATE TABLE audit_logs (
  id UUID PRIMARY KEY,
  job_id UUID,
  user_id UUID,
  action TEXT,  -- "pass1_completed", "pass2_completed", etc.
  details JSONB,
  created_at TIMESTAMP
);
```

## Testing

### Run Integration Tests
```bash
# Install pytest
pip install pytest pandas

# Run integration tests
pytest test_integration.py -v

# Run specific test
pytest test_integration.py::TestPass1Worker::test_pass1_with_csv -v

# Run with coverage
pytest test_integration.py --cov=. --cov-report=html
```

### Run Demo
```bash
# Quick-start demo (10K rows)
python demo_quickstart.py

# Output:
# - Generates dirty dataset
# - Runs Pass 1
# - Runs Pass 2
# - Shows before/after comparison
# - Prints quality metrics
```

### Run Benchmark
```bash
# Generate 1M row dataset
python benchmark_generator.py --size 1m --format csv

# Generate 10M row dataset (larger file, takes longer)
python benchmark_generator.py --size 10m --format parquet
```

## Deployment

### Local Development (No Docker)

```bash
# 1. Create input data
python benchmark_generator.py --size 1m --output-dir ./test_data

# 2. Run Pass 1
python worker_pass1.py --job-id test-1 --input-file ./test_data/benchmark_1000000_rows.csv

# 3. Run Pass 2
python worker_pass2.py --job-id test-1 \
  --input-file ./test_data/benchmark_1000000_rows.csv \
  --output-file ./test_data/cleaned.csv
```

### Local Development (With Docker Compose)

```bash
# Start all services (Postgres, Milvus, Redis, API)
docker-compose up -d

# Check services are healthy
docker-compose ps

# API is available at http://localhost:8000
# OpenAPI docs at http://localhost:8000/api/docs

# Stop services
docker-compose down

# View logs
docker-compose logs -f api
```

### Production Deployment (Kubernetes)

See `docs/DEPLOYMENT.md` for complete Terraform and Kubernetes manifests.

## API Usage Examples

### Ingest a Dataset
```bash
curl -X POST "http://localhost:8000/api/v1/datasets/tenant-123/ingest" \
  -H "X-API-Key: tenant-123:key123" \
  -F "file=@/path/to/data.csv" \
  -F "dataset_name=my_dataset"

# Response:
{
  "job_id": "job-uuid-123",
  "dataset_id": "ds-uuid-123",
  "status": "queued",
  "created_at": "2025-11-16T10:00:00Z"
}
```

### Check Job Status
```bash
curl "http://localhost:8000/api/v1/jobs/job-uuid-123" \
  -H "X-API-Key: tenant-123:key123"

# Response:
{
  "job_id": "job-uuid-123",
  "status": "success",
  "progress": {"pass1_progress": 1.0, "pass2_progress": 1.0},
  "metrics": {
    "rows_processed": 1000000,
    "duplicates_found": 150000,
    "imputations_applied": 45000
  }
}
```

### Get Cleaning Report
```bash
curl "http://localhost:8000/api/v1/jobs/job-uuid-123/report" \
  -H "X-API-Key: tenant-123:key123"

# Response:
{
  "summary": {
    "original_row_count": 1000000,
    "cleaned_row_count": 850000,
    "deduplication_rate": 0.85
  },
  "quality_metrics": {
    "duplicates_detected": 150000,
    "false_positive_rate": 0.02,
    "confidence_score_avg": 0.92
  }
}
```

### Download Cleaned Data
```bash
# Download as Parquet
curl "http://localhost:8000/api/v1/jobs/job-uuid-123/download?format=parquet" \
  -H "X-API-Key: tenant-123:key123" \
  -o cleaned_data.parquet

# Download as CSV
curl "http://localhost:8000/api/v1/jobs/job-uuid-123/download?format=csv" \
  -H "X-API-Key: tenant-123:key123" \
  -o cleaned_data.csv
```

## Configuration

### Environment Variables
```bash
# Postgres
export PG_HOST=localhost
export PG_PORT=5432
export PG_DATABASE=data_sanitizer
export PG_USER=postgres
export PG_PASSWORD=postgres

# Milvus
export MILVUS_HOST=localhost
export MILVUS_PORT=19530

# Redis
export REDIS_HOST=localhost
export REDIS_PORT=6379

# API
export API_HOST=0.0.0.0
export API_PORT=8000
export LOG_LEVEL=INFO
```

### Configuration File (Optional)
Create `config.yaml`:
```yaml
postgres:
  host: localhost
  port: 5432
  database: data_sanitizer
  user: postgres
  password: postgres
  min_connections: 2
  max_connections: 10

milvus:
  host: localhost
  port: 19530

redis:
  host: localhost
  port: 6379
  db: 0

api:
  host: 0.0.0.0
  port: 8000
  log_level: INFO
```

## Performance Targets

### Throughput
- **Pass 1**: 50K-100K rows/sec per worker
- **Pass 2**: 40K-70K rows/sec per worker
- **Combined**: 10M rows/hour on standard cluster

### Latency
- **API response**: < 2 seconds (p95)
- **Job completion**: < 5 minutes for 1M rows
- **LLM enrichment** (if enabled): < 10 seconds per batch

### Accuracy
- **Duplicate detection**: > 90% F1 score
- **False positives**: < 5%
- **Data correctness**: 100% (deterministic)

### Scalability
- **Horizontal**: Add workers to scale throughput
- **Vertical**: Larger Postgres/Milvus instances for larger datasets
- **Multi-tenant**: Isolated via tenant_id in all tables

## Monitoring & Observability

### Health Checks
```bash
# API health check
curl http://localhost:8000/api/v1/health

# Postgres
pg_isready -h localhost -U postgres

# Milvus (if running)
curl http://localhost:9091/healthz

# Redis
redis-cli ping
```

### Logging
All modules use Python's standard logging:
```python
import logging
logger = logging.getLogger(__name__)
logger.info("Message")
logger.error("Error")
```

Configure logging level:
```bash
export LOG_LEVEL=DEBUG  # More verbose
export LOG_LEVEL=INFO   # Standard
export LOG_LEVEL=WARN   # Less verbose
```

### Metrics
The API server exposes Prometheus metrics at `/api/v1/metrics`:
```
data_sanitizer_jobs_total{tenant_id, status}
data_sanitizer_rows_processed_total{job_id}
data_sanitizer_api_request_duration_seconds{method, endpoint}
data_sanitizer_worker_processing_time_seconds{pass, status}
```

## Troubleshooting

### Storage Backend Errors
```
Error: "Failed to initialize Postgres"
→ Check Postgres is running and accessible
→ Run: psql -U postgres -h localhost

Error: "Failed to initialize Milvus"
→ Milvus is optional (LSH disabled)
→ Check if Milvus is running: telnet localhost 19530
```

### API Errors
```
Error: "job_id not found"
→ Job hasn't been created yet
→ Check POST to /ingest endpoint was successful

Error: "status is queued" when calling /report
→ Job is still running
→ Poll /api/v1/jobs/{job_id} until status = "success"
```

### Worker Errors
```
Error: "Unsupported file format"
→ Only CSV, JSONL, Parquet, Excel supported
→ Check file extension matches actual format

Error: "Output file not found"
→ Check output_path directory exists
→ Check disk space available
```

## Next Steps

### Week 1 Tasks
- [ ] Test storage_backend.py with local Postgres (can be skipped if no DB available)
- [ ] Run demo_quickstart.py to verify core algorithms work
- [ ] Run test_integration.py to verify worker implementations
- [ ] Generate benchmark datasets

### Week 2 Tasks
- [ ] Integrate workers with API (via RabbitMQ or Redis queue)
- [ ] Test full API flow: ingest → pass1 → pass2 → download
- [ ] Add proper error handling and retries

### Week 3 Tasks
- [ ] Build Docker images
- [ ] Test with docker-compose
- [ ] Performance testing on 10M row dataset

### Week 4 Tasks
- [ ] Kubernetes manifests
- [ ] Load testing (100 concurrent jobs)
- [ ] Security audit
- [ ] Production deployment

## Additional Resources

- **Architecture**: `docs/ARCHITECTURE.md`
- **Deployment**: `docs/DEPLOYMENT.md`
- **30-Day Roadmap**: `docs/30DAY_ROADMAP.md`
- **API Docs**: http://localhost:8000/api/docs (when running locally)
- **Benchmark Generator**: `benchmark_generator.py --help`

## Support

For issues or questions:
1. Check this guide and linked documentation
2. Review error logs in relevant module
3. Check test files for usage examples
4. Refer to docstrings in source code

---

**Last Updated**: 2025-11-16  
**Version**: 1.0 (MVP)
