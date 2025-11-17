# Data Sanitizer: 30-Day Implementation Roadmap

## Executive Summary

Transform the Colab prototype into a production-ready platform in 30 days. Focus on:
1. **Storage layer migration** (SQLite → Postgres + Milvus)
2. **Cloud storage integration** (S3/GCS + Parquet)
3. **Job orchestration** (async queue + workers)
4. **REST API** with full CRUD operations
5. **Comprehensive testing** (unit + integration)
6. **Performance benchmarking** (baseline metrics)
7. **Observability** (logging, metrics, tracing)

---

## Week 1: Storage Migration & Cloud Integration

### Day 1–2: Postgres Schema & Migration

**Deliverables:**
- [ ] Complete `storage_backend.py` (Postgres module)
- [ ] Create all tables: jobs, row_hashes, imputation_stats, cell_provenance, audit_logs
- [ ] Set up connection pooling (psycopg2)
- [ ] Unit tests for CRUD operations

**Commands:**
```bash
# Start local Postgres (Docker)
docker run --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:14

# Create database
psql -U postgres -c "CREATE DATABASE data_sanitizer;"

# Run migration script
python storage_backend.py  # Initialize schema
```

**Code Checklist:**
- [x] PostgresConfig dataclass
- [x] StorageBackend.__init__() with connection pool
- [x] _init_postgres_schema() with all table definitions
- [x] CRUD methods: create_job, get_job, update_job_status, batch_insert_row_hashes, check_existing_hashes
- [x] Imputation stats storage (store_imputation_stats, get_imputation_stats)
- [x] Cell provenance insertion (batch_insert_provenance)
- [x] Audit log insertion (insert_audit_log)

### Day 3–4: Milvus Vector DB Integration

**Deliverables:**
- [ ] Milvus connection & collection setup
- [ ] LSH sample insertion logic
- [ ] Vector similarity queries
- [ ] Unit tests for Milvus operations

**Commands:**
```bash
# Start Milvus (Docker)
docker run -d --name milvus -p 19530:19530 -p 9091:9091 milvusdb/milvus:latest

# Test connection
python -c "from pymilvus import connections; connections.connect(host='localhost', port=19530); print('Connected')"
```

**Code Checklist:**
- [x] MilvusConfig dataclass
- [x] _init_milvus() connection
- [x] _create_lsh_collection() with proper schema
- [x] batch_insert_lsh_samples() method
- [x] query_lsh_candidates() method
- [x] Index creation for minhash_vector

### Day 5: Cloud Storage Connectors (S3/GCS)

**Deliverables:**
- [ ] S3FileReader class with streaming support (CSV, JSON, JSONL, Parquet)
- [ ] GCSFileReader stub
- [ ] ParquetStreamWriter
- [ ] CSVStreamWriter (fallback)
- [ ] Tests with small files

**Code Checklist:**
- [x] S3FileReader.__init__() with boto3
- [x] stream_chunks() method (format auto-detection)
- [x] _stream_csv(), _stream_jsonl(), _stream_json(), _stream_parquet(), _stream_excel()
- [x] ParquetStreamWriter with chunked writes
- [x] CSVStreamWriter for backward compatibility
- [x] create_file_reader() factory function
- [x] Error handling for missing files

**Test Dataset:**
```bash
# Create small test file
python -c "
import pandas as pd
df = pd.DataFrame({'id': range(100), 'name': ['test']*100, 'value': range(100)})
df.to_csv('/tmp/test.csv', index=False)
"

# Test S3 reader
python -c "
from cloud_storage import S3FileReader
reader = S3FileReader('my-bucket', 'test.csv')
for chunk in reader.stream_chunks():
    print(f'Read chunk: {len(chunk)} rows')
"
```

---

## Week 2: REST API & Job Orchestration

### Day 6–7: FastAPI Server & Endpoints

**Deliverables:**
- [ ] Complete `api_server.py`
- [ ] All endpoints working (ingest, status, report, download)
- [ ] Authentication & rate limiting middleware
- [ ] OpenAPI documentation

**Endpoints to Implement:**
- `POST /api/v1/datasets/{tenant_id}/ingest` - Upload & start job
- `GET /api/v1/jobs/{job_id}` - Get job status
- `GET /api/v1/jobs/{job_id}/report` - Get cleaning report
- `GET /api/v1/jobs/{job_id}/download?format=parquet` - Download cleaned data
- `POST /api/v1/jobs/{job_id}/audit-log` - Audit trail
- `POST /api/v1/jobs/{job_id}/confidence-scores` - Confidence data
- `GET /api/v1/health` - Health check
- `GET /api/v1/metrics` - Platform metrics

**Commands:**
```bash
# Start API server
uvicorn api_server:app --host 0.0.0.0 --port 8000 --reload

# Test endpoint
curl -X POST http://localhost:8000/api/v1/datasets/tenant-123/ingest \
  -H "X-API-Key: tenant-123:key123" \
  -F "file=@/tmp/test.csv" \
  -F "dataset_name=test_dataset"

# Check docs
open http://localhost:8000/api/docs
```

**Code Checklist:**
- [x] Pydantic models (IngestRequest, JobStatus, CleaningReport, etc.)
- [x] FastAPI app initialization
- [x] verify_api_key() middleware
- [x] check_rate_limit() middleware
- [x] POST /ingest endpoint
- [x] GET /jobs/{job_id} endpoint
- [x] GET /jobs/{job_id}/report endpoint
- [x] GET /jobs/{job_id}/download endpoint
- [x] Health check endpoint
- [x] Error handling & logging

### Day 8–9: Job Orchestrator (Queue + Workers)

**Deliverables:**
- [ ] Job queue setup (RabbitMQ or Redis-based)
- [ ] Pass 1 worker skeleton
- [ ] Pass 2 worker skeleton
- [ ] Background job processing

**Option A: Celery + RabbitMQ**
```bash
# Start RabbitMQ
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

# Start Celery worker
celery -A workers worker --loglevel=info
```

**Option B: Simple Redis Queue (RQ)**
```bash
# Start Redis
docker run -d --name redis -p 6379:6379 redis:latest

# Start RQ worker
rq worker --with-scheduler
```

**Code Checklist:**
- [ ] Worker base class
- [ ] Pass 1 worker (compute_global_stats_v2)
- [ ] Pass 2 worker (clean_with_sqlite_dedupe_v2)
- [ ] Job queue integration with storage backend
- [ ] Error handling & retries
- [ ] Logging & progress tracking

### Day 10: Integration Tests

**Deliverables:**
- [ ] End-to-end test: upload → process → download
- [ ] Error scenarios (invalid file, bad schema)
- [ ] Concurrency tests (multiple simultaneous jobs)

**Test Cases:**
```python
# test_integration.py
def test_full_pipeline():
    # 1. Create job
    job_id = client.ingest(file="test.csv", dataset_name="test")
    
    # 2. Poll status
    while True:
        status = client.get_status(job_id)
        if status == "success":
            break
    
    # 3. Get report
    report = client.get_report(job_id)
    assert report["summary"]["deduplication_rate"] > 0.8
    
    # 4. Download
    data = client.download(job_id, format="parquet")
    assert len(data) > 0
```

---

## Week 3: Benchmarking & Performance

### Day 11–12: Benchmark Dataset Generation

**Deliverables:**
- [ ] `benchmark_generator.py` complete
- [ ] 1M row dataset (CSV, JSONL, Parquet)
- [ ] 10M row dataset (at least CSV)
- [ ] Dirty data patterns implemented

**Commands:**
```bash
# Generate 1M row dataset
python benchmark_generator.py --size 1m --output-dir /tmp/benchmarks

# Verify files
ls -lh /tmp/benchmarks/
# Expected:
#   benchmark_1000000_rows.csv (~500 MB)
#   benchmark_1000000_rows.jsonl (~800 MB)
#   benchmark_1000000_rows.parquet (~200 MB)
```

**Dirty Data Patterns:**
- [x] Exact duplicates (5-10%)
- [x] Near-duplicates with typos (2-5%)
- [x] Missing values (5-15%)
- [x] Outliers (1-2%)
- [x] Schema drift (JSONL only)
- [x] Case inconsistencies
- [x] Extra whitespace

### Day 13–14: Baseline Performance Metrics

**Deliverables:**
- [ ] Pass 1 timing (1M, 10M rows)
- [ ] Pass 2 timing (1M, 10M rows)
- [ ] Memory usage (peak, avg)
- [ ] CPU utilization
- [ ] Throughput (rows/sec)

**Test Script:**
```python
import time
import psutil
from data_cleaning import run_full_cleaning_pipeline_two_pass_sqlite_batched

def benchmark(csv_path, num_rows):
    process = psutil.Process()
    
    start_time = time.time()
    initial_memory = process.memory_info().rss
    
    cleaned_path, report_path = run_full_cleaning_pipeline_two_pass_sqlite_batched(
        path=csv_path,
        chunksize=50_000
    )
    
    elapsed = time.time() - start_time
    peak_memory = process.memory_info().rss
    
    return {
        "rows": num_rows,
        "elapsed_seconds": elapsed,
        "throughput_rows_per_sec": num_rows / elapsed,
        "memory_peak_mb": (peak_memory - initial_memory) / (1024*1024),
        "csv_mb": os.path.getsize(csv_path) / (1024*1024)
    }

# Run benchmarks
for size in ["1m", "10m"]:
    csv_path = f"/tmp/benchmarks/benchmark_{size}_rows.csv"
    results = benchmark(csv_path, int(size[:-1]) * 1_000_000)
    print(json.dumps(results, indent=2))
```

**Expected Baseline (on modern hardware):**
| Dataset | Pass 1 (sec) | Pass 2 (sec) | Throughput (rows/sec) | Memory (MB) |
|---------|------------|------------|-----------------|-----------|
| 1M CSV  | 8–15      | 12–20      | 40k–70k        | 200–400   |
| 10M CSV | 80–150    | 120–200    | 40k–70k        | 300–500   |

---

## Week 4: Testing, Observability & Documentation

### Day 15–16: Comprehensive Testing

**Deliverables:**
- [ ] `tests.py` expanded with all test classes
- [ ] Unit tests: JSON flatten, MinHash, LSH, Reservoir
- [ ] Integration tests: CSV/JSONL cleaning
- [ ] Property-based tests (Hypothesis)
- [ ] >80% code coverage

**Commands:**
```bash
# Run all tests with coverage
pytest tests.py -v --cov=. --cov-report=html

# Run specific test class
pytest tests.py::TestMinHash -v

# Run with markers
pytest -m "not slow" tests.py
```

**Coverage Target:**
- data_cleaning.py: >85%
- storage_backend.py: >80%
- api_server.py: >75%

### Day 17–18: Observability

**Deliverables:**
- [ ] Prometheus metrics endpoints
- [ ] OpenTelemetry tracing (optional)
- [ ] Structured JSON logging
- [ ] Sample Grafana dashboard

**Metrics to Add:**
```python
from prometheus_client import Counter, Histogram, Gauge

# Counters
jobs_total = Counter('data_sanitizer_jobs_total', 'Total jobs', ['tenant_id', 'status'])
rows_processed_total = Counter('data_sanitizer_rows_processed_total', 'Total rows', ['job_id'])

# Histograms
job_duration_seconds = Histogram('data_sanitizer_job_duration_seconds', 'Job duration')

# Gauges
active_jobs_count = Gauge('data_sanitizer_active_jobs_count', 'Active jobs')
```

**Logging:**
```python
import json_log_formatter

formatter = json_log_formatter.JSONFormatter()
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)

# Log structured events
logger.info("job_started", extra={"job_id": job_id, "rows": num_rows})
```

### Day 19–20: Documentation

**Deliverables:**
- [ ] API documentation (OpenAPI/Swagger)
- [ ] Architecture diagram (ARCHITECTURE.md)
- [ ] Deployment guide (DEPLOYMENT.md)
- [ ] Quickstart guide
- [ ] Security & compliance docs

**Files to Complete:**
- [x] docs/ARCHITECTURE.md
- [x] docs/DEPLOYMENT.md
- [ ] docs/QUICKSTART.md
- [ ] docs/API.md
- [ ] docs/SECURITY.md

---

## Week 4 (Continued): Launch Preparation

### Day 21–22: Docker & Container Preparation

**Deliverables:**
- [ ] Dockerfile for API
- [ ] Dockerfile for Pass 1 worker
- [ ] Dockerfile for Pass 2 worker
- [ ] Docker Compose for local testing
- [ ] All images build & run successfully

**Docker Compose:**
```yaml
version: '3.9'
services:
  postgres:
    image: postgres:14
    environment:
      POSTGRES_DB: data_sanitizer
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"
  
  milvus:
    image: milvusdb/milvus:latest
    ports:
      - "19530:19530"
  
  redis:
    image: redis:latest
    ports:
      - "6379:6379"
  
  api:
    build:
      context: .
      dockerfile: docker/api/Dockerfile
    environment:
      PG_HOST: postgres
      MILVUS_HOST: milvus
      REDIS_HOST: redis
    ports:
      - "8000:8000"
    depends_on:
      - postgres
      - milvus
      - redis
  
  worker-pass1:
    build:
      context: .
      dockerfile: docker/worker-pass1/Dockerfile
    environment:
      PG_HOST: postgres
      MILVUS_HOST: milvus
      REDIS_HOST: redis
    depends_on:
      - postgres
      - milvus
      - redis
```

### Day 23–24: Requirements & Configuration

**Deliverables:**
- [x] `requirements.txt` with all dependencies
- [ ] Environment variable documentation
- [ ] Configuration management (dotenv)
- [ ] CI/CD pipeline skeleton (.github/workflows)

**Dependency Groups:**
- Core: pandas, numpy, polars
- Storage: psycopg2, pymilvus, redis
- API: fastapi, uvicorn, pydantic
- Cloud: boto3, google-cloud-storage
- Testing: pytest, hypothesis
- Observability: prometheus-client

### Day 25–26: Pilot Program & Outreach

**Deliverables:**
- [ ] Identify 3 target customers (ICPs)
- [ ] Draft 4–6 week pilot offer
- [ ] Prepare sales collateral (1-pager, ROI calc)
- [ ] Outreach emails/calls

**Pilot Offer Template:**
```
===== DATA SANITIZER: 4-WEEK PILOT =====

What You Get:
- Clean your sample dataset (up to 1M rows)
- Before/after cleanliness report
- Deduplication metrics (% removed, false positive rate)
- Performance benchmarks (throughput, latency)

Deliverables:
1. Week 1: Onboarding & dataset upload
2. Week 2: Running full cleaning pipeline
3. Week 3: Analyzing results & tuning rules
4. Week 4: ROI presentation & next steps

Success Metrics:
- >90% duplicate detection accuracy
- <5% false positives
- 10M rows/hour throughput

Investment: FREE (or $X for on-prem deployment)
```

### Day 27–28: Security & Compliance

**Deliverables:**
- [ ] PII detection module (regex patterns)
- [ ] Encryption at-rest (S3 SSE, RDS TDE)
- [ ] TLS for all endpoints
- [ ] Audit logging
- [ ] Security checklist & whitepaper

**PII Patterns:**
```python
PII_PATTERNS = {
    "email": r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
    "phone": r"(\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}",
    "ssn": r"\d{3}-\d{2}-\d{4}",
    "credit_card": r"\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}",
    "url": r"https?://[^\s]+",
}
```

### Day 29–30: Final Testing & Handoff

**Deliverables:**
- [ ] All tests passing (>80% coverage)
- [ ] Load test (simulate 100 concurrent jobs)
- [ ] Failover test (kill a worker, auto-recovery)
- [ ] Disaster recovery test (restore from backup)
- [ ] Final documentation review
- [ ] Handoff to DevOps/SRE

**Load Test:**
```bash
# Using Apache JMeter or similar
jmeter -n -t load_test.jmx -l results.jtl -e -o report/

# Expected: p99 latency < 5 seconds for 100M row jobs
# Expected: success rate > 99.5%
```

---

## Success Criteria (EOD Day 30)

### Must-Haves ✅
- [ ] Storage layer migrated (Postgres + Milvus)
- [ ] S3 connectors working (CSV, JSONL, Parquet)
- [ ] REST API fully functional
- [ ] Job orchestration (queue + workers)
- [ ] All tests passing (>80% coverage)
- [ ] Docker images building
- [ ] Documentation complete
- [ ] 1M row baseline benchmarks documented

### Nice-to-Haves
- [ ] 10M row benchmarks
- [ ] Kubernetes manifests ready
- [ ] Terraform scaffolding
- [ ] Initial pilot customers engaged
- [ ] OpenTelemetry tracing
- [ ] Grafana dashboard

---

## Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| Storage migration delays | Start Postgres/Milvus setup Day 1, parallelize work |
| API complexity | Use FastAPI boilerplate, focus on core endpoints first |
| Performance issues | Weekly benchmarking, early detection |
| Team capacity | Prioritize ruthlessly, drop nice-to-haves if needed |
| Customer unavailability | Start outreach Week 1, secure commitments early |

---

## Communication Plan

### Daily (Team Standup)
- What was completed
- What blocks need resolution
- Planned tasks for today

### Weekly (Stakeholder Update)
- Progress toward milestones
- Any risks/issues
- Demo of working features

### EOD Day 30 (Executive Handoff)
- Live demo of platform
- Benchmark results
- Pilot program status
- 90-day roadmap

---

## Appendix: Critical Path Tasks

```
Day 1  → Postgres schema + tests
Day 2  → ↓
Day 3  → Milvus integration
Day 4  → ↓
Day 5  → S3 connectors
         ↓
Day 6  → REST API
Day 7  → ↓
Day 8  → Job orchestration
Day 9  → ↓
Day 10 → Integration tests
         ↓
Day 11 → Benchmark generation
Day 12 → ↓
Day 13 → Performance baseline
Day 14 → ↓
Day 15 → Comprehensive testing
Day 16 → ↓
Day 17 → Observability
Day 18 → ↓
Day 19 → Documentation
Day 20 → ↓
Day 21 → Dockerization
Day 22 → ↓
Day 23 → Requirements & Config
Day 24 → ↓
Day 25 → Pilot outreach
Day 26 → ↓
Day 27 → Security & Compliance
Day 28 → ↓
Day 29 → Final testing
Day 30 → Handoff & celebration! 🎉
```
