# Data Sanitizer: Production Architecture v1.0

## 1. High-Level System Overview

**Data Sanitizer** is a distributed, scalable data cleaning platform designed to handle multi-terabyte workloads with deterministic, auditable transformations.

### Core Value Proposition
- **Data Engineers**: "Automatically dedupe, impute, normalize, and monitor data quality at scale with deterministic, auditable fixes."
- **ML/Model Ops**: "Reduce model retraining incidents and upstream data failures by providing confidence-scored fixes and drift alerts."
- **Business/Analytics**: "Cleaner data → fewer billing/reporting errors and faster BI insights."

---

## 2. System Architecture (Microservices + Data Plane)

```
┌─────────────────────────────────────────────────────────────────────┐
│                           CLIENT LAYER                              │
├─────────────────────────────────────────────────────────────────────┤
│  REST API Gateway (FastAPI)  │  Admin UI (React/Vue)  │  SDKs       │
└────────────┬──────────────────────────────────────────┬─────────────┘
             │                                          │
┌────────────▼──────────────────────────────────────────▼─────────────┐
│                    ORCHESTRATION LAYER                              │
├─────────────────────────────────────────────────────────────────────┤
│  Job Scheduler (RabbitMQ/async)                                     │
│  - Job queue with retries, idempotency, tenant quotas              │
│  - State tracking (pending → running → success/failed)              │
│  - Concurrency limits per tenant                                    │
│  - Automatic worker autoscaling (K8s HPA)                          │
└────────────┬──────────────────────────────────────────┬─────────────┘
             │                                          │
┌────────────▼──────────────────────────────────────────▼─────────────┐
│                     COMPUTE LAYER (Stateless Workers)               │
├─────────────────────────────────────────────────────────────────────┤
│  Pass 1 Workers (Sampling + LSH Insertion)                          │
│  - Stream files from S3/GCS/upload storage                         │
│  - Deterministic reservoir sampling (per column)                    │
│  - MinHash/LSH computation for near-dupe detection                  │
│  - Write samples to Milvus, metadata to Postgres                    │
│                                                                     │
│  Pass 2 Workers (Cleaning + Deduplication)                          │
│  - Query Postgres for existing hashes (exact dedup)                 │
│  - Query Milvus for LSH candidates (near-dupe detection)            │
│  - Apply imputation, normalization, outlier detection              │
│  - Stream cleaned output to S3 (Parquet by default)                 │
│  - Insert audit logs and confidence scores to Postgres              │
│                                                                     │
│  LLM Enrichment Workers (Optional, Phase 2)                         │
│  - Query Postgres for low-confidence rows                           │
│  - Call LLM service (with RAG context)                              │
│  - Store suggestions, apply with human approval                     │
└──────────────────────────────────────────────────────────────────────┘
             │                                          │
┌────────────▼──────────────────────────────────────────▼─────────────┐
│                      DATA STORAGE LAYER                             │
├─────────────────────────────────────────────────────────────────────┤
│  Object Storage (S3 / GCS / Azure Blob)                             │
│  ├─ Raw artifacts (input files, checksums)                          │
│  ├─ Cleaned artifacts (Parquet by default, CSV on demand)          │
│  └─ Audit logs (immutable per job)                                  │
│                                                                     │
│  Vector Database (Milvus / Weaviate / Qdrant)                       │
│  ├─ LSH samples for near-duplicate detection                        │
│  ├─ Vectorized text for semantic search (future)                    │
│  └─ Partitioned by job_id for easy cleanup                          │
│                                                                     │
│  Metadata Database (Postgres)                                       │
│  ├─ Jobs table (id, tenant_id, status, created_at, updated_at)     │
│  ├─ Row hashes (partitioned by date, hash, first_seen_row)        │
│  ├─ Imputation stats (global medians, modes per column)             │
│  ├─ Confidence scores & transformation IDs (per cell)               │
│  ├─ Audit logs (immutable, searchable by user/date/job)             │
│  ├─ Cleaning rules (PII policies, canonicalization rules)           │
│  └─ Tenant quotas & usage tracking                                  │
│                                                                     │
│  Fast KV Store (Redis, optional)                                    │
│  ├─ Short-lived job state (sampling progress)                       │
│  ├─ Cached LLM responses (with TTL)                                 │
│  ├─ Rate limit counters (per tenant)                                │
│  └─ Session state for UI                                            │
└──────────────────────────────────────────────────────────────────────┘
             │                                          │
┌────────────▼──────────────────────────────────────────▼─────────────┐
│                  OBSERVABILITY & MONITORING LAYER                   │
├─────────────────────────────────────────────────────────────────────┤
│  Prometheus Metrics                                                  │
│  - Rows processed/sec, duplicates flagged, imputations applied      │
│  - API latency, error rates, job success rates                      │
│  - LLM API costs, cache hit rates                                   │
│                                                                     │
│  OpenTelemetry Tracing                                              │
│  - Trace each job across workers, store/API calls                   │
│  - Per-row transformation trace IDs (for audit)                     │
│                                                                     │
│  Logging (ELK or managed solution)                                   │
│  - Structured logs per component (JSON format)                      │
│  - Searchable by job_id, tenant_id, error_type                      │
│                                                                     │
│  Grafana Dashboard                                                   │
│  - Real-time metrics: throughput, latency, success rates            │
│  - Business KPIs: cost per GB, duplicates removed, false positives  │
│  - Alerts: job failures, cost overruns, SLA breaches                │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 3. Data Model & Contracts

### 3.1 Canonical Dataset Contract

Every dataset ingested into the system is assigned a versioned schema:

```json
{
  "dataset_id": "ds-123-uuid",
  "version": 1,
  "created_at": "2025-11-16T10:00:00Z",
  "schema": {
    "columns": [
      {
        "name": "customer_id",
        "type": "integer",
        "nullable": false,
        "pii": false,
        "canonical_values": null,
        "business_rules": ["unique", "non-negative"],
        "source": "external_crm"
      },
      {
        "name": "email",
        "type": "string",
        "nullable": false,
        "pii": true,
        "pii_strategy": "hash",  // or "redact", "exclude"
        "source": "user_input"
      },
      {
        "name": "status",
        "type": "string",
        "nullable": false,
        "canonical_values": ["active", "inactive", "suspended"],
        "business_rules": ["one_of"],
        "source": "external_crm"
      }
    ]
  }
}
```

### 3.2 Versioning & Lineage

```sql
-- cleaned_datasets table
CREATE TABLE cleaned_datasets (
  id UUID PRIMARY KEY,
  job_id UUID NOT NULL,
  version INTEGER NOT NULL,
  s3_path TEXT NOT NULL,  -- where cleaned data lives
  checksum TEXT,
  created_at TIMESTAMP,
  transformations JSONB NOT NULL,  -- array of {id, type, params, applied_at}
  UNIQUE(id, job_id, version)
);

-- transformations logged per dataset version
{
  "transformations": [
    {"id": "t-1", "type": "dedupe", "params": {"hash_algo": "md5"}, "rows_removed": 15},
    {"id": "t-2", "type": "impute", "params": {"strategy": "median", "columns": ["age"]}, "rows_modified": 42},
    {"id": "t-3", "type": "normalize_text", "params": {}, "rows_modified": 1000}
  ]
}
```

### 3.3 Confidence & Provenance Per Cell

Each cleaned cell stores:
```sql
-- cell_provenance table (one row per modified cell)
CREATE TABLE cell_provenance (
  id UUID PRIMARY KEY,
  job_id UUID NOT NULL,
  row_id INTEGER NOT NULL,
  column_name TEXT NOT NULL,
  original_value TEXT,
  cleaned_value TEXT,
  transformation_id TEXT NOT NULL,
  confidence_score FLOAT DEFAULT 1.0,  -- 1.0 = deterministic, <1.0 = heuristic/LLM
  source TEXT,  -- "deterministic_impute", "lsm_suggest", "human_override"
  created_at TIMESTAMP,
  PRIMARY KEY (job_id, row_id, column_name),
  FOREIGN KEY (job_id) REFERENCES jobs(id)
);
```

### 3.4 API Contracts

#### Ingest Endpoint
```http
POST /api/v1/datasets/{tenant_id}/ingest
Content-Type: multipart/form-data

{
  "file": <binary>,
  "dataset_name": "customer_data",
  "schema_config": {
    "pii_strategy": "hash",
    "strict_schema": false
  }
}

RESPONSE 202:
{
  "job_id": "job-uuid-123",
  "dataset_id": "ds-uuid-123",
  "status": "queued",
  "estimated_completion_ms": 45000
}
```

#### Status Endpoint
```http
GET /api/v1/jobs/{job_id}

RESPONSE 200:
{
  "job_id": "job-uuid-123",
  "status": "running",  // queued, running, success, failed
  "progress": {
    "pass1_progress": 0.3,
    "pass2_progress": 0.1,
    "estimated_seconds_remaining": 30
  },
  "metrics": {
    "rows_processed": 500000,
    "duplicates_found": 15000,
    "imputations_applied": 2000
  }
}
```

#### Report Endpoint
```http
GET /api/v1/jobs/{job_id}/report

RESPONSE 200:
{
  "summary": {
    "original_row_count": 1000000,
    "cleaned_row_count": 985000,
    "rows_dropped": 15000,
    "deduplication_rate": 0.985
  },
  "quality_metrics": {
    "duplicates_detected": 15000,
    "false_positive_rate": 0.02,
    "imputation_rate": 0.002,
    "confidence_score_avg": 0.92
  },
  "transformations_applied": [...]
}
```

#### Download Endpoint
```http
GET /api/v1/jobs/{job_id}/download?format=parquet
# OR
GET /api/v1/jobs/{job_id}/download?format=csv

RESPONSE 200 with binary stream (S3 signed URL redirect)
```

---

## 4. Ingestion Layer (Stateless Workers)

### Responsibilities
1. Accept files from multiple sources (S3, GCS, Azure, upload endpoints, FTP)
2. Stream chunked reads into memory-bounded buffers
3. Perform schema discovery (safe_read_v3 refactored)
4. Pass chunks to Pass 1 workers

### Design
```python
# Example: S3FileReader
class S3FileReader:
    def __init__(self, s3_client, bucket, key, chunksize=50_000):
        self.s3_client = s3_client
        self.bucket = bucket
        self.key = key
        self.chunksize = chunksize
    
    def stream_chunks(self):
        """Generator yielding DataFrames of chunksize rows."""
        # Uses boto3 streaming for O(chunk) memory
        pass
    
    def schema_discovery(self):
        """Two-pass schema discovery, return column list."""
        pass
```

### Supported Formats
- CSV / TSV (via pandas.read_csv with chunksize)
- JSONL / NDJSON (via ijson line-by-line)
- JSON array (via ijson streaming)
- Parquet (via pyarrow streaming)
- Excel (via openpyxl in read_only mode)

---

## 5. Orchestrator: Job Scheduler & State Management

### Architecture
- **Queue**: RabbitMQ or Redis-based task queue
- **State Machine**: job moves through `pending → queued → pass1 → pass2 → success/failed`
- **Idempotency**: Each job has UUID; retrying same job is safe (tracked in DB)

### Pseudocode
```python
@app.post("/ingest")
def ingest_dataset(file, tenant_id):
    job_id = generate_uuid()
    job = Job(
        id=job_id,
        tenant_id=tenant_id,
        status="queued",
        created_at=now()
    )
    db.insert(job)
    
    # Enqueue for Pass 1
    queue.publish("pass1_tasks", {
        "job_id": job_id,
        "s3_path": "s3://bucket/uploads/...",
        "schema_config": {...}
    })
    
    return {"job_id": job_id, "status": "queued"}

# Worker process
def pass1_worker(task):
    job_id = task["job_id"]
    try:
        db.update(job_id, status="pass1_running")
        stats = compute_global_stats_v2(...)  # refactored
        db.insert(pass1_results_table, stats)
        
        # Enqueue for Pass 2
        queue.publish("pass2_tasks", {...})
    except Exception as e:
        db.update(job_id, status="failed", error=str(e))
```

### Scaling Strategy
- **Horizontal**: Add more workers by deploying K8s Pods (HPA watches queue length)
- **Autoscaling**: If queue depth > threshold, scale up; if queue depth < low_threshold, scale down
- **Backpressure**: Job queue has max depth; ingest API returns 429 if queue full

---

## 6. Compute Workers: Pass 1 & Pass 2

### Pass 1: Sampling & Index Building
1. Stream input chunks from S3
2. For each chunk:
   - Compute deterministic reservoirs per column
   - Insert MinHash/LSH samples into Milvus
   - Insert imputation stats (medians, modes) into Postgres
3. At end: return summary stats

### Pass 2: Cleaning & Deduplication
1. Stream input chunks from S3
2. For each chunk:
   - Compute row hashes, query Postgres for existing (exact dedup)
   - Query Milvus for LSH candidates (near-dupe detection)
   - Apply imputations, normalization, outlier detection
   - Stream cleaned output to S3 (Parquet + metadata)
   - Insert confidence scores + audit logs into Postgres
3. At end: trigger report generation

---

## 7. Storage & State Management

### Object Storage (S3 / GCS / Azure)
```
s3://bucket/
├── uploads/
│   └── {job_id}/
│       └── original_file.csv
├── cleaned/
│   └── {job_id}/
│       ├── cleaned_data_v1.parquet
│       ├── metadata.json
│       └── audit.json
└── archive/
    └── {job_id}/
        └── {date}/
```

### Metadata Database (Postgres)
```sql
-- Core tables
CREATE TABLE jobs (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  dataset_name TEXT,
  status TEXT,  -- pending, queued, pass1_running, pass2_running, success, failed
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  error_message TEXT,
  FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);

CREATE TABLE row_hashes (
  id BIGSERIAL,
  job_id UUID NOT NULL,
  hash TEXT NOT NULL,
  first_seen_row INTEGER,
  created_at TIMESTAMP,
  UNIQUE(job_id, hash),
  FOREIGN KEY (job_id) REFERENCES jobs(id)
) PARTITION BY RANGE (created_at);  -- Monthly partitions

CREATE TABLE imputation_stats (
  job_id UUID PRIMARY KEY,
  column_name TEXT,
  medians JSONB,  -- {col: value, ...}
  modes JSONB,    -- {col: value, ...}
  FOREIGN KEY (job_id) REFERENCES jobs(id)
);

CREATE TABLE audit_logs (
  id UUID PRIMARY KEY,
  job_id UUID NOT NULL,
  user_id UUID,
  action TEXT,  -- "dataset_uploaded", "cleaning_started", "rule_applied", ...
  details JSONB,
  created_at TIMESTAMP,
  FOREIGN KEY (job_id) REFERENCES jobs(id),
  INDEX (created_at, job_id)
);

-- Optional: cell-level provenance
CREATE TABLE cell_provenance (
  id UUID PRIMARY KEY,
  job_id UUID NOT NULL,
  row_id INTEGER,
  column_name TEXT,
  original_value TEXT,
  cleaned_value TEXT,
  transformation_id TEXT,
  confidence_score FLOAT,
  source TEXT,  -- "deterministic", "lsm_suggest", "human"
  created_at TIMESTAMP,
  FOREIGN KEY (job_id) REFERENCES jobs(id)
);
```

### Vector Database (Milvus / Qdrant)
- Store LSH bucket hashes + snippets for near-dupe detection
- Partition by job_id for isolation
- Example schema:
  ```json
  {
    "collection": "lsh_samples",
    "fields": [
      {"name": "job_id", "type": "varchar"},
      {"name": "bucket_key", "type": "varchar"},
      {"name": "sampled_row_id", "type": "int64"},
      {"name": "snippet", "type": "varchar"},
      {"name": "minhash_vector", "type": "float_vector", "dim": 64}
    ]
  }
  ```

### Redis (Optional, for caching & short-lived state)
```
job:{job_id}:progress → {"pass1": 0.3, "pass2": 0.1, "eta_ms": 30000}
job:{job_id}:lsm_cache → {bucket_key: [snippets...]}  # TTL 1 hour
tenant:{tenant_id}:rate_limit → current request count
llm_cache:{query_hash} → {response, confidence}  # TTL 24 hours
```

---

## 8. API & UI Layer

### REST API (FastAPI)
- **Endpoints**: `/api/v1/datasets/{tenant_id}/ingest`, `/api/v1/jobs/{job_id}`, `/api/v1/jobs/{job_id}/report`, `/api/v1/jobs/{job_id}/download`
- **Authentication**: Tenant API keys (JWT or custom header)
- **Rate Limiting**: Per-tenant quota (rows/month, API calls/sec)
- **OpenAPI**: Fully documented via FastAPI auto-generation

### Admin UI (React/Vue)
- Upload dataset form
- Configure cleaning rules (PII, canonicalization)
- View job status (real-time progress bar)
- Inspect sample fixes (before/after comparison)
- Approve/reject LLM suggestions
- Audit log explorer (search by transformation ID, user, date)

---

## 9. Security, Privacy & Compliance

### PII Handling
1. **Detection**: Regex patterns + optional ML model (PyDantic, Named Entity Recognition)
2. **Strategies**: (configurable per column)
   - **Redact**: Replace with placeholder (e.g., `<EMAIL>`)
   - **Hash**: SHA-256 with salt, deterministic
   - **Exclude**: Drop from output
   - **Tokenize**: Store mapping in separate secure DB

### Encryption
- **TLS**: All API endpoints use HTTPS
- **At-rest**: S3 SSE-KMS, Postgres TDE
- **DB Credentials**: Vault-managed, rotated quarterly

### Access Control
- **Role-Based**: Admin, DataEngineer, Reviewer
- **Row-Level Security**: Postgres RLS policies by tenant_id
- **API Key Scoping**: Keys can be limited to specific datasets/actions

### Audit Trail
- **Immutable Logs**: All transformations logged to `audit_logs` table
- **GDPR/CCPA**: Data deletion jobs recorded, no orphaned rows
- **PII Access**: Separate audit trail for PII column queries

---

## 10. Observability, Metrics & Alerts

### Prometheus Metrics
```
# Processing
data_sanitizer_rows_processed_total{job_id, tenant_id}
data_sanitizer_rows_cleaned_total{job_id, tenant_id}
data_sanitizer_duplicates_flagged_total{job_id}
data_sanitizer_imputations_applied_total{job_id}

# API
data_sanitizer_api_request_duration_seconds{method, endpoint, status}
data_sanitizer_api_errors_total{method, endpoint, error_type}

# Infrastructure
data_sanitizer_worker_processing_time_seconds{pass, status}
data_sanitizer_lsm_query_latency_seconds{bucket}
data_sanitizer_db_connection_pool_usage{service}

# LLM (if enabled)
data_sanitizer_llm_calls_total{model, status}
data_sanitizer_llm_cost_usd_total
data_sanitizer_llm_cache_hits_total
```

### Alerts
- Job failure rate > 5% in 5min window → page on-call
- Avg latency > SLA (e.g., > 5 min for 1M rows) → warning
- Cost per GB > budget → warning
- Vector DB connection errors → critical

### Dashboard (Grafana)
1. **Operational**: Throughput (rows/sec), latency (p50/p95/p99), success rates
2. **Business**: Cost per GB, duplicates removed, false positive rate
3. **Alerts**: Job failures, SLA breaches, cost overruns

---

## 11. LLM Integration (Phase 2 – Optional)

### When to Use LLMs
- **High-value, non-deterministic tasks**:
  - Canonicalization of ambiguous categories
  - Entity linking (company names, addresses)
  - Rich imputation (infer country from address)
  - Conflict resolution when multiple values exist

### Design
1. **Low-confidence rows only**: LLM processes rows where confidence < threshold
2. **RAG context**: Ground LLM with company-specific rules + historical cleanings
3. **Human-in-the-loop**: LLM provides candidates; humans approve before applying
4. **Cost control**: Cache responses, batch calls, use smaller models for cheap tasks

### Pseudocode
```python
# Pass 2 worker, after deterministic cleaning
low_conf_rows = db.query(
    "SELECT * FROM cleaned_rows WHERE confidence < 0.7 AND needs_enrichment"
)

for batch in chunk(low_conf_rows, 100):
    cached = cache.get(batch)  # Try cache
    uncached = [r for r in batch if r not in cached]
    
    if uncached:
        # Call LLM with RAG context
        suggestions = llm_service.suggest(
            rows=uncached,
            context=rag.fetch_rules(tenant_id),
            model="gpt-4-turbo"
        )
        cache.set(suggestions, ttl=86400)
    
    # Save suggestions for human review
    db.insert_suggestions(suggestions, status="pending_review")
```

---

## 12. Deployment & CI/CD Pipeline

### Infrastructure as Code (Terraform)
```
terraform/
├── main.tf          (RDS Postgres, Milvus, S3, Redis)
├── k8s.tf           (EKS cluster, worker node groups)
├── iam.tf           (IAM roles, service accounts)
└── monitoring.tf    (CloudWatch, Prometheus, Grafana)
```

### Containerization (Docker)
```
docker/
├── api/Dockerfile       (FastAPI server)
├── worker-pass1/Dockerfile
├── worker-pass2/Dockerfile
├── llm-service/Dockerfile
└── ui/Dockerfile       (React, served via nginx)
```

### Kubernetes Manifests (GitOps)
```
k8s/
├── namespaces.yaml
├── api-deployment.yaml  (replicas: 3, HPA enabled)
├── pass1-worker-deployment.yaml  (HPA: min=2, max=10, target_cpu=70%)
├── pass2-worker-deployment.yaml  (HPA: min=2, max=10)
├── secrets.yaml  (encrypted via SOPS)
├── configmaps.yaml
└── services.yaml
```

### CI/CD Pipeline (GitHub Actions + ArgoCD)
```
On PR:
  1. Unit tests + lint
  2. Integration tests (ephemeral DB + Milvus)
  3. Code coverage > 80%

On merge to main:
  1. Build Docker images
  2. Push to ECR
  3. Tag with commit SHA
  4. Run canary tests (5% traffic)

On approval:
  1. ArgoCD syncs manifests (Kustomize overlays)
  2. Progressive rollout (5% → 25% → 50% → 100%)
  3. Health checks + rollback if > 1% error rate
```

---

## 13. Performance Targets & SLAs

### MVP Success Metrics
1. **Throughput**: ≥10M rows/hour on standard worker cluster
2. **Accuracy**: ≥90% true duplicate detection (F1 score)
3. **False Positives**: <5% of fixes flagged as incorrect by human audit
4. **API Latency**: p95 < 2sec for status check, p95 < 5min for 1M-row cleaning
5. **Reliability**: 99.9% job success rate

### Scaling Goals
- **Month 0–2**: 100M rows/day (baseline)
- **Month 3–6**: 1B rows/day (10x)
- **Month 6–12**: 10B rows/day (100x)

---

## 14. Roadmap (12 Months)

| Phase | Timeline | Deliverables |
|-------|----------|--------------|
| **Foundation** | Month 0–2 | Postgres + Milvus migration, S3 connectors, Parquet output, cell provenance |
| **API & Orchestration** | Month 2–4 | FastAPI REST API, job scheduler (RabbitMQ), worker autoscaling |
| **UI & Review Flow** | Month 4–6 | Admin UI, human review interface, audit explorer |
| **LLM Enrichment** | Month 6–8 | LLM module with RAG, human-in-the-loop, cost controls |
| **SaaS Launch** | Month 8–10 | Multi-tenant isolation, billing, SSO, VPC agents |
| **Enterprise Features** | Month 10–12 | Custom connectors, priority SLA, on-prem deployment |

---

## 15. Acceptance Criteria for MVP Launch

- [ ] Reliability: 99.9% job success rate on scheduled load
- [ ] Performance: Process ≥10M rows/hour on standard worker cluster
- [ ] Accuracy: >90% true duplicate detection (validated on benchmark datasets)
- [ ] Security: Pen test completed; PII handling validated
- [ ] UX: Admin can onboard dataset & run job in <10 minutes
- [ ] Documentation: Quickstart, API docs, security whitepaper
- [ ] Testing: >80% code coverage, automated end-to-end tests

---

## Appendix: Key References

- **MinHash & LSH**: See Section 3 of `data_cleaning.py` (deterministic hashing)
- **Reservoir Sampling**: DeterministicReservoir class (O(1) per row, O(capacity) memory)
- **Parquet Format**: Better compression than CSV, faster for columnar queries (PyArrow)
- **Milvus Docs**: https://milvus.io/docs (collection operations, partitioning)
- **Postgres Partitioning**: Logical partitioning for `row_hashes` by date (faster inserts/deletes)
- **Kubernetes HPA**: https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/
