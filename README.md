# Data Sanitizer: Production Data Cleaning Platform

> **Automatically dedupe, impute, normalize, and monitor data quality at scale with deterministic, auditable fixes.**

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-brightgreen.svg)](https://www.python.org/)
[![Code Coverage](https://img.shields.io/badge/coverage-%3E80%25-brightgreen.svg)]()

## 🎯 Overview

Data Sanitizer is a **production-ready data cleaning platform** designed for:

- **Data Engineers**: Automatically dedupe, impute, normalize data at scale
- **ML/Model Ops**: Reduce model retraining from bad upstream data
- **Business/Analytics**: Cleaner data → fewer billing errors & faster BI insights

### Key Features

✅ **High-Quality Deduplication**  
- 90%+ accuracy duplicate detection (MinHash + LSH)
- Exact + near-duplicate detection
- Deterministic, auditable fixes

✅ **Multi-Format Ingestion**  
- CSV, JSON, JSONL, Parquet, Excel
- S3 / GCS / Azure Blob Storage
- Streaming processing (O(chunk) memory)

✅ **Intelligent Imputation**  
- Median/mode-based fills
- Confidence scoring (0.0–1.0)
- Per-cell provenance tracking

✅ **Production-Grade Architecture**  
- Stateless, horizontally scalable workers
- Postgres metadata + Milvus vector DB + Redis cache
- REST API with authentication & rate limiting
- Full audit trail & compliance-ready

✅ **Enterprise Features**  
- PII detection & redaction
- Multi-tenant isolation
- Customizable cleaning rules
- Human-in-the-loop review flow

✅ **Industry-Level Quality** ⭐ **NEW**
- Comprehensive configuration management
- Structured logging with correlation IDs
- Prometheus metrics & health checks
- Input validation & security hardening
- Error recovery & circuit breakers
- CI/CD pipeline with automated testing
- Production-ready deployment guides

---

## 🚀 Quick Start (5 Minutes)

### Prerequisites
- Python 3.11+
- Docker & Docker Compose
- 4GB RAM minimum

### 1. Clone & Install

```bash
git clone https://github.com/CodersAcademy006/Data-Sanitizer.git
cd data-sanitizer

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Start Infrastructure (Local)

```bash
# Start Postgres, Milvus, Redis, API server
docker-compose up -d

# Verify health
curl http://localhost:8000/api/v1/health
# Expected: {"status": "healthy", "storage_backend": "ready"}
```

### 3. Generate Test Data

```bash
python benchmark_generator.py --size 1m --output-dir ./test_data
# Generates: test_data/benchmark_1000000_rows.csv (~500 MB)
```

### 4. Clean Your Data

```bash
# Option A: Via Python
from data_cleaning import run_full_cleaning_pipeline_two_pass_sqlite_batched

cleaned_path, report_path = run_full_cleaning_pipeline_two_pass_sqlite_batched(
    path="test_data/benchmark_1000000_rows.csv",
    output_dir="./output",
    chunksize=50_000
)

# Option B: Via REST API
curl -X POST http://localhost:8000/api/v1/datasets/my-tenant/ingest \
  -H "X-API-Key: my-tenant:key123" \
  -F "file=@test_data/benchmark_1000000_rows.csv" \
  -F "dataset_name=test_dataset"

# Response: {"job_id": "abc-123-def", "status": "queued"}

# Check status
curl http://localhost:8000/api/v1/jobs/abc-123-def

# Download report
curl http://localhost:8000/api/v1/jobs/abc-123-def/report > report.json
```

### 5. View Results

```bash
# Cleaned data (CSV)
head output/cleaned_data.csv

# Cleaning report (JSON)
cat output/cleaning_report.json | jq '.summary'
# Output:
# {
#   "original_row_count": 1000000,
#   "cleaned_row_count": 950000,
#   "rows_dropped": 50000,
#   "deduplication_rate": 0.95
# }
```

---

## 📊 Architecture

### System Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                      CLIENT LAYER                           │
│  REST API (FastAPI) │ Admin UI │ Python/JS SDKs             │
└────────────┬────────────────────────────────┬──────────────┘
             │                                │
┌────────────▼────────────────────────────────▼──────────────┐
│             ORCHESTRATION LAYER                             │
│  Job Scheduler (RabbitMQ/Redis)                             │
│  - Job state machine (queued → running → complete)          │
│  - Retries, idempotency, tenant quotas                      │
└────────────┬────────────────────────────────┬──────────────┘
             │                                │
┌────────────▼──────────────────────────────────▼────────────┐
│           COMPUTE WORKERS (Stateless, Scalable)             │
│  Pass 1: Sampling → LSH index → Postgres                    │
│  Pass 2: Dedupe → Impute → Clean → S3 (Parquet)             │
└──────────────────────────────────────────────────────────────┘
             │                    │
┌────────────▼────────┐  ┌────────▼──────────┐
│  Metadata Storage   │  │  Vector Storage   │
│  Postgres           │  │  Milvus           │
│  - Jobs, hashes     │  │  - LSH samples    │
│  - Audit logs       │  │  - Similarity     │
│  - Confidence       │  │    queries        │
│  - Cell provenance  │  │                   │
└─────────────────────┘  └───────────────────┘
```

### Data Flow

```
1. User uploads file (CSV, JSON, Parquet, etc.)
   ↓
2. API validates, stores to S3, creates Job record
   ↓
3. Pass 1 Worker:
   - Streams file in chunks
   - Samples columns (deterministic reservoir)
   - Computes MinHash/LSH signatures
   - Inserts samples to Milvus, stats to Postgres
   ↓
4. Pass 2 Worker:
   - Streams file again
   - Checks row hashes against Postgres (exact dedup)
   - Queries Milvus for near-duplicates (LSH candidates)
   - Applies imputation, normalization, cleaning
   - Streams output to S3 (Parquet)
   - Inserts confidence scores + audit logs to Postgres
   ↓
5. API serves cleaned data + report
```

---

## 🏗️ Project Structure

```
data_sanitizer/
├── data_cleaning.py           # Core algorithm (Colab prototype upgraded)
├── storage_backend.py         # Postgres + Milvus + Redis interface
├── cloud_storage.py           # S3/GCS connectors, Parquet/CSV writers
├── api_server.py              # FastAPI REST server
├── benchmark_generator.py     # Realistic dirty data generation
├── tests.py                   # 50+ unit, integration, property-based tests
├── requirements.txt           # Python dependencies
│
├── docs/
│   ├── ARCHITECTURE.md        # Full system design (2,000+ lines)
│   ├── DEPLOYMENT.md          # Terraform, Docker, K8s, CI/CD
│   ├── 30DAY_ROADMAP.md       # Week-by-week execution plan
│   ├── IMPLEMENTATION_SUMMARY.md
│   └── API.md                 # (TODO) OpenAPI reference
│
├── docker/
│   ├── api/Dockerfile
│   ├── worker-pass1/Dockerfile
│   ├── worker-pass2/Dockerfile
│   └── .dockerignore
│
├── k8s/
│   ├── base/
│   │   ├── api-deployment.yaml
│   │   ├── api-service.yaml
│   │   ├── worker-pass1-deployment.yaml
│   │   ├── configmap.yaml
│   │   └── hpa.yaml
│   └── overlays/
│       ├── dev/
│       ├── staging/
│       └── prod/
│
├── terraform/
│   ├── main.tf
│   ├── postgres.tf
│   ├── milvus.tf
│   ├── s3.tf
│   ├── eks.tf
│   └── variables.tf
│
└── docker-compose.yaml        # Local development stack
```

---

## 📖 Documentation

- **[ARCHITECTURE.md](docs/ARCHITECTURE.md)** - Complete system design, data models, API contracts
- **[DEPLOYMENT.md](docs/DEPLOYMENT.md)** - Production infrastructure, Kubernetes, Terraform, CI/CD
- **[INDUSTRY_FEATURES.md](docs/INDUSTRY_FEATURES.md)** - ⭐ **NEW**: Industry-level features guide (configuration, logging, monitoring, security)
- **[30DAY_ROADMAP.md](docs/30DAY_ROADMAP.md)** - Execution plan: Day 1 through Day 30
- **[IMPLEMENTATION_SUMMARY.md](docs/IMPLEMENTATION_SUMMARY.md)** - Overview of deliverables
- **[API.md](docs/API.md)** - (TODO) REST API reference, Swagger/OpenAPI

---

## 🧪 Testing

### Run All Tests

```bash
# Install test dependencies
pip install -e ".[dev]"

# Run tests with coverage
pytest tests.py -v --cov=. --cov-report=html --cov-report=term

# Expected: >80% coverage
```

### Test Categories

- **Unit Tests**: JSON flattening, MinHash, LSH, Reservoir sampling
- **Integration Tests**: Full pipeline on small CSV/JSONL datasets
- **Property-Based Tests**: Determinism validation with Hypothesis
- **Performance Tests**: Throughput & latency benchmarks

---

## 📈 Performance Benchmarks

Baseline metrics on modern hardware (AWS m5.xlarge):

| Dataset | File Size | Pass 1 (sec) | Pass 2 (sec) | Throughput (rows/sec) | Memory (MB) |
|---------|-----------|------------|------------|-----------------|-----------|
| 1M CSV  | ~500 MB   | 8–15      | 12–20      | 40k–70k        | 200–400   |
| 10M CSV | ~5 GB     | 80–150    | 120–200    | 40k–70k        | 300–500   |

**SLA**: 10M rows/hour throughput

To run benchmarks:
```bash
python benchmark_generator.py --size 10m
python data_cleaning.py  # Run interactive menu, option 4 (vehicles.csv)
```

---

## 🔐 Security & Compliance

### Privacy
- ✅ PII detection (email, phone, SSN, credit card regex patterns)
- ✅ Configurable PII strategies: redact, hash, exclude, tokenize
- ✅ Encrypted at-rest (S3 SSE-KMS, Postgres TDE)
- ✅ Encrypted in-transit (TLS 1.3)

### Audit & Compliance
- ✅ Immutable audit logs (every transformation recorded)
- ✅ Cell-level provenance (original → cleaned value + confidence score)
- ✅ GDPR/CCPA ready (data deletion support)
- ✅ Row-level security (multi-tenant isolation via Postgres RLS)

### Access Control
- ✅ API key authentication (tenant-scoped)
- ✅ Rate limiting (per-tenant quotas)
- ✅ Role-based access (Admin, Engineer, Reviewer)

---

## 🚀 Production Deployment

### Local Development

```bash
docker-compose up -d
uvicorn api_server:app --reload
```

### Cloud Deployment (AWS)

```bash
# 1. Initialize infrastructure
cd terraform
terraform init
terraform plan -var-file=prod.tfvars
terraform apply -var-file=prod.tfvars

# 2. Build & push Docker images
./scripts/build-and-push.sh

# 3. Deploy via GitOps (ArgoCD)
kubectl apply -f argocd/data-sanitizer-app.yaml
```

### Kubernetes

```bash
# Install Data Sanitizer
kubectl apply -k k8s/overlays/prod

# Check status
kubectl get pods -l app=data-sanitizer-api
kubectl logs deployment/data-sanitizer-api

# Scale workers
kubectl scale deployment data-sanitizer-pass1-worker --replicas=10
```

See [DEPLOYMENT.md](docs/DEPLOYMENT.md) for full instructions.

---

## 📞 Support & Roadmap

### MVP (Current Release)
- ✅ Core deduplication & imputation
- ✅ Multi-format ingestion (CSV, JSON, Parquet)
- ✅ Confidence scoring & audit logs
- ✅ REST API
- ✅ Postgres + Milvus backend

### Phase 2 (Month 2–4)
- [ ] Admin UI (React)
- [ ] Human review flow
- [ ] LLM enrichment (OpenAI/Claude)
- [ ] Advanced PII detection

### Phase 3 (Month 5–12)
- [ ] Multi-tenant SaaS
- [ ] Billing & usage tracking
- [ ] On-prem deployment
- [ ] Custom connectors (Salesforce, etc.)

---

## 🤝 Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Local Development Setup

```bash
# 1. Fork & clone
git clone https://github.com/your-fork/data-sanitizer.git
cd data-sanitizer

# 2. Create feature branch
git checkout -b feat/your-feature

# 3. Install dev dependencies
pip install -e ".[dev]"

# 4. Run tests (must pass)
pytest tests.py -v --cov=.

# 5. Format code
black .
flake8 .
mypy .

# 6. Submit PR
git push origin feat/your-feature
```

---

## 📄 License

MIT License. See [LICENSE](LICENSE) for details.

---

## 🎓 Key Concepts

### MinHash & LSH
- **MinHash**: Probabilistic fingerprint of a text that preserves Jaccard similarity
- **LSH** (Locality-Sensitive Hashing): Bucket function that maps similar items to same bucket
- **Purpose**: Efficiently find near-duplicate rows without O(n²) comparisons

### Deterministic Reservoir Sampling
- **Goal**: Sample fixed-size subset of unbounded stream
- **Method**: Use hash(row_id + salt) as priority; keep min-priority items
- **Benefit**: Same input + same salt = same sample (reproducible)

### Two-Pass Pipeline
- **Pass 1**: Build index (reservoirs, LSH) without modifying data
- **Pass 2**: Clean data using indices from Pass 1
- **Benefit**: Deterministic, can replay Pass 2 with different rules

---

## 🙏 Acknowledgments

- Built with [pandas](https://pandas.pydata.org/), [polars](https://www.pola-rs.com/), [pyarrow](https://arrow.apache.org/)
- Storage: [PostgreSQL](https://www.postgresql.org/), [Milvus](https://milvus.io/), [Redis](https://redis.io/)
- API: [FastAPI](https://fastapi.tiangolo.com/), [Pydantic](https://docs.pydantic.dev/)
- Infrastructure: [Terraform](https://www.terraform.io/), [Kubernetes](https://kubernetes.io/)

---

## 📧 Get Started

1. **Read**: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) (5 min overview)
2. **Try**: Quick start above (10 min hands-on)
3. **Explore**: [docs/30DAY_ROADMAP.md](docs/30DAY_ROADMAP.md) (plan for next month)
4. **Deploy**: [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) (production setup)

**Questions?** Open an issue or contact us at `srjnupadhyay@gmail.com`

---

**Happy cleaning! 🧹✨**
