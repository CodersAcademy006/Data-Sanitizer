# Industry-Level Features Documentation

## Overview

This document describes the industry-level features added to the Data Sanitizer platform to make it production-ready and enterprise-grade.

## Table of Contents

1. [Configuration Management](#configuration-management)
2. [Enhanced Logging](#enhanced-logging)
3. [Input Validation](#input-validation)
4. [Monitoring & Metrics](#monitoring--metrics)
5. [Error Recovery](#error-recovery)
6. [CI/CD Pipeline](#cicd-pipeline)
7. [Security Features](#security-features)

---

## Configuration Management

### Overview
Centralized configuration system with environment variable support and validation.

### Features
- Environment-based configuration (development, staging, production)
- Type-safe dataclass configuration
- Automatic validation
- Support for multiple backends (PostgreSQL, Redis, Milvus, S3, GCS, Azure)

### Usage

```python
from config import get_config, load_config

# Load configuration
config = load_config()

# Access configuration
db_config = config.database
api_config = config.api
processing_config = config.processing

# Connection string
conn_str = config.database.connection_string
```

### Environment Variables

See `.env.example` for all available configuration options. Key variables:

- `ENVIRONMENT`: Environment name (development, staging, production)
- `DEBUG`: Enable debug mode
- `POSTGRES_HOST`, `POSTGRES_PORT`, etc.: Database configuration
- `API_PORT`, `API_WORKERS`: API server configuration
- `DEFAULT_CHUNKSIZE`: Data processing chunk size
- `LOG_LEVEL`: Logging level

### Validation

Configuration is automatically validated on load:
- Database settings
- Processing parameters (e.g., MinHash hashes must be divisible by LSH bands)
- Security settings (e.g., JWT secret must be changed in production)

---

## Enhanced Logging

### Overview
Structured logging with correlation IDs, performance tracking, and audit trails.

### Features
- Structured JSON logging for production
- Pretty colored console logging for development
- Correlation IDs for request tracing
- Performance logging with duration tracking
- Security audit logging

### Usage

#### Basic Setup

```python
from logging_config import setup_logging, get_logger

# Configure logging
setup_logging(
    level="INFO",
    json_logs=False,  # Set to True for production
    log_file="/var/log/data-sanitizer.log"
)

# Get logger
logger = get_logger(__name__)
logger.info("Application started")
```

#### Correlation IDs

```python
from logging_config import set_correlation_id

# Set correlation ID (e.g., from request ID)
set_correlation_id("req-123-456")

# All subsequent logs will include this ID
logger.info("Processing request")
```

#### Performance Logging

```python
from logging_config import PerformanceLogger

logger = get_logger(__name__)

with PerformanceLogger(logger, "data_processing", dataset_id="abc-123"):
    # Your code here
    process_data()
# Automatically logs duration
```

#### Audit Logging

```python
from logging_config import audit_logger

# Log access
audit_logger.log_access(
    user="user@example.com",
    resource="dataset-123",
    action="read",
    success=True
)

# Log data modification
audit_logger.log_data_modification(
    user="user@example.com",
    dataset="dataset-123",
    operation="clean",
    row_count=10000
)

# Log security event
audit_logger.log_security_event(
    event_type="authentication_failure",
    severity="WARNING",
    details={"ip": "192.168.1.1", "attempts": 3}
)
```

---

## Input Validation

### Overview
Comprehensive validation for file uploads, API requests, and data integrity.

### Features
- File type and size validation
- MIME type checking
- SQL injection detection
- XSS detection
- Path traversal prevention
- Data structure validation

### Usage

#### File Upload Validation

```python
from validation import validate_file_upload, ValidationError

try:
    validate_file_upload(
        filename="data.csv",
        file_size=1024 * 1024 * 100,  # 100MB
        file_path="/path/to/data.csv",
        max_size_mb=500
    )
except ValidationError as e:
    print(f"Validation failed: {e}")
```

#### API Request Validation

```python
from validation import validate_api_request

try:
    validate_api_request(
        tenant_id="tenant-123",
        dataset_name="my_dataset",
        pii_strategy="hash"
    )
except ValidationError as e:
    print(f"Invalid request: {e}")
```

#### Security Validation

```python
from validation import SecurityValidator

# Validate API key
try:
    tenant_id = SecurityValidator.validate_api_key(
        api_key="tenant-123:secret-key",
        valid_keys={"tenant-123": "secret-key"}
    )
except ValidationError as e:
    print(f"Invalid API key: {e}")

# Detect SQL injection
try:
    SecurityValidator.detect_sql_injection("'; DROP TABLE users--")
except ValidationError:
    print("SQL injection attempt detected!")
```

### Validation Classes

- `FileValidator`: File upload validation
- `DataValidator`: Data structure validation
- `APIValidator`: API request validation
- `SecurityValidator`: Security-focused validation

---

## Monitoring & Metrics

### Overview
Prometheus-compatible metrics and health checks for observability.

### Features
- HTTP request metrics
- Processing metrics (rows processed, duplicates detected, etc.)
- Storage operation metrics
- Cache metrics
- Health checks (database, disk, memory)
- Custom metric decorators

### Usage

#### Recording Metrics

```python
from metrics import metrics_collector

# Record HTTP request
metrics_collector.record_http_request(
    method="POST",
    endpoint="/api/v1/datasets",
    status=200,
    duration=0.123
)

# Record rows processed
metrics_collector.record_rows_processed(
    count=10000,
    operation="cleaning"
)

# Record duplicates detected
metrics_collector.record_duplicates(
    count=150,
    method="exact"
)
```

#### Using Decorators

```python
from metrics import track_time, track_errors, processing_duration_seconds

@track_time(processing_duration_seconds, labels={"stage": "pass1"})
def process_pass1():
    # Your code here
    pass

@track_errors(component="data_processor")
def risky_operation():
    # Code that might fail
    pass
```

#### Health Checks

```python
from metrics import health_checker

# Check all health endpoints
health_status = health_checker.check_all()

if health_status["healthy"]:
    print("All systems healthy")
else:
    print("Health check failed:", health_status["checks"])
```

#### Exposing Metrics

```python
from metrics import get_metrics

# In FastAPI endpoint
@app.get("/metrics")
def metrics():
    return Response(content=get_metrics(), media_type="text/plain")
```

### Available Metrics

- `http_requests_total`: Total HTTP requests
- `http_request_duration_seconds`: Request duration
- `datasets_processed_total`: Datasets processed
- `rows_processed_total`: Rows processed
- `duplicates_detected_total`: Duplicates detected
- `missing_values_imputed_total`: Missing values imputed
- `storage_operations_total`: Storage operations
- `cache_hits_total`, `cache_misses_total`: Cache performance
- `errors_total`: Total errors by component and type

---

## Error Recovery

### Overview
Retry mechanisms, circuit breakers, and graceful degradation for resilient operations.

### Features
- Retry with exponential backoff
- Circuit breaker pattern
- Timeout handling
- Fallback values
- Specialized retry for database, network, and file operations

### Usage

#### Retry Decorator

```python
from error_recovery import retry, RetryStrategy

@retry(
    max_attempts=3,
    delay=1.0,
    backoff=2.0,
    strategy=RetryStrategy.EXPONENTIAL_BACKOFF
)
def fetch_from_api():
    # Code that might fail temporarily
    pass
```

#### Circuit Breaker

```python
from error_recovery import CircuitBreaker

# Create circuit breaker
circuit_breaker = CircuitBreaker(
    failure_threshold=5,
    recovery_timeout=60.0
)

@circuit_breaker
def call_external_service():
    # Call external service
    pass

# Manual reset if needed
circuit_breaker.reset()
```

#### Fallback Handler

```python
from error_recovery import FallbackHandler

@FallbackHandler.with_fallback(
    lambda: get_config(),
    fallback_value={"default": "config"}
)
def get_config():
    # Try to get config, use fallback if it fails
    pass
```

#### Timeout

```python
from error_recovery import with_timeout

@with_timeout(timeout_seconds=30.0)
def long_running_operation():
    # Operation with timeout
    pass
```

#### Specialized Retry

```python
from error_recovery import ErrorRecovery

# Database operations
@ErrorRecovery.retry_database_operation
def save_to_database():
    pass

# Network operations
@ErrorRecovery.retry_network_operation
def download_file():
    pass

# File operations
@ErrorRecovery.retry_file_operation
def read_file():
    pass
```

---

## CI/CD Pipeline

### Overview
Automated testing, security scanning, and deployment pipeline using GitHub Actions.

### Pipeline Stages

1. **Test**
   - Python 3.11 and 3.12
   - Code quality checks (black, isort, flake8)
   - Unit tests with coverage
   - Coverage upload to Codecov

2. **Security**
   - Bandit security linting
   - Dependency vulnerability scanning with Safety

3. **Build**
   - Docker image building
   - Multi-architecture support
   - Image pushing to registry

4. **Deploy**
   - Staging deployment (develop branch)
   - Production deployment (main branch)

### Configuration

The pipeline is defined in `.github/workflows/ci-cd.yml`.

#### Required Secrets

- `DOCKER_USERNAME`: Docker Hub username
- `DOCKER_PASSWORD`: Docker Hub password/token

#### Customization

Edit `.github/workflows/ci-cd.yml` to:
- Add more Python versions
- Modify deployment commands
- Add integration tests
- Configure notifications

### Running Locally

```bash
# Code quality
black --check --line-length 120 *.py
isort --check --profile black --line-length 120 *.py
flake8 --max-line-length=120 *.py

# Tests
pytest tests.py -v --cov=. --cov-report=term

# Security
bandit -r . -f screen
safety check
```

---

## Security Features

### Overview
Multiple layers of security for production deployments.

### Features Implemented

1. **Input Validation**
   - File type whitelisting
   - Size limits
   - SQL injection detection
   - XSS prevention
   - Path traversal prevention

2. **Authentication & Authorization**
   - API key authentication
   - JWT token support
   - Tenant-based access control

3. **Audit Logging**
   - All access logged
   - Data modifications tracked
   - Security events recorded

4. **Data Protection**
   - PII detection
   - Configurable PII handling (hash, redact, mask, tokenize)
   - Encryption support (via environment variables)

5. **Rate Limiting**
   - Per-tenant rate limits
   - Configurable limits

### Security Best Practices

1. **Environment Variables**
   - Never commit secrets to version control
   - Use `.env` file (gitignored)
   - Rotate secrets regularly

2. **Production Checklist**
   - Change `JWT_SECRET` from default
   - Enable `AUTH_ENABLED`
   - Enable `SSL_ENABLED` for HTTPS
   - Set up proper `CORS_ORIGINS`
   - Configure `RATE_LIMIT_PER_MIN`
   - Enable audit logging
   - Set appropriate `MAX_UPLOAD_SIZE_MB`

3. **Monitoring**
   - Monitor `errors_total` metric
   - Set up alerts for security events
   - Review audit logs regularly

---

## Integration Examples

### Example 1: FastAPI with All Features

```python
from fastapi import FastAPI, HTTPException, Depends
from config import get_config
from logging_config import setup_logging, get_logger, set_correlation_id
from validation import validate_file_upload, ValidationError
from metrics import metrics_collector
from error_recovery import retry

# Setup
config = get_config()
setup_logging(level=config.monitoring.log_level)
logger = get_logger(__name__)

app = FastAPI()

@app.post("/upload")
@retry(max_attempts=3)
async def upload_file(file: UploadFile):
    # Set correlation ID
    import uuid
    correlation_id = str(uuid.uuid4())
    set_correlation_id(correlation_id)
    
    logger.info(f"File upload started: {file.filename}")
    
    try:
        # Validate
        validate_file_upload(
            filename=file.filename,
            file_size=file.size
        )
        
        # Process file
        result = process_file(file)
        
        # Record metrics
        metrics_collector.record_http_request(
            method="POST",
            endpoint="/upload",
            status=200,
            duration=0.5
        )
        
        return {"status": "success", "result": result}
        
    except ValidationError as e:
        logger.error(f"Validation failed: {e}")
        raise HTTPException(status_code=400, detail=str(e))
```

---

## Performance Optimization Tips

1. **Chunking**: Use appropriate chunk sizes (50k-200k rows)
2. **Caching**: Enable Redis caching for frequently accessed data
3. **Parallel Processing**: Scale workers horizontally
4. **Database Indexing**: Ensure proper indexes on frequently queried columns
5. **Connection Pooling**: Configure appropriate pool sizes

---

## Troubleshooting

### Common Issues

1. **Configuration Validation Failed**
   - Check all required environment variables are set
   - Verify MinHash hashes are divisible by LSH bands

2. **High Memory Usage**
   - Reduce `DEFAULT_CHUNKSIZE`
   - Increase number of workers to distribute load

3. **Slow Processing**
   - Check database connection pool size
   - Enable caching
   - Verify disk I/O performance

4. **Health Check Failures**
   - Check database connectivity
   - Verify Redis is running
   - Check disk space and memory

---

## Additional Resources

- [Main README](../README.md)
- [Architecture Documentation](ARCHITECTURE.md)
- [Deployment Guide](DEPLOYMENT.md)
- [API Reference](API.md)
