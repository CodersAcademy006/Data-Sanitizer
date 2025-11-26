# Upgrade Guide - Industry-Level Features

## What's New

This release adds production-ready, industry-level features to Data Sanitizer:

### 🎯 Key Improvements

1. **Configuration Management** - Centralized, validated configuration system
2. **Enhanced Logging** - Structured logging with correlation IDs and audit trails
3. **Input Validation** - Comprehensive security and data validation
4. **Monitoring & Metrics** - Prometheus-compatible metrics and health checks
5. **Error Recovery** - Retry mechanisms, circuit breakers, and graceful degradation
6. **CI/CD Pipeline** - Automated testing, security scanning, and deployment
7. **Code Quality** - Reduced linting errors from 702 to 23, formatted with Black
8. **Security** - SQL injection detection, XSS prevention, API key validation

---

## Breaking Changes

### None! 

All new features are backwards compatible. Existing code will continue to work without modifications.

---

## Migration Guide

### Step 1: Update Dependencies

```bash
# Install new dependencies
pip install python-dotenv prometheus-client

# Optional but recommended
pip install psutil  # For system metrics
pip install python-json-logger  # For structured JSON logging
pip install bandit safety  # For security scanning
```

### Step 2: Create Environment Configuration

```bash
# Copy example configuration
cp .env.example .env

# Edit .env with your settings
nano .env
```

Key settings to configure:
- Database credentials
- API configuration
- Processing parameters
- Security settings (change JWT_SECRET!)

### Step 3: Update Application Code (Optional)

#### Using Configuration

**Before:**
```python
# Hard-coded values
POSTGRES_HOST = "localhost"
API_PORT = 8000
```

**After:**
```python
from config import get_config

config = get_config()
POSTGRES_HOST = config.database.host
API_PORT = config.api.port
```

#### Using Enhanced Logging

**Before:**
```python
import logging
logger = logging.getLogger(__name__)
logger.info("Processing started")
```

**After:**
```python
from logging_config import setup_logging, get_logger, set_correlation_id

# Setup once at application start
setup_logging(level="INFO", json_logs=False)

# Use in modules
logger = get_logger(__name__)
set_correlation_id("request-123")  # Optional: for request tracking
logger.info("Processing started")
```

#### Adding Validation

**Before:**
```python
def upload_file(filename, file_size):
    # Basic checks
    if file_size > 1000000000:
        raise ValueError("File too large")
```

**After:**
```python
from validation import validate_file_upload, ValidationError

def upload_file(filename, file_size):
    try:
        validate_file_upload(filename, file_size, max_size_mb=1000)
        # Process file
    except ValidationError as e:
        logger.error(f"Validation failed: {e}")
        raise
```

#### Adding Metrics

**Before:**
```python
def process_data():
    start = time.time()
    # Processing...
    duration = time.time() - start
    print(f"Processed in {duration}s")
```

**After:**
```python
from metrics import metrics_collector, track_time, processing_duration_seconds

@track_time(processing_duration_seconds, labels={"stage": "pass1"})
def process_data():
    # Processing...
    metrics_collector.record_rows_processed(10000)
```

#### Adding Error Recovery

**Before:**
```python
def fetch_from_database():
    return db.query("SELECT * FROM data")
```

**After:**
```python
from error_recovery import ErrorRecovery

@ErrorRecovery.retry_database_operation
def fetch_from_database():
    return db.query("SELECT * FROM data")
```

---

## New Files Overview

### Core Modules

- `config.py` - Configuration management system
- `logging_config.py` - Enhanced logging with structured logging
- `validation.py` - Input validation and security checks
- `metrics.py` - Prometheus metrics and health checks
- `error_recovery.py` - Retry mechanisms and circuit breakers

### Configuration

- `.env.example` - Environment configuration template
- `.gitignore` - Ignore build artifacts and sensitive files

### CI/CD

- `.github/workflows/ci-cd.yml` - GitHub Actions pipeline

### Documentation

- `docs/INDUSTRY_FEATURES.md` - Comprehensive feature documentation
- `docs/UPGRADE_GUIDE.md` - This file

---

## Testing the Upgrade

### 1. Verify Configuration

```bash
python -c "from config import Config; c = Config(); print('Config OK')"
```

### 2. Test Logging

```bash
python -c "from logging_config import setup_logging; setup_logging(); print('Logging OK')"
```

### 3. Test Validation

```bash
python -c "from validation import ValidationError; print('Validation OK')"
```

### 4. Test Metrics

```bash
python -c "from metrics import metrics_collector; print('Metrics OK')"
```

### 5. Run All Tests

```bash
pytest tests.py -v
```

Expected: 23 passed, 3 skipped

---

## Production Deployment Checklist

### Security

- [ ] Change `JWT_SECRET` in `.env` from default value
- [ ] Set `ENVIRONMENT=production`
- [ ] Enable `AUTH_ENABLED=True`
- [ ] Enable `SSL_ENABLED=True` if using HTTPS
- [ ] Configure proper `CORS_ORIGINS` (not `*`)
- [ ] Set appropriate `RATE_LIMIT_PER_MIN`
- [ ] Review and set `MAX_UPLOAD_SIZE_MB`

### Configuration

- [ ] Set production database credentials
- [ ] Configure cloud storage (S3/GCS/Azure)
- [ ] Set Redis password if used
- [ ] Configure monitoring endpoints
- [ ] Set appropriate log levels (`LOG_LEVEL=INFO` or `WARNING`)

### Monitoring

- [ ] Enable metrics endpoint (`/metrics`)
- [ ] Set up Prometheus scraping
- [ ] Configure health checks (`/health`)
- [ ] Set up alerts for critical metrics
- [ ] Enable structured JSON logging (`json_logs=True`)

### CI/CD

- [ ] Add Docker Hub credentials to GitHub secrets
- [ ] Configure deployment targets
- [ ] Set up staging environment
- [ ] Configure production deployment approval

---

## Performance Tuning

### Recommended Settings by Scale

#### Small (< 100k rows/hour)
```bash
DEFAULT_CHUNKSIZE=50000
API_WORKERS=2
DB_POOL_SIZE=5
```

#### Medium (100k - 1M rows/hour)
```bash
DEFAULT_CHUNKSIZE=100000
API_WORKERS=4
DB_POOL_SIZE=10
```

#### Large (> 1M rows/hour)
```bash
DEFAULT_CHUNKSIZE=200000
API_WORKERS=8
DB_POOL_SIZE=20
```

### Memory Optimization

If experiencing high memory usage:
1. Reduce `DEFAULT_CHUNKSIZE`
2. Reduce `NUMERIC_SAMPLE_SIZE` and `CATEGORICAL_SAMPLE_SIZE`
3. Enable Redis caching to reduce database load
4. Scale workers horizontally instead of increasing chunk size

---

## Rollback Procedure

If you need to rollback:

### 1. Revert Code
```bash
git checkout <previous-commit>
```

### 2. Remove New Dependencies (Optional)
```bash
pip uninstall python-dotenv prometheus-client
```

### 3. Application Will Continue to Work
All new features are optional. The core functionality remains unchanged.

---

## Getting Help

### Documentation
- [Industry Features Guide](docs/INDUSTRY_FEATURES.md) - Detailed feature documentation
- [README](README.md) - Main documentation
- [Architecture](docs/ARCHITECTURE.md) - System architecture

### Common Issues

#### "ModuleNotFoundError: No module named 'dotenv'"
```bash
pip install python-dotenv
```

#### "Configuration validation failed"
Check your `.env` file for:
- Required variables are set
- `MINHASH_NUM_HASHES` is divisible by `LSH_BANDS`
- Port numbers are valid (1-65535)

#### Tests failing
```bash
# Reinstall dependencies
pip install -r requirements.txt

# Run tests
pytest tests.py -v
```

---

## What's Next?

### Recommended Enhancements

1. **Enable Metrics**
   - Set up Prometheus
   - Configure Grafana dashboards
   - Set up alerts

2. **Improve Logging**
   - Enable JSON logging in production
   - Set up log aggregation (ELK, Splunk, CloudWatch)
   - Configure log retention policies

3. **Security Hardening**
   - Enable API authentication
   - Rotate secrets regularly
   - Set up WAF (Web Application Firewall)
   - Enable rate limiting

4. **CI/CD**
   - Add integration tests
   - Set up automated deployments
   - Configure rollback procedures

5. **Monitoring**
   - Set up uptime monitoring
   - Configure error alerting
   - Track key business metrics

---

## Support

For questions or issues:
- Open an issue on GitHub
- Check documentation in `docs/`
- Review example configurations in `.env.example`

---

## Version History

### v1.1.0 (Current)
- ✅ Configuration management
- ✅ Enhanced logging
- ✅ Input validation
- ✅ Monitoring & metrics
- ✅ Error recovery
- ✅ CI/CD pipeline
- ✅ Code quality improvements
- ✅ Security enhancements

### v1.0.0 (Previous)
- Core data cleaning functionality
- MinHash/LSH deduplication
- Basic API server
- Docker support
