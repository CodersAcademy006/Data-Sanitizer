"""
Monitoring and metrics for Data Sanitizer.

Provides:
- Prometheus metrics
- Performance tracking
- Health checks
- System metrics
"""

import logging
import os
import time
from functools import wraps
from typing import Callable, Dict, Optional

try:
    from prometheus_client import Counter, Gauge, Histogram, Info, generate_latest

    HAS_PROMETHEUS = True
except ImportError:
    HAS_PROMETHEUS = False
    # Mock classes for when prometheus is not available
    class Counter:
        def __init__(self, *args, **kwargs):
            pass

        def inc(self, *args, **kwargs):
            pass

        def labels(self, *args, **kwargs):
            return self

    class Gauge:
        def __init__(self, *args, **kwargs):
            pass

        def set(self, *args, **kwargs):
            pass

        def inc(self, *args, **kwargs):
            pass

        def dec(self, *args, **kwargs):
            pass

        def labels(self, *args, **kwargs):
            return self

    class Histogram:
        def __init__(self, *args, **kwargs):
            pass

        def observe(self, *args, **kwargs):
            pass

        def labels(self, *args, **kwargs):
            return self

        def time(self):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    class Info:
        def __init__(self, *args, **kwargs):
            pass

        def info(self, *args, **kwargs):
            pass


logger = logging.getLogger(__name__)


# =============================================================================
# METRICS DEFINITIONS
# =============================================================================

# System info
system_info = Info("data_sanitizer_system", "Data Sanitizer system information")

# Request metrics
http_requests_total = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"],
)

http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint"],
)

# Processing metrics
datasets_processed_total = Counter(
    "datasets_processed_total",
    "Total number of datasets processed",
    ["status"],
)

rows_processed_total = Counter(
    "rows_processed_total",
    "Total number of rows processed",
    ["operation"],
)

processing_duration_seconds = Histogram(
    "processing_duration_seconds",
    "Data processing duration in seconds",
    ["stage"],
)

# Quality metrics
duplicates_detected_total = Counter(
    "duplicates_detected_total",
    "Total number of duplicates detected",
    ["method"],  # exact, lsh
)

missing_values_imputed_total = Counter(
    "missing_values_imputed_total",
    "Total number of missing values imputed",
)

# Storage metrics
storage_operations_total = Counter(
    "storage_operations_total",
    "Total storage operations",
    ["backend", "operation", "status"],
)

storage_operation_duration_seconds = Histogram(
    "storage_operation_duration_seconds",
    "Storage operation duration in seconds",
    ["backend", "operation"],
)

# Job metrics
active_jobs = Gauge(
    "active_jobs",
    "Number of currently active jobs",
    ["status"],
)

job_queue_size = Gauge(
    "job_queue_size",
    "Number of jobs in queue",
)

# Worker metrics
worker_active = Gauge(
    "worker_active",
    "Number of active workers",
    ["worker_type"],
)

# Cache metrics
cache_hits_total = Counter(
    "cache_hits_total",
    "Total cache hits",
)

cache_misses_total = Counter(
    "cache_misses_total",
    "Total cache misses",
)

# Error metrics
errors_total = Counter(
    "errors_total",
    "Total errors",
    ["component", "error_type"],
)


# =============================================================================
# DECORATORS
# =============================================================================


def track_time(metric: Histogram, labels: Optional[Dict] = None):
    """Decorator to track execution time."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            if labels:
                timer = metric.labels(**labels)
            else:
                timer = metric

            with timer.time():
                return func(*args, **kwargs)

        return wrapper

    return decorator


def track_errors(component: str):
    """Decorator to track errors."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_type = type(e).__name__
                errors_total.labels(component=component, error_type=error_type).inc()
                raise

        return wrapper

    return decorator


def track_dataset_processing(func: Callable) -> Callable:
    """Decorator to track dataset processing."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            datasets_processed_total.labels(status="success").inc()
            return result
        except Exception as e:
            datasets_processed_total.labels(status="failed").inc()
            raise

    return wrapper


# =============================================================================
# MONITORING CLASSES
# =============================================================================


class MetricsCollector:
    """Centralized metrics collection."""

    def __init__(self):
        self.start_time = time.time()

    def record_http_request(self, method: str, endpoint: str, status: int, duration: float):
        """Record HTTP request metrics."""
        http_requests_total.labels(method=method, endpoint=endpoint, status=str(status)).inc()
        http_request_duration_seconds.labels(method=method, endpoint=endpoint).observe(duration)

    def record_rows_processed(self, count: int, operation: str = "cleaning"):
        """Record number of rows processed."""
        rows_processed_total.labels(operation=operation).inc(count)

    def record_duplicates(self, count: int, method: str = "exact"):
        """Record duplicates detected."""
        duplicates_detected_total.labels(method=method).inc(count)

    def record_imputation(self, count: int):
        """Record missing values imputed."""
        missing_values_imputed_total.inc(count)

    def record_storage_operation(self, backend: str, operation: str, duration: float, success: bool = True):
        """Record storage operation."""
        status = "success" if success else "failed"
        storage_operations_total.labels(backend=backend, operation=operation, status=status).inc()
        storage_operation_duration_seconds.labels(backend=backend, operation=operation).observe(duration)

    def record_cache_hit(self):
        """Record cache hit."""
        cache_hits_total.inc()

    def record_cache_miss(self):
        """Record cache miss."""
        cache_misses_total.inc()

    def set_active_jobs(self, count: int, status: str = "running"):
        """Set number of active jobs."""
        active_jobs.labels(status=status).set(count)

    def set_queue_size(self, size: int):
        """Set job queue size."""
        job_queue_size.set(size)

    def set_active_workers(self, count: int, worker_type: str):
        """Set number of active workers."""
        worker_active.labels(worker_type=worker_type).set(count)


class HealthChecker:
    """Health check functionality."""

    def __init__(self):
        self.checks = {}

    def register_check(self, name: str, check_func: Callable):
        """Register a health check function."""
        self.checks[name] = check_func

    def check_all(self) -> Dict[str, Dict]:
        """Run all health checks."""
        results = {}
        overall_healthy = True

        for name, check_func in self.checks.items():
            try:
                result = check_func()
                healthy = result.get("healthy", True)
                results[name] = {
                    "healthy": healthy,
                    "details": result.get("details", {}),
                }
                if not healthy:
                    overall_healthy = False
            except Exception as e:
                results[name] = {
                    "healthy": False,
                    "error": str(e),
                }
                overall_healthy = False
                logger.error(f"Health check failed for {name}: {e}")

        return {
            "healthy": overall_healthy,
            "checks": results,
        }

    def check_database(self) -> Dict:
        """Check database connectivity."""
        try:
            # This would check actual database connection
            # For now, return a placeholder
            return {"healthy": True, "details": {"latency_ms": 10}}
        except Exception as e:
            return {"healthy": False, "error": str(e)}

    def check_redis(self) -> Dict:
        """Check Redis connectivity."""
        try:
            # This would check actual Redis connection
            return {"healthy": True, "details": {"latency_ms": 5}}
        except Exception as e:
            return {"healthy": False, "error": str(e)}

    def check_storage(self) -> Dict:
        """Check storage backend."""
        try:
            # This would check actual storage
            return {"healthy": True, "details": {"available": True}}
        except Exception as e:
            return {"healthy": False, "error": str(e)}

    def check_disk_space(self) -> Dict:
        """Check disk space."""
        try:
            import shutil

            total, used, free = shutil.disk_usage("/")
            free_percent = (free / total) * 100

            healthy = free_percent > 10  # Alert if less than 10% free

            return {
                "healthy": healthy,
                "details": {
                    "total_gb": total / (1024**3),
                    "used_gb": used / (1024**3),
                    "free_gb": free / (1024**3),
                    "free_percent": free_percent,
                },
            }
        except Exception as e:
            return {"healthy": False, "error": str(e)}

    def check_memory(self) -> Dict:
        """Check memory usage."""
        try:
            import psutil

            memory = psutil.virtual_memory()
            healthy = memory.percent < 90  # Alert if more than 90% used

            return {
                "healthy": healthy,
                "details": {
                    "total_gb": memory.total / (1024**3),
                    "available_gb": memory.available / (1024**3),
                    "percent_used": memory.percent,
                },
            }
        except ImportError:
            # psutil not available
            return {"healthy": True, "details": {"message": "psutil not available"}}
        except Exception as e:
            return {"healthy": False, "error": str(e)}


# =============================================================================
# GLOBAL INSTANCES
# =============================================================================

metrics_collector = MetricsCollector()
health_checker = HealthChecker()

# Register default health checks
health_checker.register_check("disk", health_checker.check_disk_space)
health_checker.register_check("memory", health_checker.check_memory)


def get_metrics() -> bytes:
    """Get Prometheus metrics in text format."""
    if HAS_PROMETHEUS:
        return generate_latest()
    return b"# Prometheus client not installed\n"


def init_metrics():
    """Initialize metrics with system information."""
    system_info.info(
        {
            "version": "1.0.0",
            "python_version": os.sys.version.split()[0],
            "environment": os.getenv("ENVIRONMENT", "development"),
        }
    )
