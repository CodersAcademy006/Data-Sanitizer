"""
Enhanced logging configuration with structured logging support.

Provides:
- Structured JSON logging for production
- Pretty console logging for development
- Log correlation IDs
- Performance tracking
- Security audit logging
"""

import logging
import sys
import time
from contextvars import ContextVar
from typing import Any, Dict, Optional

try:
    from pythonjsonlogger import jsonlogger

    HAS_JSON_LOGGER = True
except ImportError:
    HAS_JSON_LOGGER = False

# Context variable for request/correlation IDs
correlation_id_var: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)


class CorrelationIdFilter(logging.Filter):
    """Add correlation ID to log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = correlation_id_var.get() or "N/A"
        return True


class PerformanceFilter(logging.Filter):
    """Add performance context to log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "duration_ms"):
            record.duration_ms = 0
        return True


class CustomJsonFormatter(logging.Formatter):
    """Custom JSON formatter with additional fields."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": getattr(record, "correlation_id", "N/A"),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add custom fields
        for key, value in record.__dict__.items():
            if key not in [
                "name",
                "msg",
                "args",
                "created",
                "filename",
                "funcName",
                "levelname",
                "levelno",
                "lineno",
                "module",
                "msecs",
                "message",
                "pathname",
                "process",
                "processName",
                "relativeCreated",
                "thread",
                "threadName",
                "exc_info",
                "exc_text",
                "stack_info",
                "correlation_id",
            ]:
                log_data[key] = value

        import json

        return json.dumps(log_data)


class ColoredFormatter(logging.Formatter):
    """Colored console formatter for better readability."""

    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        # Add color to level name
        levelname = record.levelname
        if levelname in self.COLORS:
            record.levelname = f"{self.COLORS[levelname]}{levelname}{self.RESET}"

        # Format the message
        result = super().format(record)

        # Reset level name
        record.levelname = levelname

        return result


def setup_logging(
    level: str = "INFO",
    json_logs: bool = False,
    log_file: Optional[str] = None,
) -> None:
    """
    Configure application logging.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_logs: Whether to use JSON formatting
        log_file: Optional file path for file logging
    """
    # Remove existing handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Set log level
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    root_logger.setLevel(numeric_level)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)

    # Add filters
    console_handler.addFilter(CorrelationIdFilter())
    console_handler.addFilter(PerformanceFilter())

    # Set formatter
    if json_logs and HAS_JSON_LOGGER:
        formatter = CustomJsonFormatter()
    else:
        # Pretty console format
        if sys.stdout.isatty():
            formatter = ColoredFormatter(
                "%(asctime)s - %(name)s - [%(correlation_id)s] - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        else:
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - [%(correlation_id)s] - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )

    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler (if specified)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(numeric_level)
        file_handler.addFilter(CorrelationIdFilter())
        file_handler.addFilter(PerformanceFilter())

        if json_logs and HAS_JSON_LOGGER:
            file_handler.setFormatter(CustomJsonFormatter())
        else:
            file_handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(name)s - [%(correlation_id)s] - %(levelname)s - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )

        root_logger.addHandler(file_handler)


def set_correlation_id(correlation_id: str) -> None:
    """Set correlation ID for the current context."""
    correlation_id_var.set(correlation_id)


def get_correlation_id() -> Optional[str]:
    """Get current correlation ID."""
    return correlation_id_var.get()


class PerformanceLogger:
    """Context manager for logging performance metrics."""

    def __init__(self, logger: logging.Logger, operation: str, **kwargs):
        self.logger = logger
        self.operation = operation
        self.extra = kwargs
        self.start_time = None

    def __enter__(self):
        self.start_time = time.perf_counter()
        self.logger.info(f"Starting {self.operation}", extra=self.extra)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (time.perf_counter() - self.start_time) * 1000
        extra = {**self.extra, "duration_ms": duration_ms}

        if exc_type:
            self.logger.error(f"Failed {self.operation}: {exc_val}", extra=extra, exc_info=True)
        else:
            self.logger.info(f"Completed {self.operation}", extra=extra)


class AuditLogger:
    """Audit logger for security-sensitive operations."""

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger("audit")

    def log_access(self, user: str, resource: str, action: str, success: bool, **kwargs):
        """Log access attempt."""
        self.logger.info(
            f"Access: user={user}, resource={resource}, action={action}, success={success}",
            extra={
                "event_type": "access",
                "user": user,
                "resource": resource,
                "action": action,
                "success": success,
                **kwargs,
            },
        )

    def log_data_modification(self, user: str, dataset: str, operation: str, row_count: int, **kwargs):
        """Log data modification."""
        self.logger.info(
            f"Data modification: user={user}, dataset={dataset}, operation={operation}, rows={row_count}",
            extra={
                "event_type": "data_modification",
                "user": user,
                "dataset": dataset,
                "operation": operation,
                "row_count": row_count,
                **kwargs,
            },
        )

    def log_security_event(self, event_type: str, severity: str, details: Dict[str, Any], **kwargs):
        """Log security event."""
        log_method = getattr(self.logger, severity.lower(), self.logger.info)
        log_method(
            f"Security event: {event_type}",
            extra={
                "event_type": "security",
                "security_event_type": event_type,
                "severity": severity,
                **details,
                **kwargs,
            },
        )


# Singleton audit logger
audit_logger = AuditLogger()


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with consistent configuration."""
    return logging.getLogger(name)
