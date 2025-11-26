"""
Input validation utilities for Data Sanitizer.

Provides comprehensive validation for:
- File uploads
- API requests
- Data types
- Security checks
"""

import mimetypes
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union

import pandas as pd


class ValidationError(Exception):
    """Custom validation error."""

    pass


class FileValidator:
    """Validator for uploaded files."""

    ALLOWED_EXTENSIONS: Set[str] = {".csv", ".json", ".jsonl", ".parquet", ".xlsx", ".xls"}
    ALLOWED_MIME_TYPES: Set[str] = {
        "text/csv",
        "application/json",
        "application/x-ndjson",
        "application/vnd.apache.parquet",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel",
    }

    MAX_FILE_SIZE_MB = 5000  # 5GB default

    @classmethod
    def validate_file_extension(cls, filename: str) -> bool:
        """Validate file has allowed extension."""
        ext = Path(filename).suffix.lower()
        if ext not in cls.ALLOWED_EXTENSIONS:
            raise ValidationError(
                f"File extension '{ext}' not allowed. Allowed: {', '.join(cls.ALLOWED_EXTENSIONS)}"
            )
        return True

    @classmethod
    def validate_file_size(cls, file_size: int, max_size_mb: Optional[int] = None) -> bool:
        """Validate file size is within limits."""
        max_size = (max_size_mb or cls.MAX_FILE_SIZE_MB) * 1024 * 1024
        if file_size > max_size:
            raise ValidationError(f"File size {file_size / 1024 / 1024:.2f}MB exceeds maximum {max_size_mb}MB")
        return True

    @classmethod
    def validate_mime_type(cls, filename: str) -> bool:
        """Validate file MIME type."""
        mime_type, _ = mimetypes.guess_type(filename)
        if mime_type and mime_type not in cls.ALLOWED_MIME_TYPES:
            raise ValidationError(f"MIME type '{mime_type}' not allowed")
        return True

    @classmethod
    def validate_file_path(cls, file_path: Union[str, Path]) -> bool:
        """Validate file path exists and is readable."""
        path = Path(file_path)

        if not path.exists():
            raise ValidationError(f"File not found: {file_path}")

        if not path.is_file():
            raise ValidationError(f"Not a file: {file_path}")

        if not os.access(path, os.R_OK):
            raise ValidationError(f"File not readable: {file_path}")

        return True

    @classmethod
    def validate_csv_structure(cls, file_path: Union[str, Path], max_rows_to_check: int = 100) -> bool:
        """Validate CSV file structure."""
        try:
            df = pd.read_csv(file_path, nrows=max_rows_to_check)

            if df.empty:
                raise ValidationError("CSV file is empty")

            if len(df.columns) == 0:
                raise ValidationError("CSV file has no columns")

            # Check for duplicate column names
            if len(df.columns) != len(set(df.columns)):
                raise ValidationError("CSV file has duplicate column names")

            return True

        except pd.errors.EmptyDataError:
            raise ValidationError("CSV file is empty or malformed")
        except pd.errors.ParserError as e:
            raise ValidationError(f"CSV parsing error: {e}")


class DataValidator:
    """Validator for data content."""

    @staticmethod
    def validate_column_names(columns: List[str]) -> bool:
        """Validate column names are safe."""
        # Check for empty names
        if any(not col or not col.strip() for col in columns):
            raise ValidationError("Column names cannot be empty")

        # Check for SQL injection patterns
        sql_patterns = [r";\s*drop\s+table", r";\s*delete\s+from", r"union\s+select", r"<script", r"javascript:"]

        for col in columns:
            for pattern in sql_patterns:
                if re.search(pattern, col, re.IGNORECASE):
                    raise ValidationError(f"Potentially unsafe column name: {col}")

        return True

    @staticmethod
    def validate_row_count(row_count: int, max_rows: Optional[int] = None) -> bool:
        """Validate row count is within limits."""
        if row_count < 0:
            raise ValidationError("Row count cannot be negative")

        if max_rows and row_count > max_rows:
            raise ValidationError(f"Row count {row_count} exceeds maximum {max_rows}")

        return True

    @staticmethod
    def validate_data_types(df: pd.DataFrame) -> bool:
        """Validate DataFrame has valid data types."""
        # Check for unsupported types
        unsupported_types = []
        for col in df.columns:
            dtype = df[col].dtype
            if dtype == object:
                # Object type is fine (strings, mixed types)
                continue
            elif dtype.name.startswith("datetime"):
                # Datetime types are supported
                continue
            elif pd.api.types.is_numeric_dtype(dtype):
                # Numeric types are supported
                continue
            elif pd.api.types.is_bool_dtype(dtype):
                # Boolean types are supported
                continue
            else:
                unsupported_types.append((col, dtype))

        if unsupported_types:
            raise ValidationError(f"Unsupported data types: {unsupported_types}")

        return True


class APIValidator:
    """Validator for API requests."""

    @staticmethod
    def validate_dataset_name(name: str) -> bool:
        """Validate dataset name."""
        if not name or not name.strip():
            raise ValidationError("Dataset name cannot be empty")

        if len(name) > 255:
            raise ValidationError("Dataset name too long (max 255 characters)")

        # Only allow alphanumeric, underscore, hyphen
        if not re.match(r"^[a-zA-Z0-9_-]+$", name):
            raise ValidationError("Dataset name can only contain letters, numbers, underscore, and hyphen")

        return True

    @staticmethod
    def validate_tenant_id(tenant_id: str) -> bool:
        """Validate tenant ID."""
        if not tenant_id or not tenant_id.strip():
            raise ValidationError("Tenant ID cannot be empty")

        if len(tenant_id) > 100:
            raise ValidationError("Tenant ID too long (max 100 characters)")

        # Only allow alphanumeric and hyphen
        if not re.match(r"^[a-zA-Z0-9-]+$", tenant_id):
            raise ValidationError("Tenant ID can only contain letters, numbers, and hyphen")

        return True

    @staticmethod
    def validate_pii_strategy(strategy: str) -> bool:
        """Validate PII handling strategy."""
        allowed_strategies = {"hash", "redact", "exclude", "tokenize", "mask"}

        if strategy not in allowed_strategies:
            raise ValidationError(f"Invalid PII strategy. Allowed: {', '.join(allowed_strategies)}")

        return True

    @staticmethod
    def validate_pagination(page: int, per_page: int, max_per_page: int = 1000) -> bool:
        """Validate pagination parameters."""
        if page < 1:
            raise ValidationError("Page number must be >= 1")

        if per_page < 1:
            raise ValidationError("Per page must be >= 1")

        if per_page > max_per_page:
            raise ValidationError(f"Per page cannot exceed {max_per_page}")

        return True


class SecurityValidator:
    """Security-focused validators."""

    @staticmethod
    def validate_api_key(api_key: str, valid_keys: Dict[str, str]) -> Optional[str]:
        """
        Validate API key and return tenant ID.

        Args:
            api_key: API key in format "tenant_id:key"
            valid_keys: Dictionary of {tenant_id: key}

        Returns:
            Tenant ID if valid

        Raises:
            ValidationError if invalid
        """
        if not api_key:
            raise ValidationError("API key is required")

        try:
            tenant_id, key = api_key.split(":", 1)
        except ValueError:
            raise ValidationError("Invalid API key format. Expected: tenant_id:key")

        if tenant_id not in valid_keys:
            raise ValidationError("Invalid tenant ID")

        if valid_keys[tenant_id] != key:
            raise ValidationError("Invalid API key")

        return tenant_id

    @staticmethod
    def validate_no_path_traversal(path: str) -> bool:
        """Validate path doesn't contain path traversal attempts."""
        dangerous_patterns = ["..", "~", "/etc", "/proc", "/sys", "\\"]

        for pattern in dangerous_patterns:
            if pattern in path:
                raise ValidationError(f"Potentially unsafe path: {path}")

        return True

    @staticmethod
    def detect_sql_injection(value: str) -> bool:
        """Detect potential SQL injection attempts."""
        sql_patterns = [
            r"(\bor\b|\band\b)\s+\d+\s*=\s*\d+",
            r";\s*drop\s+table",
            r";\s*delete\s+from",
            r"union\s+select",
            r"exec\s*\(",
            r"execute\s+immediate",
        ]

        for pattern in sql_patterns:
            if re.search(pattern, value, re.IGNORECASE):
                raise ValidationError(f"Potential SQL injection detected")

        return True

    @staticmethod
    def detect_xss(value: str) -> bool:
        """Detect potential XSS attempts."""
        xss_patterns = [
            r"<script[^>]*>",
            r"javascript:",
            r"onerror\s*=",
            r"onload\s*=",
            r"<iframe",
            r"eval\s*\(",
        ]

        for pattern in xss_patterns:
            if re.search(pattern, value, re.IGNORECASE):
                raise ValidationError(f"Potential XSS detected")

        return True


def validate_file_upload(
    filename: str,
    file_size: int,
    file_path: Optional[Union[str, Path]] = None,
    max_size_mb: Optional[int] = None,
) -> bool:
    """
    Comprehensive file upload validation.

    Args:
        filename: Name of the uploaded file
        file_size: Size of the file in bytes
        file_path: Optional path to validate file content
        max_size_mb: Optional maximum file size in MB

    Returns:
        True if valid

    Raises:
        ValidationError if validation fails
    """
    FileValidator.validate_file_extension(filename)
    FileValidator.validate_file_size(file_size, max_size_mb)
    FileValidator.validate_mime_type(filename)

    if file_path:
        FileValidator.validate_file_path(file_path)

        # Additional validation for CSV files
        if Path(filename).suffix.lower() == ".csv":
            FileValidator.validate_csv_structure(file_path)

    return True


def validate_api_request(
    tenant_id: str,
    dataset_name: str,
    pii_strategy: str = "hash",
) -> bool:
    """
    Validate API request parameters.

    Args:
        tenant_id: Tenant identifier
        dataset_name: Dataset name
        pii_strategy: PII handling strategy

    Returns:
        True if valid

    Raises:
        ValidationError if validation fails
    """
    APIValidator.validate_tenant_id(tenant_id)
    APIValidator.validate_dataset_name(dataset_name)
    APIValidator.validate_pii_strategy(pii_strategy)

    return True
