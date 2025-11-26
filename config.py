"""
Configuration management for Data Sanitizer.

Provides centralized configuration with environment variable support,
validation, and default values for all components.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from dotenv import load_dotenv

# Load environment variables
load_dotenv()


@dataclass
class DatabaseConfig:
    """PostgreSQL database configuration."""

    host: str = field(default_factory=lambda: os.getenv("POSTGRES_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("POSTGRES_PORT", "5432")))
    database: str = field(default_factory=lambda: os.getenv("POSTGRES_DB", "data_sanitizer"))
    user: str = field(default_factory=lambda: os.getenv("POSTGRES_USER", "postgres"))
    password: str = field(default_factory=lambda: os.getenv("POSTGRES_PASSWORD", "postgres"))
    pool_size: int = field(default_factory=lambda: int(os.getenv("DB_POOL_SIZE", "10")))
    max_overflow: int = field(default_factory=lambda: int(os.getenv("DB_MAX_OVERFLOW", "20")))

    @property
    def connection_string(self) -> str:
        """Generate PostgreSQL connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class MilvusConfig:
    """Milvus vector database configuration."""

    host: str = field(default_factory=lambda: os.getenv("MILVUS_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("MILVUS_PORT", "19530")))
    collection_name: str = field(default_factory=lambda: os.getenv("MILVUS_COLLECTION", "lsh_samples"))
    dimension: int = 64  # MinHash signature size
    index_type: str = "IVF_FLAT"
    metric_type: str = "HAMMING"


@dataclass
class RedisConfig:
    """Redis cache configuration."""

    host: str = field(default_factory=lambda: os.getenv("REDIS_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("REDIS_PORT", "6379")))
    db: int = field(default_factory=lambda: int(os.getenv("REDIS_DB", "0")))
    password: Optional[str] = field(default_factory=lambda: os.getenv("REDIS_PASSWORD"))
    ttl_seconds: int = field(default_factory=lambda: int(os.getenv("REDIS_TTL", "3600")))


@dataclass
class StorageConfig:
    """Cloud storage configuration."""

    provider: str = field(default_factory=lambda: os.getenv("STORAGE_PROVIDER", "local"))  # local, s3, gcs, azure
    bucket_name: Optional[str] = field(default_factory=lambda: os.getenv("STORAGE_BUCKET"))
    aws_access_key: Optional[str] = field(default_factory=lambda: os.getenv("AWS_ACCESS_KEY_ID"))
    aws_secret_key: Optional[str] = field(default_factory=lambda: os.getenv("AWS_SECRET_ACCESS_KEY"))
    aws_region: str = field(default_factory=lambda: os.getenv("AWS_REGION", "us-east-1"))
    gcs_project_id: Optional[str] = field(default_factory=lambda: os.getenv("GCS_PROJECT_ID"))
    gcs_credentials_path: Optional[str] = field(default_factory=lambda: os.getenv("GCS_CREDENTIALS_PATH"))


@dataclass
class APIConfig:
    """API server configuration."""

    host: str = field(default_factory=lambda: os.getenv("API_HOST", "0.0.0.0"))
    port: int = field(default_factory=lambda: int(os.getenv("API_PORT", "8000")))
    workers: int = field(default_factory=lambda: int(os.getenv("API_WORKERS", "4")))
    reload: bool = field(default_factory=lambda: os.getenv("API_RELOAD", "False").lower() == "true")
    cors_origins: List[str] = field(
        default_factory=lambda: os.getenv("CORS_ORIGINS", "*").split(",") if os.getenv("CORS_ORIGINS") else ["*"]
    )
    rate_limit_per_minute: int = field(default_factory=lambda: int(os.getenv("RATE_LIMIT_PER_MIN", "100")))
    max_upload_size_mb: int = field(default_factory=lambda: int(os.getenv("MAX_UPLOAD_SIZE_MB", "1000")))
    auth_enabled: bool = field(default_factory=lambda: os.getenv("AUTH_ENABLED", "True").lower() == "true")


@dataclass
class ProcessingConfig:
    """Data processing configuration."""

    # Chunk sizes
    default_chunksize: int = field(default_factory=lambda: int(os.getenv("DEFAULT_CHUNKSIZE", "50000")))
    max_chunksize: int = field(default_factory=lambda: int(os.getenv("MAX_CHUNKSIZE", "200000")))

    # Sampling parameters
    numeric_sample_size: int = field(default_factory=lambda: int(os.getenv("NUMERIC_SAMPLE_SIZE", "1000")))
    categorical_sample_size: int = field(default_factory=lambda: int(os.getenv("CATEGORICAL_SAMPLE_SIZE", "500")))
    lsh_sample_size: int = field(default_factory=lambda: int(os.getenv("LSH_SAMPLE_SIZE", "200")))

    # MinHash/LSH parameters
    minhash_num_hashes: int = field(default_factory=lambda: int(os.getenv("MINHASH_NUM_HASHES", "64")))
    lsh_bands: int = field(default_factory=lambda: int(os.getenv("LSH_BANDS", "16")))
    lsh_shingle_k: int = field(default_factory=lambda: int(os.getenv("LSH_SHINGLE_K", "5")))

    # Quality thresholds
    duplicate_threshold: float = field(default_factory=lambda: float(os.getenv("DUPLICATE_THRESHOLD", "0.85")))
    imputation_confidence_threshold: float = field(
        default_factory=lambda: float(os.getenv("IMPUTATION_CONFIDENCE_THRESHOLD", "0.7"))
    )

    # PII detection
    pii_detection_enabled: bool = field(default_factory=lambda: os.getenv("PII_DETECTION_ENABLED", "True").lower() == "true")
    pii_default_strategy: str = field(default_factory=lambda: os.getenv("PII_DEFAULT_STRATEGY", "hash"))


@dataclass
class MonitoringConfig:
    """Monitoring and observability configuration."""

    metrics_enabled: bool = field(default_factory=lambda: os.getenv("METRICS_ENABLED", "True").lower() == "true")
    metrics_port: int = field(default_factory=lambda: int(os.getenv("METRICS_PORT", "9090")))
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    log_format: str = field(
        default_factory=lambda: os.getenv("LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    tracing_enabled: bool = field(default_factory=lambda: os.getenv("TRACING_ENABLED", "False").lower() == "true")
    sentry_dsn: Optional[str] = field(default_factory=lambda: os.getenv("SENTRY_DSN"))


@dataclass
class SecurityConfig:
    """Security configuration."""

    api_keys: Dict[str, str] = field(default_factory=dict)
    jwt_secret: str = field(default_factory=lambda: os.getenv("JWT_SECRET", "change-me-in-production"))
    jwt_algorithm: str = "HS256"
    jwt_expiry_hours: int = field(default_factory=lambda: int(os.getenv("JWT_EXPIRY_HOURS", "24")))
    encryption_key: Optional[str] = field(default_factory=lambda: os.getenv("ENCRYPTION_KEY"))
    ssl_enabled: bool = field(default_factory=lambda: os.getenv("SSL_ENABLED", "False").lower() == "true")


@dataclass
class Config:
    """Main application configuration."""

    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    milvus: MilvusConfig = field(default_factory=MilvusConfig)
    redis: RedisConfig = field(default_factory=RedisConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    api: APIConfig = field(default_factory=APIConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)
    security: SecurityConfig = field(default_factory=SecurityConfig)

    # Environment
    environment: str = field(default_factory=lambda: os.getenv("ENVIRONMENT", "development"))
    debug: bool = field(default_factory=lambda: os.getenv("DEBUG", "False").lower() == "true")

    def validate(self) -> bool:
        """Validate configuration settings."""
        errors = []

        # Validate database config
        if not self.database.host:
            errors.append("Database host is required")
        if self.database.pool_size < 1:
            errors.append("Database pool size must be >= 1")

        # Validate processing config
        if self.processing.default_chunksize < 1000:
            errors.append("Default chunksize should be >= 1000 for efficiency")
        if self.processing.minhash_num_hashes % self.processing.lsh_bands != 0:
            errors.append("MinHash num_hashes must be divisible by LSH bands")

        # Validate API config
        if self.api.port < 1 or self.api.port > 65535:
            errors.append("API port must be between 1 and 65535")

        # Validate security in production
        if self.environment == "production":
            if self.security.jwt_secret == "change-me-in-production":
                errors.append("JWT secret must be changed in production")
            if not self.api.auth_enabled:
                errors.append("Authentication should be enabled in production")

        if errors:
            raise ValueError(f"Configuration validation failed: {'; '.join(errors)}")

        return True


# Global configuration instance
config = Config()


def load_config(config_path: Optional[str] = None) -> Config:
    """
    Load configuration from environment variables and optional config file.

    Args:
        config_path: Optional path to .env file

    Returns:
        Config object
    """
    if config_path:
        load_dotenv(config_path)

    global config
    config = Config()
    config.validate()
    return config


def get_config() -> Config:
    """Get the current configuration instance."""
    return config
