"""
Production Storage Layer:
- Postgres for metadata, row hashes, audit logs
- Milvus for LSH samples (vector DB)
- Optional Redis for short-lived state

This module replaces the SQLite implementation with scalable, 
production-grade storage backends.
"""

import os
import json
import logging
import hashlib
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from datetime import datetime
import uuid
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import Json, RealDictCursor
import psycopg2.pool

try:
    from pymilvus import connections, Collection, FieldSchema, CollectionSchema, DataType
    HAS_MILVUS = True
except ImportError:
    HAS_MILVUS = False

try:
    import redis
    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION & CONNECTION POOLING
# ============================================================================

@dataclass
class PostgresConfig:
    """Postgres connection configuration."""
    host: str = "localhost"
    port: int = 5432
    database: str = "data_sanitizer"
    user: str = "postgres"
    password: str = "postgres"
    min_connections: int = 2
    max_connections: int = 10

@dataclass
class MilvusConfig:
    """Milvus connection configuration."""
    host: str = "localhost"
    port: int = 19530
    alias: str = "default"

@dataclass
class RedisConfig:
    """Redis connection configuration (optional)."""
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None


class StorageBackend:
    """
    Unified storage backend managing Postgres, Milvus, and optional Redis.
    """
    
    def __init__(self, pg_config: PostgresConfig, milvus_config: Optional[MilvusConfig] = None, redis_config: Optional[RedisConfig] = None):
        self.pg_config = pg_config
        self.milvus_config = milvus_config or MilvusConfig()
        self.redis_config = redis_config
        
        # Connection pools
        self.pg_pool = None
        self.redis_client = None
        self.milvus_alias = milvus_config.alias if milvus_config else "default"
        
        self._init_postgres()
        self._init_milvus()
        self._init_redis()
    
    def _init_postgres(self):
        """Initialize Postgres connection pool and create tables."""
        try:
            self.pg_pool = psycopg2.pool.SimpleConnectionPool(
                self.pg_config.min_connections,
                self.pg_config.max_connections,
                host=self.pg_config.host,
                port=self.pg_config.port,
                database=self.pg_config.database,
                user=self.pg_config.user,
                password=self.pg_config.password
            )
            logger.info(f"Postgres connection pool created: {self.pg_config.host}:{self.pg_config.port}")
            self._init_postgres_schema()
        except Exception as e:
            logger.error(f"Failed to initialize Postgres: {e}")
            raise
    
    def _init_postgres_schema(self):
        """Create required Postgres tables if they don't exist."""
        with self.pg_connection() as conn:
            with conn.cursor() as cur:
                # Jobs table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS jobs (
                        id UUID PRIMARY KEY,
                        tenant_id UUID NOT NULL,
                        dataset_name TEXT,
                        status TEXT DEFAULT 'queued',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        error_message TEXT,
                        metadata JSONB DEFAULT '{}'
                    );
                    CREATE INDEX IF NOT EXISTS idx_jobs_tenant ON jobs(tenant_id);
                    CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
                """)
                
                # Row hashes table (partitioned by date)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS row_hashes (
                        id BIGSERIAL,
                        job_id UUID NOT NULL,
                        hash TEXT NOT NULL,
                        first_seen_row BIGINT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(job_id, hash),
                        FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
                    );
                    CREATE INDEX IF NOT EXISTS idx_row_hashes_job ON row_hashes(job_id);
                    CREATE INDEX IF NOT EXISTS idx_row_hashes_hash ON row_hashes(hash);
                    CREATE INDEX IF NOT EXISTS idx_row_hashes_created ON row_hashes(created_at);
                """)
                
                # Imputation stats
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS imputation_stats (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        job_id UUID NOT NULL,
                        medians JSONB,
                        modes JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(job_id),
                        FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
                    );
                """)
                
                # Cell-level provenance (confidence + transformation ID)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS cell_provenance (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        job_id UUID NOT NULL,
                        row_id BIGINT,
                        column_name TEXT NOT NULL,
                        original_value TEXT,
                        cleaned_value TEXT,
                        transformation_id TEXT,
                        confidence_score FLOAT DEFAULT 1.0,
                        source TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE,
                        UNIQUE(job_id, row_id, column_name)
                    );
                    CREATE INDEX IF NOT EXISTS idx_cell_prov_job ON cell_provenance(job_id);
                    CREATE INDEX IF NOT EXISTS idx_cell_prov_conf ON cell_provenance(confidence_score);
                """)
                
                # Audit logs (immutable)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS audit_logs (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        job_id UUID NOT NULL,
                        user_id UUID,
                        action TEXT,
                        details JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE,
                        INDEX (created_at, job_id)
                    );
                """)
                
                # Tenant quotas & usage
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS tenant_quotas (
                        tenant_id UUID PRIMARY KEY,
                        rows_per_month BIGINT DEFAULT 1000000000,
                        api_calls_per_sec INT DEFAULT 100,
                        max_concurrent_jobs INT DEFAULT 10,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                
                # Tenant usage tracking
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS tenant_usage (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        tenant_id UUID NOT NULL,
                        period_month DATE,
                        rows_processed BIGINT DEFAULT 0,
                        api_calls INT DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(tenant_id, period_month),
                        FOREIGN KEY (tenant_id) REFERENCES tenant_quotas(tenant_id) ON DELETE CASCADE
                    );
                """)
                
                conn.commit()
                logger.info("Postgres schema initialized")
    
    def _init_milvus(self):
        """Initialize Milvus collection for LSH samples."""
        if not HAS_MILVUS:
            logger.warning("Milvus not available; LSH samples will not be stored")
            return
        
        try:
            # Connect to Milvus
            connections.connect(
                alias=self.milvus_alias,
                host=self.milvus_config.host,
                port=self.milvus_config.port
            )
            logger.info(f"Connected to Milvus at {self.milvus_config.host}:{self.milvus_config.port}")
            
            # Create or reuse LSH samples collection
            collection_name = "lsh_samples"
            if collection_name not in connections.list_collections(using=self.milvus_alias):
                self._create_lsh_collection(collection_name)
            else:
                logger.info(f"Using existing Milvus collection: {collection_name}")
        except Exception as e:
            logger.error(f"Failed to initialize Milvus: {e}")
            raise
    
    def _create_lsh_collection(self, collection_name: str):
        """Create LSH samples collection in Milvus."""
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
            FieldSchema(name="job_id", dtype=DataType.VARCHAR, max_length=256),
            FieldSchema(name="bucket_key", dtype=DataType.VARCHAR, max_length=256),
            FieldSchema(name="sampled_row_id", dtype=DataType.INT64),
            FieldSchema(name="snippet", dtype=DataType.VARCHAR, max_length=1024),
            FieldSchema(name="minhash_vector", dtype=DataType.FLOAT_VECTOR, dim=64),
        ]
        schema = CollectionSchema(fields=fields, description="LSH samples for near-duplicate detection")
        collection = Collection(name=collection_name, schema=schema, using=self.milvus_alias)
        collection.create_index(
            field_name="minhash_vector",
            index_params={"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 128}}
        )
        logger.info(f"Created Milvus collection: {collection_name}")
    
    def _init_redis(self):
        """Initialize optional Redis connection."""
        if not self.redis_config or not HAS_REDIS:
            return
        
        try:
            self.redis_client = redis.Redis(
                host=self.redis_config.host,
                port=self.redis_config.port,
                db=self.redis_config.db,
                password=self.redis_config.password,
                decode_responses=True
            )
            self.redis_client.ping()
            logger.info(f"Connected to Redis at {self.redis_config.host}:{self.redis_config.port}")
        except Exception as e:
            logger.warning(f"Could not initialize Redis: {e}. Caching disabled.")
            self.redis_client = None
    
    # ========================================================================
    # POSTGRES OPERATIONS
    # ========================================================================
    
    @contextmanager
    def pg_connection(self):
        """Context manager for Postgres connections from pool."""
        conn = self.pg_pool.getconn()
        try:
            yield conn
        finally:
            self.pg_pool.putconn(conn)
    
    def create_job(self, tenant_id: str, dataset_name: str, metadata: Optional[Dict] = None) -> str:
        """Create a new job record."""
        job_id = str(uuid.uuid4())
        with self.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO jobs (id, tenant_id, dataset_name, status, metadata) VALUES (%s, %s, %s, 'queued', %s)",
                    (job_id, tenant_id, dataset_name, Json(metadata or {}))
                )
                conn.commit()
        logger.info(f"Created job {job_id} for tenant {tenant_id}")
        return job_id
    
    def get_job(self, job_id: str) -> Optional[Dict]:
        """Fetch job details."""
        with self.pg_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
                return cur.fetchone()
    
    def update_job_status(self, job_id: str, status: str, error_message: Optional[str] = None):
        """Update job status."""
        with self.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE jobs SET status = %s, error_message = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                    (status, error_message, job_id)
                )
                conn.commit()
    
    def batch_insert_row_hashes(self, job_id: str, hashes: List[Tuple[str, int]]):
        """Batch insert row hashes (hash, first_seen_row)."""
        if not hashes:
            return
        
        with self.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO row_hashes (job_id, hash, first_seen_row) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                    [(job_id, h, row_id) for h, row_id in hashes]
                )
                conn.commit()
        logger.debug(f"Inserted {len(hashes)} row hashes for job {job_id}")
    
    def check_existing_hashes(self, job_id: str, hashes: List[str]) -> set:
        """Check which hashes already exist for a job."""
        if not hashes:
            return set()
        
        with self.pg_connection() as conn:
            with conn.cursor() as cur:
                # Handle large hash lists by batching
                existing = set()
                BATCH_SIZE = 1000
                for i in range(0, len(hashes), BATCH_SIZE):
                    batch = hashes[i:i + BATCH_SIZE]
                    placeholders = ",".join(["%s"] * len(batch))
                    cur.execute(
                        f"SELECT hash FROM row_hashes WHERE job_id = %s AND hash IN ({placeholders})",
                        [job_id] + batch
                    )
                    existing.update([row[0] for row in cur.fetchall()])
        return existing
    
    def store_imputation_stats(self, job_id: str, medians: Dict[str, float], modes: Dict[str, str]):
        """Store computed medians and modes."""
        with self.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO imputation_stats (job_id, medians, modes) VALUES (%s, %s, %s) ON CONFLICT (job_id) DO UPDATE SET medians = %s, modes = %s",
                    (job_id, Json(medians), Json(modes), Json(medians), Json(modes))
                )
                conn.commit()
    
    def get_imputation_stats(self, job_id: str) -> Optional[Dict]:
        """Retrieve imputation stats."""
        with self.pg_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT medians, modes FROM imputation_stats WHERE job_id = %s", (job_id,))
                row = cur.fetchone()
                return dict(row) if row else None
    
    def batch_insert_provenance(self, job_id: str, provenance_records: List[Dict]):
        """Store cell-level provenance (confidence scores, transformation IDs)."""
        if not provenance_records:
            return
        
        with self.pg_connection() as conn:
            with conn.cursor() as cur:
                for record in provenance_records:
                    cur.execute(
                        """INSERT INTO cell_provenance 
                           (job_id, row_id, column_name, original_value, cleaned_value, transformation_id, confidence_score, source)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                           ON CONFLICT (job_id, row_id, column_name) DO UPDATE SET
                           cleaned_value = EXCLUDED.cleaned_value,
                           confidence_score = EXCLUDED.confidence_score""",
                        (
                            job_id,
                            record.get("row_id"),
                            record.get("column_name"),
                            record.get("original_value"),
                            record.get("cleaned_value"),
                            record.get("transformation_id"),
                            record.get("confidence_score", 1.0),
                            record.get("source", "deterministic")
                        )
                    )
                conn.commit()
    
    def insert_audit_log(self, job_id: str, user_id: Optional[str], action: str, details: Dict):
        """Log an audit event."""
        with self.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO audit_logs (job_id, user_id, action, details) VALUES (%s, %s, %s, %s)",
                    (job_id, user_id, action, Json(details))
                )
                conn.commit()
    
    # ========================================================================
    # MILVUS OPERATIONS (LSH SAMPLES)
    # ========================================================================
    
    def batch_insert_lsh_samples(self, job_id: str, samples: List[Dict]):
        """Insert LSH samples into Milvus."""
        if not HAS_MILVUS or not samples:
            return
        
        try:
            collection = Collection("lsh_samples", using=self.milvus_alias)
            
            # Prepare data for insertion
            data = {
                "job_id": [s["job_id"] for s in samples],
                "bucket_key": [s["bucket_key"] for s in samples],
                "sampled_row_id": [s["sampled_row_id"] for s in samples],
                "snippet": [s["snippet"] for s in samples],
                "minhash_vector": [s["minhash_vector"] for s in samples],
            }
            
            collection.insert(data)
            collection.flush()
            logger.debug(f"Inserted {len(samples)} LSH samples for job {job_id}")
        except Exception as e:
            logger.error(f"Failed to insert LSH samples: {e}")
    
    def query_lsh_candidates(self, minhash_vector: List[float], job_id: str, top_k: int = 10) -> List[Dict]:
        """Query Milvus for LSH candidates (similar snippets)."""
        if not HAS_MILVUS:
            return []
        
        try:
            collection = Collection("lsh_samples", using=self.milvus_alias)
            # Note: This is a simplified example; in practice, you'd filter by job_id as well
            results = collection.search(
                data=[minhash_vector],
                anns_field="minhash_vector",
                param={"metric_type": "L2", "params": {"nprobe": 10}},
                limit=top_k,
                expr=f"job_id == '{job_id}'"  # Filter by job
            )
            return [{"id": hit.id, "distance": hit.distance} for hit in results[0]]
        except Exception as e:
            logger.error(f"Failed to query LSH candidates: {e}")
            return []
    
    # ========================================================================
    # REDIS OPERATIONS (CACHING & SHORT-LIVED STATE)
    # ========================================================================
    
    def set_job_progress(self, job_id: str, progress: Dict, ttl: int = 3600):
        """Store job progress (pass1, pass2, ETA)."""
        if not self.redis_client:
            return
        
        key = f"job:{job_id}:progress"
        self.redis_client.setex(key, ttl, json.dumps(progress))
    
    def get_job_progress(self, job_id: str) -> Optional[Dict]:
        """Retrieve job progress."""
        if not self.redis_client:
            return None
        
        key = f"job:{job_id}:progress"
        data = self.redis_client.get(key)
        return json.loads(data) if data else None
    
    def cache_lsm_response(self, query_hash: str, response: Dict, ttl: int = 86400):
        """Cache LLM response with TTL."""
        if not self.redis_client:
            return
        
        key = f"llm_cache:{query_hash}"
        self.redis_client.setex(key, ttl, json.dumps(response))
    
    def get_lsm_cache(self, query_hash: str) -> Optional[Dict]:
        """Retrieve cached LLM response."""
        if not self.redis_client:
            return None
        
        key = f"llm_cache:{query_hash}"
        data = self.redis_client.get(key)
        return json.loads(data) if data else None
    
    def increment_rate_limit(self, tenant_id: str, ttl: int = 60) -> int:
        """Increment API call counter for tenant."""
        if not self.redis_client:
            return 0
        
        key = f"tenant:{tenant_id}:api_calls"
        count = self.redis_client.incr(key)
        if count == 1:
            self.redis_client.expire(key, ttl)
        return count
    
    # ========================================================================
    # CLEANUP & SHUTDOWN
    # ========================================================================
    
    def cleanup_job(self, job_id: str):
        """Clean up job data from Postgres and Milvus."""
        # Delete from Postgres (cascades to related tables)
        with self.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM jobs WHERE id = %s", (job_id,))
                conn.commit()
        
        # Delete from Milvus (filter by job_id)
        if HAS_MILVUS:
            try:
                collection = Collection("lsh_samples", using=self.milvus_alias)
                collection.delete(f"job_id == '{job_id}'")
            except Exception as e:
                logger.warning(f"Could not clean Milvus data for {job_id}: {e}")
        
        logger.info(f"Cleaned up job {job_id}")
    
    def close(self):
        """Close all connections."""
        if self.pg_pool:
            self.pg_pool.closeall()
        if self.redis_client:
            self.redis_client.close()
        if HAS_MILVUS:
            connections.disconnect(alias=self.milvus_alias)
        logger.info("Storage backend closed")


# ============================================================================
# HELPER FUNCTIONS (BACKWARD COMPATIBLE)
# ============================================================================

def create_storage_backend(
    pg_host: str = "localhost",
    pg_port: int = 5432,
    pg_db: str = "data_sanitizer",
    pg_user: str = "postgres",
    pg_password: str = "postgres",
    milvus_host: str = "localhost",
    milvus_port: int = 19530,
    redis_host: Optional[str] = None,
    redis_port: int = 6379
) -> StorageBackend:
    """Convenience function to create StorageBackend with defaults."""
    pg_config = PostgresConfig(
        host=pg_host,
        port=pg_port,
        database=pg_db,
        user=pg_user,
        password=pg_password
    )
    milvus_config = MilvusConfig(host=milvus_host, port=milvus_port)
    redis_config = None
    if redis_host:
        redis_config = RedisConfig(host=redis_host, port=redis_port)
    
    return StorageBackend(pg_config, milvus_config, redis_config)
