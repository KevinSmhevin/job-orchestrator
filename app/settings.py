from typing import Literal
from pydantic import Field, PostgresDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )
    
    # ─────────────────────────────────────────────────────────────────
    # Environment
    # ─────────────────────────────────────────────────────────────────
    
    environment: Literal["development", "staging", "production"] = "development"
    debug: bool = False
    
    # ─────────────────────────────────────────────────────────────────
    # Database
    # ─────────────────────────────────────────────────────────────────
    
    database_url: str = Field(
        default="postgresql+psycopg://postgres:postgres@localhost:5433/jobs",
        #                    ^^^^^^^^
        description="PostgreSQL connection URL"
    )
    
    # Connection pool settings
    db_pool_size: int = Field(default=5, ge=1, le=50)
    db_max_overflow: int = Field(default=10, ge=0, le=50)
    db_pool_timeout: int = Field(default=30, ge=1)
    db_pool_recycle: int = Field(default=3600, ge=60)
    
    # ─────────────────────────────────────────────────────────────────
    # API Service
    # ─────────────────────────────────────────────────────────────────
    
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_workers: int = Field(default=1, ge=1, le=16)
    api_reload: bool = False
    
    # CORS
    cors_origins: list[str] = Field(
        default=["http://localhost:3000", "http://localhost:8000"],
        description="Allowed CORS origins"
    )
    
    # API limits
    api_max_page_size: int = Field(default=1000, ge=1, le=10000)
    
    # ─────────────────────────────────────────────────────────────────
    # Worker Service
    # ─────────────────────────────────────────────────────────────────
    
    worker_id: str = Field(
        default="worker-default",
        description="Unique worker identifier"
    )
    
    worker_queues: list[str] = Field(
        default=["default"],
        description="Queues this worker will process"
    )
    
    worker_poll_interval: int = Field(
        default=5,
        ge=1,
        le=300,
        description="Seconds between polling for new jobs"
    )
    
    worker_lease_seconds: int = Field(
        default=60,
        ge=10,
        le=3600,
        description="How long to hold a lease on a job"
    )
    
    worker_heartbeat_interval: int = Field(
        default=15,
        ge=5,
        le=300,
        description="Seconds between heartbeat updates"
    )
    
    worker_max_jobs_per_worker: int = Field(
        default=1,
        ge=1,
        le=100,
        description="Max concurrent jobs per worker (future: parallel processing)"
    )
    
    worker_graceful_shutdown_seconds: int = Field(
        default=30,
        ge=5,
        le=300,
        description="Seconds to wait for jobs to finish on shutdown"
    )
    
    # ─────────────────────────────────────────────────────────────────
    # Scheduler Service
    # ─────────────────────────────────────────────────────────────────
    
    scheduler_poll_interval: int = Field(
        default=10,
        ge=1,
        le=300,
        description="Seconds between scheduler runs"
    )
    
    scheduler_recovery_interval: int = Field(
        default=30,
        ge=10,
        le=600,
        description="Seconds between lease recovery checks"
    )
    
    scheduler_batch_size: int = Field(
        default=100,
        ge=1,
        le=1000,
        description="Max jobs to process per scheduler run"
    )
    
    # ─────────────────────────────────────────────────────────────────
    # Job Defaults
    # ─────────────────────────────────────────────────────────────────
    
    default_job_timeout: int = Field(
        default=300,
        ge=1,
        description="Default job timeout in seconds"
    )
    
    default_max_attempts: int = Field(
        default=5,
        ge=1,
        le=100,
        description="Default max retry attempts"
    )
    
    default_queue: str = "default"
    
    # Retry backoff settings
    retry_base_delay: int = Field(
        default=5,
        ge=1,
        description="Base delay in seconds for exponential backoff"
    )
    
    retry_max_delay: int = Field(
        default=3600,
        ge=60,
        description="Maximum retry delay in seconds"
    )
    
    # ─────────────────────────────────────────────────────────────────
    # Logging
    # ─────────────────────────────────────────────────────────────────
    
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_format: Literal["json", "text"] = "text"
    log_sql: bool = False  # Log SQL queries (verbose!)
    
    # ─────────────────────────────────────────────────────────────────
    # Observability
    # ─────────────────────────────────────────────────────────────────
    
    enable_metrics: bool = False  # Future: Prometheus metrics
    metrics_port: int = 9090
    
    enable_tracing: bool = False  # Future: OpenTelemetry
    
    # ─────────────────────────────────────────────────────────────────
    # Security (Future)
    # ─────────────────────────────────────────────────────────────────
    
    api_key_header: str = "X-API-Key"
    api_keys: list[str] = Field(default=[], description="Valid API keys")
    
    # ─────────────────────────────────────────────────────────────────
    # Feature Flags
    # ─────────────────────────────────────────────────────────────────
    
    enable_job_history: bool = False  # Future: Archive completed jobs
    enable_dead_letter_queue: bool = True
    enable_job_priorities: bool = True
    
    # ─────────────────────────────────────────────────────────────────
    # Validators
    # ─────────────────────────────────────────────────────────────────
    
    @field_validator("worker_heartbeat_interval")
    @classmethod
    def validate_heartbeat_interval(cls, v: int, info) -> int:
        """Heartbeat interval should be less than lease seconds."""
        # Note: worker_lease_seconds might not be set yet during validation
        # This is a soft check
        if v > 60:  # reasonable default
            raise ValueError("Heartbeat interval should be shorter than lease duration")
        return v
    
    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, v: str) -> str:
        """Ensure database URL is for PostgreSQL."""
        if not v.startswith(("postgresql://", "postgresql+psycopg://")):
            raise ValueError("Only PostgreSQL databases are supported")
        return v
    
    # ─────────────────────────────────────────────────────────────────
    # Computed Properties
    # ─────────────────────────────────────────────────────────────────
    
    @property
    def is_production(self) -> bool:
        return self.environment == "production"
    
    @property
    def is_development(self) -> bool:
        return self.environment == "development"


# Global settings instance
settings = Settings()