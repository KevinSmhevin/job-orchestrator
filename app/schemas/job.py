from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, ConfigDict


class JobCreate(BaseModel):
    """Schema for creating a new job."""
    handler: str = Field(..., description="Handler function name to execute")
    queue: str = Field(default="default", description="Queue name")
    payload: dict[str, Any] = Field(default_factory=dict, description="Job payload data")
    run_at: datetime | None = Field(default=None, description="When to run the job (None = immediate)")
    priority: int = Field(default=0, description="Job priority (higher = more important)")
    max_attempts: int = Field(default=5, ge=1, le=100, description="Maximum retry attempts")
    timeout_secs: int = Field(default=300, ge=1, description="Job timeout in seconds")


class JobUpdate(BaseModel):
    """Schema for updating a job."""
    priority: int | None = Field(default=None, ge=-100, le=100)
    run_at: datetime | None = None
    max_attempts: int | None = Field(default=None, ge=1, le=100)
    timeout_secs: int | None = Field(default=None, ge=1)


class JobResponse(BaseModel):
    """Schema for job responses."""
    model_config = ConfigDict(from_attributes=True)
    
    id: UUID
    queue: str
    handler: str
    status: str
    run_at: datetime
    priority: int
    payload: dict[str, Any]
    max_attempts: int
    attempts: int
    timeout_secs: int
    lease_owner: str | None
    lease_expires_at: datetime | None
    heartbeat_at: datetime | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime


class JobListResponse(BaseModel):
    """Schema for paginated job list."""
    jobs: list[JobResponse]
    total: int
    page: int
    page_size: int


class JobFilter(BaseModel):
    """Schema for filtering jobs."""
    queue: str | None = None
    handler: str | None = None
    status: str | None = None
    limit: int = Field(default=50, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)