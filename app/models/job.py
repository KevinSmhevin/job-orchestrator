import enum as py_enum
import uuid
from datetime import datetime, timezone


from sqlalchemy import (
    DateTime,
    String,
    Text,
    Index,
    Integer,
    Enum as SqlEnum,
)

from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

class JobStatus(str, py_enum.Enum):
    scheduled = "scheduled"
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"
    dead = "dead"
    
class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        unique=True,
        index=True,
        nullable=False,
    )
    queue: Mapped[str] = mapped_column(String(64), default="default", index=True)
    
    handler: Mapped[str] = mapped_column(String(128), index=True)
    
    status: Mapped[JobStatus] = mapped_column(
        SqlEnum(JobStatus, name="job_status"), index=True)
    
    
    run_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, index=True)
    
    priority: Mapped[int] = mapped_column(Integer, default=0, index=True)
    
    payload: Mapped[dict] = mapped_column(JSONB, default=dict)
    
    #retry controls
    max_attempts: Mapped[int] = mapped_column(Integer, default=5)
    
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    
    timeout_secs: Mapped[int] = mapped_column(Integer, default=300)
    
    #Leasing fields (prevent multiple workers from picking up the same job)
    
    lease_owner: Mapped[str | None] = mapped_column(
        String(128), nullable=True, index=True)
    
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True)
    
    heartbeat_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True)
    
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow)
    
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow)
    
    __table_args__ = (
        Index(
            "ix_jobs_runnable",
            "status",
            "queue",
            "run_at",
            "priority",
        ),
    )