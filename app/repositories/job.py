from __future__ import annotations

from datetime import datetime
from typing import Sequence
from uuid import UUID

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from app.models.job import Job, JobStatus
from app.schemas.job import JobCreate, JobUpdate, JobFilter


class JobRepository:
    """Repository for managing job operations."""
    
    # ─────────────────────────────────────────────────────────────────
    # Basic CRUD
    # ─────────────────────────────────────────────────────────────────
    
    @staticmethod
    def get_by_id(session: Session, job_id: UUID) -> Job | None:
        return session.get(Job, job_id)
    
    
    @staticmethod
    def create(
        session: Session,
        handler: str,
        queue: str,
        payload: dict,
        run_at: datetime,
        priority: int,
        max_attempts: int,
        timeout_secs: int,
        status: JobStatus,
    ) -> Job:
        job = Job(
            handler=handler,
            queue=queue,
            payload=payload,
            run_at=run_at,
            priority=priority,
            max_attempts=max_attempts,
            timeout_secs=timeout_secs,
            status=status,
        )
        session.add(job)
        session.flush()
        return job
    
    @staticmethod
    def delete(session: Session, job: Job) -> None:
        session.delete(job)
        session.flush()

    @staticmethod
    def update(session: Session, job: Job, updates: dict) -> Job:
        for field, value in updates.items():
            setattr(job, field, value)
        session.flush()
        return job
    
    # ─────────────────────────────────────────────────────────────────
    # Queries
    # ─────────────────────────────────────────────────────────────────
    
    @staticmethod
    def list_jobs(session: Session, job_filter: JobFilter) -> tuple[list[Job], int]:
        stmt = select(Job)
        
        if job_filter.queue is not None:
            stmt = stmt.where(Job.queue == job_filter.queue)
        
        if job_filter.handler is not None:
            stmt = stmt.where(Job.handler == job_filter.handler)
            
        if job_filter.status is not None:
            stmt = stmt.where(Job.status == job_filter.status)

        count_stmt = select(func.count()).select_from(stmt.subquery())
        total = session.execute(count_stmt).scalar() or 0

        stmt = stmt.order_by(Job.created_at.desc())
        stmt = stmt.offset(job_filter.offset).limit(job_filter.limit)

        jobs = list(session.execute(stmt).scalars().all())
        
        return jobs, total
    
    
    @staticmethod
    def find_next_runnable_job(
        session: Session,
        queues: Sequence[str],
        now: datetime,
    ) -> Job | None:
        """SELECT ... FOR UPDATE SKIP LOCKED to find next available job."""
        stmt = (
            select(Job)
            .where(
                Job.queue.in_(list(queues)),
                Job.status.in_([JobStatus.queued, JobStatus.scheduled]),
                Job.run_at <= now,
            )
            .order_by(
                Job.priority.desc(),
                Job.run_at.asc(),
                Job.created_at.asc(),
            )
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        return session.execute(stmt).scalars().first()
    
    @staticmethod
    def find_expired_leases(session: Session, now: datetime) -> list[Job]:
        stmt = (
            select(Job)
            .where(
                Job.status == JobStatus.running,
                Job.lease_expires_at < now,
            )
        )
        return list(session.execute(stmt).scalars().all())
    
    
    # ─────────────────────────────────────────────────────────────────
    # Atomic Updates
    # ─────────────────────────────────────────────────────────────────

    @staticmethod
    def set_running(
        session: Session,
        job: Job,
        lease_owner: str,
        lease_expires_at: datetime,
        heartbeat_at: datetime,
    ) -> None:
        job.status = JobStatus.running
        job.lease_owner = lease_owner
        job.lease_expires_at = lease_expires_at
        job.heartbeat_at = heartbeat_at
        session.flush()

    @staticmethod
    def set_heartbeat(
        session: Session,
        job: Job,
        lease_expires_at: datetime,
        heartbeat_at: datetime,
    ) -> None:
        job.lease_expires_at = lease_expires_at
        job.heartbeat_at = heartbeat_at
        session.flush()

    @staticmethod
    def set_succeeded(session: Session, job: Job) -> None:
        job.status = JobStatus.succeeded
        job.lease_owner = None
        job.lease_expires_at = None
        session.flush()

    @staticmethod
    def set_failed(session: Session, job: Job, error: str) -> None:
        job.status = JobStatus.failed
        job.last_error = error
        job.lease_owner = None
        job.lease_expires_at = None
        session.flush()

    @staticmethod
    def set_dead(session: Session, job: Job, error: str) -> None:
        job.status = JobStatus.dead
        job.last_error = error
        job.lease_owner = None
        job.lease_expires_at = None
        session.flush()

    @staticmethod
    def set_queued_for_retry(
        session: Session,
        job: Job,
        run_at: datetime,
    ) -> None:
        job.status = JobStatus.queued
        job.run_at = run_at
        job.lease_owner = None
        job.lease_expires_at = None
        job.heartbeat_at = None
        session.flush()

    @staticmethod
    def increment_attempts(session: Session, job: Job) -> int:
        job.attempts += 1
        session.flush()
        return job.attempts

    @staticmethod
    def update_fields(session: Session, job: Job, **fields) -> None:
        for key, value in fields.items():
            setattr(job, key, value)
        session.flush()