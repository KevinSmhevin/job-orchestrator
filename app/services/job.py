from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy.orm import Session

from app.core.leasing import utcnow
from app.models.job import Job, JobStatus
from app.repositories.job import JobRepository
from app.schemas.job import JobCreate, JobUpdate, JobFilter, JobListResponse, JobResponse


class JobService:
    """
    Orchestrates job CRUD operations.
    Used by API layer.
    """

    def create_job(self, session: Session, job_create: JobCreate) -> Job:
        """Create a new job."""
        now = utcnow()

        # Determine initial status
        if job_create.run_at is None or job_create.run_at <= now:
            status = JobStatus.queued
            run_at = now
        else:
            status = JobStatus.scheduled
            run_at = job_create.run_at

        return JobRepository.create(
            session,
            handler=job_create.handler,
            queue=job_create.queue,
            payload=job_create.payload,
            run_at=run_at,
            priority=job_create.priority,
            max_attempts=job_create.max_attempts,
            timeout_secs=job_create.timeout_secs,
            status=status,
        )

    def get_job(self, session: Session, job_id: UUID) -> Job | None:
        """Get a job by ID."""
        return JobRepository.get_by_id(session, job_id)

    def update_job(
        self,
        session: Session,
        job_id: UUID,
        job_update: JobUpdate,
    ) -> Job | None:
        """Update a job. Returns None if not found."""
        job = JobRepository.get_by_id(session, job_id)
        if job is None:
            return None

        update_data = job_update.model_dump(exclude_unset=True)
        if update_data:
            JobRepository.update_fields(session, job, **update_data)

        return job

    def delete_job(self, session: Session, job_id: UUID) -> bool:
        """Delete a job. Returns True if deleted, False if not found."""
        job = JobRepository.get_by_id(session, job_id)
        if job is None:
            return False

        JobRepository.delete(session, job)
        return True

    def list_jobs(
        self,
        session: Session,
        job_filter: JobFilter,
    ) -> tuple[list[Job], int]:
        """List jobs with filtering."""
        return JobRepository.list_jobs(session, job_filter)

    def cancel_job(self, session: Session, job_id: UUID) -> Job | None:
        """
        Cancel a job if it's cancellable.
        Returns the job if cancelled, None if not found or not cancellable.
        """
        job = JobRepository.get_by_id(session, job_id)
        if job is None:
            return None

        # Can only cancel jobs that aren't already terminal
        if job.status in (JobStatus.succeeded, JobStatus.failed, JobStatus.dead, JobStatus.cancelled):
            return None

        JobRepository.update_fields(
            session,
            job,
            status=JobStatus.cancelled,
            lease_owner=None,
            lease_expires_at=None,
        )
        return job

    def retry_job(self, session: Session, job_id: UUID) -> Job | None:
        """
        Retry a failed/dead job.
        Returns the job if retried, None if not found or not retriable.
        """
        job = JobRepository.get_by_id(session, job_id)
        if job is None:
            return None

        # Can only retry failed or dead jobs
        if job.status not in (JobStatus.failed, JobStatus.dead):
            return None

        now = utcnow()
        JobRepository.update_fields(
            session,
            job,
            status=JobStatus.queued,
            run_at=now,
            attempts=0,
            last_error=None,
            lease_owner=None,
            lease_expires_at=None,
            heartbeat_at=None,
        )
        return job