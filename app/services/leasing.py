from __future__ import annotations

from datetime import datetime
from typing import Sequence
from uuid import UUID

from sqlalchemy.orm import Session

from app.models.job import Job, JobStatus
from app.repositories.job import JobRepository
from app.core.leasing import (
    utcnow,
    compute_lease_expiry,
    compute_next_run_at,
    owns_lease,
    has_retries_remaining,
    decide_completion_outcome,
    JobOutcome,
)  

class LeasingService:
    """
    Orchestrates leasing operations.
    Connects repository (data) with core (logic)
    """
    
    def __init__(self, default_lease_seconds: int = 60):
        self.default_lease_seconds = default_lease_seconds
        
    def claim_next_job(
        self,
        session: Session,
        worker_id: str,
        queues: Sequence[str],
        lease_seconds: int | None = None,
        ) -> Job | None:
        """
        Claim the next available job for a worker.
        
        
        Must be called within a transaction.
        """
        
        lease_seconds = lease_seconds or self.default_lease_seconds
        now = utcnow()
        lease_expires_at = compute_lease_expiry(now, lease_seconds)
        
         # Query for next runnable job (with row lock)
        job = JobRepository.find_next_runnable(session, queues, now)
        if job is None:
            return None

        # Acquire the lease
        JobRepository.set_running(
            session,
            job,
            lease_owner=worker_id,
            lease_expires_at=lease_expires_at,
            heartbeat_at=now,
        )

        return job
    
    def heartbeat(
        self,
        session: Session,
        job_id: UUID,
        worker_id: str,
        lease_seconds: int | None = None,
    ) -> bool:
        """
        Extend the lease for a running job.
        
        Returns True if successful, False if we don't own the lease.
        """
        lease_seconds = lease_seconds or self.default_lease_seconds
        now = utcnow()
        
        job = JobRepository.get_by_id(session, job_id)
        
        # Validate ownership
        if not owns_lease(job, worker_id):
            return False
        
        # Extend lease
        lease_expires_at = compute_lease_expiry(now, lease_seconds)
        JobRepository.set_heartbeat(session, job, lease_expires_at, now)

        return True
    
    def complete_job(
        self,
        session: Session,
        job_id: UUID,
        worker_id: str,
        success: bool,
        error: str | None = None,
    ) -> bool:
        """
        Complete a job (success or failure).

        Handles retry logic automatically.
        Returns True if successful, False if we don't own the lease.
        """
        job = JobRepository.get_by_id(session, job_id)

        # Validate ownership
        if not owns_lease(job, worker_id):
            return False

        # Decide outcome
        outcome = decide_completion_outcome(job, success)

        if outcome == JobOutcome.SUCCEEDED:
            JobRepository.set_succeeded(session, job)

        elif outcome == JobOutcome.DEAD:
            JobRepository.increment_attempts(session, job)
            JobRepository.set_dead(session, job, error or "Max attempts exceeded")

        elif outcome == JobOutcome.RETRY:
            JobRepository.increment_attempts(session, job)
            now = utcnow()
            next_run = compute_next_run_at(now, job.attempts)
            JobRepository.set_failed(session, job, error or "Unknown error")
            JobRepository.set_queued_for_retry(session, job, next_run)

        return True
    
    def recover_expired_leases(self, session: Session) -> int:
        """
        Recover jobs whose leases have expired.

        Called by scheduler to handle dead workers.
        Returns number of jobs recovered.
        """
        now = utcnow()
        expired_jobs = JobRepository.find_expired_leases(session, now)

        recovered = 0
        for job in expired_jobs:
            JobRepository.increment_attempts(session, job)

            if has_retries_remaining(job):
                next_run = compute_next_run_at(now, job.attempts)
                JobRepository.set_queued_for_retry(session, job, next_run)
                recovered += 1
            else:
                JobRepository.set_dead(
                    session, job, "Lease expired - worker presumed dead"
                )

        return recovered