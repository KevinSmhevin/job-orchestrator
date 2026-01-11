from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.models.job import Job, JobStatus

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

# ─────────────────────────────────────────────────────────────────
# Lease Calculations (pure functions)
# ─────────────────────────────────────────────────────────────────

def compute_lease_expiry(now: datetime, lease_seconds: int) -> datetime:
    """Calculate when a lease should expire."""
    return now + timedelta(seconds=lease_seconds)

def calculate_retry_delay(attempts: int) -> timedelta:
    """
    Exponential backoff: min(5 * 2^attempts, 3600) seconds.
    """
    base_seconds = 5
    max_delay_seconds = 3600
    delay_seconds = min(base_seconds * (2 ** attempts), max_delay_seconds)
    return timedelta(seconds=delay_seconds)

def compute_next_run_at(now: datetime, attempts: int) -> datetime:
    """Calculate next run after a failure."""
    return now + calculate_retry_delay(attempts)


# ─────────────────────────────────────────────────────────────────
# Validation (pure functions)
# ─────────────────────────────────────────────────────────────────

def can_claim_job(job: Job | None, now: datetime) -> bool:
    """Check if a job can be claimed by the current worker."""
    if job is None:
        return False
    if job.status not in (JobStatus.queued, JobStatus.failed):
        return False
    if job.run_at > now:
        return False
    return True

def owns_lease(job: Job | None, worker_id: str) -> bool:
    """Check if a worker owns the lease on a job."""
    if job is None:
        return False
    if job.status != JobStatus.running:
        return False
    if job.lease_owner != worker_id:
        return False
    return True

def is_lease_expired(job: Job, now: datetime) -> bool:
    """Check if a job's lease has expired."""
    if job.lease_expires_at is None:
        return True
    return job.lease_expires_at < now

def has_retries_remaining(job: Job) -> bool:
    """Check if a job has retries remaining."""
    return job.attempts < job.max_attempts


# ─────────────────────────────────────────────────────────────────
# Decision Logic (pure functions)
# ─────────────────────────────────────────────────────────────────

class JobOutcome:
    """Result of a job completion decision."""
    SUCCEEDED = "succeeded"
    RETRY = "retry"
    DEAD = "dead"


def decide_completion_outcome(
    job: Job,
    success: bool,
) -> str:
    """Decide what should happen when a job completes."""
    if success:
        return JobOutcome.SUCCEEDED
    
    # After incrementing attempts, check if retries remain
    if job.attempts + 1 >= job.max_attempts:
        return JobOutcome.DEAD
    
    return JobOutcome.RETRY