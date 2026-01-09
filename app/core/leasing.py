from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.job import Job, JobStatus


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def claim_next_job(
    session: Session,
    *,
    worker_id: str,
    queues: Sequence[str],
    lease_seconds: int = 60,
) -> Job | None:
    """
    Claim a single runnable job using SELECT ... FOR UPDATE SKIP LOCKED.

    MUST be called inside an open transaction:
        with SessionLocal.begin() as session:
            job = claim_next_job(...)
    """
    now = utcnow()
    lease_until = now + timedelta(seconds=lease_seconds)

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

    job = session.execute(stmt).scalars().first()
    if job is None:
        return None

    job.status = JobStatus.running
    job.lease_owner = worker_id
    job.lease_expires_at = lease_until
    job.heartbeat_at = now

    # Flush keeps this atomic within the transaction
    session.flush()

    return job


def heartbeat_lease(
    session: Session,
    *,
    job_id,
    worker_id: str,
    lease_seconds: int = 60,
) -> bool:
    """
    Extend the lease for a running job if we still own it.

    Returns True if the lease was extended, False otherwise.
    """
    now = utcnow()
    lease_until = now + timedelta(seconds=lease_seconds)

    job = session.get(Job, job_id)
    if job is None:
        return False

    if job.status != JobStatus.running:
        return False

    if job.lease_owner != worker_id:
        return False

    job.lease_expires_at = lease_until
    job.heartbeat_at = now

    session.flush()
    return True