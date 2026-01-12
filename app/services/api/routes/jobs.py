from uuid import UUID
from fastapi import APIRouter, status, HTTPException
from app.services.api.deps import DbSession, JobServiceDep
from app.schemas.job import JobCreate, JobResponse
router = APIRouter(tags=["jobs"])


@router.post("/", response_model=JobResponse, status_code=status.HTTP_201_CREATED)
def create_job(
    job_create: JobCreate,
    db: DbSession,
    job_service: JobServiceDep,
    ) -> JobResponse:
    """
    Create a new job.
    """
    job = job_service.create_job(db, job_create)
    return job

@router.get("/{job_id}", response_model=JobResponse)
def get_job(
    job_id: UUID,
    db: DbSession,
    job_service: JobServiceDep,
) -> JobResponse:
    """
    Get a job by ID.
    """
    job = job_service.get_job(db, job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job {job_id} not found")
    return job