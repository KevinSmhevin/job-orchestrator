from uuid import UUID
from fastapi import APIRouter, status, HTTPException
from app.services.api.deps import DbSession, JobServiceDep, PaginationDep
from app.schemas.job import JobCreate, JobResponse, JobListResponse, JobFilter, JobUpdate
router = APIRouter(tags=["jobs"])

#get jobs
@router.get("/", response_model=JobListResponse)
def list_jobs(
    db: DbSession,
    job_service: JobServiceDep,
    pagination: PaginationDep,
    queue: str | None = None,
    handler: str | None = None,
    status: str | None = None,
) -> JobListResponse:
    """
    List jobs.
    """
    
    job_filter = JobFilter(
        queue=queue, 
        handler=handler, 
        offset=pagination.offset,
        limit=pagination.limit,
        status=status)
    
    jobs, total = job_service.list_jobs(db, job_filter)
    page = (pagination.offset // pagination.limit) + 1 if pagination.limit else 1
    
    return {
        "jobs": jobs,
        "total": total,
        "page": page,
        "page_size": pagination.limit,
    }

#get job by id
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

#create job
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

#delete job
@router.delete("/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_job(
    job_id: UUID,
    db: DbSession,
    job_service: JobServiceDep,
) -> None:
    """
    Delete a job by ID.
    """
    deleted = job_service.delete_job(db, job_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job {job_id} not found")
    return None

#update a job by id
@router.patch("/{job_id}", response_model=JobResponse)
def update_job(
    job_id: UUID,
    job_update: JobUpdate,
    db: DbSession,
    job_service: JobServiceDep,
) -> JobResponse:
    """
    Update a job by ID.
    """
    job = job_service.update_job(db, job_id, job_update)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job {job_id} not found")
    return job

#cancel a job by id
@router.post("/{job_id}/cancel", response_model=JobResponse)
def cancel_job(
    job_id: UUID,
    db: DbSession,
    job_service: JobServiceDep,
) -> JobResponse:
    """
    Cancel a job by ID.
    """
    cancelled_job = job_service.cancel_job(db, job_id)
    if not cancelled_job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job {job_id} not found or cannot be cancelled")
    return cancelled_job


#retry a job by id
@router.post("/{job_id}/retry", response_model=JobResponse)
def retry_job(
    job_id: UUID,
    db: DbSession,
    job_service: JobServiceDep,
) -> JobResponse:
    """
    Retry a job by ID.
    """
    retried_job = job_service.retry_job(db, job_id)
    if not retried_job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job {job_id} not found or cannot be retried")
    return retried_job