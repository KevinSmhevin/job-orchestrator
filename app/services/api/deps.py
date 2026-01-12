"""
Dependency injection for FastAPI endpoints.

Provides database sessions, services, and other shared dependencies.
"""
from typing import Generator, Annotated
from uuid import UUID

from fastapi import Depends, HTTPException, status, Header
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.services.job import JobService
from app.services.leasing import LeasingService
from app.settings import settings


# ─────────────────────────────────────────────────────────────────
# Database Session
# ─────────────────────────────────────────────────────────────────

def get_db() -> Generator[Session, None, None]:
    """
    Provides a database session with automatic transaction management.
    
    - Yields a session for the request
    - Commits on success
    - Rolls back on exception
    - Always closes the session
    
    Usage:
        @app.get("/endpoint")
        def my_endpoint(db: Session = Depends(get_db)):
            # Use db here
            pass
    """
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


# Type alias for cleaner annotations
DbSession = Annotated[Session, Depends(get_db)]


# ─────────────────────────────────────────────────────────────────
# Service Dependencies
# ─────────────────────────────────────────────────────────────────

def get_job_service() -> JobService:
    """
    Provides a JobService instance.
    
    Usage:
        @app.post("/jobs")
        def create_job(
            job_service: JobService = Depends(get_job_service),
        ):
            pass
    """
    return JobService()


def get_leasing_service() -> LeasingService:
    """
    Provides a LeasingService instance with default lease settings.
    
    Usage:
        @app.post("/internal/claim")
        def claim_job(
            leasing_service: LeasingService = Depends(get_leasing_service),
        ):
            pass
    """
    return LeasingService(default_lease_seconds=settings.worker_lease_seconds)


# Type aliases for cleaner annotations
JobServiceDep = Annotated[JobService, Depends(get_job_service)]
LeasingServiceDep = Annotated[LeasingService, Depends(get_leasing_service)]


# ─────────────────────────────────────────────────────────────────
# Authentication (Optional - for future use)
# ─────────────────────────────────────────────────────────────────

def verify_api_key(
    x_api_key: str | None = Header(None, alias=settings.api_key_header)
) -> str:
    """
    Verify API key from request header.
    
    Only enforced if api_keys are configured in settings.
    
    Usage:
        @app.post("/jobs")
        def create_job(api_key: str = Depends(verify_api_key)):
            pass
    """
    # If no API keys configured, allow all requests
    if not settings.api_keys:
        return "no-auth-required"
    
    if x_api_key is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    
    if x_api_key not in settings.api_keys:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )
    
    return x_api_key


# Type alias for API key dependency
ApiKeyDep = Annotated[str, Depends(verify_api_key)]


# ─────────────────────────────────────────────────────────────────
# Pagination Helpers (Optional)
# ─────────────────────────────────────────────────────────────────

class PaginationParams:
    """Common pagination parameters."""
    
    def __init__(
        self,
        offset: int = 0,
        limit: int = 50,
    ):
        if offset < 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="offset must be >= 0"
            )
        
        if limit < 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="limit must be >= 1"
            )
        
        if limit > settings.api_max_page_size:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"limit must be <= {settings.api_max_page_size}"
            )
        
        self.offset = offset
        self.limit = limit


def get_pagination_params(
    offset: int = 0,
    limit: int = 50,
) -> PaginationParams:
    """
    Provides validated pagination parameters.
    
    Usage:
        @app.get("/jobs")
        def list_jobs(
            pagination: PaginationParams = Depends(get_pagination_params)
        ):
            # Use pagination.offset and pagination.limit
            pass
    """
    return PaginationParams(offset=offset, limit=limit)


# Type alias for pagination dependency
PaginationDep = Annotated[PaginationParams, Depends(get_pagination_params)]


# ─────────────────────────────────────────────────────────────────
# Common Path Parameter Dependencies
# ─────────────────────────────────────────────────────────────────

def get_job_or_404(
    job_id: UUID,
    db: DbSession,
    job_service: JobServiceDep,
):
    """
    Get a job by ID or raise 404.
    
    Usage:
        from app.models.job import Job
        
        @app.get("/jobs/{job_id}")
        def get_job(job: Job = Depends(get_job_or_404)):
            return job
    """
    job = job_service.get_job(db, job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )
    return job


# Type alias
JobDep = Annotated[object, Depends(get_job_or_404)]  # Will be Job model


# ─────────────────────────────────────────────────────────────────
# Usage Examples
# ─────────────────────────────────────────────────────────────────

"""
Example 1: Simple endpoint with just DB
    @router.get("/health/db")
    def health_check_db(db: DbSession):
        db.execute(text("SELECT 1"))
        return {"status": "ok"}

Example 2: Endpoint with service
    @router.post("/jobs", response_model=JobResponse)
    def create_job(
        job_create: JobCreate,
        db: DbSession,
        job_service: JobServiceDep,
    ):
        job = job_service.create_job(db, job_create)
        return job

Example 3: Endpoint with job lookup
    @router.delete("/{job_id}")
    def delete_job(
        job: JobDep,
        db: DbSession,
        job_service: JobServiceDep,
    ):
        job_service.delete_job(db, job.id)
        return {"deleted": True}

Example 4: Endpoint with pagination
    @router.get("/jobs")
    def list_jobs(
        db: DbSession,
        job_service: JobServiceDep,
        pagination: PaginationDep,
    ):
        jobs, total = job_service.list_jobs(db, ...)
        return {"jobs": jobs, "total": total}

Example 5: Protected endpoint (when API keys configured)
    @router.delete("/{job_id}")
    def delete_job(
        job_id: UUID,
        api_key: ApiKeyDep,  # Requires valid API key
        db: DbSession,
        job_service: JobServiceDep,
    ):
        job_service.delete_job(db, job_id)
        return {"deleted": True}
"""