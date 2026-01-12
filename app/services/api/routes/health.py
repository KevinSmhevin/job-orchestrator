from fastapi import APIRouter, HTTPException, status
from sqlalchemy import text

from app.services.api.deps import DbSession

router = APIRouter(tags=["health"])

@router.get("/health")
def health_check():
    """
    Check the health of the application.
    """
    return {"status": "ok"}

@router.get("/health/db")
def health_check_db(db: DbSession):
    """
    Check the health of the database.
    """
    try:
        db.execute(text("SELECT 1"))
        return {"status": "ok", "message": "Database is healthy"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database unhealthy: {str(e)}"
        )