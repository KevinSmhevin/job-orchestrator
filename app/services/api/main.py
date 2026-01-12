from contextlib import asynccontextmanager


from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.settings import settings
from app.services.api.routes import health, jobs


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print(f"Starting API on {settings.api_host}:{settings.api_port}")
    yield
    # Shutdown
    print("Shutting down API")


# ─────────────────────────────────────────────────────────────────
# Create App
# ─────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Job Orchestrator", 
    version="0.1.0",
    description="Distributed job queue with scheduling",
    docs_url="/docs", # Swagger UI
    redoc_url="/redoc", # ReDoc UI
    lifespan=lifespan,
    )

# ─────────────────────────────────────────────────────────────────
# Add Middleware
# ─────────────────────────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────────
# Register Routers
# ─────────────────────────────────────────────────────────────────

app.include_router(health.router)
app.include_router(jobs.router, prefix="/jobs")
