# Make sure your DB is running
docker compose up -d

# Start the API with auto-reload
uv run uvicorn app.services.api.main:app --reload --port 8000