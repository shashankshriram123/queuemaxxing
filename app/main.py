"""Application entry point for the Queuemaxxing service."""

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles


PROJECT_ROOT = Path(__file__).resolve().parent.parent
STATIC_DIR = PROJECT_ROOT / "static"

app = FastAPI(title="Queuemaxxing", version="0.1.0")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/health")
async def health() -> dict[str, str]:
    """Report whether the service is available."""

    return {"status": "ok", "service": "queuemaxxing"}


@app.get("/", response_class=FileResponse)
async def index() -> FileResponse:
    """Serve the placeholder QueueLab interface."""

    return FileResponse(STATIC_DIR / "index.html")
