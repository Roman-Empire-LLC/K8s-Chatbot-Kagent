"""FastAPI application for RAG document processing."""

import logging
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from pydantic import BaseModel

from .config import settings
from .embeddings import get_model
from .processor import get_processor

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan - initialize DB and preload embedding model."""
    logger.info("Starting RAG processor...")

    # Initialize database (create embeddings table if needed)
    processor = get_processor()
    processor.initialize_database()

    # Preload the embedding model
    get_model()

    logger.info("RAG processor ready")
    yield
    logger.info("Shutting down RAG processor...")


app = FastAPI(
    title="kagent RAG Processor",
    description="Document processing service for RAG embeddings",
    version="0.1.0",
    lifespan=lifespan,
)


class HealthResponse(BaseModel):
    status: str
    message: str


@app.get("/health", response_model=HealthResponse)
async def health():
    """Health check endpoint."""
    return HealthResponse(status="ok", message="RAG processor is running")


@app.post("/webhook")
async def minio_webhook(request: Request, background_tasks: BackgroundTasks):
    """Handle MinIO webhook events.

    MinIO sends events in this format:
    {
        "EventName": "s3:ObjectCreated:Put",
        "Key": "bucket-name/object-key",
        "Records": [...]
    }
    """
    try:
        payload = await request.json()
    except Exception as e:
        logger.error(f"Failed to parse webhook payload: {e}")
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    logger.debug(f"Received webhook: {payload}")

    # Handle MinIO event format
    event_name = payload.get("EventName", "")
    key = payload.get("Key", "")

    # Also handle standard S3 notification format
    records = payload.get("Records", [])
    if records:
        for record in records:
            event_name = record.get("eventName", event_name)
            s3_info = record.get("s3", {})
            bucket = s3_info.get("bucket", {}).get("name", "")
            obj = s3_info.get("object", {}).get("key", "")
            if bucket and obj:
                key = f"{bucket}/{obj}"

    if not key or "/" not in key:
        logger.warning(f"Invalid key in webhook: {key}")
        return {"status": "ignored", "reason": "invalid key"}

    # Parse bucket and filename from key
    parts = key.split("/", 1)
    bucket = parts[0]
    filename = parts[1] if len(parts) > 1 else ""

    if not filename:
        logger.warning(f"No filename in key: {key}")
        return {"status": "ignored", "reason": "no filename"}

    # Skip metadata file
    if filename == ".metadata.json":
        return {"status": "ignored", "reason": "metadata file"}

    processor = get_processor()

    # Handle different event types
    if "ObjectCreated" in event_name or "Put" in event_name:
        logger.info(f"Processing upload event: {bucket}/{filename}")
        background_tasks.add_task(processor.process_upload, bucket, filename)
        return {"status": "processing", "bucket": bucket, "filename": filename}

    elif "ObjectRemoved" in event_name or "Delete" in event_name:
        logger.info(f"Processing delete event: {bucket}/{filename}")
        background_tasks.add_task(processor.process_delete, bucket, filename)
        return {"status": "processing", "bucket": bucket, "filename": filename}

    else:
        logger.info(f"Ignoring event type: {event_name}")
        return {"status": "ignored", "reason": f"unsupported event: {event_name}"}


def main():
    """Run the FastAPI server."""
    uvicorn.run(
        "kagent.rag_processor.main:app",
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level,
    )


if __name__ == "__main__":
    main()
