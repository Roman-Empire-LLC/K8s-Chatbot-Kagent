"""Document processing logic."""

import logging

import httpx
import psycopg2
from minio import Minio

from .config import settings
from .embeddings import chunk_text, generate_embedding
from .extractors import extract_text

logger = logging.getLogger(__name__)


class DocumentProcessor:
    """Processes documents from MinIO and stores embeddings in pgvector."""

    def __init__(self):
        self.minio_client = Minio(
            settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
        )
        self._pg_conn = None

    @property
    def pg_conn(self):
        """Get PostgreSQL connection (lazy initialization)."""
        if self._pg_conn is None or self._pg_conn.closed:
            self._pg_conn = psycopg2.connect(settings.postgres_url)
        return self._pg_conn

    def initialize_database(self) -> None:
        """Create the document_embeddings table if it doesn't exist."""
        logger.info("Initializing document_embeddings table...")
        cursor = self.pg_conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS document_embeddings (
                    id SERIAL PRIMARY KEY,
                    index_name VARCHAR(255) NOT NULL,
                    filename VARCHAR(255) NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    chunk_text TEXT NOT NULL,
                    embedding vector(384),
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(index_name, filename, chunk_index)
                )
            """)
            self.pg_conn.commit()
            logger.info("document_embeddings table ready")
        finally:
            cursor.close()

    def process_upload(self, bucket: str, filename: str) -> None:
        """Process a newly uploaded document.

        Args:
            bucket: MinIO bucket name (RAG index name)
            filename: Name of the uploaded file
        """
        logger.info(f"Processing upload: {bucket}/{filename}")

        try:
            # Update status to processing
            self._update_status(bucket, filename, "processing")

            # Download file from MinIO
            response = self.minio_client.get_object(bucket, filename)
            content = response.read()
            response.close()
            response.release_conn()

            # Extract text
            text = extract_text(content, filename)
            logger.info(f"Extracted {len(text)} characters from {filename}")

            # Delete existing embeddings for this file (in case of re-upload)
            self._delete_embeddings(bucket, filename)

            # Chunk and embed
            chunks = list(chunk_text(text))
            logger.info(f"Split into {len(chunks)} chunks")

            # Store embeddings
            for chunk_index, chunk_text_content in chunks:
                embedding = generate_embedding(chunk_text_content)
                self._store_embedding(bucket, filename, chunk_index, chunk_text_content, embedding)

            # Update status to processed
            self._update_status(bucket, filename, "processed")
            logger.info(f"Successfully processed {bucket}/{filename}")

        except Exception as e:
            logger.error(f"Failed to process {bucket}/{filename}: {e}")
            self._update_status(bucket, filename, "failed", str(e))
            raise

    def process_delete(self, bucket: str, filename: str) -> None:
        """Process a document deletion.

        Args:
            bucket: MinIO bucket name (RAG index name)
            filename: Name of the deleted file
        """
        logger.info(f"Processing delete: {bucket}/{filename}")

        try:
            # Delete embeddings
            self._delete_embeddings(bucket, filename)

            # Delete status (via API)
            self._delete_status(bucket, filename)

            logger.info(f"Successfully cleaned up {bucket}/{filename}")

        except Exception as e:
            logger.error(f"Failed to process delete for {bucket}/{filename}: {e}")
            raise

    def _store_embedding(
        self, index_name: str, filename: str, chunk_index: int, chunk_text: str, embedding: list[float]
    ) -> None:
        """Store embedding in pgvector."""
        cursor = self.pg_conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO document_embeddings (index_name, filename, chunk_index, chunk_text, embedding)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (index_name, filename, chunk_index)
                DO UPDATE SET chunk_text = EXCLUDED.chunk_text, embedding = EXCLUDED.embedding
                """,
                (index_name, filename, chunk_index, chunk_text, embedding),
            )
            self.pg_conn.commit()
        finally:
            cursor.close()

    def _delete_embeddings(self, index_name: str, filename: str) -> None:
        """Delete all embeddings for a document."""
        cursor = self.pg_conn.cursor()
        try:
            cursor.execute(
                "DELETE FROM document_embeddings WHERE index_name = %s AND filename = %s",
                (index_name, filename),
            )
            self.pg_conn.commit()
            logger.info(f"Deleted embeddings for {index_name}/{filename}")
        finally:
            cursor.close()

    def _update_status(self, index_name: str, filename: str, status: str, error_msg: str = "") -> None:
        """Update document status via kagent API."""
        try:
            with httpx.Client() as client:
                response = client.put(
                    f"{settings.kagent_api_url}/api/indices/{index_name}/documents/{filename}/status",
                    json={"status": status, "error_msg": error_msg},
                    timeout=10.0,
                )
                if response.status_code not in (200, 204):
                    logger.warning(f"Failed to update status: {response.status_code}")
        except Exception as e:
            logger.warning(f"Failed to update status: {e}")

    def _delete_status(self, index_name: str, filename: str) -> None:
        """Delete document status via kagent API."""
        try:
            with httpx.Client() as client:
                response = client.delete(
                    f"{settings.kagent_api_url}/api/indices/{index_name}/documents/{filename}/status",
                    timeout=10.0,
                )
                if response.status_code not in (200, 204, 404):
                    logger.warning(f"Failed to delete status: {response.status_code}")
        except Exception as e:
            logger.warning(f"Failed to delete status: {e}")


# Global processor instance
_processor: DocumentProcessor | None = None


def get_processor() -> DocumentProcessor:
    """Get or create the document processor instance."""
    global _processor
    if _processor is None:
        _processor = DocumentProcessor()
    return _processor
