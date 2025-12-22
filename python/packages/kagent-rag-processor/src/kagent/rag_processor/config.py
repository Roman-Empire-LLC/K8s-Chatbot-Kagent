"""Configuration for RAG processor."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # MinIO configuration
    minio_endpoint: str = "kagent-minio:9000"
    minio_access_key: str = "admin"
    minio_secret_key: str = "minioadmin123"
    minio_secure: bool = False

    # PostgreSQL configuration
    postgres_url: str = "postgres://postgres:postgres@kagent-postgresql:5432/postgres"

    # kagent controller API
    kagent_api_url: str = "http://kagent-controller:8083"

    # Embedding configuration
    embedding_model: str = "all-MiniLM-L6-v2"
    chunk_size: int = 500  # tokens
    chunk_overlap: int = 50  # tokens

    # Server configuration
    host: str = "0.0.0.0"
    port: int = 8080
    log_level: str = "info"

    class Config:
        env_prefix = "RAG_"


settings = Settings()
