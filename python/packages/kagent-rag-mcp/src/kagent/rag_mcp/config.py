"""Configuration for RAG MCP server."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(env_prefix="RAG_MCP_")

    # PostgreSQL configuration
    postgres_url: str = "postgres://postgres:postgres@kagent-postgresql:5432/postgres"

    # kagent controller API
    kagent_api_url: str = "http://kagent-controller:8083"

    # Embedding configuration
    embedding_model: str = "all-MiniLM-L6-v2"

    # Tool refresh interval (seconds)
    refresh_interval: int = 30

    # Default number of results per query
    top_k: int = 5

    # Server configuration
    host: str = "0.0.0.0"
    port: int = 8080


settings = Settings()
