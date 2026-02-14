"""Embedding generation for query vectors."""

import logging

from sentence_transformers import SentenceTransformer

from .config import settings

logger = logging.getLogger(__name__)

# Global model instance (loaded once)
_model: SentenceTransformer | None = None


def get_model() -> SentenceTransformer:
    """Get or initialize the embedding model."""
    global _model
    if _model is None:
        logger.info(f"Loading embedding model: {settings.embedding_model}")
        _model = SentenceTransformer(settings.embedding_model)
        logger.info("Embedding model loaded successfully")
    return _model


def generate_embedding(text: str) -> list[float]:
    """Generate embedding for a query text.

    Args:
        text: Query text to embed

    Returns:
        Embedding vector (384 dimensions for all-MiniLM-L6-v2)
    """
    model = get_model()
    embedding = model.encode([text], show_progress_bar=False)
    return embedding[0].tolist()
