"""Embedding generation and text chunking."""

import logging
import re
from typing import Iterator

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


def chunk_text(text: str, chunk_size: int = None, overlap: int = None) -> Iterator[tuple[int, str]]:
    """Split text into overlapping chunks.

    Args:
        text: Input text to chunk
        chunk_size: Approximate tokens per chunk (uses words as proxy)
        overlap: Number of tokens to overlap between chunks

    Yields:
        Tuple of (chunk_index, chunk_text)
    """
    if chunk_size is None:
        chunk_size = settings.chunk_size
    if overlap is None:
        overlap = settings.chunk_overlap

    # Split into sentences for better chunking
    sentences = _split_into_sentences(text)

    current_chunk = []
    current_size = 0
    chunk_index = 0

    for sentence in sentences:
        sentence_size = _estimate_tokens(sentence)

        if current_size + sentence_size > chunk_size and current_chunk:
            # Yield current chunk
            yield chunk_index, " ".join(current_chunk)
            chunk_index += 1

            # Keep overlap sentences
            overlap_text = " ".join(current_chunk)
            overlap_size = _estimate_tokens(overlap_text)

            # Find sentences to keep for overlap
            current_chunk = []
            current_size = 0
            for s in reversed(current_chunk):
                s_size = _estimate_tokens(s)
                if current_size + s_size <= overlap:
                    current_chunk.insert(0, s)
                    current_size += s_size
                else:
                    break

        current_chunk.append(sentence)
        current_size += sentence_size

    # Yield remaining chunk
    if current_chunk:
        yield chunk_index, " ".join(current_chunk)


def _split_into_sentences(text: str) -> list[str]:
    """Split text into sentences."""
    # Simple sentence splitting on common delimiters
    sentences = re.split(r"(?<=[.!?])\s+", text)
    return [s.strip() for s in sentences if s.strip()]


def _estimate_tokens(text: str) -> int:
    """Estimate token count (roughly 4 chars per token)."""
    return len(text) // 4


def generate_embeddings(texts: list[str]) -> list[list[float]]:
    """Generate embeddings for a list of texts.

    Args:
        texts: List of text strings to embed

    Returns:
        List of embedding vectors
    """
    model = get_model()
    embeddings = model.encode(texts, show_progress_bar=False)
    return embeddings.tolist()


def generate_embedding(text: str) -> list[float]:
    """Generate embedding for a single text.

    Args:
        text: Text string to embed

    Returns:
        Embedding vector
    """
    return generate_embeddings([text])[0]
