"""Dynamic tool registration and query logic for RAG indices."""

import logging
from dataclasses import dataclass

import httpx
import psycopg2
from pgvector.psycopg2 import register_vector

from .config import settings
from .embeddings import generate_embedding

logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
    """A single result from a RAG query."""
    filename: str
    chunk_index: int
    chunk_text: str
    similarity: float


class RAGToolManager:
    """Manages dynamic tool registration for RAG indices."""

    def __init__(self, mcp_server):
        """Initialize the tool manager.

        Args:
            mcp_server: FastMCP server instance
        """
        self.mcp = mcp_server
        self.registered_indices: dict[str, str] = {}  # name -> description
        self.pg_conn = None
        self._connect_postgres()

    def _connect_postgres(self) -> None:
        """Connect to PostgreSQL and register pgvector."""
        try:
            self.pg_conn = psycopg2.connect(settings.postgres_url)
            register_vector(self.pg_conn)
            logger.info("Connected to PostgreSQL with pgvector")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def _fetch_indices(self) -> list[dict]:
        """Fetch list of RAG indices from kagent API.

        Returns:
            List of dicts with 'name' and 'description' keys
        """
        try:
            with httpx.Client(timeout=10.0) as client:
                response = client.get(f"{settings.kagent_api_url}/api/indices")
                response.raise_for_status()
                data = response.json()
                # API returns {"data": [...], "message": "...", "error": false}
                indices = data.get("data", [])
                return [
                    {"name": idx["name"], "description": idx.get("description", "")}
                    for idx in indices if "name" in idx
                ]
        except Exception as e:
            logger.error(f"Failed to fetch indices: {e}")
            return []

    def refresh_tools(self) -> None:
        """Refresh tools based on current indices."""
        indices = self._fetch_indices()
        current_map = {idx["name"]: idx["description"] for idx in indices}
        current_names = set(current_map.keys())
        registered_names = set(self.registered_indices.keys())

        # Register new indices
        new_names = current_names - registered_names
        for name in new_names:
            self._register_tool(name, current_map.get(name, ""))
            self.registered_indices[name] = current_map.get(name, "")

        # Update indices with changed descriptions
        for name in current_names & registered_names:
            old_desc = self.registered_indices.get(name, "")
            new_desc = current_map.get(name, "")
            if old_desc != new_desc:
                logger.info(f"Description changed for {name}, re-registering tool")
                self._register_tool(name, new_desc)
                self.registered_indices[name] = new_desc

        # Unregister removed indices
        removed_names = registered_names - current_names
        for name in removed_names:
            self._unregister_tool(name)
            del self.registered_indices[name]

    def _register_tool(self, index_name: str, description: str) -> None:
        """Register a query tool for an index."""
        tool_name = f"query-{index_name}"
        logger.info(f"Registering tool: {tool_name}")

        # Create the query function for this index
        def make_query_fn(idx_name: str):
            def query_fn(query: str, top_k: int = settings.top_k) -> list[dict]:
                """Query the RAG index for relevant documents.

                Args:
                    query: The search query
                    top_k: Number of results to return

                Returns:
                    List of matching document chunks with similarity scores
                """
                return self._execute_query(idx_name, query, top_k)
            return query_fn

        # Register with FastMCP
        query_fn = make_query_fn(index_name)
        query_fn.__name__ = tool_name

        # Use the index description if provided, otherwise use generic description
        if description:
            query_fn.__doc__ = description
        else:
            query_fn.__doc__ = f"Query the '{index_name}' RAG index for relevant documents."

        self.mcp.tool()(query_fn)
        logger.info(f"Registered tool: {tool_name} with description: {description or '(default)'}")

    def _unregister_tool(self, index_name: str) -> None:
        """Unregister a query tool for an index."""
        tool_name = f"query-{index_name}"
        logger.info(f"Unregistering tool: {tool_name}")
        # Note: FastMCP doesn't have a built-in unregister, but we track it
        # The tool will be stale but won't cause errors

    def _execute_query(self, index_name: str, query: str, top_k: int) -> list[dict]:
        """Execute a similarity search query.

        Args:
            index_name: Name of the RAG index
            query: Search query text
            top_k: Number of results to return

        Returns:
            List of matching chunks with metadata
        """
        # Generate embedding for query
        query_embedding = generate_embedding(query)

        # Execute pgvector similarity search
        cursor = self.pg_conn.cursor()
        try:
            cursor.execute(
                """
                SELECT filename, chunk_index, chunk_text,
                       1 - (embedding <=> %s::vector) as similarity
                FROM document_embeddings
                WHERE index_name = %s
                ORDER BY embedding <=> %s::vector
                LIMIT %s
                """,
                (query_embedding, index_name, query_embedding, top_k)
            )
            rows = cursor.fetchall()

            results = []
            for row in rows:
                results.append({
                    "filename": row[0],
                    "chunk_index": row[1],
                    "chunk_text": row[2],
                    "similarity": round(row[3], 4)
                })

            logger.info(f"Query '{query[:50]}...' on '{index_name}' returned {len(results)} results")
            return results

        except Exception as e:
            logger.error(f"Query failed: {e}")
            self.pg_conn.rollback()
            return []
        finally:
            cursor.close()
