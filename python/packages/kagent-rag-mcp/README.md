# kagent-rag-mcp

MCP server for querying RAG indices. Dynamically registers a `query-<index-name>` tool for each RAG index.

## Features

- Polls kagent API for indices every 30 seconds
- Automatically registers/unregisters tools as indices are created/deleted
- Uses pgvector for similarity search
- Same embedding model as RAG processor (all-MiniLM-L6-v2)

## Environment Variables

- `RAG_MCP_POSTGRES_URL` - PostgreSQL connection string
- `RAG_MCP_KAGENT_API_URL` - kagent controller API URL
- `RAG_MCP_EMBEDDING_MODEL` - Embedding model name (default: all-MiniLM-L6-v2)
- `RAG_MCP_REFRESH_INTERVAL` - Index refresh interval in seconds (default: 30)
- `RAG_MCP_TOP_K` - Default number of results (default: 5)
