"""FastMCP server entry point for RAG queries."""

import asyncio
import logging
import os
import sys
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

from fastmcp import FastMCP

from .config import settings
from .embeddings import get_model
from .tools import RAGToolManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

# Create FastMCP server
mcp = FastMCP("kagent-rag")

# Global tool manager
tool_manager: RAGToolManager | None = None


class HealthHandler(BaseHTTPRequestHandler):
    """Simple health check handler."""

    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "healthy"}')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        # Suppress health check logs
        if "/health" not in str(args):
            logger.info("%s - %s", self.address_string(), format % args)


def start_health_server(port: int):
    """Start a simple health check server on a separate port."""
    health_port = port + 1  # Use port 8081 for health
    server = HTTPServer(("0.0.0.0", health_port), HealthHandler)
    logger.info(f"Health check server running on port {health_port}")
    server.serve_forever()


async def periodic_refresh():
    """Periodically refresh tools from the indices API."""
    while True:
        try:
            if tool_manager:
                tool_manager.refresh_tools()
        except Exception as e:
            logger.error(f"Error refreshing tools: {e}")
        await asyncio.sleep(settings.refresh_interval)


def main():
    """Main entry point."""
    global tool_manager

    logger.info("Starting kagent-rag MCP server")

    # Pre-load the embedding model
    logger.info("Pre-loading embedding model...")
    get_model()

    # Initialize tool manager
    logger.info("Initializing tool manager...")
    tool_manager = RAGToolManager(mcp)

    # Initial tool registration
    logger.info("Performing initial tool discovery...")
    tool_manager.refresh_tools()

    # Start periodic refresh in background
    async def run_server():
        # Start periodic refresh task
        asyncio.create_task(periodic_refresh())

        # Run the MCP server
        # FastMCP handles the transport (stdio or http)
        await mcp.run_async()

    # Check if we should run in HTTP mode
    if os.environ.get("MCP_HTTP_MODE", "").lower() == "true":
        # Start health check server in background thread
        health_thread = threading.Thread(
            target=start_health_server,
            args=(settings.port,),
            daemon=True
        )
        health_thread.start()

        logger.info(f"Running in HTTP mode on {settings.host}:{settings.port}")
        mcp.run(
            transport="streamable-http",
            host=settings.host,
            port=settings.port,
        )
    else:
        logger.info("Running in stdio mode")
        asyncio.run(run_server())


if __name__ == "__main__":
    main()
