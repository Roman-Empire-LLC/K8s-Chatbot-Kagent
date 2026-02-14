FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency management
RUN pip install --no-cache-dir uv

# Copy project files
COPY pyproject.toml README.md ./
COPY src/ ./src/

# Install CPU-only PyTorch first (avoid GPU dependencies)
RUN uv pip install --system --no-cache torch --index-url https://download.pytorch.org/whl/cpu

# Install the package
RUN uv pip install --system --no-cache .

# Run in HTTP mode for Kubernetes
ENV MCP_HTTP_MODE=true

EXPOSE 8080

CMD ["kagent-rag-mcp"]
