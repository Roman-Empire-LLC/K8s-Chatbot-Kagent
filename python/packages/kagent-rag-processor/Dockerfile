FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast package installation
RUN pip install --no-cache-dir uv

# Copy package files
COPY pyproject.toml README.md ./
COPY src/ ./src/

# Install CPU-only PyTorch first (much smaller than GPU version)
RUN uv pip install --system --no-cache torch --index-url https://download.pytorch.org/whl/cpu

# Install the package
RUN uv pip install --system --no-cache .

# Run the server
EXPOSE 8080
CMD ["kagent-rag-processor"]
