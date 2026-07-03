FROM python:3.11-slim

# System deps: git (required for Git Integration feature) + build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (layer-cached unless requirements change)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY . .

# Install the package itself
RUN pip install --no-cache-dir -e .

# Runtime directories (may be overridden by volume mounts)
RUN mkdir -p data logs pipelines data/git-clones

# Non-root user for security
RUN useradd -m -u 1000 dpflow && chown -R dpflow:dpflow /app
USER dpflow

# Expose the default port (overridable via DATAPLATFORM_PORT)
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

CMD ["python3", "-m", "dataplatform.cli.main", "serve", "--host", "0.0.0.0", "--port", "8000"]
