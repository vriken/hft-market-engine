# Build stage
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt


# Runtime stage
FROM python:3.11-slim

# Security: Run as non-root user
RUN useradd --create-home --shell /bin/bash appuser
WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /root/.local /home/appuser/.local
ENV PATH=/home/appuser/.local/bin:$PATH

# Copy application code
COPY src/ ./src/
COPY requirements.txt .

# Create directories for output
RUN mkdir -p /app/output /app/data && chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Health check endpoint (for K8s probes)
EXPOSE 8080

# Default command (can be overridden)
ENTRYPOINT ["python"]
CMD ["src/realtime_aggregator.py", "--help"]

# Labels for container registry
LABEL maintainer="data-engineering@example.com"
LABEL version="1.0.0"
LABEL description="OHLCV Real-Time Aggregator for energy trading"
