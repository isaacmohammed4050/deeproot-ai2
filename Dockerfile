# Base image
FROM python:3.12-slim

# Metadata
LABEL maintainer="Isaac <isaacmohammed4050@gmail.com>"
LABEL description="DeepRoot AI - AI-powered RCA over OpenTelemetry data"

# Environment settings (important for logs + ML)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TRANSFORMERS_CACHE=/app/.cache \
    PIP_NO_CACHE_DIR=1

# Set working directory
WORKDIR /app

# Install system dependencies (needed for some Python/ML libs)
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy full application (IMPORTANT)
COPY . .

# Ensure required directories exist (prevents runtime crashes)
RUN mkdir -p /app/static /app/.cache

# Expose FastAPI port
EXPOSE 8000

# Healthcheck (production-ready)
HEALTHCHECK --interval=30s --timeout=10s --retries=3 CMD \
  curl -f http://localhost:8000/health || exit 1

# Run application (single worker = better for ML + containers)
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
