# ==========================================================================
# Multi-stage Dockerfile for the stream pipeline.
#
# Stages:
#   1. base: Python runtime with system dependencies
#   2. deps: Install Python packages
#   3. app:  Copy source code, create non-root user
#
# The same image runs as producer, consumer, or dashboard depending on
# the command specified in docker-compose.yml.
# ==========================================================================

FROM python:3.12-slim AS base

# System dependencies for confluent-kafka (librdkafka) and psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        librdkafka-dev \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ---------------------------------------------------------------------------
FROM base AS deps

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---------------------------------------------------------------------------
FROM deps AS app

# Copy source
COPY src/ src/
COPY scripts/ scripts/
COPY sql/ sql/

# Create non-root user
RUN groupadd -r pipeline && useradd -r -g pipeline pipeline \
    && chown -R pipeline:pipeline /app
USER pipeline

# Default: run the consumer (overridden per service in docker-compose)
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "src.consumer.consumer"]
