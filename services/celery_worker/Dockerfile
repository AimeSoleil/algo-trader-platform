# =============================================================================
# Universal Celery Worker Dockerfile
# =============================================================================
# This image can run ANY Celery worker in the algo-trader-platform project.
# All service dependencies are installed so that a single image serves
# data_service, backfill_service, signal_service, analysis_service, and
# trade_service workers.  The concrete queue / concurrency / loglevel is
# controlled at runtime via docker-compose command overrides.
#
# Flower (the Celery monitoring UI) is also included and can be launched by
# overriding ENTRYPOINT or CMD in the compose file.
# =============================================================================

# ---------------------------------------------------------------------------
# Stage 1 – Builder: install every service in editable mode
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy source trees needed for pip install
COPY shared/          /app/shared/
COPY config/          /app/config/
COPY services/data_service/      /app/services/data_service/
COPY services/backfill_service/  /app/services/backfill_service/
COPY services/signal_service/    /app/services/signal_service/
COPY services/analysis_service/  /app/services/analysis_service/
COPY services/trade_service/     /app/services/trade_service/

# Install all service packages (editable) + Flower for monitoring
RUN pip install --upgrade pip setuptools wheel && \
    pip install -e /app/shared && \
    pip install -e /app/services/data_service && \
    pip install -e /app/services/backfill_service && \
    pip install -e /app/services/signal_service && \
    pip install -e /app/services/analysis_service && \
    pip install -e /app/services/trade_service && \
    pip install flower

# ---------------------------------------------------------------------------
# Stage 2 – Runtime: lean image with only what's needed to run workers
# ---------------------------------------------------------------------------
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# curl is required by the Flower healthcheck (HTTP probe)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root user for security
RUN groupadd --system app && useradd --system --gid app --create-home app

# Bring over installed Python packages from the builder stage
COPY --from=builder /usr/local /usr/local

# Copy ALL service source code so any worker can import any module
COPY shared/          /app/shared/
COPY config/          /app/config/
COPY services/        /app/services/

RUN mkdir -p /tmp/algo_trader_cache && \
    chown -R app:app /app /tmp/algo_trader_cache

USER app

# Workers don't listen on ports; Flower's port is controlled externally via
# docker-compose, so we intentionally omit EXPOSE here.

# Default healthcheck: ask the worker if it's alive.
# This is overridden per-service in docker-compose for workers that need
# specific destination checks (e.g. -d celery@data_worker).
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD ["celery", "-A", "shared.celery_app.celery_app", "inspect", "ping", "--timeout=5"]

ENTRYPOINT ["celery", "-A", "shared.celery_app.celery_app"]
CMD ["worker", "--loglevel=INFO"]
