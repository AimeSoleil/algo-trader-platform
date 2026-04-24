FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY shared/ /app/shared/
COPY services/data_service/ /app/services/data_service/

RUN pip install --upgrade pip setuptools wheel && \
    pip install --no-compile -e /app/shared && \
    pip install --no-compile -e /app/services/data_service


FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN groupadd --system app && useradd --system --gid app --create-home app

COPY --from=builder /usr/local /usr/local
COPY shared/ /app/shared/
COPY config/config.yaml /app/config/config.yaml
COPY services/data_service/ /app/services/data_service/

RUN mkdir -p /tmp/algo_trader_cache && \
    chown -R app:app /app /tmp/algo_trader_cache

USER app

EXPOSE 8001

CMD ["uvicorn", "services.data_service.app.main:app", "--host", "0.0.0.0", "--port", "8001"]
