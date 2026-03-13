FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY shared/ /app/shared/
COPY config/ /app/config/
COPY services/trade_service/ /app/services/trade_service/

RUN pip install --upgrade pip setuptools wheel && \
    pip install -e /app/shared && \
    pip install -e /app/services/trade_service


FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN groupadd --system app && useradd --system --gid app --create-home app

COPY --from=builder /usr/local /usr/local
COPY shared/ /app/shared/
COPY config/ /app/config/
COPY services/trade_service/ /app/services/trade_service/

RUN chown -R app:app /app

USER app

EXPOSE 8004

CMD ["uvicorn", "services.trade_service.app.main:app", "--host", "0.0.0.0", "--port", "8004"]
