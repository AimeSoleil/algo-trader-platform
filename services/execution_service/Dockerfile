FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY shared/ /app/shared/
COPY config/ /app/config/
COPY services/execution_service/ /app/services/execution_service/

RUN pip install --no-cache-dir -e /app/shared && \
    pip install --no-cache-dir -e /app/services/execution_service

EXPOSE 8004

CMD ["uvicorn", "services.execution_service.app.main:app", "--host", "0.0.0.0", "--port", "8004"]
