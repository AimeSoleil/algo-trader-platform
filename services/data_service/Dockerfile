FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy shared library first (for caching)
COPY shared/ /app/shared/
COPY config/ /app/config/

# Copy service
COPY services/data_service/ /app/services/data_service/

# Install dependencies
RUN pip install --no-cache-dir -e /app/shared && \
    pip install --no-cache-dir -e /app/services/data_service

# Create cache directory
RUN mkdir -p /tmp/algo_trader_cache

EXPOSE 8001

CMD ["uvicorn", "services.data_service.app.main:app", "--host", "0.0.0.0", "--port", "8001"]
