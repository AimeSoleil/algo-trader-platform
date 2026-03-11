# Gateway Service

统一 API 网关：聚合所有微服务 OpenAPI，并提供单入口 Swagger。

## What it does
- 聚合服务 `openapi.json` 到一个文档
- 反向代理到各服务 (`/{service}/{path}`)
- 健康检查与文档刷新端点

## HTTP API
- Base URL: `http://localhost:8000`
- Unified docs: `http://localhost:8000/docs`
- Health: `GET /health`
- Health all: `GET /health/all`
- Refresh specs: `POST /specs/refresh`

## Manual start (without Docker)
From repo root:

```bash
# 1) Install deps (workspace)
uv sync

# 2) Start gateway
uv run uvicorn services.gateway_service.app.main:app --host 0.0.0.0 --port 8000 --reload
```

## Notes
- Gateway expects target services reachable via configured URLs.
- Override defaults with env vars like `GATEWAY_DATA_URL`, `GATEWAY_ANALYSIS_URL`.
