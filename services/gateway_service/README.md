# Gateway Service

统一 API 网关：聚合所有微服务 OpenAPI，并提供单入口 Swagger。

## What it does
- 聚合服务 `openapi.json` 到一个文档
- 反向代理到各服务 (`/{service}/{path}`)
- 健康检查与文档刷新端点

## HTTP API
- Base URL: `http://localhost:8000`
- Unified docs: `http://localhost:8000/docs`
- Health: `GET /api/v1/health` (`/health` 保留 307 兼容跳转)
- Health all: `GET /api/v1/health/all` (`/health/all` 保留 307 兼容跳转)
- Refresh specs: `POST /specs/refresh`
- Merged OpenAPI: `GET /openapi.json`
- Gateway-only OpenAPI: `GET /openapi/gateway.json`
- Service-scoped OpenAPI: `GET /openapi/{service}.json`

参数约定（跨服务统一）：
- 所有“交易日”查询参数统一使用 `trading_date`（例如 `?trading_date=2026-03-12`）
- 不再兼容旧参数名 `date`

示例（通过 Gateway 代理访问）：

```bash
# Signal query (unified — supports batch, date range, filters)
curl "http://localhost:8000/signal/api/v1/signals?symbols=AAPL&start_date=2026-03-12&end_date=2026-03-12"

# Trade portfolio performance
curl "http://localhost:8000/trade/api/v1/portfolio/performance?trading_date=2026-03-12"

# Trade blueprint status
curl "http://localhost:8000/trade/api/v1/blueprint/status?trading_date=2026-03-12"
```

OpenAPI 聚合行为：
- Gateway 从每个后端服务的标准端点 `/{service_base}/openapi.json` 拉取原始 spec。
- 保持约定：各服务自身 OpenAPI 入口仍是其本地 `GET /openapi.json`，无需改成其他路径。

## Manual start (without Docker)
From repo root:

```bash
# 1) Install deps (workspace)
uv sync --package gateway-service

# 2) Start gateway
export GATEWAY_DATA_URL=http://127.0.0.1:8001
export GATEWAY_SIGNAL_URL=http://127.0.0.1:8002
export GATEWAY_ANALYSIS_URL=http://127.0.0.1:8003
export GATEWAY_TRADE_URL=http://127.0.0.1:8004
export GATEWAY_MONITORING_URL=http://127.0.0.1:8006

uv run uvicorn services.gateway_service.app.main:app --host 0.0.0.0 --port 8000 --reload
```

## Notes
- Gateway expects target services reachable via configured URLs.
- Override defaults with env vars like `GATEWAY_DATA_URL`, `GATEWAY_ANALYSIS_URL`.
