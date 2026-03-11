# Monitoring Service

系统健康检查与 Prometheus 指标暴露服务。

## What it does
- 数据库连通性检查
- 调度配置检查
- 暴露 Prometheus 指标

## HTTP API
- Base URL: `http://localhost:8006`
- Docs: `http://localhost:8006/docs`
- Health: `GET /health`
- Services health: `GET /api/v1/health/services`
- Schedule health: `GET /api/v1/health/schedule`
- Metrics: `GET /metrics`

## Manual start (without Docker)
From repo root:

```bash
# 1) Install deps (workspace)
uv sync

# 2) Start API server
uv run uvicorn services.monitoring_service.app.main:app --host 0.0.0.0 --port 8006 --reload
```

## Notes
- Requires Postgres + TimescaleDB running.
