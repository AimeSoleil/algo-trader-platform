# Data Service

行情与期权链采集服务（盘中缓存 + 盘后批量入库）。

## What it does
- 提供实时与历史查询 API
- 提供手动采集 API（异步 Celery task）
- 盘后任务入口：`data_service.tasks.capture_post_market_data`

## HTTP API
- Base URL: `http://localhost:8001`
- Docs: `http://localhost:8001/docs`
- Health: `GET /api/v1/health`
- Manual collect: `POST /api/v1/collect`
- Task status: `GET /api/v1/collect/{task_id}`

## Manual start (without Docker)
From repo root:

```bash
# 1) Install deps (workspace)
uv sync

# 2) Start API server
uv run uvicorn services.data_service.app.main:app --host 0.0.0.0 --port 8001 --reload

# 3) (Optional) start Celery worker for data tasks
uv run celery -A shared.celery_app.celery_app worker -Q data -l info
```

## Notes
- Requires TimescaleDB + Postgres + Redis + RabbitMQ running.
