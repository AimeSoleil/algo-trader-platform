# Portfolio Service

持仓查询、组合敞口和绩效归因服务。

## What it does
- 持仓快照与持仓明细查询
- 绩效归因查询
- 报告任务：`portfolio_service.tasks.generate_daily_report`

## HTTP API
- Base URL: `http://localhost:8005`
- Docs: `http://localhost:8005/docs`
- Health: `GET /health`
- Snapshot: `GET /api/v1/portfolio/snapshot`
- Positions: `GET /api/v1/portfolio/positions`
- Performance: `GET /api/v1/portfolio/performance`

## Manual start (without Docker)
From repo root:

```bash
# 1) Install deps (workspace)
uv sync

# 2) Start API server
uv run uvicorn services.portfolio_service.app.main:app --host 0.0.0.0 --port 8005 --reload

# 3) (Optional) start Celery worker for report tasks
uv run celery -A shared.celery_app.celery_app worker -Q portfolio -l info
```

## Notes
- Requires Postgres running.
