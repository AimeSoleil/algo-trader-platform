# Analysis Service

LLM 驱动的交易蓝图生成服务。

## What it does
- 提供蓝图查询 API
- 提供手动分析 API（异步 Celery task）
- 盘后任务：`analysis_service.tasks.generate_daily_blueprint`

## HTTP API
- Base URL: `http://localhost:8003`
- Docs: `http://localhost:8003/docs`
- Health: `GET /health`
- Blueprint query: `GET /api/v1/blueprint/{trading_date}`
- Manual analyze: `POST /api/v1/analyze`
- Task status: `GET /api/v1/analyze/{task_id}`

## Manual start (without Docker)
From repo root:

```bash
# 1) Install deps (workspace)
uv sync

# 2) Start API server
uv run uvicorn services.analysis_service.app.main:app --host 0.0.0.0 --port 8003 --reload

# 3) (Optional) start Celery worker for analysis tasks
uv run celery -A shared.celery_app.celery_app worker -Q analysis -l info
```

## Notes
- Requires Postgres + Redis + RabbitMQ running.
- LLM provider/config is loaded from `config/config.yaml` and env vars.
