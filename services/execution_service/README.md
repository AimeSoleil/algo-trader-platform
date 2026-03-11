# Execution Service

蓝图加载与规则执行服务。

## What it does
- 加载/查询蓝图运行状态
- 手动暂停/恢复执行
- 任务：`execution_service.tasks.load_daily_blueprint`、`execution_service.tasks.finalize_daily_blueprint`

## HTTP API
- Base URL: `http://localhost:8004`
- Docs: `http://localhost:8004/docs`
- Health: `GET /health`
- Status: `GET /api/v1/blueprint/status`
- Load: `POST /api/v1/blueprint/load`
- Override: `POST /api/v1/override`

## Manual start (without Docker)
From repo root:

```bash
# 1) Install deps (workspace)
uv sync

# 2) Start API server
uv run uvicorn services.execution_service.app.main:app --host 0.0.0.0 --port 8004 --reload

# 3) (Optional) start Celery worker for execution tasks
uv run celery -A shared.celery_app.celery_app worker -Q execution -l info
```

## Notes
- Requires Postgres + Redis running.
