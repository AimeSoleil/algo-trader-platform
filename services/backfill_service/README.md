# Backfill Service

数据缺口检测与历史回填服务（纯 Celery，无 FastAPI HTTP 入口）。

## What it does
- 盘后缺口检测与回填：`backfill_service.tasks.detect_and_backfill_gaps`
- 历史完整性检查：`backfill_service.tasks.check_historical_gaps`
- 新标的冷启动回填：`backfill_service.tasks.backfill_new_symbol`

## Manual start (without Docker)
From repo root:

```bash
# 1) Install deps (workspace)
uv sync --package backfill-service

# 2) Start worker (listening to backfill queue)
uv run celery -A shared.celery_app.celery_app worker -Q backfill -l info

# 3) (Optional) trigger a task manually from Python
uv run python -c "from shared.celery_app import celery_app; r=celery_app.send_task('backfill_service.tasks.check_historical_gaps'); print(r.id)"
```

## Notes
- Requires TimescaleDB + Redis + RabbitMQ running.
- Option-related historical gaps are currently logged-only due to data-source limits.
