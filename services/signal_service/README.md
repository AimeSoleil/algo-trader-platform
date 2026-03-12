# Signal Service

盘后特征计算与交易信号生成服务。

## What it does
- 提供信号查询 API
- 盘后批量计算任务：`signal_service.tasks.compute_daily_signals`

## HTTP API
- Base URL: `http://localhost:8002`
- Docs: `http://localhost:8002/docs`
- Health: `GET /health`
- Signal query: `GET /api/v1/signals/{symbol}`
- Trigger signal compute: `POST /api/v1/signals/compute`
- Signal compute status: `GET /api/v1/signals/compute/{task_id}`

### Manual signal compute

```bash
# Trigger (default today_trading)
curl -X POST http://localhost:8002/api/v1/signals/compute \
	-H "Content-Type: application/json" \
	-d '{}'

# Trigger for specific trading date
curl -X POST http://localhost:8002/api/v1/signals/compute \
	-H "Content-Type: application/json" \
	-d '{"trading_date": "2026-03-12"}'

# Poll task status
curl http://localhost:8002/api/v1/signals/compute/<task_id>
```

## Manual start (without Docker)
From repo root:

```bash
# 1) Install deps (workspace)
uv sync --package signal-service

# 2) Start API server
uv run uvicorn services.signal_service.app.main:app --host 0.0.0.0 --port 8002 --reload

# 3) (Optional) start Celery worker for signal tasks
uv run celery -A shared.celery_app.celery_app worker -Q signal -l info
```

## Notes
- Requires TimescaleDB + Postgres + RabbitMQ running.
