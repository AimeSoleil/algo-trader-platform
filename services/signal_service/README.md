# Signal Service

盘后特征计算与交易信号生成服务。

## What it does
- 提供信号查询 API
- 盘后批量计算任务：`signal_service.tasks.compute_daily_signals`

## HTTP API
- Base URL: `http://localhost:8002`
- Docs: `http://localhost:8002/docs`
- Health: `GET /health`
- **Signal query (unified)**: `GET /api/v1/signals`
- Signal single (alias): `GET /api/v1/signals/{symbol}`
- Trigger signal compute: `POST /api/v1/signals/compute`
- Signal compute status: `GET /api/v1/signals/compute/{task_id}`

### Signal query examples

```bash
# Today, all symbols
curl http://localhost:8002/api/v1/signals

# Specific symbols
curl "http://localhost:8002/api/v1/signals?symbols=AAPL&symbols=MSFT"

# Date range
curl "http://localhost:8002/api/v1/signals?start_date=2026-03-10&end_date=2026-03-14"

# Skip cache
curl "http://localhost:8002/api/v1/signals?symbols=AAPL&bypass_cache=true"

# Filter by volatility regime and trend
curl "http://localhost:8002/api/v1/signals?volatility_regime=high&trend=bullish"

# Sort and paginate
curl "http://localhost:8002/api/v1/signals?sort_by=daily_return&sort_order=desc&limit=20&offset=0"

# Single symbol (shortcut, backward compatible)
curl http://localhost:8002/api/v1/signals/AAPL
curl "http://localhost:8002/api/v1/signals/AAPL?trading_date=2026-03-12&bypass_cache=true"
```

### Query parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `symbols` | list[str] | all | Filter by symbols (repeatable) |
| `start_date` | date | today | Start of date range (YYYY-MM-DD) |
| `end_date` | date | start_date | End of date range (YYYY-MM-DD) |
| `bypass_cache` | bool | false | Skip Redis cache, read from DB |
| `volatility_regime` | str | — | Filter: `high` / `normal` / `low` |
| `trend` | str | — | Filter: `bullish` / `bearish` / `neutral` |
| `sort_by` | str | — | Sort by field (e.g. `close_price`, `daily_return`) |
| `sort_order` | str | `asc` | `asc` or `desc` |
| `limit` | int | 500 | Max results (1-2000) |
| `offset` | int | 0 | Pagination offset |

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
