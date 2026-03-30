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

> **Error handling:** 如果指定日期所有标的都无数据（如非交易日或数据尚未入库），任务结果会返回错误信息：
>
> ```json
> {
>   "date": "2026-03-15",
>   "symbols_computed": 0,
>   "errors": ["No stock data found for trading_date=2026-03-15. Symbols checked: AAPL, MSFT, ..."]
> }
> ```
>
> 如果部分标的有数据、部分无数据，结果会额外包含 `symbols_no_data` 字段列出无数据的标的。

### About `bar_type` in signal responses

信号查询 API 返回的每条记录包含 `bar_type` 字段，取值及含义：

| bar_type | 含义 |
|---|---|
| `daily` | 信号基于 **日线数据** (`stock_daily`) 计算 —— 优先数据源 |
| `intraday_1min` | 该标的无日线数据，系统自动从 **1 分钟分笔数据** (`stock_1min_bars`) 聚合成日 OHLCV 后计算信号 |

不同标的 `bar_type` 不同是正常现象——取决于数据源为该标的提供了哪种粒度的数据。

#### 数据加载优先级

**Stock（股票行情）：**
1. 优先查询 `stock_daily`（日线），最多 260 天
2. 若无日线数据，则切换到 `stock_1min_bars`（1 分钟），按天聚合为 OHLCV（要求每天 ≥30 条）

**Options（期权链）：**
1. 优先查询 `option_daily`（日快照）
2. 若无日快照，则切换到 `option_5min_snapshots`（5 分钟盘中快照）

> **注意：** 无论 `bar_type` 值如何，所有技术指标（均线、布林带等）均基于日级别 OHLCV 计算；
> `bar_type` 标记的是数据**来源**，供下游策略判断数据粒度与质量。

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
