# Data Service

行情与期权链采集服务（盘中缓存 + 盘后批量入库）。

## What it does
- 提供历史数据查询 API（stock daily / option daily）
- 提供手动采集 API（异步 Celery task）
- 盘后任务入口：`data_service.tasks.capture_post_market_data`
- 通过 FetcherProtocol 抽象数据源，支持配置切换（当前实现：yfinance）

## HTTP API
- Base URL: `http://localhost:8001`
- Docs: `http://localhost:8001/docs`

### Health & Config
- `GET /api/v1/health` — 服务健康检查
- `GET /api/v1/data/config` — 当前运行模式与配置
- `POST /api/v1/data/config` — 切换 intraday 模式

### Stock Data
- `GET /data/{symbol}/stock` — 日线列表
  - Filters: `start_date`, `end_date`
  - Pagination: `page`, `page_size`
  - Response model: `StockDailyResponse`
- `GET /data/{symbol}/stock/dates` — 已有数据日期列表

### Option Data
- `GET /data/{symbol}/options` — 期权链列表
  - Filters: `snapshot_date` | `start_date`+`end_date`（范围查询）, `expiry`, `option_type`, `min_strike`, `max_strike`, `min_volume`, `min_open_interest`
  - Pagination: `page`, `page_size`
  - Response model: `OptionDailyResponse`（含 `underlying_price`）
- `GET /data/{symbol}/options/dates` — 已有快照日期列表

### Manual Collection
- `POST /api/v1/collect` — 触发手动采集（异步 Celery task）
- `POST /api/v1/collect/options` — 触发期权链采集（支持 `historical_date`）
- `GET /api/v1/collect/{task_id}` — 查询采集任务状态

Manual Collection 日期规则：
- `start_date` 必须 `<= end_date`。
- `end_date` 不能晚于 `today_trading()`（按交易时区计算）。
- 若 `end_date == today_trading()` 且当前时间早于开盘（`data_service.market_hours.start`），接口会返回 `422`（不再静默归一化）。
- 所有日期相关 `422` 错误会在 `detail.suggested_request_body` 中返回可直接重试的建议请求体。

## Data Providers (FetcherProtocol)
配置位于 `config/config.yaml` → `data_service.providers`:
```yaml
providers:
  stock: "yfinance"
  options: "yfinance"
  options_historical: "none"   # yfinance 不支持通用历史期权链 API
```
说明：当 `options="yfinance"` 且当前时间早于开盘时，`/collect/options`
允许 `historical_date=previous_trading_day(today_trading())`，并使用盘前 live snapshot
作为上一交易日回填（落库 `snapshot_date` 会写为请求的 `historical_date`）。

新增数据源只需：
1. 实现 `StockFetcherProtocol` / `OptionFetcherProtocol`
2. 在 `fetchers/registry.py` 注册
3. 修改 config 即可切换

## Manual start (without Docker)
From repo root:

```bash
# 1) Install deps (workspace)
uv sync --package data-service

# 2) Start API server
uv run uvicorn services.data_service.app.main:app --host 0.0.0.0 --port 8001 --reload

# 3) (Optional) start Celery worker for data tasks
uv run celery -A shared.celery_app.celery_app worker -Q data -l info
```

## Notes
- Requires TimescaleDB + Postgres + Redis + RabbitMQ running.
- 首次运行前执行 `uv run python -m scripts.init_db` 初始化表结构。
- 手动采集在盘前触发时可能看到 `end_date` 被自动回退到上一个交易日（避免请求尚未开盘当日数据）。
