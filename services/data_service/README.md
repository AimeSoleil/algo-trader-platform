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
- `GET /api/v1/collect/{task_id}` — 查询采集任务状态

## Data Providers (FetcherProtocol)
配置位于 `config/config.yaml` → `data_service.providers`:
```yaml
providers:
  stock: "yfinance"
  options: "yfinance"
  options_historical: "none"   # yfinance 不支持历史期权链
```
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
