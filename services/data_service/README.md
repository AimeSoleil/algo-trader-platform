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
- `POST /api/v1/collect/stock` — 触发手动股票数据采集（异步 Celery task）
- `POST /api/v1/collect/options` — 触发期权链采集（支持 `snapshot_date`）
- `GET /api/v1/collect/{task_id}` — 查询采集任务状态

#### Stock Collection 日期规则 (`end_date`)
| 场景 | 行为 |
|------|------|
| `end_date` 在未来 | `422`，建议改为今日 |
| `end_date == today` + 盘前 | `422`，建议改为上一交易日 |
| `end_date == today` + 盘中 | `422`，提示盘后再执行 |
| `end_date == today` + 盘后 | 正常执行 |
| `end_date` 在过去 | 正常执行 |

#### Options Collection 日期规则 (`snapshot_date`)
| 场景 | 行为 |
|------|------|
| `snapshot_date` 在未来 | `422`，建议改为今日 |
| `snapshot_date` == 上一交易日 + 盘前 | 正常执行（live chain 仍反映昨日收盘） |
| `snapshot_date` 在过去 + 无 historical provider | `422`，提示配置 `options_historical` |
| `snapshot_date` 在过去 + 有 historical provider | 正常执行（调用 historical provider） |
| `snapshot_date == today` + 盘前 | `422`，建议改为上一交易日 |
| `snapshot_date == today` + 盘中 | `422`，提示盘后再执行 |
| `snapshot_date == today` + 盘后 | 正常执行（live chain） |
| `snapshot_date` 未指定 | 等同于 today，适用同样规则 |

所有日期相关 `422` 错误会在 `detail.suggested_request_body` 中返回可直接重试的建议请求体。

## Data Providers (FetcherProtocol)
配置位于 `config/config.yaml` → `data_service.providers`:
```yaml
providers:
  stock: "yfinance"
  options: "yfinance"                # live option chain provider
  options_historical: "none"         # historical options (none = not available)
```
- `options`: 盘后 live 期权链采集使用的 provider（如 yfinance）
- `options_historical`: 历史期权数据 provider。设为 `"none"` 时，`snapshot_date` 为过去日期的请求会被拒绝
- 当配置了支持历史数据的 provider（如 orats、thetadata）时，过去日期的期权采集可正常执行

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
