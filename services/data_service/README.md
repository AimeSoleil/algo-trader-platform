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
- `GET /api/v1/collect/{task_id}` — 查询采集任务状态

#### Stock Collection 日期规则 (`end_date`)
| 场景 | 行为 |
|------|------|
| `end_date` 在未来 | `422`，建议改为今日 |
| `end_date == today` + 盘前 | `422`，建议改为上一交易日 |
| `end_date == today` + 盘中 | `422`，提示盘后再执行 |
| `end_date == today` + 盘后 | 正常执行 |
| `end_date` 在过去 | 正常执行 |

所有日期相关 `422` 错误会在 `detail.suggested_request_body` 中返回可直接重试的建议请求体。

## Data Providers (FetcherProtocol)
配置位于 `config/config.yaml` → `data_service.providers`:
```yaml
providers:
  stock: "yfinance"
  options: "yfinance"                # intraday option chain provider (盘中 5min 采集)
```
- `options`: 盘中 5 分钟期权链采集使用的 provider（如 yfinance）。`option_daily` 不再盘后直接采集，改由 `aggregate_option_daily` 从盘中快照聚合

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

## 盘后管线 (Post-Market Pipeline)

```
capture_post_market_data   — 仅采集 1m bars / daily bar → DB（不含期权）
  → batch_flush_to_db      — 盘中 Parquet 缓存 → option_5min_snapshots
  → aggregate_option_daily  — 仅从 option_5min_snapshots 聚合 → option_daily + option_iv_daily
  → detect_and_backfill_gaps → compute_daily_signals → generate_daily_blueprint
```

> **注意**：`capture_post_market_data` 不再采集期权数据。盘后 yfinance 期权链 bid=ask=0、IV 不可靠
> （Yahoo Finance 在非交易时段清空报价）。`aggregate_option_daily` 也不会改用 yfinance 兜底 —
> 如果当天没有盘中 5 分钟快照（intraday 未启用或非交易日），该天的 `option_daily` 和 `option_iv_daily`
> 为空，这是预期行为。

### 为什么需要 option_iv_daily

`option_daily` 已改由盘中 5 分钟快照的最后一条回填（而非盘后 yfinance 采集）。

`option_iv_daily` 是在此基础上按标的聚合的 **每日 IV 汇总表**，用于 IV Rank / IV Percentile 计算：

| 查询源 | 扫描行数 (10 标的 × 252 天) | 需要运行时聚合 |
|--------|---------------------------|--------------|
| `option_5min_snapshots` | **~39,000,000** (200 合约 × 78 条/天) | ATM 过滤 + AVG + GROUP BY |
| `option_daily` | **~500,000** (200 合约/标的) | ATM 过滤 + AVG + GROUP BY |
| **`option_iv_daily`** | **2,520** | **直接读，无聚合** |

字段：
- `avg_iv` — 全链平均 IV
- `atm_iv` — ATM 合约 IV（strike 在标的价 ±5% 内），**IV Rank 的核心输入**
- `call_iv` / `put_iv` — 分 call/put 的平均 IV，用于偏差分析
- `sample_size` — 参与聚合的合约数（数据质量校验）

写入成本：每天 ~10 行（= watchlist 标的数），存储开销可忽略。

## Notes
- Requires TimescaleDB + Postgres + Redis + RabbitMQ running.
- 首次运行前执行 `uv run python -m scripts.init_db` 初始化表结构。
- 手动采集在盘前触发时可能看到 `end_date` 被自动调整为上一个交易日（避免请求尚未开盘当日数据）。
