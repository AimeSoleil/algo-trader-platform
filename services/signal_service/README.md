# Signal Service

Signal Service 负责从 stock bars、option rows、benchmark returns 和 VIX 数据中计算日级信号特征，并把结果写入 `signal_features` 供 analysis service 使用。

## Scope

- 提供统一 signals 查询 API
- 提供手动信号计算 API
- 提供 daily batch compute task
- 支持被 data service 以 chunk 方式并行派发
- 负责信号缓存写穿与查询缓存

核心文件：

- `services/signal_service/app/main.py`
- `services/signal_service/app/routes.py`
- `services/signal_service/app/tasks/signal.py`
- `services/signal_service/app/queries.py`
- `services/signal_service/app/data_loaders.py`
- `services/signal_service/app/signal_generator.py`

## Architecture Overview

Signal Service 的主要流程是：

- 从 stock 数据加载器获取历史 bars
- 从 option 数据加载器获取 option rows
- 预加载 benchmark returns 与 VIX
- 计算 stock indicators
- 计算 option indicators
- 构造 cross-asset indicators
- 生成最终 `SignalFeatures`
- 写入 `signal_features` 表
- 刷新 Redis 缓存

这是一个“批量日级特征计算服务”，不是盘中逐 tick 决策服务。

## Compute Tasks

### `signal_service.tasks.compute_daily_signals`

用途：

- 手动触发整日计算
- 被上游 data service 作为下游阶段触发

特点：

- 支持指定 `trading_date`
- 支持指定 `symbols`
- 使用并发 semaphore 控制单标的处理并发
- 预加载 benchmark 和 VIX，避免重复查询

### `signal_service.tasks.compute_signals_chunk`

用途：

- 供 data service 的 post-market pipeline 以 chord/group 形式并行触发
- 每个 chunk 只处理一部分 symbols

这两个任务共享同一套核心异步逻辑 `_compute_daily_signals`。

## Data Inputs And Fallbacks

### Stock Data Priority

- 优先读 `stock_daily`
- 若无可用日线，则回退到 `stock_1min_bars` 按天聚合

### Option Data Priority

- 优先读 `option_daily`
- 若无 `option_daily`，回退到 `option_5min_snapshots`

### `bar_type`

信号结果中的 `bar_type` 表示数据来源，不表示技术指标计算粒度变化：

- `daily`
- `intraday_1min`

无论来源如何，技术指标最终都按日级数据计算。

## Cache Model

Signal Service 有 Redis L1 查询缓存：

- 单标的单日期查询优先查缓存
- 命中 DB 后会回填缓存
- 写 signal 时会主动刷新缓存
- cache key 粒度为 `symbol + date`

相关文件：

- `services/signal_service/app/queries.py`

## HTTP API

- Base URL: `http://localhost:8002`
- Docs: `http://localhost:8002/docs`
- Health: `GET /api/v1/health`
- Legacy health redirect: `GET /health`

### Query APIs

- `GET /api/v1/signals`
- `GET /api/v1/signals/{symbol}`

统一查询支持：

- `symbols`
- `start_date`
- `end_date`
- `bypass_cache`
- `volatility_regime`
- `trend`
- `sort_by`
- `sort_order`
- `limit`
- `offset`

### Compute APIs

- `POST /api/v1/signals/compute`
- `GET /api/v1/signals/compute/{task_id}`

## Common Developer Flows

查询今日全部 signals：

```bash
curl http://localhost:8002/api/v1/signals
```

查询指定标的：

```bash
curl "http://localhost:8002/api/v1/signals?symbols=AAPL,MSFT"
```

手动触发计算：

```bash
curl -X POST http://localhost:8002/api/v1/signals/compute \
  -H "Content-Type: application/json" \
  -d '{}'
```

指定日期触发计算：

```bash
curl -X POST http://localhost:8002/api/v1/signals/compute \
  -H "Content-Type: application/json" \
  -d '{"trading_date": "2026-03-12"}'
```

## Workers And Dependencies

Signal Service 主要依赖 `signal` 队列 worker。

signal worker 负责：

- 全量 daily signal 计算
- chunked signal 计算

运行它之前，通常需要确保：

- `stock_daily` / `stock_1min_bars` 已有数据
- `option_daily` 或 `option_5min_snapshots` 已有数据
- Postgres、TimescaleDB、Redis、RabbitMQ 可用

如果 signals 是由 post-market pipeline 自动触发，还需要 data worker 存在作为上游协调者。

## Local Run

从仓库根目录运行：

```bash
# 1) Install deps
uv sync --package signal-service

# 2) Start API server
uv run uvicorn services.signal_service.app.main:app --host 0.0.0.0 --port 8002 --reload

# 3) Start signal worker
uv run celery -A shared.celery_app.celery_app worker -Q signal -l info
```

## Dev Notes

- Signal Service 是 analysis service 的上游，不直接生成 blueprint
- 查询默认会选“最新可用 signal date”，而不一定是今天
- 若指定日期没有任何可计算数据，任务结果会返回错误摘要而不是空成功
