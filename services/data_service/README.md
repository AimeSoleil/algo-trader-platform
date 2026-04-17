# Data Service

Data Service 负责市场数据采集、盘中期权快照缓冲、盘后股票入库、期权日级聚合，以及下游 signals → blueprint 流水线的触发。

## Scope

- 提供 stock_daily 与 option_daily 查询 API
- 提供手动股票采集 API
- 提供 earnings 查询与缓存
- 运行盘后统一流水线入口
- 将下游流程派发到 signal queue 与 analysis queue

核心入口：

- `services/data_service/app/main.py`
- `services/data_service/app/routes.py`
- `services/data_service/app/tasks/pipeline.py`
- `services/data_service/app/tasks/capture.py`
- `services/data_service/app/tasks/aggregation.py`
- `services/data_service/app/tasks/coordination.py`

## Architecture Overview

Data Service 当前分成两类职责：

- API 进程：对外提供查询与手动触发接口
- Celery data worker：执行采集、聚合、协调、通知等任务

核心设计点：

- 盘中期权链先进入缓存/快照表，不直接生成日级 option 数据
- 盘后股票数据单独采集
- `option_daily` 与 `option_iv_daily` 只从 intraday snapshots 聚合，不走盘后 yfinance 兜底
- 下游 `compute_daily_signals` 与 `generate_daily_blueprint` 由 data service 统一派发

## Post-Market Pipeline

统一盘后流水线入口：`data_service.tasks.run_post_market_pipeline`

当前流程：

- Step 1: `aggregate_option_daily`
- Step 2: `capture_post_market_chunk` fan-out 采集股票 1m bars 和 daily bars
- Step 3: chord callback `_post_market_finalize`
- Step 4: `dispatch_downstream` 触发 `compute_daily_signals` → `generate_daily_blueprint`
- Step 5: 安排 `coordination_timeout_check` 作为兜底超时检查

注意：

- `capture_post_market_data` / `capture_post_market_chunk` 不再盘后采集期权链
- 如果当天没有 intraday 5min snapshots，`option_daily` 和 `option_iv_daily` 为空是预期行为
- `stop_after` 可以在配置里裁剪下游链路

## Why `option_iv_daily` Exists

`option_iv_daily` 是按标的聚合后的日级 IV 汇总表，目的是让 signal service 快速计算 IV Rank / IV Percentile，而不是每次从大量 option rows 实时聚合。

典型字段：

- `avg_iv`
- `atm_iv`
- `call_iv`
- `put_iv`
- `sample_size`

这是一个典型的“为下游分析降维”的表，而不是面向交易直接执行的表。

## Data Model And Providers

Data Service 使用 fetcher registry 做 provider 抽象。

当前默认：

- `data_service.providers.stock = yfinance`
- `data_service.providers.options = yfinance`

切换 provider 的步骤：

- 实现对应 fetcher protocol
- 在 registry 注册
- 修改 `config/config.yaml`

相关目录：

- `services/data_service/app/fetchers`
- `services/data_service/app/storage.py`
- `services/data_service/app/cache.py`

## HTTP API

- Base URL: `http://localhost:8001`
- Docs: `http://localhost:8001/docs`
- Health: `GET /api/v1/health`

### Market Data Queries

- `GET /api/v1/data/{symbol}/stock`
- `GET /api/v1/data/{symbol}/stock/dates`
- `GET /api/v1/data/{symbol}/options`
- `GET /api/v1/data/{symbol}/options/dates`

### Manual Collection

- `POST /api/v1/data/collect/stock`
- `GET /api/v1/data/collect/{task_id}`

### Earnings

- `POST /api/v1/data/earnings`

## Manual Collection Rules

`POST /api/v1/data/collect/stock` 目前只做股票数据手动采集，支持：

- `bars_1m`
- `bars_daily`

对 `end_date` 有明确校验：

- 未来日期会返回 `422`
- 当天盘前会返回 `422`，并建议改到上一交易日
- 当天盘中会返回 `422`，要求盘后再跑
- 当天盘后允许正常执行

很多 `422` 会附带 `suggested_request_body`，可以直接重试。

## Workers And Queues

Data Service 主要依赖 `data` 队列 worker。

data worker 负责：

- 盘后统一流水线
- 股票盘后采集
- option_daily / option_iv_daily 聚合
- 手动股票采集
- 协调 downstream 派发
- timeout fallback

但完整盘后链路还需要：

- `signal` 队列 worker，用于 `compute_signals_chunk`
- `analysis` 队列 worker，用于 `generate_daily_blueprint`

如果你只启动 data worker，下游 signals / blueprint 不会自动完成。

## Local Run

从仓库根目录运行：

```bash
# 1) Install deps
uv sync --package data-service

# 2) Start API server
uv run uvicorn services.data_service.app.main:app --host 0.0.0.0 --port 8001 --reload

# 3) Start data worker
uv run celery -A shared.celery_app.celery_app worker -Q data -l info
```

如果要跑完整盘后流程，还应同时启动：

```bash
uv run celery -A shared.celery_app.celery_app worker -Q signal -l info
uv run celery -A shared.celery_app.celery_app worker -Q analysis -l info
```

如果你依赖定时调度，还需要 Celery Beat。

## Dev Notes

- 需要 TimescaleDB、Postgres、Redis、RabbitMQ 可用
- 首次运行前执行 `uv run python -m scripts.init_db`
- Data Service 本身不负责执行分析或交易，只负责数据层和流水线协调
- API 进程不是采集调度器；真正的盘后执行由 Celery tasks / Beat 驱动
