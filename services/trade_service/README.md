# Trade Service

Trade Service 负责 blueprint 加载、运行时调度、规则执行、入场/出场处理、止损监控、持仓查询和日报生成。当前是合并版服务：execution + portfolio 在同一个 FastAPI 进程中运行。

## Scope

- 启动时自动拉起 broker 与 execution scheduler
- 加载并管理当天 blueprint 的 runtime state
- 执行规则引擎 entry/exit 检查
- 运行 stop-loss 风控检查
- 提供 portfolio snapshot / positions / performance API
- 提供若干 Celery tasks 用于 load / finalize / report / intraday entry evaluation

核心文件：

- `services/trade_service/app/main.py`
- `services/trade_service/app/execution/scheduler.py`
- `services/trade_service/app/execution/routes.py`
- `services/trade_service/app/portfolio/routes.py`
- `services/trade_service/app/execution/tasks.py`
- `services/trade_service/app/execution/tasks_intraday.py`
- `services/trade_service/app/portfolio/tasks.py`

## Architecture Overview

Trade Service 当前有两套执行面：

- FastAPI 进程内 APScheduler
- Celery tasks

其中真正的运行时执行主循环在 FastAPI 进程里，而不是完全依赖 Celery worker。

启动应用时会：

- 初始化 logging
- 启动 broker
- 初始化 Redis
- 启动 execution scheduler

关闭应用时会：

- 停止 scheduler
- 关闭 broker
- 关闭 Redis pool

## Runtime Scheduler

执行主调度在 `execution/scheduler.py`，职责包括：

- 定时加载 blueprint
- 周期性执行 evaluation tick
- 调用 rule engine 检查 entry / exit
- 调用 risk monitor 做 stop-loss
- 应用 data quality gate
- 通过 broker 下单

设计要点：

- 有 distributed lock，避免多副本重复执行
- 有 entry confidence gate
- 有 entry cooldown
- 有 idempotency key
- 风控与执行事件会写审计日志

## Blueprint Lifecycle

Trade Service 关注的是 blueprint 的执行阶段，而不是生成阶段。

常见生命周期：

- analysis service 生成 blueprint，状态为 `pending`
- trade service 加载 blueprint，进入 runtime `active`
- 盘中执行 entry/exit 与 risk checks
- 收盘后 finalize，当天 blueprint 标记完成

运行时状态放在 `runtime_state`，包含：

- 当前加载的 blueprint id
- 当前 trading date
- 是否 paused
- 最近 tick / risk check 时间

## Intraday Optimizer

除主 execution scheduler 外，还存在一个 Celery 任务型的 intraday optimizer：

- 任务名：`trade_service.tasks.evaluate_entry_windows`
- 文件：`services/trade_service/app/execution/tasks_intraday.py`

它会：

- 仅在 market hours 内运行
- 读取当前 blueprint
- 对每个 symbol_plan 计算 intraday score
- 输出 `enter` / `wait` / `skip`
- 在 `auto` 模式下尝试直接下单
- 在 `notify` 模式下发送入场通知
- 把评分结果写入 Redis，供 dashboard 使用

该任务默认走 `data` 队列，不在 `trade` 专用队列上。

## Broker And Risk Model

默认 broker 是 paper broker，trade service 通过 broker abstraction 下单。

风控层包含：

- portfolio stop-loss
- per-position stop-loss
- cooldown 防重复止损
- data quality gate 对仓位缩减或跳过

Trade Service 本身不负责生成行情或信号，只消费 blueprint 和市场上下文。

## HTTP API

- Base URL: `http://localhost:8004`
- Docs: `http://localhost:8004/docs`
- Health: `GET /api/v1/health`
- Legacy health redirect: `GET /health`
- Business routes prefix: `/api/v1/trade`

### Execution APIs

- `GET /api/v1/trade/trade/blueprint/status`
- `POST /api/v1/trade/trade/blueprint/load`
- `POST /api/v1/trade/trade/override`

### Portfolio APIs

- `GET /api/v1/trade/portfolio/snapshot`
- `GET /api/v1/trade/portfolio/positions`
- `GET /api/v1/trade/portfolio/performance`

说明：当前 execution router 自身已经包含 `/trade/...` 前缀，再被 app 挂到 `/api/v1/trade` 下，所以最终路径会出现 `/trade/trade/...`。README 按当前代码真实路径记录。

## Celery Tasks

Trade Service 目前相关任务包括：

- `trade_service.tasks.load_daily_blueprint`
- `trade_service.tasks.finalize_daily_blueprint`
- `trade_service.tasks.evaluate_entry_windows`
- `trade_service.tasks.generate_daily_report`
- `trade_service.tasks.send_daily_report`

这些任务用于：

- 手工或定时加载 blueprint
- 日终 finalize
- 盘中 entry window 评分
- 生成和发送日报

## Local Run

从仓库根目录运行：

```bash
# 1) Install deps
uv sync --package trade-service

# 2) Start API server (this also starts the in-process scheduler)
uv run uvicorn services.trade_service.app.main:app --host 0.0.0.0 --port 8004 --reload
```

如果你还要运行 Celery 相关任务，需要额外启动对应 worker。当前比较相关的是 `data` 队列，因为 intraday optimizer 和日报发送任务使用该队列：

```bash
uv run celery -A shared.celery_app.celery_app worker -Q data -l info
```

如果要让整个平台联动，还需要 analysis、signal、data 侧服务和 worker 已经运行。

## Typical Dev Workflow

1. 启动 Postgres、TimescaleDB、Redis、RabbitMQ
2. 启动 analysis/data/signal 服务与对应 workers
3. 启动 trade service API
4. 确认某个交易日已有 `pending` blueprint
5. 调用 load blueprint 接口或等待定时加载
6. 查看 runtime state、portfolio snapshot、performance

## Dev Notes

- Trade Service 的核心调度逻辑在 FastAPI 进程内，不要误以为只开 worker 就够了
- Intraday optimizer 是额外的 Celery 路径，不替代主 execution scheduler
- 当前 execution API 路由有双重 `/trade` 前缀，这是现状文档化，不是 README 错误
