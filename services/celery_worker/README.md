# Celery Worker

`celery_worker` 不是一个独立业务服务，而是平台所有后台任务共用的 Celery 运行镜像与入口配置。它承载 data、signal、analysis、trade 相关任务，也负责 Beat 定时调度和 Flower 监控。

## Scope

- 提供通用 Celery worker 运行镜像
- 加载所有服务的 Celery tasks
- 定义统一的 broker / backend / beat schedule
- 支持按队列拆分 worker 容器
- 支持运行 `celery-beat` 和 `flower`

核心文件：

- `shared/celery_app.py`
- `services/celery_worker/Dockerfile`
- `docker-compose.worker.yml`

## Architecture Overview

当前 Celery 设计不是“每个服务一个独立 Python worker 镜像”，而是：

- 一个通用镜像
- 多个按队列区分的 worker 进程/容器
- 一个 beat 调度器
- 一个 Flower 监控面板

也就是说，`celery_worker` 的职责是“运行时载体”，不是“业务域本身”。

## Included Tasks

Celery app 当前 include 了这些任务模块：

- data service
  - capture
  - intraday
  - intraday_stock
  - aggregation
  - pipeline
  - coordination
  - earnings
  - manual
- signal service
  - `services.signal_service.app.tasks.signal`
- analysis service
  - blueprint
  - analyze
- trade service
  - execution tasks
  - intraday tasks
  - portfolio tasks
  - daily report task

这意味着任意一个 worker 容器都能 import 全部任务模块，但实际消费哪个任务由队列决定。

## Queues And Routing

当前任务路由：

- `data_service.tasks.*` → `data`
- `signal_service.tasks.*` → `signal`
- `analysis_service.tasks.*` → `analysis`
- `trade_service.tasks.*` → `data`

注意：trade tasks 目前也走 `data` 队列，不是单独的 `trade` 队列。

## Worker Roles

在 `docker-compose.worker.yml` 中，默认定义了：

- `celery-data`
- `celery-signal`
- `celery-analysis`
- `celery-beat`
- `flower`

### `celery-data`

负责：

- data service 全部后台任务
- trade service 当前的 Celery tasks
- 例如 post-market pipeline、aggregation、manual collect、intraday optimizer、daily report

### `celery-signal`

负责：

- `signal_service.tasks.compute_daily_signals`
- `signal_service.tasks.compute_signals_chunk`

### `celery-analysis`

负责：

- `analysis_service.tasks.generate_daily_blueprint`
- `analysis_service.tasks.manual_analyze`

### `celery-beat`

负责：

- 定时调度所有平台级任务
- 使用 RedBeat，把调度状态与锁存放在 Redis

### `flower`

负责：

- 提供 Celery 监控 UI
- 查看 worker、任务、失败情况、broker 状态

## Beat Schedule

Beat 调度定义在 `shared/celery_app.py`。

当前关键计划包括：

- `post-market-pipeline`
- `intraday-option-capture`
- `intraday-option-capture-close`
- `intraday-stock-capture`
- `intraday-stock-capture-close`
- `refresh-earnings-cache`
- `intraday-entry-optimizer`
- `daily-trading-report`（启用 notifier 时）

这意味着 Celery 层是整个平台时间驱动自动化的核心入口。

## Broker, Backend, And Scheduler

当前配置：

- Broker: RabbitMQ
- Result backend: Redis DB 1
- RedBeat storage: Redis DB 2

Celery 通用运行参数来自：

- `common.celery.*`
- `common.beat.*`

例如：

- `task_acks_late`
- `task_soft_time_limit`
- `task_time_limit`
- `worker_prefetch_multiplier`
- `worker_max_memory_per_child`

## Docker Image Design

`services/celery_worker/Dockerfile` 是一个通用多阶段镜像：

- builder stage 安装 shared + data + signal + analysis + trade 所有包
- runtime stage 复制全部代码与已安装依赖
- 默认 ENTRYPOINT 是 `celery -A shared.celery_app.celery_app`

因此同一镜像可以运行：

- worker
- beat
- flower

区别只在命令参数，而不在镜像本身。

## Local Run

### Run a Single Queue Worker

从仓库根目录运行：

```bash
uv run celery -A shared.celery_app.celery_app worker -Q data -l info
uv run celery -A shared.celery_app.celery_app worker -Q signal -l info
uv run celery -A shared.celery_app.celery_app worker -Q analysis -l info
```

### Run Beat

```bash
uv run celery -A shared.celery_app.celery_app beat -l info
```

注意：本地直接跑 beat 时，如果你想完全复现 Docker 里的行为，需要确保 RedBeat 依赖已安装并且 Redis 可用。

### Run Flower

```bash
uv run celery -A shared.celery_app.celery_app flower --port=5555
```

## Docker Compose Run

完整 worker 栈通常这样启动：

```bash
docker compose -f docker-compose.yml -f docker-compose.worker.yml up -d
```

按队列扩容示例：

```bash
docker compose -f docker-compose.yml -f docker-compose.worker.yml up -d \
  --scale celery-data=2 --scale celery-signal=2
```

## Operational Notes

- 如果只启动 app 服务、不启动对应 queue worker，很多 API 只能入队，任务不会真正执行
- 如果不启动 beat，所有定时采集/盘后流水线/日报都不会自动触发
- 如果 RabbitMQ 或 Redis 异常，Celery 层会直接影响整个自动化链路
- `celery-analysis` 建议低并发；`celery-data` 和 `celery-signal` 更适合做横向扩容
