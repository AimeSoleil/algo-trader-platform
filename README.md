# 期权聚焦型量化交易平台

面向期权交易的量化微服务平台，采用“盘后智能 + 盘中机械执行”模式：盘后批量计算特征并生成次日交易蓝图，盘中仅按蓝图条件执行。

## 当前实现状态

已完成阶段 0/1 骨架搭建（可启动、可扩展）：

- 基础设施：TimescaleDB、PostgreSQL、Redis、RabbitMQ、MinIO、Prometheus、Grafana
- 共享层：Pydantic 模型、配置系统、双数据库会话、Celery 共享实例
- 服务层：Data / Backfill / Signal / Analysis / Trade / Gateway
- 可观测性：各服务内置 Prometheus `/metrics`，配套 Prometheus + Grafana
- 运维脚本：数据库初始化、watchlist 种子、Celery worker 启动脚本

## 快速开始

```bash
# 1) 复制环境变量
cp .env.example .env

# 2) 启动本地基础设施（默认，含 Prometheus + Grafana）
docker compose up -d

# 3) 安装 Python 依赖
uv sync --all-packages

# 4) 初始化双数据库与 Timescale hypertable
uv run python -m scripts.init_db

# 5) 初始化 watchlist
uv run python -m scripts.seed_watchlist

# 6) 启动 Celery workers + beat（本地开发）
./scripts/run_workers.sh              # 基础模式
./scripts/run_workers.sh --with-flower  # 含 Flower 监控面板

# 6b) 或使用 Docker 部署 workers（生产推荐）
docker compose --profile worker up -d
```

### 开发模式：服务按需逐个启动

```bash
# 只启动基础设施（timescaledb/postgres/redis/rabbitmq/minio/prometheus/grafana）
docker compose up -d

# 单独启动某个服务（示例：data_service）
docker compose up -d data_service

# 启动全部应用服务（data/signal/analysis/trade/gateway）
docker compose --profile app up -d

# 启动 Celery workers + beat + Flower（进程监控、自动重启、健康检查）
docker compose --profile worker up -d

# 启动全栈（应用 + Workers）
docker compose --profile app --profile worker up -d

# 查看所有容器状态
docker compose ps
```

## 核心调度模型

| 时间 | 服务 | 动作 |
|------|------|------|
| 09:20 | Trade | 加载当日 `llm_trading_blueprint` |
| 09:30-16:00（每5分钟） | Data | 拉取行情并写入 L1 内存 + L2 Parquet 缓存 |
| 09:30-16:00（每5分钟） | Rule Engine + Trade | 校验蓝图条件，触发下单 |
| 16:30 | Data (Celery) | 批量入库（股票1min + 期权5min） |
| 16:35 | Backfill (Celery) | 检测并补齐缺口 |
| 17:00 | Signal (Celery) | 批量计算特征（IV/PCR/趋势/曲面） |
| 17:10 | Analysis (Celery) | 生成次日蓝图并写入 PostgreSQL |

## 手动触发链路（Collect → Signal → Analysis）

当你不想等定时任务，可按下面顺序手动触发：

```bash
# 1) Data Service: 手动补数据（示例只拉日线）
curl -X POST http://localhost:8001/api/v1/collect \
  -H "Content-Type: application/json" \
  -d '{
    "symbols": ["AAPL","MSFT","TSLA","SPY","QQQ","AMZN","META","GOOGL","AMD","NVDA"],
    "start_date": "2025-01-01",
    "end_date": "2026-03-11",
    "data_types": ["bars_daily"]
  }'

# 轮询 data collect 任务状态
curl http://localhost:8001/api/v1/collect/<task_id>

# 说明：若 end_date=今日 且当前时间早于开盘，系统会自动将 end_date
# 归一化为上一个交易日，并在响应/任务结果中返回 warning。

# 2) Signal Service: 手动触发信号计算（默认 today_trading）
curl -X POST http://localhost:8002/api/v1/signals/compute \
  -H "Content-Type: application/json" \
  -d '{}'

# 轮询 signal 任务状态
curl http://localhost:8002/api/v1/signals/compute/<task_id>

# 3) Analysis Service: 手动触发分析（需先有 signal_features）
curl -X POST http://localhost:8003/api/v1/analyze \
  -H "Content-Type: application/json" \
  -d '{"symbols":["AAPL"],"trading_date":"2026-03-12"}'

# 轮询 analysis 任务状态
curl http://localhost:8003/api/v1/analyze/<task_id>

# 4) 通过 Gateway 查询（日期参数统一使用 trading_date）
curl "http://localhost:8000/signal/api/v1/signals/batch?trading_date=2026-03-12&symbols=AAPL&symbols=MSFT"
curl "http://localhost:8000/trade/api/v1/portfolio/performance?trading_date=2026-03-12"
curl "http://localhost:8000/trade/api/v1/blueprint/status?trading_date=2026-03-12"
```

运行上述接口前，请确认对应 worker 已启动并监听正确队列（`data` / `signal` / `analysis`）。

## 服务清单

| 服务 | 目录 | 说明 |
|------|------|------|
| Data Service | `services/data_service` | 盘中采集与双层缓存、盘后批量入库 |
| Backfill Service | `services/backfill_service` | 缺口检测、冷启动历史回填 |
| Signal Service | `services/signal_service` | 盘后批量指标计算与信号生成 |
| Analysis Service | `services/analysis_service` | LLM 蓝图生成（OpenAI / Copilot，自动回退） |
| Trade Service | `services/trade_service` | 蓝图加载、规则评估、止损风控、持仓快照、绩效查询 |
| Gateway Service | `services/gateway_service` | 聚合文档与服务反向代理入口 |

## 监控与指标

- 每个 API 服务都暴露自己的 Prometheus 指标端点：`/metrics`
- 默认已接入的服务：Gateway、Data、Signal、Analysis、Trade
- 指标采集由 [config/prometheus.yml](config/prometheus.yml) 配置，Prometheus 默认运行在 `http://localhost:9090`
- Grafana 默认运行在 `http://localhost:3000`，默认账号密码均为 `admin`
- 当前 MVP 先提供通用 HTTP 指标（请求数、延迟、响应大小等）；后续可在各服务内补充业务指标

### Celery Worker 监控

- **Flower 仪表盘**: `http://localhost:5555`（需要 `FLOWER_USER`/`FLOWER_PASSWORD` 认证）
- 查看 Worker 在线状态、任务执行历史、队列深度
- Flower 内建 Prometheus `/metrics` 端点，已接入 Prometheus 采集

### 常用检查方式：

```bash
# 查看 Prometheus targets
open http://localhost:9090/targets

# 查看 Grafana
open http://localhost:3000

# 直接查看某个服务的 metrics（示例：gateway）
curl http://localhost:8000/metrics
```

## LLM 配置

系统支持 **OpenAI** 和 **Copilot SDK** 两种 LLM provider，互为回退。在 `config/config.yaml` 的 `llm` 段按 provider 独立配置：

```yaml
llm:
  provider: "copilot"           # openai / copilot

  openai:
    api_key: ""                # 或通过环境变量 LLM__OPENAI__API_KEY 设置
    model: "gpt-4o"
    temperature: 0.1
    max_tokens: 8192

  copilot:
    cli_path: "copilot"
    github_token: ""            # 留空则使用已登录 GitHub 用户
    model: "gpt-4o"
    reasoning_effort: "medium"  # low / medium / high

  # ── Common ──
  cache_enabled: true
  cache_ttl: 3600
  skill_dir: ""                 # 留空自动解析
```

- `provider` 决定主 provider；失败时自动回退到另一个。
- OpenAI 支持 `model` / `temperature` / `max_tokens`；Copilot SDK 支持 `model` / `reasoning_effort`。

## 技术栈

- Python 3.11+
- FastAPI + gRPC
- SQLAlchemy Async + Pydantic v2
- Celery + RabbitMQ + Redis
- TimescaleDB（时序）+ PostgreSQL（业务）
- Docker Compose（本地）

## 设计文档

完整设计与决策记录见 [platform_design.md](platform_design.md)。
