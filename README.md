# 期权聚焦型量化交易平台

面向期权交易的量化微服务平台，采用“盘后智能 + 盘中机械执行”模式：盘后批量计算特征并生成次日交易蓝图，盘中仅按蓝图条件执行。

## 当前实现状态

已完成阶段 0/1 骨架搭建（可启动、可扩展）：

- 基础设施：TimescaleDB、PostgreSQL、Redis、RabbitMQ、MinIO
- 共享层：Pydantic 模型、配置系统、双数据库会话、Celery 共享实例
- 服务层：Data / Backfill / Signal / Analysis / Execution / Portfolio / Monitoring
- 运维脚本：数据库初始化、watchlist 种子、Celery worker 启动脚本

## 快速开始

```bash
# 1) 复制环境变量
cp .env.example .env

# 2) 启动本地基础设施 + 服务
docker-compose up -d

# 3) 安装 Python 依赖
uv sync

# 4) 初始化双数据库与 Timescale hypertable
uv run python scripts/init_db.py

# 5) 初始化 watchlist
uv run python scripts/seed_watchlist.py

# 6) 启动 Celery workers + beat（可选）
./scripts/run_workers.sh
```

## 核心调度模型

| 时间 | 服务 | 动作 |
|------|------|------|
| 09:20 | Execution | 加载当日 `llm_trading_blueprint` |
| 09:30-16:00（每5分钟） | Data | 拉取行情并写入 L1 内存 + L2 Parquet 缓存 |
| 09:30-16:00（每5分钟） | Rule Engine + Execution | 校验蓝图条件，触发下单 |
| 16:30 | Data (Celery) | 批量入库（股票1min + 期权5min） |
| 16:35 | Backfill (Celery) | 检测并补齐缺口 |
| 17:00 | Signal (Celery) | 批量计算特征（IV/PCR/趋势/曲面） |
| 17:10 | Analysis (Celery) | 生成次日蓝图并写入 PostgreSQL |

## 服务清单

| 服务 | 目录 | 说明 |
|------|------|------|
| Data Service | `services/data_service` | 盘中采集与双层缓存、盘后批量入库 |
| Backfill Service | `services/backfill_service` | 缺口检测、冷启动历史回填 |
| Signal Service | `services/signal_service` | 盘后批量指标计算与信号生成 |
| Analysis Service | `services/analysis_service` | LLM 蓝图生成（OpenAI / Copilot，自动回退） |
| Execution Service | `services/execution_service` | 蓝图加载、规则评估、Paper Broker |
| Portfolio Service | `services/portfolio_service` | 持仓快照、绩效查询、报表任务 |
| Monitoring Service | `services/monitoring_service` | 健康检查、Prometheus 指标暴露 |

## LLM 配置

系统支持 **OpenAI** 和 **Copilot SDK** 两种 LLM provider，互为回退。在 `config/config.yaml` 的 `llm` 段按 provider 独立配置：

```yaml
llm:
  provider: "copilot"           # openai / copilot

  # ── OpenAI ──
  openai_api_key: ""            # 或通过环境变量 LLM__OPENAI_API_KEY 设置
  openai_model: "gpt-4o"
  openai_temperature: 0.1
  openai_max_tokens: 4096

  # ── Copilot SDK ──
  copilot_cli_path: "copilot"
  copilot_github_token: ""      # 留空则使用已登录 GitHub 用户
  copilot_model: "gpt-4o"
  copilot_temperature: 0.1
  copilot_max_tokens: 4096

  # ── Common ──
  cache_enabled: true
  cache_ttl: 3600
  skill_dir: ""                 # 留空自动解析
```

- `provider` 决定主 provider；失败时自动回退到另一个。
- 每个 provider 的 `model` / `temperature` / `max_tokens` 独立配置，互不干扰。

## 技术栈

- Python 3.11+
- FastAPI + gRPC
- SQLAlchemy Async + Pydantic v2
- Celery + RabbitMQ + Redis
- TimescaleDB（时序）+ PostgreSQL（业务）
- Docker Compose（本地）

## 设计文档

完整设计与决策记录见 [platform_design.md](platform_design.md)。
