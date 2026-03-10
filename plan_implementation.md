# 实现计划总结 — 期权聚焦型量化交易平台

> 本文档用于跟踪从设计到落地的全部工作进度，方便随时恢复上下文继续开发。

---

## 一、核心设计决策回顾

| # | 决策点 | 最终方案 | 理由 |
|---|--------|----------|------|
| 1 | 盘中黑天鹅应对 | **人工通过 Dashboard 干预**，不开放盘中 LLM 通道 | 降低盘中复杂度；极端行情由人判断更安全 |
| 2 | 任务调度器 | **Celery + Beat** | 成熟的链式编排（chord/chain），原生支持队列路由与重试 |
| 3 | 数据回填策略 | **同时支持**冷启动历史初始化 + 盘中缺口补全 | 首次部署需大量历史，日常需补小缺口 |
| 4 | 运行模式 | 盘后智能（LLM 生成蓝图）+ 盘中机械执行（规则匹配） | 盘中零 LLM 调用，延迟可控 |
| 5 | 数据写入 | 盘中仅 L1 内存 + L2 Parquet 缓存，16:30 批量入库 | 减少交易时段 DB 压力 |
| 6 | 工程结构 | Monorepo 微服务，uv workspace | 共享模型/配置，本地 Docker Compose，生产迁 K8s |

---

## 二、分阶段实现计划

### Phase 0 — 项目骨架搭建 ✅ 已完成

**目标**：所有服务目录、模型、配置、基础设施就位，`docker-compose up -d` 能正常启动。

| 任务 | 状态 | 产出文件 |
|------|------|----------|
| 根项目配置 | ✅ | `pyproject.toml`, `docker-compose.yml`, `.env.example`, `.gitignore`, `README.md` |
| 共享模型层 | ✅ | `shared/models/{option,blueprint,signal,order,portfolio}.py` |
| 共享配置/DB 层 | ✅ | `shared/config/settings.py`, `shared/db/{session,tables}.py`, `shared/utils/logging.py` |
| Celery + 配置 + gRPC Proto | ✅ | `shared/celery_app.py`, `config/config.yaml`, `proto/*.proto` |
| Data Service | ✅ | `services/data_service/app/{main,routes,cache,scheduler,tasks}.py`, `fetchers/` |
| Backfill Service | ✅ | `services/backfill_service/app/{backfiller,gap_detector,tasks}.py` |
| Signal Service | ✅ | `services/signal_service/app/{main,tasks,signal_generator,queries}.py`, `indicators/` |
| Analysis Service | ✅ | `services/analysis_service/app/{main,queries,tasks}.py`, `llm/{adapter,base,openai_provider,copilot_provider,prompts}.py`, `skills/trading-analysis/` |
| Execution Service | ✅ | `services/execution_service/app/{main,routes,blueprint_loader,rule_engine,scheduler,tasks,models}.py`, `broker/{base,paper}.py` |
| Portfolio Service | ✅ | `services/portfolio_service/app/{main,routes,service,tasks}.py` |
| Monitoring Service | ✅ | `services/monitoring_service/app/{main,routes,metrics}.py` |
| DB 初始化 & 运维脚本 | ✅ | `scripts/{init_db.py,seed_watchlist.py,run_workers.sh}` |
| Docker Compose 服务互连 | ✅ | `docker-compose.yml` 已包含 7 个微服务 + 基础设施 |
| 文档同步 | ✅ | `README.md`, `platform_design.md` 更新至 V3.1 |

---

### Phase 1 — 本地可运行（Data Pipeline 端到端）🔲 未开始

**目标**：`docker-compose up` → 数据采集 → 缓存 → 批量入库 → 回填，全链路跑通。

| # | 任务 | 详细描述 | 预计工作量 |
|---|------|----------|-----------|
| 1.1 | 配置 `.env` 实际值 | 填入 TimescaleDB/PostgreSQL/Redis/RabbitMQ 连接串；按使用的 LLM provider 配置对应 key（OpenAI: `openai_api_key`；Copilot: `copilot_github_token` 或使用已登录用户） | 0.5h |
| 1.2 | `docker-compose up -d` 验证 | 确保所有容器健康启动，检查端口映射 | 0.5h |
| 1.3 | `uv sync` 安装依赖 | 解决所有 import 问题 | 0.5h |
| 1.4 | 运行 `init_db.py` | 创建双数据库 schema + TimescaleDB hypertable | 0.5h |
| 1.5 | 运行 `seed_watchlist.py` | 写入 watchlist（AAPL, TSLA, SPY 等） | 0.5h |
| 1.6 | 手动触发 Data Service 采集 | 调用 `/api/fetch/{symbol}` 验证 Yahoo Finance 数据拉取 | 1h |
| 1.7 | 验证双层缓存 | 检查 L1 内存数据 + L2 Parquet 文件正确写入 | 1h |
| 1.8 | 触发 `batch_flush_to_db` | 手动调用 Celery task 验证 TimescaleDB 入库 | 1h |
| 1.9 | 触发 Backfill | 运行 gap_detector + backfiller，验证缺口补全 | 1h |
| 1.10 | 端到端 Celery 链测试 | `batch_flush → backfill → compute_signals → generate_blueprint` 全链路 | 2h |

---

### Phase 2 — Signal + Analysis 链路 🔲 未开始

**目标**：盘后信号计算能输出 `SignalFeatures`，LLM 能生成有效 `LLMTradingBlueprint`。

| # | 任务 | 详细描述 |
|---|------|----------|
| 2.1 | Signal Service 单元测试 | `option_indicators.py` 的 IV Rank / PCR / Skew 计算正确性 |
| 2.2 | Signal Service 集成测试 | 从 TimescaleDB 读取历史数据 → 计算 → 写入 signal_features 表 |
| 2.3 | LLM Prompt 调优 | 用真实 SignalFeatures 数据测试 prompt，确保结构化 JSON 输出可被 `LLMTradingBlueprint.model_validate` 解析 |
| 2.4 | LLM Fallback 测试 | Primary provider 失败后自动回退 secondary（OpenAI ↔ Copilot 双向） |
| 2.5 | Blueprint 持久化测试 | 验证蓝图写入 `blueprint_records` 表 + 状态管理 |

---

### Phase 3 — Execution 链路 🔲 未开始

**目标**：盘中规则引擎能正确加载蓝图、评估条件、通过 PaperBroker 模拟下单。

| # | 任务 | 详细描述 |
|---|------|----------|
| 3.1 | TriggerCondition 单元测试 | 覆盖全部 `ConditionOperator`（>, >=, <, <=, ==, between, crosses_above, crosses_below） |
| 3.2 | BlueprintLoader 集成测试 | 09:20 从 DB 加载蓝图，验证状态转为 `active` |
| 3.3 | RuleEngine 端到端测试 | 模拟市场数据 → 条件匹配 → PaperBroker 下单 → 订单记录 |
| 3.4 | APScheduler 验证 | 5 分钟调度循环正常触发 |
| 3.5 | 多蓝图并行测试 | 同时加载 3+ 个 symbol 的蓝图，验证互不干扰 |

---

### Phase 4 — Portfolio + Monitoring 🔲 未开始

**目标**：持仓跟踪、绩效计算、Prometheus 指标可用。

| # | 任务 | 详细描述 |
|---|------|----------|
| 4.1 | 持仓快照 | PaperBroker 成交后 → PortfolioService 记录持仓 |
| 4.2 | Greeks 聚合 | 期权持仓的 Delta/Gamma/Theta/Vega 组合级汇总 |
| 4.3 | 每日绩效报表 | Celery task 盘后生成日报 |
| 4.4 | Prometheus 指标 | 所有 Counter/Gauge 上报正确（order_count, active_blueprints, portfolio_pnl 等） |
| 4.5 | 健康检查 | `/health` 端点检查 DB + Redis + RabbitMQ 连通 |

---

### Phase 5 — 测试体系与 CI 🔲 未开始

**目标**：单元测试覆盖核心逻辑，CI pipeline 可运行。

| # | 任务 | 详细描述 |
|---|------|----------|
| 5.1 | pytest 框架搭建 | conftest.py、fixtures（mock DB session, mock Redis, mock broker）|
| 5.2 | 共享模型测试 | blueprint/option/order/signal 模型序列化与校验 |
| 5.3 | 服务层单元测试 | 每个服务至少覆盖核心函数（~70% coverage） |
| 5.4 | 集成测试 | Testcontainers 启动 TimescaleDB + PostgreSQL + Redis |
| 5.5 | CI Pipeline | GitHub Actions: lint → type-check → test → build Docker images |
| 5.6 | Ruff + Mypy 修复 | 全量 lint 和类型检查通过 |

---

### Phase 6 — 实盘对接准备 🔲 未开始

**目标**：接入真实 Broker API（如 IBKR / Alpaca），替换 PaperBroker。

| # | 任务 | 详细描述 |
|---|------|----------|
| 6.1 | Broker SDK 集成 | 实现 `IBKRBroker(BrokerInterface)` 或 `AlpacaBroker(BrokerInterface)` |
| 6.2 | 实时行情接入 | WebSocket 行情替代 Yahoo Finance 轮询 |
| 6.3 | 风控模块加固 | 保证金检查、最大持仓限制、日内亏损上限 |
| 6.4 | Dashboard UI | 前端（React/Streamlit）展示蓝图状态、持仓、PnL、手动干预按钮 |
| 6.5 | Kubernetes 部署 | Helm Chart + HPA，从 Compose 迁移至 K8s |
| 6.6 | 可观测性 | Grafana Dashboard + AlertManager 报警规则 |

---

## 三、完整文件清单（Phase 0 产出）

```
algo-trader-platform/
├── .env.example
├── .gitignore
├── README.md
├── docker-compose.yml
├── platform_design.md
├── pyproject.toml
├── config/
│   └── config.yaml
├── proto/
│   ├── data_service.proto
│   ├── execution_service.proto
│   └── signal_service.proto
├── scripts/
│   ├── README.txt
│   ├── init_db.py
│   ├── run_workers.sh
│   └── seed_watchlist.py
├── shared/
│   ├── __init__.py
│   ├── celery_app.py
│   ├── pyproject.toml
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py
│   ├── db/
│   │   ├── __init__.py
│   │   ├── session.py
│   │   └── tables.py
│   ├── models/
│   │   ├── __init__.py
│   │   ├── blueprint.py    ← 核心：LLMTradingBlueprint + TriggerCondition
│   │   ├── option.py       ← OptionContract, OptionChainSnapshot, Greeks
│   │   ├── order.py        ← Order (multi-leg), OrderLeg, OrderStatus
│   │   ├── portfolio.py    ← OptionPosition, PortfolioSnapshot
│   │   └── signal.py       ← SignalFeatures, OptionIndicators
│   └── utils/
│       ├── __init__.py
│       └── logging.py
└── services/
    ├── data_service/         (port 8001)
    ├── backfill_service/     (Celery worker only)
    ├── signal_service/       (port 8002)
    ├── analysis_service/     (port 8003)
    ├── execution_service/    (port 8004)
    ├── portfolio_service/    (port 8005)
    └── monitoring_service/   (port 8006)
```

共 **83 个文件**，7 个微服务 + 1 个共享库。

---

## 四、关键模型快速参考

### 4.1 LLMTradingBlueprint（交易蓝图）
```
LLMTradingBlueprint
├── blueprint_id: UUID
├── generated_at: datetime
├── valid_date: date
├── market_regime: str (bullish/bearish/neutral/volatile)
├── confidence_score: float [0,1]
├── status: BlueprintStatus (draft/active/partially_filled/completed/expired/cancelled)
├── plans: list[SymbolPlan]
│   ├── symbol: str
│   ├── strategy_type: StrategyType (12种)
│   ├── direction: str
│   ├── entry_conditions: list[TriggerCondition]  ← 盘中评估
│   ├── exit_conditions: list[TriggerCondition]
│   ├── adjustment_rules: list[AdjustmentRule]
│   ├── option_legs: list[OptionLeg]
│   ├── max_position_size: int
│   └── max_loss_per_trade: float
└── metadata: dict
```

### 4.2 TriggerCondition（触发条件）
支持的 operator：`>`, `>=`, `<`, `<=`, `==`, `between`, `crosses_above`, `crosses_below`
支持的 field：`price`, `iv`, `iv_rank`, `delta`, `gamma`, `theta`, `vega`, `pcr`, `volume`, `open_interest`, `rsi`, `macd`, `bid_ask_spread`

### 4.3 Celery 盘后流水线
```
batch_flush_to_db (16:30)
    → detect_and_backfill (16:35)
        → compute_signals (17:00)
            → generate_blueprint (17:10)
```
队列路由：`data_queue`, `backfill_queue`, `signal_queue`, `analysis_queue`

---

## 五、已知问题 & 技术债

| # | 问题 | 影响 | 优先级 |
|---|------|------|--------|
| 1 | 所有服务的 `tests/` 目录仅有 `__init__.py` | 无测试覆盖 | Phase 5 解决 |
| 2 | `platform_design.md` 有 ~50 条 markdownlint 格式告警 | 仅影响格式，不影响内容 | 低 |
| 3 | Yahoo Finance 作为唯一数据源，可能被限速 | 生产环境需替换 | Phase 6 |
| 4 | PaperBroker 无滑点/延迟模拟 | 回测结果可能偏乐观 | Phase 3 可加 |
| 5 | 未实现 gRPC server 端 | proto 已定义，server 端未实现 | Phase 2 |
| 6 | Alembic migration 尚未配置 | 目前用 `init_db.py` 直接 create_all | Phase 1 |
| 7 | 前端 Dashboard 未实现 | 黑天鹅人工干预需要 UI | Phase 6 |

---

## 六、下次继续工作的入口

**推荐从 Phase 1 启动**，按以下顺序：

```bash
# 1. 确保 Docker Desktop 运行中
docker-compose up -d timescaledb postgres redis rabbitmq minio

# 2. 安装依赖
uv sync

# 3. 初始化数据库
uv run python scripts/init_db.py

# 4. 种子数据
uv run python scripts/seed_watchlist.py

# 5. 启动 Data Service
uv run uvicorn services.data_service.app.main:app --host 0.0.0.0 --port 8001

# 6. 测试数据拉取
curl http://localhost:8001/api/fetch/AAPL

# 7. 手动触发批量入库
uv run celery -A shared.celery_app call data_service.app.tasks.batch_flush_to_db
```

Phase 1 完成标志：上述步骤全部无错误执行，TimescaleDB 中可查到 AAPL 的 1min K 线和期权快照数据。

---

*最后更新：2026-03-10*
