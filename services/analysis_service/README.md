# Analysis Service

LLM 驱动的交易蓝图生成服务，当前采用 agentic-first 架构：多专家并行分析，再由 synthesizer 汇总并由 critic 审核，最终生成可审计、可落库、可执行门控的次日交易蓝图。

## What It Does

- 提供 daily blueprint 生成任务
- 提供 manual analyze API 与异步 Celery task
- 提供 blueprint 查询与 reasoning 查询 API
- 对 LLM 输出执行 JSON 修复、模型校验、deterministic validation
- 对不合格蓝图执行 soft-block，阻止自动执行

## Architecture Overview

核心入口：

- `services/analysis_service/app/llm/adapter.py`
- `services/analysis_service/app/llm/agents/orchestrator.py`
- `services/analysis_service/app/tasks/blueprint.py`
- `services/analysis_service/app/tasks/analyze.py`

主流程：

1. 从 `signal_features` 读取指定交易日信号
2. 获取当前持仓与上一个交易日执行摘要
3. 进入 agentic pipeline
4. 生成 `LLMTradingBlueprint`
5. 注入 data quality 标注
6. 执行 deterministic validation
7. 根据校验结果写入 `pending` / `cancelled` / `manual`
8. 刷新缓存并返回摘要

## Agentic Pipeline

当前 orchestrator 使用以下多阶段结构：

- 6 个 specialist agents 并行运行：`trend`、`volatility`、`flow`、`chain`、`spread`、`cross_asset`
- 计算 consensus 与 market condition
- `synthesizer` 产出最终 blueprint
- `critic` 审核 blueprint
- 若 critic 返回 `revise`，带反馈进入 revision synthesis
- 附加 `reasoning_context`，包含 agent outputs 与 critic history

实现文件：

- `services/analysis_service/app/llm/agents/orchestrator.py`
- `services/analysis_service/app/llm/agents/synthesizer_agent.py`
- `services/analysis_service/app/llm/agents/critic_agent.py`

## Chunking, Benchmarks, And Circuit Break

当交易标的数量超过 `analysis_service.llm.orchestrator_chunk_size` 时：

- trade symbols 会分块执行
- benchmark symbols 会注入每个 chunk，作为 cross-asset context
- chunk 结果会合并、去重，并仅保留交易标的的 plans
- 并发受 `analysis_service.llm.orchestrator_max_parallel` 限制

另外有 circuit-break：

- 如果某个交易标的在股票与期权两个维度都完全降级，会跳过 LLM 分析
- 避免坏数据进入后续 synthesis / critic

## JSON Reliability Design

Analysis Service 不依赖“模型自己乖乖输出 JSON”，而是采用多层保障：

1. Prompt 层：所有 agents 都要求只输出合法 JSON，并给出明确 schema
2. Provider 层：OpenAI provider 使用 `json_object`；Copilot provider 强制“只返回一个合法 JSON 对象”
3. JSON 工具层：统一清洗与修复常见脏输出
4. Pydantic 层：specialist、critic、blueprint 都经过强类型校验
5. Retry 层：JSON 解析失败、模型校验失败会触发指数退避重试

JSON 工具会处理：

- markdown code fences
- 单引号 JSON
- trailing commas
- Python `True` / `False` / `None`
- JSON 前后额外 prose
- 拼接 JSON / extra data 场景

相关文件：

- `services/analysis_service/app/llm/json_utils.py`
- `services/analysis_service/app/llm/agents/base_agent.py`
- `services/analysis_service/app/llm/agents/_openai_agent_provider.py`
- `services/analysis_service/app/llm/agents/_copilot_agent_provider.py`

## Blueprint Model And Validation

最终输出模型为 `LLMTradingBlueprint`，核心特性：

- `symbol_plans` 为结构化期权计划
- 每个 plan 具备 strategy type、direction、legs、entry/exit conditions、risk fields
- 使用 Pydantic 枚举和范围校验限制字段合法性
- 使用 model validator 校验 strategy 与 legs 数量一致性

例如：

- `option_type` 只能是 `call | put`
- `side` 只能是 `buy | sell`
- `confidence` 必须在 0 到 1 之间
- `vertical_spread` 必须恰好 2 legs
- `iron_condor` 必须恰好 4 legs

相关文件：

- `shared/models/blueprint.py`
- `services/analysis_service/app/llm/agents/models.py`

## Deterministic Validation And Soft-Block

LLM 生成蓝图后，Analysis Service 会执行确定性规则校验，并将摘要写入 `reasoning_context.deterministic_validation`。

- 校验来源：`services/analysis_service/app/evaluation/rule_checker.py`
- 结果字段：`passed`、`error_count`、`warning_count`、`errors[]`、`warnings[]`

### Soft-Block 策略

Daily 任务 `generate_daily_blueprint`：

1. 当 `error_count == 0`：状态写入 `pending`
2. 当 `error_count > 0`：状态写入 `cancelled`
3. 返回值包含：`status`、`soft_blocked`、`deterministic_validation`

Manual 任务 `manual_analyze`：

1. 状态固定为 `manual`
2. 返回值附带 `soft_blocked` 与 `deterministic_validation`，供人工审核

说明：soft-block 属于“非异常阻断”。任务本身可以成功完成，但状态会降级，阻止下游自动执行。

## Data Quality Annotation

在 blueprint 写库前，系统会把 signal data quality 注入每个 `symbol_plan`：

- `data_quality_score`
- `data_quality_warnings`
- `signal_data_quality`

同时汇总 blueprint 级别字段：

- `min_data_quality_score`
- `data_quality_summary`

这一步发生在 LLM 输出之后、deterministic validation 之前。

## Daily And Manual Tasks

### Daily Task

- 任务名：`analysis_service.tasks.generate_daily_blueprint`
- 文件：`services/analysis_service/app/tasks/blueprint.py`
- 特性：
  - `max_retries=0`
  - 失败立即失败，不自动重试
  - 成功/失败通知
  - Redis 去重，防止同一交易日终态通知重复发送

### Manual Task

- 任务名：`analysis_service.tasks.manual_analyze`
- 文件：`services/analysis_service/app/tasks/analyze.py`
- 特性：
  - 支持 `symbols` 字符串或列表
  - `max_retries=1`
  - 复用与 daily 相同的 `_run_blueprint_pipeline`
  - 结果状态写为 `manual`

## HTTP API

- Base URL: `http://localhost:8003`
- Docs: `http://localhost:8003/docs`
- Health: `GET /api/v1/health`
- Legacy health redirect: `GET /health`

### Routes

- `GET /api/v1/analysis/blueprint/{trading_date}`
  - 查询某天 blueprint
  - 支持 `symbols=AAPL,NVDA` 过滤 `symbol_plans`
  - 支持 `by_pass_cache=true`

- `GET /api/v1/analysis/blueprint/reasoning/{blueprint_id}`
  - 查询完整 reasoning context
  - 支持 `symbols=AAPL,NVDA` 过滤只返回相关 symbol 的分析

- `POST /api/v1/analysis`
  - 触发 manual analysis
  - body 中 `symbols` 可为列表或逗号分隔字符串
  - `signal_date` 可选

- `GET /api/v1/analysis/{task_id}`
  - 轮询 manual task 状态

## Configuration

配置来自：

- `config/config.yaml`
- 环境变量覆盖

关键配置项：

- `analysis_service.llm.provider`
- `analysis_service.llm.openai.*`
- `analysis_service.llm.copilot.*`
- `analysis_service.llm.agent_models_override.*`
- `analysis_service.llm.max_critic_revisions`
- `analysis_service.llm.orchestrator_chunk_size`
- `analysis_service.llm.orchestrator_max_parallel`
- `analysis_service.llm.max_retries`
- `analysis_service.llm.backoff_base_seconds`
- `analysis_service.llm.backoff_max_seconds`

当前默认 provider 为 `copilot`。

## Observability

当前 orchestrator 已增加阶段级日志，便于观察长耗时阶段：

- `orchestrator.phase_started`
- `orchestrator.phase_completed`
- `orchestrator.consensus_computed`
- `orchestrator.llm_usage_summary`
- `orchestrator.completed`

这些日志会覆盖：

- specialists
- synthesizer
- critic
- synthesizer_revision

并包含 `elapsed_s` 等耗时字段，用于定位真实瓶颈。

## Query Layer And Cache

查询层使用 Redis L1 + Postgres L2：

- blueprint 查询优先读缓存
- miss 后读 DB
- 命中 DB 后回填缓存
- manual analyze 完成后会主动失效相关缓存

相关文件：

- `services/analysis_service/app/queries.py`
- `services/analysis_service/app/cache.py`

## Tests

测试目录：`services/analysis_service/tests`

当前覆盖包括：

- `test_json_utils.py`
  - JSON 清洗与修复
- `test_blueprint_validation.py`
  - blueprint Pydantic 约束
- `test_rule_checker.py`
  - deterministic rule checker
- `test_adapter.py`
  - adapter 行为
- `test_prompts.py`
  - prompt 构造

建议运行：

```bash
uv run pytest services/analysis_service/tests -q
```

## Manual Start (Without Docker)

From repo root:

```bash
# 1) Install deps
uv sync --package analysis-service

# 2) Start API server
uv run uvicorn services.analysis_service.app.main:app --host 0.0.0.0 --port 8003 --reload

# 3) Start Celery worker for analysis tasks
uv run celery -A shared.celery_app.celery_app worker -Q analysis -l info
```

## Notes

- Requires Postgres, Redis, and RabbitMQ.
- LLM provider and runtime config are loaded from `config/config.yaml` and env vars.
- Analysis considers market context, current positions, previous execution, and risk budget.
- Analysis Service 负责“生成与校验 blueprint”；是否实际入场由下游 trade service 决定。
