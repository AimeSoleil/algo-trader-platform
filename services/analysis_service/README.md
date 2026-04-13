# Analysis Service

LLM 驱动的交易蓝图生成服务。

## What it does

- 提供蓝图查询 API
- 提供手动分析 API（异步 Celery task）
- 盘后任务：`analysis_service.tasks.generate_daily_blueprint`

## Deterministic Validation & Soft-Block

Analysis Service 在 LLM 生成蓝图后会执行确定性校验（rule checker），并将摘要写入 `reasoning_context.deterministic_validation`。

- 校验来源：`services/analysis_service/app/evaluation/rule_checker.py`
- 结果字段：`passed`、`error_count`、`warning_count`、`errors[]`、`warnings[]`

### Soft-Block 策略（当前默认）

- Daily 任务（`generate_daily_blueprint`）

1. 当 `error_count == 0`：状态写入 `pending`（可被次日加载执行）
2. 当 `error_count > 0`：状态写入 `cancelled`（soft-block，不自动执行）
3. 任务返回值包含：`status`、`soft_blocked`、`deterministic_validation`

- Manual 任务（`manual_analyze`）

1. 状态保持 `manual`
2. 返回值附带 `soft_blocked` 与 `deterministic_validation` 供人工审核

说明：soft-block 为“非异常阻断”。任务不会失败重试，但会通过状态降级阻止自动执行。

## HTTP API

- Base URL: `http://localhost:8003`
- Docs: `http://localhost:8003/docs`
- Health: `GET /health`
- Blueprint query: `GET /api/v1/blueprint/{trading_date}`
- Manual analyze: `POST /api/v1/analyze`
- Task status: `GET /api/v1/analyze/{task_id}`

## Manual start (without Docker)

From repo root:

```bash
# 1) Install deps (workspace)
uv sync --package analysis-service

# 2) Start API server
uv run uvicorn services.analysis_service.app.main:app --host 0.0.0.0 --port 8003 --reload

# 3) (Optional) start Celery worker for analysis tasks
uv run celery -A shared.celery_app.celery_app worker -Q analysis -l info
```

## Notes

- Requires Postgres + Redis + RabbitMQ running.
- LLM provider/config is loaded from `config/config.yaml` and env vars.
- The analysis shall consider 市场环境、持仓状态、风险预算
