# Trade Service

合并执行与持仓能力的服务（execution + portfolio）。

## What it does

- 蓝图加载/状态查询与手动暂停恢复
- 执行调度与风控止损监控
- 持仓快照、持仓明细、绩效查询
- Celery 任务：`trade_service.tasks.load_daily_blueprint`、`trade_service.tasks.finalize_daily_blueprint`、`trade_service.tasks.generate_daily_report`

## HTTP API

- Base URL: `http://localhost:8004`
- Docs: `http://localhost:8004/docs`
- Health: `GET /health`
- All business routes are under: `/api/v1/trade`
