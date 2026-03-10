"""Analysis Service — Celery 盘后蓝图生成任务"""
from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta

from sqlalchemy import text

from shared.celery_app import celery_app
from shared.config import get_settings
from shared.db.session import get_postgres_session
from shared.models.signal import SignalFeatures
from shared.utils import get_logger

logger = get_logger("analysis_tasks")


@celery_app.task(name="analysis_service.tasks.generate_daily_blueprint", bind=True, max_retries=2)
def generate_daily_blueprint(self, trading_date: str | None = None, prev_result=None) -> dict:
    """
    17:10 Celery 任务：生成次日交易蓝图
    prev_result: 上游任务 (compute_signals) 的结果
    """
    return asyncio.run(_generate_blueprint_async(trading_date))


async def _generate_blueprint_async(trading_date_str: str | None = None) -> dict:
    from services.analysis_service.app.llm.adapter import LLMAdapter

    settings = get_settings()
    td = date.fromisoformat(trading_date_str) if trading_date_str else date.today()

    # 1) 从 DB 读取当日信号特征
    signal_features: list[SignalFeatures] = []
    async with get_postgres_session() as session:
        result = await session.execute(
            text(
                "SELECT features_json FROM signal_features "
                "WHERE date = :date"
            ),
            {"date": td},
        )
        for row in result.fetchall():
            try:
                sf = SignalFeatures.model_validate_json(row[0])
                signal_features.append(sf)
            except Exception as e:
                logger.warning("blueprint.signal_parse_error", error=str(e))

    if not signal_features:
        logger.warning("blueprint.no_signals", date=str(td))
        return {"error": "No signal features available", "date": str(td)}

    # 2) 读取前日执行摘要（如有）
    previous_execution = None
    yesterday = td - timedelta(days=1)
    async with get_postgres_session() as session:
        result = await session.execute(
            text(
                "SELECT execution_summary FROM llm_trading_blueprint "
                "WHERE trading_date = :date AND status = 'completed'"
            ),
            {"date": yesterday},
        )
        row = result.fetchone()
        if row:
            previous_execution = row[0]

    # 3) 调用 LLM 生成蓝图
    adapter = LLMAdapter()
    blueprint = await adapter.generate_blueprint(
        signal_features=signal_features,
        current_positions=None,  # TODO: read from portfolio service
        previous_execution=previous_execution,
    )

    # 4) 写入 DB
    async with get_postgres_session() as session:
        await session.execute(
            text(
                "INSERT INTO llm_trading_blueprint "
                "(id, trading_date, generated_at, model_provider, model_version, blueprint_json, status) "
                "VALUES (:id, :trading_date, :generated_at, :model_provider, :model_version, :blueprint_json, 'pending') "
                "ON CONFLICT (trading_date) DO UPDATE SET "
                "blueprint_json = :blueprint_json, generated_at = :generated_at, "
                "model_provider = :model_provider, model_version = :model_version, status = 'pending'"
            ),
            {
                "id": blueprint.id,
                "trading_date": blueprint.trading_date,
                "generated_at": blueprint.generated_at,
                "model_provider": blueprint.model_provider,
                "model_version": blueprint.model_version,
                "blueprint_json": blueprint.model_dump_json(),
            },
        )

    logger.info(
        "blueprint.generated",
        trading_date=str(blueprint.trading_date),
        plans=len(blueprint.symbol_plans),
        provider=blueprint.model_provider,
    )
    return {
        "trading_date": str(blueprint.trading_date),
        "blueprint_id": blueprint.id,
        "plans_count": len(blueprint.symbol_plans),
        "provider": blueprint.model_provider,
    }
