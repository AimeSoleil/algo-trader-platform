"""Stock post-market pipeline — capture stock data and signal readiness for coordination."""
from __future__ import annotations

import asyncio

from celery import chord, group

from shared.celery_app import celery_app
from shared.config import get_settings
from shared.pipeline import chunk_symbols
from shared.redis_pool import get_redis
from shared.utils import get_logger, today_trading

from services.data_service.app.tasks.capture import capture_post_market_chunk

logger = get_logger("data_tasks")

_FLAG_TTL_SECONDS = 86_400  # 24 h


def _stock_done_key(trading_date: str) -> str:
    return f"pipeline:stock_done:{trading_date}"


# ── Pipeline 入口 ──────────────────────────────────────────


@celery_app.task(name="data_service.tasks.run_stock_pipeline", queue="data")
def run_stock_pipeline(trading_date: str | None = None) -> str:
    """盘后股票采集流水线（18:30 ET 由 Beat 触发）

    1. Fan-out capture_post_market_chunk (chord)
    2. Set Redis flag ``pipeline:stock_done:{date}``
    3. Trigger coordination check
    """
    td = trading_date or today_trading().isoformat()

    settings = get_settings()
    symbols = settings.common.watchlist.all
    chunk_size = settings.data_service.worker.pipeline.chunk_size
    chunks = chunk_symbols(symbols, chunk_size)

    logger.info(
        "stock_pipeline.start",
        trading_date=td,
        total_symbols=len(symbols),
        chunks=len(chunks),
    )

    # Build chord: parallel chunk capture → barrier → flag + coordination
    pipeline = chord(
        group(
            capture_post_market_chunk.si(chunk, td).set(queue="data")
            for chunk in chunks
        ),
        _stock_pipeline_finalize.si(td).set(queue="data"),
    )
    result = pipeline.apply_async()

    logger.info("stock_pipeline.dispatched", trading_date=td, task_id=str(result.id))
    return f"Stock pipeline started: {result.id}"


@celery_app.task(
    name="data_service.tasks._stock_pipeline_finalize",
    queue="data",
)
def _stock_pipeline_finalize(results, trading_date: str) -> dict:
    """Chord callback — set done-flag and trigger coordination."""
    logger.info(
        "stock_pipeline.capture_complete",
        trading_date=trading_date,
        chunks=len(results) if isinstance(results, list) else 1,
    )

    # Set Redis flag
    asyncio.run(_set_done_flag(trading_date))
    logger.info("stock_pipeline.flag_set", trading_date=trading_date)

    # Trigger coordination
    from services.data_service.app.tasks.coordination import check_pipelines_and_continue

    check_pipelines_and_continue.delay(trading_date)

    # Schedule timeout check
    _schedule_timeout_check(trading_date)

    return {"status": "stock_pipeline_complete", "trading_date": trading_date}


def _schedule_timeout_check(trading_date: str) -> None:
    """Schedule a coordination timeout check after configured minutes."""
    settings = get_settings()
    timeout_minutes = settings.data_service.worker.pipeline.coordination_timeout_minutes
    timeout_seconds = timeout_minutes * 60

    from services.data_service.app.tasks.coordination import coordination_timeout_check

    coordination_timeout_check.apply_async(
        args=[trading_date],
        countdown=timeout_seconds,
        queue="data",
    )
    logger.info(
        "stock_pipeline.timeout_scheduled",
        trading_date=trading_date,
        timeout_minutes=timeout_minutes,
    )


async def _set_done_flag(trading_date: str) -> None:
    redis = get_redis()
    await redis.set(_stock_done_key(trading_date), "1", ex=_FLAG_TTL_SECONDS)


# ── Legacy alias (backward compat for manual invocations) ──


@celery_app.task(name="data_service.tasks.run_post_market_pipeline", queue="data")
def run_post_market_pipeline(trading_date: str | None = None) -> str:
    """Deprecated — redirects to run_stock_pipeline."""
    logger.warning("run_post_market_pipeline is deprecated, use run_stock_pipeline")
    return run_stock_pipeline(trading_date)
