"""Intraday option chain capture — direct DB writes."""
from __future__ import annotations

import asyncio

from celery import group

from shared.celery_app import celery_app
from shared.config import get_settings
from shared.distributed_lock import distributed_once
from shared.pipeline import chunk_symbols
from shared.utils import get_logger

logger = get_logger("data_tasks")


@celery_app.task(
    name="data_service.tasks.capture_intraday_options",
    bind=True,
    max_retries=0,   # fire-and-forget per tick; dropped ticks are acceptable
    queue="data",
)
def capture_intraday_options(self) -> dict:
    """盘中定时任务：采集期权链快照 → 直接写入 DB (option_5min_snapshots)

    Triggered by Celery Beat via crontab during market hours.
    Splits watchlist into chunks and dispatches them in parallel via group.
    Uses @distributed_once to ensure only one data worker per tick runs this
    orchestrator when celery-data is scaled horizontally.
    """
    return asyncio.run(_capture_intraday_orchestrator())


@distributed_once("data:intraday_capture", ttl=240, service="data_service")
async def _capture_intraday_orchestrator() -> dict:
    from shared.utils import is_market_open

    if not is_market_open():
        logger.info("capture_intraday.skipped", reason="outside_market_hours")
        return {"captured": 0, "skipped": True}

    settings = get_settings()
    symbols = [s for s in settings.common.watchlist.all if not s.startswith("^")]
    chunk_size = settings.data_service.worker.pipeline.chunk_size
    chunks = chunk_symbols(symbols, chunk_size)

    logger.info(
        "capture_intraday.fan_out",
        symbols=len(symbols),
        chunks=len(chunks),
        chunk_size=chunk_size,
    )

    job = group(
        capture_intraday_chunk.si(chunk).set(queue="data")
        for chunk in chunks
    )
    # apply_async is sync-safe; we don't await sub-task results
    job.apply_async()
    return {"dispatched_chunks": len(chunks), "symbols": len(symbols)}


@celery_app.task(
    name="data_service.tasks.capture_intraday_chunk",
    bind=True,
    max_retries=0,
    queue="data",
)
def capture_intraday_chunk(self, symbols: list[str]) -> dict:
    """盘中采集一组 symbols 的期权链快照 → 直接写入 DB。"""
    return asyncio.run(_capture_intraday_chunk_async(symbols))


async def _capture_intraday_chunk_async(symbols: list[str]) -> dict:
    from services.data_service.app.converters import contracts_to_rows
    from services.data_service.app.fetchers.registry import get_option_fetcher
    from services.data_service.app.filters import apply_option_pipeline
    from services.data_service.app.storage import write_intraday_options

    captured = 0
    rows_written = 0
    errors: list[str] = []

    for symbol in symbols:
        try:
            snapshot = await get_option_fetcher().fetch_current(symbol)
            if snapshot:
                snapshot, _ = apply_option_pipeline(snapshot)
                rows = contracts_to_rows(snapshot, top_expiries=None)
                written = await write_intraday_options(rows)
                rows_written += written
                captured += 1
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{symbol}: {exc}")
            logger.error("capture_intraday.symbol_error", symbol=symbol, error=str(exc))

    logger.info(
        "capture_intraday_chunk.done",
        symbols_total=len(symbols),
        captured=captured,
        rows_written=rows_written,
        errors=len(errors),
    )
    return {"captured": captured, "rows_written": rows_written, "errors": errors}
