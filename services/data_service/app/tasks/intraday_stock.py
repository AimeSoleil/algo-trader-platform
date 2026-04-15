"""Intraday 5-minute stock bar capture — direct DB writes."""
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
    name="data_service.tasks.capture_intraday_stock",
    bind=True,
    max_retries=0,
    queue="data",
)
def capture_intraday_stock(self) -> dict:
    """盘中定时任务：采集 5m stock bars → 直接写入 DB (stock_5min_bars)

    Triggered by Celery Beat via crontab during market hours.
    Splits watchlist into chunks and dispatches them in parallel via group.
    Uses @distributed_once to ensure only one data worker per tick runs this
    orchestrator when celery-data is scaled horizontally.
    """
    return asyncio.run(_capture_intraday_stock_orchestrator())


@distributed_once("data:intraday_stock_capture", ttl=240, service="data_service")
async def _capture_intraday_stock_orchestrator() -> dict:
    from shared.utils import is_market_open

    if not is_market_open():
        logger.info("capture_intraday_stock.skipped", reason="outside_market_hours")
        return {"captured": 0, "skipped": True}

    settings = get_settings()
    symbols = [s for s in settings.common.watchlist.for_trade if not s.startswith("^")]
    chunk_size = settings.data_service.worker.pipeline.chunk_size
    chunks = chunk_symbols(symbols, chunk_size)

    logger.info(
        "capture_intraday_stock.fan_out",
        symbols=len(symbols),
        chunks=len(chunks),
        chunk_size=chunk_size,
    )

    job = group(
        capture_intraday_stock_chunk.si(chunk).set(queue="data")
        for chunk in chunks
    )
    job.apply_async()
    return {"dispatched_chunks": len(chunks), "symbols": len(symbols)}


@celery_app.task(
    name="data_service.tasks.capture_intraday_stock_chunk",
    bind=True,
    max_retries=0,
    queue="data",
)
def capture_intraday_stock_chunk(self, symbols: list[str]) -> dict:
    """盘中采集一组 symbols 的 5m stock bars → 直接写入 DB。"""
    return asyncio.run(_capture_intraday_stock_chunk_async(symbols))


async def _capture_intraday_stock_chunk_async(symbols: list[str]) -> dict:
    from services.data_service.app.fetchers.registry import get_stock_fetcher
    from services.data_service.app.storage import write_intraday_stock_5min

    captured = 0
    rows_written = 0
    errors: list[str] = []

    for symbol in symbols:
        try:
            bars = await get_stock_fetcher().fetch_bars(symbol, period="1d", interval="5m")
            if bars:
                bar = bars[-1]  # only the latest bar
                row = {
                    "symbol": bar["symbol"],
                    "timestamp": bar["timestamp"],
                    "open": bar["open"],
                    "high": bar["high"],
                    "low": bar["low"],
                    "close": bar["close"],
                    "volume": bar["volume"],
                    "vwap": (bar["high"] + bar["low"] + bar["close"]) / 3,
                }
                written = await write_intraday_stock_5min([row])
                rows_written += written
                captured += 1
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{symbol}: {exc}")
            logger.error("capture_intraday_stock.symbol_error", symbol=symbol, error=str(exc))

    logger.info(
        "capture_intraday_stock_chunk.done",
        symbols_total=len(symbols),
        captured=captured,
        rows_written=rows_written,
        errors=len(errors),
    )
    return {"captured": captured, "rows_written": rows_written, "errors": errors}
