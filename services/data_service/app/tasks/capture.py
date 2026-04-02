"""Post-market data capture — 1m bars + daily bars → DB."""
from __future__ import annotations

import asyncio
from datetime import date, datetime
from time import perf_counter

from shared.celery_app import celery_app
from shared.config import get_settings
from shared.pipeline import chunk_symbols
from shared.utils import get_logger, today_trading

logger = get_logger("data_tasks")


@celery_app.task(
    name="data_service.tasks.capture_post_market_data",
    queue="data",
)
def capture_post_market_data(trading_date: str | None = None) -> str:
    """盘后采集编排：将 watchlist 分块 → 并行 capture_post_market_chunk → barrier。

    作为 pipeline 的第一阶段，chord 结束后触发下一阶段。
    """
    settings = get_settings()
    td = trading_date or today_trading().isoformat()
    symbols = settings.common.watchlist.all
    chunk_size = settings.data_service.worker.pipeline.chunk_size
    chunks = chunk_symbols(symbols, chunk_size)

    from celery import group

    logger.info(
        "capture_post_market.fan_out",
        trading_date=td,
        symbols=len(symbols),
        chunks=len(chunks),
        chunk_size=chunk_size,
    )

    job = group(
        capture_post_market_chunk.si(chunk, td).set(queue="data")
        for chunk in chunks
    )
    result = job.apply_async()
    return f"capture_post_market fan-out: {len(chunks)} chunks, group_id={result.id}"


@celery_app.task(
    name="data_service.tasks.capture_post_market_chunk",
    bind=True,
    max_retries=3,
    queue="data",
)
def capture_post_market_chunk(self, symbols: list[str], trading_date: str) -> dict:
    """采集一组 symbols 的盘后数据（1m bars + daily bar）→ 写 DB。"""
    logger.debug(
        "capture_post_market_chunk.start",
        log_event="task_start",
        stage="entry",
        task_id=getattr(self.request, "id", None),
        trading_date=trading_date,
        symbols=len(symbols),
        retry=getattr(self.request, "retries", 0),
    )
    try:
        return asyncio.run(_capture_post_market_chunk_async(symbols, trading_date))
    except Exception as exc:
        logger.error("capture_post_market_chunk.failed", error=str(exc))
        raise self.retry(exc=exc, countdown=120) from exc


async def _capture_post_market_chunk_async(symbols: list[str], trading_date_str: str) -> dict:
    from services.data_service.app.fetchers.registry import get_stock_fetcher
    from services.data_service.app.storage import (
        write_intraday_stock,
        write_swing_stock,
    )

    stock_fetcher = get_stock_fetcher()
    td = date.fromisoformat(trading_date_str)
    started = perf_counter()
    logger.debug(
        "capture_post_market_chunk.context",
        log_event="pipeline_context",
        stage="start",
        trading_date=str(td),
        symbols=len(symbols),
        provider="yfinance",
    )

    result = {
        "date": str(td),
        "stock_1min_rows": 0,
        "stock_daily_rows": 0,
        "errors": [],
    }

    for symbol in symbols:
        try:
            logger.debug(
                "capture_post_market.symbol_started",
                log_event="symbol_start",
                stage="collect",
                symbol=symbol,
                trading_date=str(td),
            )
            # ── (a) 当天全天 1 分钟 K 线 → stock_1min_bars ──
            logger.debug(
                "capture_post_market.fetch_stock_1m_started",
                log_event="external_call",
                stage="before_fetch",
                symbol=symbol,
                provider="yfinance",
                period="1d",
                interval="1m",
            )
            bars_1m = await stock_fetcher.fetch_bars(symbol, period="1d", interval="1m")
            logger.debug(
                "capture_post_market.fetch_stock_1m_finished",
                log_event="external_call",
                stage="after_fetch",
                symbol=symbol,
                rows=len(bars_1m) if bars_1m else 0,
            )
            if bars_1m:
                intraday_rows = [
                    {
                        "symbol": bar["symbol"],
                        "timestamp": bar["timestamp"],
                        "open": bar["open"],
                        "high": bar["high"],
                        "low": bar["low"],
                        "close": bar["close"],
                        "volume": bar["volume"],
                    }
                    for bar in bars_1m
                ]
                logger.debug(
                    "capture_post_market.write_stock_1m_started",
                    log_event="db_write",
                    stage="before_write",
                    symbol=symbol,
                    rows=len(intraday_rows),
                )
                written = await write_intraday_stock(intraday_rows)
                result["stock_1min_rows"] += written
                logger.debug(
                    "capture_post_market.write_stock_1m_finished",
                    log_event="db_write",
                    stage="after_write",
                    symbol=symbol,
                    rows=written,
                )

            # ── (b) 日线 → stock_daily ──
            logger.debug(
                "capture_post_market.fetch_stock_daily_started",
                log_event="external_call",
                stage="before_fetch",
                symbol=symbol,
                provider="yfinance",
                period="5d",
                interval="1d",
            )
            bars_daily = await stock_fetcher.fetch_bars(symbol, period="5d", interval="1d")
            logger.debug(
                "capture_post_market.fetch_stock_daily_finished",
                log_event="external_call",
                stage="after_fetch",
                symbol=symbol,
                rows=len(bars_daily) if bars_daily else 0,
            )
            if bars_daily:
                latest = bars_daily[-1]
                # timestamp is already a datetime object from ensure_utc()
                ts = latest["timestamp"]
                trading_date_val = ts.date() if isinstance(ts, datetime) else datetime.fromisoformat(str(ts)).date()
                daily_row = {
                    "symbol": latest["symbol"],
                    "trading_date": trading_date_val,
                    "open": latest["open"],
                    "high": latest["high"],
                    "low": latest["low"],
                    "close": latest["close"],
                    "volume": latest["volume"],
                }
                logger.debug(
                    "capture_post_market.write_stock_daily_started",
                    log_event="db_write",
                    stage="before_write",
                    symbol=symbol,
                    rows=1,
                )
                written = await write_swing_stock([daily_row])
                result["stock_daily_rows"] += written
                logger.debug(
                    "capture_post_market.write_stock_daily_finished",
                    log_event="db_write",
                    stage="after_write",
                    symbol=symbol,
                    rows=written,
                )

            # ── (c) option_daily 已改由盘中快照聚合回填（aggregate_option_daily 任务）──
            # 盘后 yfinance 期权链 bid=ask=0 且 IV 不可靠，不再直接采集

        except Exception as e:
            error_msg = f"{symbol}: {str(e)}"
            result["errors"].append(error_msg)
            logger.error("capture_post_market.symbol_error", symbol=symbol, error=str(e))

    logger.debug(
        "capture_post_market.summary",
        log_event="task_summary",
        stage="completed",
        trading_date=str(td),
        stock_1min_rows=result["stock_1min_rows"],
        stock_daily_rows=result["stock_daily_rows"],
        errors=len(result["errors"]),
        duration_ms=round((perf_counter() - started) * 1000, 2),
    )

    logger.info("capture_post_market.done", **{k: v for k, v in result.items() if k != "errors"})
    return result
