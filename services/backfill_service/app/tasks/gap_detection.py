"""Gap detection and backfill — pipeline step + chunked variant."""
from __future__ import annotations

import asyncio
from datetime import date, timedelta
from time import perf_counter

from shared.celery_app import celery_app
from shared.config import get_settings
from shared.utils import get_logger, resolve_trading_date_arg, today_trading

logger = get_logger("backfill_tasks")

# yfinance 1-min data only available for last 7 calendar days
_MAX_1MIN_LOOKBACK = 7
# Daily gap lookback window for post-market check
_DAILY_GAP_LOOKBACK = 5


# ── Pipeline Step 3: 当日缺口检测与回填 ───────────────────


@celery_app.task(
    name="backfill_service.tasks.detect_and_backfill_gaps",
    bind=True,
    max_retries=2,
    soft_time_limit=1800,
    time_limit=2100,
)
def detect_and_backfill_gaps(
    self,
    trading_date: str | None = None,
    prev_result=None,
) -> dict:
    """盘后 pipeline：检测 4 表缺口并回填可修复项"""
    resolved_trading_date = resolve_trading_date_arg(trading_date, prev_result)
    logger.debug(
        "backfill.detect.start",
        log_event="task_start",
        stage="entry",
        task_id=getattr(self.request, "id", None),
        trading_date=trading_date,
        resolved_trading_date=resolved_trading_date,
        retry=getattr(self.request, "retries", 0),
    )
    try:
        return asyncio.run(_detect_and_backfill_async(resolved_trading_date))
    except Exception as exc:
        logger.error("backfill.detect_failed", error=str(exc))
        raise self.retry(exc=exc, countdown=120) from exc


async def _detect_and_backfill_async(trading_date_str: str | None = None) -> dict:
    from services.backfill_service.app.backfiller import (
        backfill_stock_1min,
        backfill_stock_daily,
    )
    from services.backfill_service.app.gap_detector import (
        detect_option_5min_gaps,
        detect_option_daily_gaps,
        detect_stock_1min_gaps,
        detect_stock_daily_gaps,
    )

    settings = get_settings()
    td = date.fromisoformat(trading_date_str) if trading_date_str else today_trading()
    lookback_start = td - timedelta(days=_DAILY_GAP_LOOKBACK)
    started = perf_counter()
    logger.debug(
        "backfill.detect.context",
        log_event="task_context",
        stage="start",
        trading_date=str(td),
        symbols=len(settings.common.watchlist.all),
        daily_lookback_start=str(lookback_start),
    )

    result = {
        "date": str(td),
        "stock_1min_gaps": 0,
        "stock_1min_filled": 0,
        "stock_daily_gaps": 0,
        "stock_daily_filled": 0,
        "option_daily_gaps": 0,
        "option_5min_gaps": 0,
    }

    for symbol in settings.common.watchlist.all:
        symbol_started = perf_counter()
        logger.debug(
            "backfill.detect.symbol_started",
            log_event="symbol_start",
            stage="detect",
            symbol=symbol,
            trading_date=str(td),
        )
        # ── 1. stock_1min_bars 缺口 ──
        s1m_gaps = await detect_stock_1min_gaps(symbol, td)
        result["stock_1min_gaps"] += len(s1m_gaps)
        if s1m_gaps:
            logger.debug(
                "backfill.detect.stock_1min_backfill_started",
                log_event="backfill",
                stage="before_backfill",
                symbol=symbol,
                trading_date=str(td),
                gaps=len(s1m_gaps),
            )
            rows = await backfill_stock_1min(symbol, td, td)
            result["stock_1min_filled"] += rows
            logger.debug(
                "backfill.detect.stock_1min_backfill_finished",
                log_event="backfill",
                stage="after_backfill",
                symbol=symbol,
                trading_date=str(td),
                rows=rows,
            )

        # ── 2. stock_daily 缺口 ──
        sd_gaps = await detect_stock_daily_gaps(symbol, lookback_start, td)
        result["stock_daily_gaps"] += len(sd_gaps)
        if sd_gaps:
            logger.debug(
                "backfill.detect.stock_daily_backfill_started",
                log_event="backfill",
                stage="before_backfill",
                symbol=symbol,
                trading_date=str(td),
                gaps=len(sd_gaps),
                start_date=str(sd_gaps[0]),
                end_date=str(sd_gaps[-1]),
            )
            rows = await backfill_stock_daily(symbol, sd_gaps[0], sd_gaps[-1])
            result["stock_daily_filled"] += rows
            logger.debug(
                "backfill.detect.stock_daily_backfill_finished",
                log_event="backfill",
                stage="after_backfill",
                symbol=symbol,
                trading_date=str(td),
                rows=rows,
            )

        # ── 3. option_daily 缺口（仅记录） ──
        od_gaps = await detect_option_daily_gaps(symbol, lookback_start, td)
        result["option_daily_gaps"] += len(od_gaps)
        if od_gaps:
            logger.warning(
                "backfill.option_daily_not_fillable",
                symbol=symbol,
                gaps=len(od_gaps),
                dates=[str(d) for d in od_gaps[:5]],
            )

        # ── 4. option_5min_snapshots 缺口（仅记录） ──
        o5m_gaps = await detect_option_5min_gaps(symbol, td)
        result["option_5min_gaps"] += len(o5m_gaps)
        if o5m_gaps:
            logger.warning(
                "backfill.option_5min_not_fillable",
                symbol=symbol,
                gaps=len(o5m_gaps),
            )

        logger.debug(
            "backfill.detect.symbol_summary",
            log_event="symbol_summary",
            stage="completed",
            symbol=symbol,
            trading_date=str(td),
            stock_1min_gaps=len(s1m_gaps),
            stock_daily_gaps=len(sd_gaps),
            option_daily_gaps=len(od_gaps),
            option_5min_gaps=len(o5m_gaps),
            duration_ms=round((perf_counter() - symbol_started) * 1000, 2),
        )

    logger.debug(
        "backfill.detect.summary",
        log_event="task_summary",
        stage="completed",
        trading_date=str(td),
        stock_1min_gaps=result["stock_1min_gaps"],
        stock_1min_filled=result["stock_1min_filled"],
        stock_daily_gaps=result["stock_daily_gaps"],
        stock_daily_filled=result["stock_daily_filled"],
        option_daily_gaps=result["option_daily_gaps"],
        option_5min_gaps=result["option_5min_gaps"],
        duration_ms=round((perf_counter() - started) * 1000, 2),
    )
    logger.info("backfill.completed", **result)
    return result


# ── Pipeline Step 3 (chunked): 分块缺口检测与回填 ─────────


@celery_app.task(
    name="backfill_service.tasks.detect_gaps_chunk",
    bind=True,
    max_retries=2,
    queue="backfill",
    soft_time_limit=1200,
    time_limit=1500,
)
def detect_gaps_chunk(
    self,
    symbols: list[str],
    trading_date: str,
) -> dict:
    """Pipeline chunk task: 检测一组 symbols 的 4 表缺口并回填可修复项。

    Called from the post-market pipeline via ``chord(group([detect_gaps_chunk(c1), ...]))``
    to enable parallel backfill across symbol subsets.
    """
    logger.debug(
        "backfill.detect_chunk.start",
        log_event="task_start",
        stage="entry",
        task_id=getattr(self.request, "id", None),
        trading_date=trading_date,
        symbols=len(symbols),
        retry=getattr(self.request, "retries", 0),
    )
    try:
        return asyncio.run(_detect_gaps_chunk_async(symbols, trading_date))
    except Exception as exc:
        logger.error("backfill.detect_chunk_failed", error=str(exc))
        raise self.retry(exc=exc, countdown=120) from exc


async def _detect_gaps_chunk_async(symbols: list[str], trading_date_str: str) -> dict:
    from services.backfill_service.app.backfiller import (
        backfill_stock_1min,
        backfill_stock_daily,
    )
    from services.backfill_service.app.gap_detector import (
        detect_option_5min_gaps,
        detect_option_daily_gaps,
        detect_stock_1min_gaps,
        detect_stock_daily_gaps,
    )

    td = date.fromisoformat(trading_date_str)
    lookback_start = td - timedelta(days=_DAILY_GAP_LOOKBACK)
    started = perf_counter()

    result = {
        "date": str(td),
        "symbols": len(symbols),
        "stock_1min_gaps": 0,
        "stock_1min_filled": 0,
        "stock_daily_gaps": 0,
        "stock_daily_filled": 0,
        "option_daily_gaps": 0,
        "option_5min_gaps": 0,
    }

    for symbol in symbols:
        s1m_gaps = await detect_stock_1min_gaps(symbol, td)
        result["stock_1min_gaps"] += len(s1m_gaps)
        if s1m_gaps:
            rows = await backfill_stock_1min(symbol, td, td)
            result["stock_1min_filled"] += rows

        sd_gaps = await detect_stock_daily_gaps(symbol, lookback_start, td)
        result["stock_daily_gaps"] += len(sd_gaps)
        if sd_gaps:
            rows = await backfill_stock_daily(symbol, sd_gaps[0], sd_gaps[-1])
            result["stock_daily_filled"] += rows

        od_gaps = await detect_option_daily_gaps(symbol, lookback_start, td)
        result["option_daily_gaps"] += len(od_gaps)
        if od_gaps:
            logger.warning("backfill.option_daily_not_fillable", symbol=symbol, gaps=len(od_gaps))

        o5m_gaps = await detect_option_5min_gaps(symbol, td)
        result["option_5min_gaps"] += len(o5m_gaps)
        if o5m_gaps:
            logger.warning("backfill.option_5min_not_fillable", symbol=symbol, gaps=len(o5m_gaps))

    logger.info(
        "backfill.detect_chunk.done",
        symbols=len(symbols),
        trading_date=str(td),
        stock_1min_gaps=result["stock_1min_gaps"],
        stock_daily_gaps=result["stock_daily_gaps"],
        duration_ms=round((perf_counter() - started) * 1000, 2),
    )
    return result
