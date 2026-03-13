"""Backfill Service — Celery 任务

Tasks:
  detect_and_backfill_gaps  — 盘后 pipeline step 3：检测 4 表缺口并回填可修复项
  check_historical_gaps     — 18:00 daily：回溯检查近期数据完整性
  backfill_new_symbol       — 手动触发：新标的冷启动
"""
from __future__ import annotations

import asyncio
from datetime import date, timedelta
from time import perf_counter

from shared.celery_app import celery_app
from shared.config import get_settings
from shared.utils import get_logger, today_trading

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
)
def detect_and_backfill_gaps(
    self,
    trading_date: str | None = None,
    prev_result=None,
) -> dict:
    """盘后 pipeline：检测 4 表缺口并回填可修复项"""
    logger.debug(
        "backfill.detect.start",
        log_event="task_start",
        stage="entry",
        task_id=getattr(self.request, "id", None),
        trading_date=trading_date,
        retry=getattr(self.request, "retries", 0),
    )
    try:
        return asyncio.run(_detect_and_backfill_async(trading_date))
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
        symbols=len(settings.watchlist),
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

    for symbol in settings.watchlist:
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


# ── 18:00 Historical Gap Check ────────────────────────────


@celery_app.task(name="backfill_service.tasks.check_historical_gaps")
def check_historical_gaps() -> dict:
    """18:00 每日检查：回溯验证近期数据完整性并自动修复"""
    return asyncio.run(_check_historical_async())


async def _check_historical_async() -> dict:
    from services.backfill_service.app.backfiller import (
        backfill_stock_1min,
        backfill_stock_daily,
    )
    from services.backfill_service.app.gap_detector import (
        detect_stock_1min_gaps,
        detect_stock_daily_gaps,
    )

    settings = get_settings()
    today = today_trading()
    started = perf_counter()
    logger.debug(
        "backfill.historical.context",
        log_event="task_context",
        stage="start",
        trading_date=str(today),
        symbols=len(settings.watchlist),
    )
    result = {
        "stock_daily_gaps": 0,
        "stock_daily_filled": 0,
        "stock_1min_gaps": 0,
        "stock_1min_filled": 0,
    }

    # stock_daily: 检查过去 90 天
    daily_start = today - timedelta(days=90)
    for symbol in settings.watchlist:
        symbol_daily_filled = 0
        sd_gaps = await detect_stock_daily_gaps(symbol, daily_start, today)
        result["stock_daily_gaps"] += len(sd_gaps)
        if sd_gaps:
            rows = await backfill_stock_daily(symbol, sd_gaps[0], sd_gaps[-1])
            result["stock_daily_filled"] += rows
            symbol_daily_filled = rows
        logger.debug(
            "backfill.historical.symbol_daily_summary",
            log_event="symbol_summary",
            stage="daily_check",
            symbol=symbol,
            gaps=len(sd_gaps),
            rows_filled=symbol_daily_filled,
            start_date=str(daily_start),
            end_date=str(today),
        )

    # stock_1min: 检查最近 7 天（yfinance 限制）
    min_start = today - timedelta(days=_MAX_1MIN_LOOKBACK)
    for symbol in settings.watchlist:
        symbol_1min_gaps = 0
        symbol_1min_filled = 0
        check_date = min_start
        while check_date <= today:
            if check_date.weekday() < 5:  # 仅交易日
                gaps = await detect_stock_1min_gaps(symbol, check_date)
                result["stock_1min_gaps"] += len(gaps)
                symbol_1min_gaps += len(gaps)
                if gaps:
                    rows = await backfill_stock_1min(symbol, check_date, check_date)
                    result["stock_1min_filled"] += rows
                    symbol_1min_filled += rows
            check_date += timedelta(days=1)

        logger.debug(
            "backfill.historical.symbol_1min_summary",
            log_event="symbol_summary",
            stage="intraday_check",
            symbol=symbol,
            gaps=symbol_1min_gaps,
            rows_filled=symbol_1min_filled,
            start_date=str(min_start),
            end_date=str(today),
        )

    logger.debug(
        "backfill.historical.summary",
        log_event="task_summary",
        stage="completed",
        stock_daily_gaps=result["stock_daily_gaps"],
        stock_daily_filled=result["stock_daily_filled"],
        stock_1min_gaps=result["stock_1min_gaps"],
        stock_1min_filled=result["stock_1min_filled"],
        duration_ms=round((perf_counter() - started) * 1000, 2),
    )
    logger.info("backfill.historical_check_done", **result)
    return result


# ── 手动触发：新标的冷启动 ─────────────────────────────────


@celery_app.task(name="backfill_service.tasks.backfill_new_symbol")
def backfill_new_symbol(symbol: str, days: int = 90) -> dict:
    """手动触发：为新标的回填历史数据"""
    logger.debug(
        "backfill.new_symbol.start",
        log_event="task_start",
        stage="entry",
        symbol=symbol,
        days=days,
    )
    return asyncio.run(_backfill_new_symbol_async(symbol, days))


async def _backfill_new_symbol_async(symbol: str, days: int) -> dict:
    from services.backfill_service.app.backfiller import backfill_history

    started = perf_counter()
    logger.debug(
        "backfill.new_symbol.backfill_started",
        log_event="backfill",
        stage="before_backfill",
        symbol=symbol,
        days=days,
    )
    result = await backfill_history(symbol, days)
    logger.debug(
        "backfill.new_symbol.backfill_finished",
        log_event="backfill",
        stage="after_backfill",
        symbol=symbol,
        days=days,
        duration_ms=round((perf_counter() - started) * 1000, 2),
    )
    return result
