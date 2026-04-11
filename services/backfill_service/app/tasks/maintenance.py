"""Backfill maintenance — historical gap check + new symbol cold start."""
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


# ── 18:00 Historical Gap Check ────────────────────────────


@celery_app.task(name="backfill_service.tasks.check_historical_gaps",
                 soft_time_limit=1800, time_limit=2100)
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
        symbols=len(settings.common.watchlist.all),
    )
    result = {
        "stock_daily_gaps": 0,
        "stock_daily_filled": 0,
        "stock_1min_gaps": 0,
        "stock_1min_filled": 0,
    }

    # stock_daily: 检查过去 90 天
    daily_start = today - timedelta(days=90)
    for symbol in settings.common.watchlist.all:
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
    for symbol in settings.common.watchlist.all:
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


@celery_app.task(name="backfill_service.tasks.backfill_new_symbol",
                 soft_time_limit=1200, time_limit=1500)
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
