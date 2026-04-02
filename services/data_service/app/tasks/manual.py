"""Manual data collection — triggered via REST API."""
from __future__ import annotations

import asyncio
from datetime import date
from time import perf_counter

from shared.celery_app import celery_app
from shared.utils import get_logger, today_trading

logger = get_logger("data_tasks")


@celery_app.task(
    name="data_service.tasks.manual_collect",
    bind=True,
    max_retries=1,
    queue="data",
)
def manual_collect(
    self,
    symbols: list[str],
    start_date: str,
    end_date: str,
    data_types: list[str],
) -> dict:
    """Manually collect historical data for given symbols and date range.

    data_types: subset of ["bars_1m", "bars_daily"]
    Fires as a Celery task — caller gets task_id to poll progress.
    """
    logger.debug(
        "manual_collect.start",
        log_event="task_start",
        stage="entry",
        task_id=getattr(self.request, "id", None),
        symbols=len(symbols),
        start_date=start_date,
        end_date=end_date,
        data_types=data_types,
        retry=getattr(self.request, "retries", 0),
    )
    try:
        return asyncio.run(
            _manual_collect_async(self, symbols, start_date, end_date, data_types)
        )
    except Exception as exc:
        logger.error("manual_collect.failed", error=str(exc))
        raise self.retry(exc=exc, countdown=30) from exc


async def _manual_collect_async(
    task,
    symbols: list[str],
    start_date_str: str,
    end_date_str: str,
    data_types: list[str],
) -> dict:
    from services.data_service.app.fetchers.registry import get_stock_fetcher
    from services.data_service.app.storage import (
        write_intraday_stock,
        write_swing_stock,
    )

    stock_fetcher = get_stock_fetcher()
    sd = date.fromisoformat(start_date_str)
    ed = date.fromisoformat(end_date_str)

    today = today_trading()
    started = perf_counter()
    logger.debug(
        "manual_collect.context",
        log_event="task_context",
        stage="start",
        symbols=len(symbols),
        data_types=data_types,
        start_date=start_date_str,
        end_date=end_date_str,
    )

    result: dict = {
        "status": "completed",
        "start_date": start_date_str,
        "end_date": end_date_str,
        "symbols": symbols,
        "data_types": data_types,
        "bars_1m_rows": 0,
        "bars_daily_rows": 0,
        "warnings": [],
        "errors": [],
    }

    if sd > ed:
        result["status"] = "completed_with_errors"
        result["errors"].append(
            f"Invalid effective date range after normalization: start_date {sd} > end_date {ed}"
        )
        return result

    total_steps = len(symbols) * len(data_types)
    current_step = 0

    for symbol in symbols:
        logger.debug(
            "manual_collect.symbol_started",
            log_event="symbol_start",
            stage="collect",
            symbol=symbol,
        )
        # ── bars_daily: one yfinance call covers the full range ──
        if "bars_daily" in data_types:
            current_step += 1
            task.update_state(
                state="PROGRESS",
                meta={
                    "current_step": current_step,
                    "total_steps": total_steps,
                    "symbol": symbol,
                    "data_type": "bars_daily",
                },
            )
            try:
                logger.debug(
                    "manual_collect.fetch_bars_daily_started",
                    log_event="external_call",
                    stage="before_fetch",
                    symbol=symbol,
                    provider="yfinance",
                )
                rows, warns = await stock_fetcher.fetch_bars_range(symbol, sd, ed, interval="1d")
                logger.debug(
                    "manual_collect.fetch_bars_daily_finished",
                    log_event="external_call",
                    stage="after_fetch",
                    symbol=symbol,
                    rows=len(rows),
                    warnings=len(warns),
                )
                result["warnings"].extend(warns)
                if rows:
                    logger.debug(
                        "manual_collect.write_bars_daily_started",
                        log_event="db_write",
                        stage="before_write",
                        symbol=symbol,
                        rows=len(rows),
                    )
                    written = await write_swing_stock(rows)
                    result["bars_daily_rows"] += written
                    logger.debug(
                        "manual_collect.write_bars_daily_finished",
                        log_event="db_write",
                        stage="after_write",
                        symbol=symbol,
                        rows=written,
                    )
            except Exception as e:
                result["errors"].append(f"{symbol}/bars_daily: {e}")
                logger.error("manual_collect.bars_daily_error", symbol=symbol, error=str(e))

        # ── bars_1m: one yfinance call for the range (clamped internally) ──
        if "bars_1m" in data_types:
            current_step += 1
            task.update_state(
                state="PROGRESS",
                meta={
                    "current_step": current_step,
                    "total_steps": total_steps,
                    "symbol": symbol,
                    "data_type": "bars_1m",
                },
            )
            try:
                logger.debug(
                    "manual_collect.fetch_bars_1m_started",
                    log_event="external_call",
                    stage="before_fetch",
                    symbol=symbol,
                    provider="yfinance",
                )
                rows, warns = await stock_fetcher.fetch_bars_range(symbol, sd, ed, interval="1m")
                logger.debug(
                    "manual_collect.fetch_bars_1m_finished",
                    log_event="external_call",
                    stage="after_fetch",
                    symbol=symbol,
                    rows=len(rows),
                    warnings=len(warns),
                )
                result["warnings"].extend(warns)
                if rows:
                    logger.debug(
                        "manual_collect.write_bars_1m_started",
                        log_event="db_write",
                        stage="before_write",
                        symbol=symbol,
                        rows=len(rows),
                    )
                    written = await write_intraday_stock(rows)
                    result["bars_1m_rows"] += written
                    logger.debug(
                        "manual_collect.write_bars_1m_finished",
                        log_event="db_write",
                        stage="after_write",
                        symbol=symbol,
                        rows=written,
                    )
            except Exception as e:
                result["errors"].append(f"{symbol}/bars_1m: {e}")
                logger.error("manual_collect.bars_1m_error", symbol=symbol, error=str(e))

    if result["errors"]:
        result["status"] = "completed_with_errors"

    logger.info(
        "manual_collect.done",
        symbols=len(symbols),
        bars_1m=result["bars_1m_rows"],
        bars_daily=result["bars_daily_rows"],
        warnings=len(result["warnings"]),
        errors=len(result["errors"]),
    )
    logger.debug(
        "manual_collect.summary",
        log_event="task_summary",
        stage="completed",
        symbols=len(symbols),
        bars_1m_rows=result["bars_1m_rows"],
        bars_daily_rows=result["bars_daily_rows"],
        warnings=len(result["warnings"]),
        errors=len(result["errors"]),
        duration_ms=round((perf_counter() - started) * 1000, 2),
    )
    return result
