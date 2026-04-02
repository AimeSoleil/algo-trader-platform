"""Option daily aggregation — intraday snapshots → option_daily + option_iv_daily."""
from __future__ import annotations

import asyncio
from datetime import date
from time import perf_counter

from shared.celery_app import celery_app
from shared.utils import get_logger, resolve_trading_date_arg, today_trading

logger = get_logger("data_tasks")


@celery_app.task(
    name="data_service.tasks.aggregate_option_daily",
    bind=True,
    max_retries=3,
    queue="data",
)
def aggregate_option_daily(self, trading_date: str | None = None, prev_result=None) -> dict:
    """Aggregate intraday 5-min snapshots into option_daily + option_iv_daily.

    Must run AFTER intraday capture (snapshots need to be in DB first).
    If no 5-min snapshots exist for the day, the task is a no-op (returns zero rows).
    """
    resolved_trading_date = resolve_trading_date_arg(trading_date, prev_result)
    logger.debug(
        "aggregate_option_daily.start",
        log_event="task_start",
        stage="entry",
        task_id=getattr(self.request, "id", None),
        trading_date=trading_date,
        resolved_trading_date=resolved_trading_date,
        retry=getattr(self.request, "retries", 0),
    )
    try:
        return asyncio.run(_aggregate_option_daily_async(resolved_trading_date))
    except Exception as exc:
        logger.error("aggregate_option_daily.failed", error=str(exc))
        raise self.retry(exc=exc, countdown=60) from exc


async def _aggregate_option_daily_async(trading_date_str: str | None = None) -> dict:
    from services.data_service.app.storage import (
        aggregate_daily_from_snapshots,
        aggregate_iv_daily,
    )

    td = date.fromisoformat(trading_date_str) if trading_date_str else today_trading()
    started = perf_counter()

    result = {
        "date": str(td),
        "daily_rows": 0,
        "daily_symbols": 0,
        "iv_underlyings": 0,
    }

    # ── 1) Aggregate last intraday snapshot → option_daily ──
    daily_result = await aggregate_daily_from_snapshots(td)
    result["daily_rows"] = daily_result["rows_upserted"]
    result["daily_symbols"] = daily_result["symbols_covered"]

    if result["daily_rows"] == 0:
        logger.warning(
            "aggregate_option_daily.no_intraday_data",
            trading_date=str(td),
            reason="no 5-min snapshots found; option_daily and option_iv_daily will be empty for this date",
        )

    # ── 2) Aggregate IV summary → option_iv_daily ──
    iv_result = await aggregate_iv_daily(td)
    result["iv_underlyings"] = iv_result["underlyings_written"]

    logger.info(
        "aggregate_option_daily.done",
        trading_date=str(td),
        daily_rows=result["daily_rows"],
        daily_symbols=result["daily_symbols"],
        iv_underlyings=result["iv_underlyings"],
        duration_ms=round((perf_counter() - started) * 1000, 2),
    )
    return result
