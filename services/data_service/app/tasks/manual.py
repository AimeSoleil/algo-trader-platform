"""Manual data collection & full-pipeline dry-run — triggered via REST API."""
from __future__ import annotations

import asyncio
import traceback
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


# ── Manual full-pipeline dry-run ───────────────────────────
#
# Simulates the automatic pipeline end-to-end for a given set of symbols.
# Every stage catches errors and continues — used to verify the automatic
# pipeline can run normally.


def _run_stage(name: str, fn, stage_result: dict) -> None:
    """Execute *fn*, capture result or error into *stage_result*."""
    started = perf_counter()
    try:
        result = fn()
        stage_result["status"] = "ok"
        stage_result["result"] = result
    except Exception as exc:
        stage_result["status"] = "error"
        stage_result["error"] = str(exc)
        stage_result["traceback"] = traceback.format_exc()
        logger.error("manual_pipeline.stage_error", stage=name, error=str(exc))
    stage_result["duration_ms"] = round((perf_counter() - started) * 1000, 2)


_MANUAL_PIPELINE_STAGES: list[str] = [
    "capture_stock",
    "aggregate_options",
    "detect_and_backfill_gaps",
    "compute_daily_signals",
    "generate_daily_blueprint",
]


@celery_app.task(
    name="data_service.tasks.run_manual_pipeline",
    bind=True,
    queue="data",
)
def run_manual_pipeline(
    self,
    symbols: list[str],
    trading_date: str | None = None,
    stop_after: str | None = None,
) -> dict:
    """Run the full pipeline synchronously for specified symbols.

    Simulates the automatic pipeline:
      1. capture_stock       — fetch 1m bars + daily bars
      2. aggregate_options   — 5-min snapshots → option_daily + option_iv_daily
      3. detect_and_backfill — gap detection + backfill
      4. compute_signals     — compute daily signals
      5. generate_blueprint  — generate trading blueprint

    Every stage catches errors and continues to the next.
    ``stop_after`` truncates the pipeline after the named stage (inclusive).
    """
    td = trading_date or today_trading().isoformat()
    pipeline_started = perf_counter()

    logger.info(
        "manual_pipeline.start",
        trading_date=td,
        symbols=symbols,
        task_id=getattr(self.request, "id", None),
    )

    report: dict = {
        "trading_date": td,
        "symbols": symbols,
        "stages": {},
    }

    # Determine which stages to run
    if stop_after:
        if stop_after not in _MANUAL_PIPELINE_STAGES:
            return {
                "error": f"Invalid stop_after: {stop_after!r}. "
                         f"Must be one of: {_MANUAL_PIPELINE_STAGES}",
            }
        cutoff = _MANUAL_PIPELINE_STAGES.index(stop_after) + 1
    else:
        cutoff = len(_MANUAL_PIPELINE_STAGES)
    stages_to_run = set(_MANUAL_PIPELINE_STAGES[:cutoff])

    # ── Stage 1: capture stock data ──
    if "capture_stock" in stages_to_run:
        stage = report["stages"]["capture_stock"] = {}
        _run_stage("capture_stock", lambda: asyncio.run(
            _mp_capture_stock(symbols, td)
        ), stage)

    # ── Stage 2: aggregate options ──
    if "aggregate_options" in stages_to_run:
        stage = report["stages"]["aggregate_options"] = {}
        _run_stage("aggregate_options", lambda: asyncio.run(
            _mp_aggregate_options(td)
        ), stage)

    # ── Stage 3: detect and backfill gaps ──
    if "detect_and_backfill_gaps" in stages_to_run:
        stage = report["stages"]["detect_and_backfill_gaps"] = {}
        _run_stage("detect_and_backfill_gaps", lambda: asyncio.run(
            _mp_detect_and_backfill(symbols, td)
        ), stage)

    # ── Stage 4: compute daily signals ──
    if "compute_daily_signals" in stages_to_run:
        stage = report["stages"]["compute_daily_signals"] = {}
        _run_stage("compute_daily_signals", lambda: asyncio.run(
            _mp_compute_signals(symbols, td)
        ), stage)

    # ── Stage 5: generate daily blueprint ──
    if "generate_daily_blueprint" in stages_to_run:
        stage = report["stages"]["generate_daily_blueprint"] = {}
        _run_stage("generate_daily_blueprint", lambda: asyncio.run(
            _mp_generate_blueprint(td)
        ), stage)

    # ── Summary ──
    total_ms = round((perf_counter() - pipeline_started) * 1000, 2)
    ok_stages = [s for s, v in report["stages"].items() if v.get("status") == "ok"]
    err_stages = [s for s, v in report["stages"].items() if v.get("status") == "error"]

    report["summary"] = {
        "total_duration_ms": total_ms,
        "stages_ok": ok_stages,
        "stages_error": err_stages,
        "all_ok": len(err_stages) == 0,
    }

    logger.info(
        "manual_pipeline.done",
        trading_date=td,
        ok=len(ok_stages),
        errors=len(err_stages),
        duration_ms=total_ms,
    )
    return report


# ── Stage implementations ──────────────────────────────────
# Each calls the same async functions used by the automatic pipeline,
# just with the provided symbols instead of the full watchlist.


async def _mp_capture_stock(symbols: list[str], td: str) -> dict:
    from services.data_service.app.tasks.capture import (
        _capture_post_market_chunk_async,
    )
    return await _capture_post_market_chunk_async(symbols, td)


async def _mp_aggregate_options(td: str) -> dict:
    from services.data_service.app.tasks.aggregation import (
        _aggregate_option_daily_async,
    )
    return await _aggregate_option_daily_async(td)


async def _mp_detect_and_backfill(symbols: list[str], td: str) -> dict:
    from services.backfill_service.app.tasks.gap_detection import (
        _detect_gaps_chunk_async,
    )
    return await _detect_gaps_chunk_async(symbols, td)


async def _mp_compute_signals(symbols: list[str], td: str) -> dict:
    from services.signal_service.app.tasks.signal import _compute_daily_signals
    return await _compute_daily_signals(td, symbols=symbols)


async def _mp_generate_blueprint(td: str) -> dict:
    from services.analysis_service.app.tasks.blueprint import (
        _generate_blueprint_async,
    )
    return await _generate_blueprint_async(td)
