"""Data Service — Celery 盘后批量任务

Pipeline 顺序:
  1. capture_post_market_data  — 采集 1m bars / daily bar → 直接写 DB
  2. aggregate_option_daily   — 盘中快照聚合 → option_daily + option_iv_daily
  3. detect_and_backfill_gaps  — 缺口检测与回填  (Backfill Service)
  4. compute_daily_signals     — 信号计算          (Signal Service)
  5. generate_daily_blueprint  — 生成交易蓝图      (Analysis Service)
"""
from __future__ import annotations

import asyncio
from datetime import date, datetime
from time import perf_counter

from celery import chain as celery_chain, chord, group

from shared.celery_app import celery_app
from shared.config import get_settings
from shared.db.session import get_timescale_session
from shared.distributed_lock import distributed_once
from shared.pipeline import chunk_symbols
from shared.utils import (
    get_logger,
    previous_trading_day,
    resolve_trading_date_arg,
    today_trading,
)

logger = get_logger("data_tasks")


# ── Step 1: 盘后数据采集 ──────────────────────────────────


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


# ── Step 2: 盘中快照聚合 → option_daily + option_iv_daily ──


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


# ── 流水线步骤注册表（有序）─────────────────────────────────

# Valid stop_after values, in execution order.
_PIPELINE_STEP_NAMES: list[str] = [
    "capture_post_market_data",
    "aggregate_option_daily",
    "detect_and_backfill_gaps",
    "compute_daily_signals",
    "generate_daily_blueprint",
]


@celery_app.task(
    name="data_service.tasks.stage_barrier",
    queue="data",
)
def stage_barrier(results, stage_name: str, trading_date: str) -> dict:
    """Chord callback — logs stage completion and forwards trading_date.

    *results* is the list of return values from all chunk tasks in the group.
    This task is intentionally lightweight and lives on the ``data`` queue.
    """
    logger.info(
        "pipeline.stage_completed",
        stage=stage_name,
        trading_date=trading_date,
        chunks=len(results) if isinstance(results, list) else 1,
    )
    return {"stage": stage_name, "trading_date": trading_date}


def _build_pipeline_steps(td: str) -> list[tuple[str, object]]:
    """Return ALL pipeline steps as (name, signature) pairs, in order.

    Chunked stages (capture / backfill / signals) are expressed as
    ``chord(group([chunk_task, ...]), stage_barrier.s())`` so that all chunks
    in a stage execute in parallel, and the next stage only starts when all
    chunks are done.

    Non-chunked stages (flush / aggregate / blueprint) remain single tasks.
    """
    settings = get_settings()
    symbols = settings.common.watchlist.all
    chunk_size = settings.data_service.worker.pipeline.chunk_size
    chunks = chunk_symbols(symbols, chunk_size)

    return [
        (
            "capture_post_market_data",
            chord(
                group(
                    capture_post_market_chunk.si(chunk, td).set(queue="data")
                    for chunk in chunks
                ),
                stage_barrier.si("capture_post_market_data", td).set(queue="data"),
            ),
        ),
        (
            "aggregate_option_daily",
            aggregate_option_daily.si(td).set(queue="data"),
        ),
        (
            "detect_and_backfill_gaps",
            chord(
                group(
                    celery_app.signature(
                        "backfill_service.tasks.detect_gaps_chunk",
                        args=[chunk, td],
                        queue="backfill",
                        immutable=True,
                    )
                    for chunk in chunks
                ),
                stage_barrier.si("detect_and_backfill_gaps", td).set(queue="data"),
            ),
        ),
        (
            "compute_daily_signals",
            chord(
                group(
                    celery_app.signature(
                        "signal_service.tasks.compute_signals_chunk",
                        args=[chunk, td],
                        queue="signal",
                        immutable=True,
                    )
                    for chunk in chunks
                ),
                stage_barrier.si("compute_daily_signals", td).set(queue="data"),
            ),
        ),
        (
            "generate_daily_blueprint",
            celery_app.signature(
                "analysis_service.tasks.generate_daily_blueprint",
                args=[td],
                queue="analysis",
                immutable=True,
            ),
        ),
    ]


# ── Pipeline 入口 ──────────────────────────────────────────


@celery_app.task(name="data_service.tasks.run_post_market_pipeline", queue="data")
def run_post_market_pipeline(trading_date: str | None = None) -> str:
    """盘后流水线入口（由 Celery Beat 触发）

    Stages with symbol-level work (capture / backfill / signals) are expressed
    as ``chord(group([chunk_tasks...]), barrier)`` so all chunks in a stage
    execute in parallel.  Stages without symbol iteration (aggregate /
    blueprint) run as single tasks.

    The chain is gated by ``data_service.worker.pipeline.stop_after``:
    all steps up to and including *stop_after* execute; later steps are skipped.

    Valid stop_after values (ordered):
      capture_post_market_data → aggregate_option_daily
      → detect_and_backfill_gaps → compute_daily_signals → generate_daily_blueprint
    """
    settings = get_settings()
    stop_after = settings.data_service.worker.pipeline.stop_after
    td = trading_date or today_trading().isoformat()

    if stop_after not in _PIPELINE_STEP_NAMES:
        raise ValueError(
            f"Invalid stop_after value: {stop_after!r}. "
            f"Must be one of: {_PIPELINE_STEP_NAMES}"
        )

    all_steps = _build_pipeline_steps(td)
    cutoff = _PIPELINE_STEP_NAMES.index(stop_after)
    included = all_steps[: cutoff + 1]
    gated_out = [name for name, _ in all_steps[cutoff + 1 :]]

    logger.debug(
        "post_market_pipeline.building",
        log_event="pipeline_start",
        stage="compose_chain",
        trading_date=td,
        stop_after=stop_after,
        included_steps=[name for name, _ in included],
        gated_out_steps=gated_out,
    )
    if gated_out:
        logger.info(
            "post_market_pipeline.steps_gated_out",
            trading_date=td,
            gated_out=gated_out,
            stop_after=stop_after,
        )

    # Build the sequential pipeline.
    # chord objects and plain signatures can be composed via celery_chain.
    pipeline = celery_chain(*[sig for _, sig in included])
    result = pipeline.apply_async()
    logger.info(
        "post_market_pipeline.started",
        trading_date=td,
        task_id=str(result.id),
        stop_after=stop_after,
        steps=len(included),
    )
    return f"Pipeline started: {result.id}"


# ── 盘中期权链采集（Celery Worker）─────────────────────────


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


# ── Post-market data collection only (no signals / blueprint) ──


@celery_app.task(
    name="data_service.tasks.collect_post_market_data",
    bind=True,
    max_retries=3,
    queue="data",
)
def collect_post_market_data(self, trading_date: str | None = None) -> dict:
    """只执行盘后数据采集（1m bars / daily bars / aggregate option daily）。

    Chain:  capture_post_market_data → aggregate_option_daily
    不触发 backfill / signals / blueprint，适合手动补采。
    """
    td = trading_date or today_trading().isoformat()
    logger.info(
        "collect_post_market.building",
        log_event="task_start",
        stage="compose_chain",
        trading_date=td,
        task_id=getattr(self.request, "id", None),
    )

    pipeline = celery_chain(
        capture_post_market_data.si(td).set(queue="data"),
        aggregate_option_daily.si(td).set(queue="data"),
    )

    result = pipeline.apply_async()
    logger.info("collect_post_market.started", trading_date=td, chain_id=str(result.id))
    return {
        "status": "chain_dispatched",
        "trading_date": td,
        "chain_id": str(result.id),
        "steps": [
            "capture_post_market_data",
            "aggregate_option_daily",
        ],
    }


# ── Manual collection task (triggered via REST API) ────────


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
