"""Data Service — Celery 盘后批量任务

Pipeline 顺序:
  1. capture_post_market_data  — 采集 1m bars / daily bar / option chain → 直接写 DB
  2. batch_flush_to_db         — 将盘中 option Parquet 缓存批量入库（仅 intraday 模式）
  3. detect_and_backfill_gaps  — 缺口检测与回填  (Backfill Service)
  4. compute_daily_signals     — 信号计算          (Signal Service)
  5. generate_daily_blueprint  — 生成交易蓝图      (Analysis Service)
"""
from __future__ import annotations

import asyncio
from datetime import date, datetime, time
from time import perf_counter

from celery import chain as celery_chain

from shared.celery_app import celery_app
from shared.config import get_settings
from shared.db.session import get_timescale_session
from shared.utils import (
    get_logger,
    previous_trading_day,
    resolve_trading_date_arg,
    today_trading,
)

logger = get_logger("data_tasks")


def _normalize_manual_end_date(end_date: date) -> tuple[date, str | None]:
    """Task-level fallback normalization for pre-market manual collection.

    Keeps behavior consistent when task is invoked directly (bypassing REST route).
    Uses trading.timezone for all time comparisons.
    """
    from shared.utils import before_market_open, now_market as _now_market

    today = today_trading()
    if end_date != today:
        return end_date, None

    if before_market_open():
        now_mkt = _now_market()
        settings = get_settings()
        normalized = previous_trading_day(today)
        warning = (
            f"manual_collect: end_date {end_date} adjusted to {normalized}; "
            f"current market time {now_mkt.strftime('%H:%M')} is before open "
            f"{settings.data_service.market_hours.start}"
        )
        return normalized, warning

    return end_date, None


# ── Step 1: 盘后数据采集 ──────────────────────────────────


@celery_app.task(
    name="data_service.tasks.capture_post_market_data",
    bind=True,
    max_retries=3,
)
def capture_post_market_data(self, trading_date: str | None = None) -> dict:
    """盘后统一采集：1m bars → stock_1min_bars, daily bar → stock_daily, option chain → option_daily"""
    logger.debug(
        "capture_post_market.start",
        log_event="task_start",
        stage="entry",
        task_id=getattr(self.request, "id", None),
        trading_date=trading_date,
        retry=getattr(self.request, "retries", 0),
    )
    try:
        return asyncio.run(_capture_post_market_async(trading_date))
    except Exception as exc:
        logger.error("capture_post_market.failed", error=str(exc))
        raise self.retry(exc=exc, countdown=120) from exc


async def _capture_post_market_async(trading_date_str: str | None = None) -> dict:
    from services.data_service.app.converters import contracts_to_rows
    from services.data_service.app.fetchers.option_fetcher import fetch_option_chain
    from services.data_service.app.fetchers.stock_fetcher import fetch_stock_bars
    from services.data_service.app.storage import (
        write_intraday_stock,
        write_swing_options,
        write_swing_stock,
    )

    settings = get_settings()
    symbols = settings.watchlist
    td = date.fromisoformat(trading_date_str) if trading_date_str else today_trading()
    started = perf_counter()
    logger.debug(
        "capture_post_market.context",
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
        "option_daily_rows": 0,
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
            bars_1m = await fetch_stock_bars(symbol, period="1d", interval="1m")
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
            bars_daily = await fetch_stock_bars(symbol, period="5d", interval="1d")
            logger.debug(
                "capture_post_market.fetch_stock_daily_finished",
                log_event="external_call",
                stage="after_fetch",
                symbol=symbol,
                rows=len(bars_daily) if bars_daily else 0,
            )
            if bars_daily:
                latest = bars_daily[-1]
                daily_row = {
                    "symbol": latest["symbol"],
                    "trading_date": datetime.fromisoformat(latest["timestamp"]).date(),
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

            # ── (c) 期权链快照 → option_daily ──
            logger.debug(
                "capture_post_market.fetch_option_chain_started",
                log_event="external_call",
                stage="before_fetch",
                symbol=symbol,
                provider="yfinance",
            )
            snapshot = await fetch_option_chain(symbol)
            if snapshot:
                option_rows = contracts_to_rows(snapshot, include_snapshot_date=True)
                logger.debug(
                    "capture_post_market.write_option_daily_started",
                    log_event="db_write",
                    stage="before_write",
                    symbol=symbol,
                    rows=len(option_rows),
                )
                written = await write_swing_options(option_rows)
                result["option_daily_rows"] += written
                logger.debug(
                    "capture_post_market.write_option_daily_finished",
                    log_event="db_write",
                    stage="after_write",
                    symbol=symbol,
                    rows=written,
                )
            else:
                logger.debug(
                    "capture_post_market.option_chain_empty",
                    log_event="external_call",
                    stage="after_fetch",
                    symbol=symbol,
                )

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
        option_daily_rows=result["option_daily_rows"],
        errors=len(result["errors"]),
        duration_ms=round((perf_counter() - started) * 1000, 2),
    )

    logger.info("capture_post_market.done", **{k: v for k, v in result.items() if k != "errors"})
    return result


# ── Step 2: 盘中缓存批量入库 ──────────────────────────────


@celery_app.task(
    name="data_service.tasks.batch_flush_to_db",
    bind=True,
    max_retries=3,
)
def batch_flush_to_db(self, trading_date: str | None = None, prev_result=None) -> dict:
    """将盘中期权链 Parquet 缓存批量写入 option_5min_snapshots（仅 intraday 模式产生数据）"""
    resolved_trading_date = resolve_trading_date_arg(trading_date, prev_result)
    logger.debug(
        "batch_flush.start",
        log_event="task_start",
        stage="entry",
        task_id=getattr(self.request, "id", None),
        trading_date=trading_date,
        resolved_trading_date=resolved_trading_date,
        retry=getattr(self.request, "retries", 0),
    )
    try:
        return asyncio.run(_batch_flush_to_db_async(resolved_trading_date))
    except Exception as exc:
        logger.error("batch_flush.failed", error=str(exc))
        raise self.retry(exc=exc, countdown=60) from exc


async def _batch_flush_to_db_async(trading_date_str: str | None = None) -> dict:
    from services.data_service.app.cache import MarketHoursCache

    trading_date = date.fromisoformat(trading_date_str) if trading_date_str else today_trading()
    started = perf_counter()
    cache = MarketHoursCache()
    logger.debug(
        "batch_flush.cache_flush_started",
        log_event="cache_flush",
        stage="before_flush",
        trading_date=str(trading_date),
    )
    cache.flush_all()
    logger.debug(
        "batch_flush.cache_flush_finished",
        log_event="cache_flush",
        stage="after_flush",
        trading_date=str(trading_date),
    )

    result = {"option_rows": 0}

    df = cache.read_parquet("option", trading_date)
    if df is None or df.empty:
        logger.info("batch_flush.no_option_data", date=str(trading_date))
        return result

    logger.debug(
        "batch_flush.parquet_loaded",
        log_event="cache_read",
        stage="after_read",
        trading_date=str(trading_date),
        rows=len(df),
    )

    async with get_timescale_session() as session:
        logger.debug(
            "batch_flush.db_write_started",
            log_event="db_write",
            stage="before_write",
            trading_date=str(trading_date),
            rows=len(df),
        )
        conn = await session.connection()
        raw_conn = await conn.get_raw_connection()
        df.to_sql(
            "option_5min_snapshots",
            con=raw_conn.dbapi_connection,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )
        logger.debug(
            "batch_flush.db_write_finished",
            log_event="db_write",
            stage="after_write",
            trading_date=str(trading_date),
            rows=len(df),
        )

    result["option_rows"] = len(df)
    cache.clear_parquet("option", trading_date)
    logger.debug(
        "batch_flush.summary",
        log_event="task_summary",
        stage="completed",
        trading_date=str(trading_date),
        option_rows=result["option_rows"],
        duration_ms=round((perf_counter() - started) * 1000, 2),
    )
    logger.info("batch_flush.success", option_rows=result["option_rows"])
    return result


# ── Pipeline 入口 ──────────────────────────────────────────


@celery_app.task(name="data_service.tasks.run_post_market_pipeline")
def run_post_market_pipeline(trading_date: str | None = None) -> str:
    """盘后流水线入口（16:30 由 Celery Beat 触发）

    Chain: capture → flush → backfill → signals → blueprint
    """
    td = trading_date or today_trading().isoformat()
    logger.debug(
        "post_market_pipeline.building",
        log_event="pipeline_start",
        stage="compose_chain",
        trading_date=td,
    )

    pipeline = celery_chain(
        capture_post_market_data.si(td),
        batch_flush_to_db.si(td),
        celery_app.signature(
            "backfill_service.tasks.detect_and_backfill_gaps",
            args=[td],
            queue="backfill",
            immutable=True,
        ),
        celery_app.signature(
            "signal_service.tasks.compute_daily_signals",
            args=[td],
            queue="signal",
            immutable=True,
        ),
        celery_app.signature(
            "analysis_service.tasks.generate_daily_blueprint",
            args=[td],
            queue="analysis",
            immutable=True,
        ),
    )

    result = pipeline.apply_async()
    logger.info("post_market_pipeline.started", trading_date=td, task_id=str(result.id))
    logger.debug(
        "post_market_pipeline.dispatched",
        log_event="pipeline_start",
        stage="dispatched",
        trading_date=td,
        task_id=str(result.id),
    )
    return f"Pipeline started: {result.id}"


# ── Manual collection task (triggered via REST API) ────────


@celery_app.task(
    name="data_service.tasks.manual_collect",
    bind=True,
    max_retries=1,
)
def manual_collect(
    self,
    symbols: list[str],
    start_date: str,
    end_date: str,
    data_types: list[str],
) -> dict:
    """Manually collect historical data for given symbols and date range.

    data_types: subset of ["bars_1m", "bars_daily", "options_daily"]
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
    from services.data_service.app.converters import contracts_to_rows
    from services.data_service.app.fetchers.option_fetcher import fetch_option_chain
    from services.data_service.app.fetchers.stock_fetcher import fetch_stock_bars_range
    from services.data_service.app.storage import (
        write_intraday_stock,
        write_swing_options,
        write_swing_stock,
    )

    sd = date.fromisoformat(start_date_str)
    ed = date.fromisoformat(end_date_str)

    normalized_end_date, normalization_warning = _normalize_manual_end_date(ed)
    ed = normalized_end_date

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
        "options_daily_rows": 0,
        "warnings": [],
        "errors": [],
    }

    if normalization_warning:
        result["warnings"].append(normalization_warning)

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
                rows, warns = await fetch_stock_bars_range(symbol, sd, ed, interval="1d")
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
                rows, warns = await fetch_stock_bars_range(symbol, sd, ed, interval="1m")
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

        # ── options_daily: yfinance only returns current snapshot ──
        if "options_daily" in data_types:
            current_step += 1
            task.update_state(
                state="PROGRESS",
                meta={
                    "current_step": current_step,
                    "total_steps": total_steps,
                    "symbol": symbol,
                    "data_type": "options_daily",
                },
            )
            if ed < today:
                result["warnings"].append(
                    f"{symbol}: options_daily skipped — yfinance has no historical "
                    f"option chain; requested end_date {ed} < today {today}"
                )
            else:
                try:
                    logger.debug(
                        "manual_collect.fetch_options_started",
                        log_event="external_call",
                        stage="before_fetch",
                        symbol=symbol,
                        provider="yfinance",
                    )
                    snapshot = await fetch_option_chain(symbol)
                    if snapshot:
                        option_rows = contracts_to_rows(snapshot, include_snapshot_date=True)
                        logger.debug(
                            "manual_collect.write_options_started",
                            log_event="db_write",
                            stage="before_write",
                            symbol=symbol,
                            rows=len(option_rows),
                        )
                        written = await write_swing_options(option_rows)
                        result["options_daily_rows"] += written
                        logger.debug(
                            "manual_collect.write_options_finished",
                            log_event="db_write",
                            stage="after_write",
                            symbol=symbol,
                            rows=written,
                        )
                    else:
                        result["warnings"].append(f"{symbol}: no option chain returned")
                        logger.debug(
                            "manual_collect.fetch_options_empty",
                            log_event="external_call",
                            stage="after_fetch",
                            symbol=symbol,
                        )
                except Exception as e:
                    result["errors"].append(f"{symbol}/options_daily: {e}")
                    logger.error("manual_collect.options_error", symbol=symbol, error=str(e))

    if result["errors"]:
        result["status"] = "completed_with_errors"

    logger.info(
        "manual_collect.done",
        symbols=len(symbols),
        bars_1m=result["bars_1m_rows"],
        bars_daily=result["bars_daily_rows"],
        options_daily=result["options_daily_rows"],
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
        options_daily_rows=result["options_daily_rows"],
        warnings=len(result["warnings"]),
        errors=len(result["errors"]),
        duration_ms=round((perf_counter() - started) * 1000, 2),
    )
    return result


# ── Options collection task (triggered via REST API) ───────


@celery_app.task(
    name="data_service.tasks.collect_options",
    bind=True,
    max_retries=1,
)
def collect_options(
    self,
    symbols: list[str],
    historical_date: str | None = None,
) -> dict:
    """Collect option chain data for given symbols.

    - If ``historical_date`` is None → fetch today's live chain.
    - If ``historical_date`` is provided → use the options_historical provider.
    """
    logger.debug(
        "collect_options.start",
        log_event="task_start",
        stage="entry",
        task_id=getattr(self.request, "id", None),
        symbols=len(symbols),
        historical_date=historical_date,
        retry=getattr(self.request, "retries", 0),
    )
    try:
        return asyncio.run(
            _collect_options_async(self, symbols, historical_date)
        )
    except Exception as exc:
        logger.error("collect_options.failed", error=str(exc))
        raise self.retry(exc=exc, countdown=30) from exc


async def _collect_options_async(
    task,
    symbols: list[str],
    historical_date_str: str | None = None,
) -> dict:
    from services.data_service.app.converters import contracts_to_rows
    from services.data_service.app.fetchers.option_fetcher import fetch_option_chain
    from services.data_service.app.storage import write_swing_options

    settings = get_settings()
    started = perf_counter()
    options_provider = settings.data_service.providers.options.strip().lower()
    historical_provider = settings.data_service.providers.options_historical.strip().lower()

    result: dict = {
        "status": "completed",
        "symbols": symbols,
        "mode": "historical" if historical_date_str else "live",
        "options_rows": 0,
        "warnings": [],
        "errors": [],
    }

    total_steps = len(symbols)

    if historical_date_str is not None:
        # ── Historical mode ──
        target_date = date.fromisoformat(historical_date_str)
        today = today_trading()
        use_premarket_yf_fallback = False
        if options_provider == "yfinance":
            from shared.utils import before_market_open

            if before_market_open():
                prev_day = previous_trading_day(today)
                use_premarket_yf_fallback = target_date in {today, prev_day}

        result["historical_date"] = historical_date_str
        result["historical_provider"] = historical_provider
        if use_premarket_yf_fallback and historical_provider == "none":
            result["historical_provider"] = "yfinance_live_premarket_fallback"

        if historical_provider == "none" and not use_premarket_yf_fallback:
            result["status"] = "failed"
            result["errors"].append("options_historical provider is 'none' — no historical data available")
            return result

        if use_premarket_yf_fallback:
            result["warnings"].append(
                "Using yfinance live pre-market snapshot as previous-trading-day historical fallback"
            )
            for idx, symbol in enumerate(symbols, 1):
                task.update_state(
                    state="PROGRESS",
                    meta={
                        "current_step": idx,
                        "total_steps": total_steps,
                        "symbol": symbol,
                        "mode": "historical",
                    },
                )
                try:
                    logger.debug(
                        "collect_options.fetch_live_fallback_start",
                        symbol=symbol,
                        target_date=str(target_date),
                    )
                    snapshot = await fetch_option_chain(symbol)
                    if snapshot:
                        option_rows = contracts_to_rows(snapshot, include_snapshot_date=True)
                        for row in option_rows:
                            row["snapshot_date"] = target_date
                        written = await write_swing_options(option_rows)
                        result["options_rows"] += written
                        logger.info(
                            "collect_options.historical_fallback_written",
                            symbol=symbol,
                            date=str(target_date),
                            rows=written,
                            provider="yfinance",
                        )
                    else:
                        result["warnings"].append(
                            f"{symbol}: no option chain returned for pre-market fallback ({target_date})"
                        )
                except Exception as e:
                    result["errors"].append(f"{symbol}/historical_fallback: {e}")
                    logger.error(
                        "collect_options.historical_fallback_error",
                        symbol=symbol,
                        error=str(e),
                    )
            if result["errors"]:
                result["status"] = "completed_with_errors"
            return result

        # Mock provider — placeholder for future real implementation
        for idx, symbol in enumerate(symbols, 1):
            task.update_state(
                state="PROGRESS",
                meta={
                    "current_step": idx,
                    "total_steps": total_steps,
                    "symbol": symbol,
                    "mode": "historical",
                },
            )
            try:
                snapshot = await _fetch_historical_options_mock(symbol, target_date, historical_provider)
                if snapshot:
                    option_rows = contracts_to_rows(snapshot, include_snapshot_date=True)
                    written = await write_swing_options(option_rows)
                    result["options_rows"] += written
                    logger.info(
                        "collect_options.historical_written",
                        symbol=symbol,
                        date=str(target_date),
                        rows=written,
                        provider=historical_provider,
                    )
                else:
                    result["warnings"].append(
                        f"{symbol}: historical options mock returned no data for {target_date}"
                    )
            except Exception as e:
                result["errors"].append(f"{symbol}/historical: {e}")
                logger.error("collect_options.historical_error", symbol=symbol, error=str(e))
    else:
        # ── Live mode (today's option chain) ──
        for idx, symbol in enumerate(symbols, 1):
            task.update_state(
                state="PROGRESS",
                meta={
                    "current_step": idx,
                    "total_steps": total_steps,
                    "symbol": symbol,
                    "mode": "live",
                },
            )
            try:
                logger.debug("collect_options.fetch_live_start", symbol=symbol)
                snapshot = await fetch_option_chain(symbol)
                if snapshot:
                    option_rows = contracts_to_rows(snapshot, include_snapshot_date=True)
                    written = await write_swing_options(option_rows)
                    result["options_rows"] += written
                    logger.info("collect_options.live_written", symbol=symbol, rows=written)
                else:
                    result["warnings"].append(f"{symbol}: no option chain returned")
            except Exception as e:
                result["errors"].append(f"{symbol}/live: {e}")
                logger.error("collect_options.live_error", symbol=symbol, error=str(e))

    if result["errors"]:
        result["status"] = "completed_with_errors"

    logger.info(
        "collect_options.done",
        symbols=len(symbols),
        mode=result["mode"],
        options_rows=result["options_rows"],
        warnings=len(result["warnings"]),
        errors=len(result["errors"]),
    )
    logger.debug(
        "collect_options.summary",
        log_event="task_summary",
        stage="completed",
        symbols=len(symbols),
        mode=result["mode"],
        options_rows=result["options_rows"],
        duration_ms=round((perf_counter() - started) * 1000, 2),
    )
    return result


async def _fetch_historical_options_mock(
    symbol: str,
    target_date: date,
    provider: str,
):
    """Mock historical options data fetch — placeholder for real implementation.

    When a real historical options provider is configured (e.g. 'cboe', 'orats',
    'thetadata'), this function should be replaced with an actual data fetch.
    Currently returns None to indicate no data available.

    TODO: implement real providers:
      - 'cboe': CBOE DataShop historical options
      - 'orats': ORATS historical IV & chain data
      - 'thetadata': ThetaData historical options
    """
    logger.info(
        "collect_options.historical_mock",
        symbol=symbol,
        target_date=str(target_date),
        provider=provider,
        message="Historical options mock — no real data returned. "
                "Implement provider-specific fetcher to enable.",
    )
    # Return None — mock has no data. Real implementation would return
    # an OptionChainSnapshot populated from the historical provider.
    return None
