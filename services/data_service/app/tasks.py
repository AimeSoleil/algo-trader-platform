"""Data Service — Celery 盘后批量任务

Pipeline 顺序:
  1. capture_post_market_data  — 采集 1m bars / daily bar → 直接写 DB
  2. batch_flush_to_db         — 将盘中 option Parquet 缓存批量入库（仅 intraday 模式）
  2b. aggregate_option_daily   — 盘中快照聚合 → option_daily + option_iv_daily
  3. detect_and_backfill_gaps  — 缺口检测与回填  (Backfill Service)
  4. compute_daily_signals     — 信号计算          (Signal Service)
  5. generate_daily_blueprint  — 生成交易蓝图      (Analysis Service)
"""
from __future__ import annotations

import asyncio
from datetime import date, datetime
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


# ── Step 1: 盘后数据采集 ──────────────────────────────────


@celery_app.task(
    name="data_service.tasks.capture_post_market_data",
    bind=True,
    max_retries=3,
    queue="data",
)
def capture_post_market_data(self, trading_date: str | None = None) -> dict:
    """盘后统一采集：1m bars → stock_1min_bars, daily bar → stock_daily（不含期权）"""
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
    from services.data_service.app.fetchers.registry import get_stock_fetcher
    from services.data_service.app.storage import (
        write_intraday_stock,
        write_swing_stock,
    )

    settings = get_settings()
    stock_fetcher = get_stock_fetcher()
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


# ── Step 2: 盘中缓存批量入库 ──────────────────────────────


@celery_app.task(
    name="data_service.tasks.batch_flush_to_db",
    bind=True,
    max_retries=3,
    queue="data",
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
    import pyarrow.parquet as pq
    from services.data_service.app.cache import MarketHoursCache
    from services.data_service.app.storage import write_intraday_options

    trading_date = date.fromisoformat(trading_date_str) if trading_date_str else today_trading()
    started = perf_counter()
    cache = MarketHoursCache()

    result = {"option_rows": 0}

    parquet_path = cache.get_parquet_path("option", trading_date)
    if not parquet_path.exists():
        logger.info("batch_flush.no_option_data", date=str(trading_date))
        return result

    pf = pq.ParquetFile(str(parquet_path))
    total_rows = pf.metadata.num_rows
    logger.debug(
        "batch_flush.parquet_opened",
        log_event="cache_read",
        stage="after_open",
        trading_date=str(trading_date),
        total_rows=total_rows,
    )

    batch_size = 100_000  # 每批读取行数，~~20-30 MB 内存
    rows_written = 0

    for batch in pf.iter_batches(batch_size=batch_size):
        chunk_df = batch.to_pandas()
        if chunk_df.empty:
            continue

        # Backward compatibility: old Parquet files may lack new columns
        if "vanna" not in chunk_df.columns:
            chunk_df["vanna"] = 0.0
        if "charm" not in chunk_df.columns:
            chunk_df["charm"] = 0.0
        if "is_tradeable" not in chunk_df.columns:
            chunk_df["is_tradeable"] = False

        logger.debug(
            "batch_flush.chunk_write_start",
            log_event="db_write",
            stage="chunk_start",
            trading_date=str(trading_date),
            chunk_rows=len(chunk_df),
            rows_written=rows_written,
        )

        chunk_dicts = chunk_df.to_dict("records")
        written = await write_intraday_options(chunk_dicts)
        rows_written += written

    logger.debug(
        "batch_flush.db_write_finished",
        log_event="db_write",
        stage="after_write",
        trading_date=str(trading_date),
        rows=rows_written,
    )

    result["option_rows"] = rows_written
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


# ── Step 2b: 盘中快照聚合 → option_daily + option_iv_daily ──


@celery_app.task(
    name="data_service.tasks.aggregate_option_daily",
    bind=True,
    max_retries=3,
    queue="data",
)
def aggregate_option_daily(self, trading_date: str | None = None, prev_result=None) -> dict:
    """Aggregate intraday 5-min snapshots into option_daily + option_iv_daily.

    Must run AFTER batch_flush_to_db (snapshots need to be in DB first).
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


# ── Pipeline 入口 ──────────────────────────────────────────


@celery_app.task(name="data_service.tasks.run_post_market_pipeline", queue="data")
def run_post_market_pipeline(trading_date: str | None = None) -> str:
    """盘后流水线入口（由 Celery Beat 触发）

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
        capture_post_market_data.si(td).set(queue="data"),
        batch_flush_to_db.si(td).set(queue="data"),
        aggregate_option_daily.si(td).set(queue="data"),
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


# ── Post-market data collection only (no signals / blueprint) ──


@celery_app.task(
    name="data_service.tasks.collect_post_market_data",
    bind=True,
    max_retries=3,
    queue="data",
)
def collect_post_market_data(self, trading_date: str | None = None) -> dict:
    """只执行盘后数据采集（1m bars / daily bars / flush option parquet / aggregate）。

    Chain:  capture_post_market_data → batch_flush_to_db → aggregate_option_daily
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
        batch_flush_to_db.si(td).set(queue="data"),
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
            "batch_flush_to_db",
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
