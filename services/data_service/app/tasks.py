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
from datetime import date, datetime

from celery import chain as celery_chain

from shared.celery_app import celery_app
from shared.config import get_settings
from shared.db.session import get_timescale_session
from shared.utils import get_logger

logger = get_logger("data_tasks")


# ── Step 1: 盘后数据采集 ──────────────────────────────────


@celery_app.task(
    name="data_service.tasks.capture_post_market_data",
    bind=True,
    max_retries=3,
)
def capture_post_market_data(self, trading_date: str | None = None) -> dict:
    """盘后统一采集：1m bars → stock_1min_bars, daily bar → stock_daily, option chain → option_daily"""
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
    td = date.fromisoformat(trading_date_str) if trading_date_str else date.today()

    result = {
        "date": str(td),
        "stock_1min_rows": 0,
        "stock_daily_rows": 0,
        "option_daily_rows": 0,
        "errors": [],
    }

    for symbol in symbols:
        try:
            # ── (a) 当天全天 1 分钟 K 线 → stock_1min_bars ──
            bars_1m = await fetch_stock_bars(symbol, period="1d", interval="1m")
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
                written = await write_intraday_stock(intraday_rows)
                result["stock_1min_rows"] += written

            # ── (b) 日线 → stock_daily ──
            bars_daily = await fetch_stock_bars(symbol, period="5d", interval="1d")
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
                written = await write_swing_stock([daily_row])
                result["stock_daily_rows"] += written

            # ── (c) 期权链快照 → option_daily ──
            snapshot = await fetch_option_chain(symbol)
            if snapshot:
                option_rows = contracts_to_rows(snapshot, include_snapshot_date=True)
                written = await write_swing_options(option_rows)
                result["option_daily_rows"] += written

        except Exception as e:
            error_msg = f"{symbol}: {str(e)}"
            result["errors"].append(error_msg)
            logger.error("capture_post_market.symbol_error", symbol=symbol, error=str(e))

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
    try:
        return asyncio.run(_batch_flush_to_db_async(trading_date))
    except Exception as exc:
        logger.error("batch_flush.failed", error=str(exc))
        raise self.retry(exc=exc, countdown=60) from exc


async def _batch_flush_to_db_async(trading_date_str: str | None = None) -> dict:
    from services.data_service.app.cache import MarketHoursCache

    trading_date = date.fromisoformat(trading_date_str) if trading_date_str else date.today()
    cache = MarketHoursCache()
    cache.flush_all()

    result = {"option_rows": 0}

    df = cache.read_parquet("option", trading_date)
    if df is None or df.empty:
        logger.info("batch_flush.no_option_data", date=str(trading_date))
        return result

    async with get_timescale_session() as session:
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

    result["option_rows"] = len(df)
    cache.clear_parquet("option", trading_date)
    logger.info("batch_flush.success", option_rows=result["option_rows"])
    return result


# ── Pipeline 入口 ──────────────────────────────────────────


@celery_app.task(name="data_service.tasks.run_post_market_pipeline")
def run_post_market_pipeline(trading_date: str | None = None) -> str:
    """盘后流水线入口（16:30 由 Celery Beat 触发）

    Chain: capture → flush → backfill → signals → blueprint
    """
    td = trading_date or date.today().isoformat()

    pipeline = celery_chain(
        capture_post_market_data.s(td),
        batch_flush_to_db.s(td),
        celery_app.signature(
            "backfill_service.tasks.detect_and_backfill_gaps",
            args=[td],
            queue="backfill",
        ),
        celery_app.signature(
            "signal_service.tasks.compute_daily_signals",
            args=[td],
            queue="signal",
        ),
        celery_app.signature(
            "analysis_service.tasks.generate_daily_blueprint",
            args=[td],
            queue="analysis",
        ),
    )

    result = pipeline.apply_async()
    logger.info("post_market_pipeline.started", trading_date=td, task_id=str(result.id))
    return f"Pipeline started: {result.id}"
