"""Signal Service — Celery 盘后批量计算任务

职责：纯编排层 — 加载数据、调用指标计算模块、写入 DB/缓存。
所有计算逻辑已拆分至：
  • data_loaders.py   — 市场数据加载（stock bars, option chain）
  • cross_asset.py    — 跨资产指标（SPY beta, IV correlation 等）
  • indicators/       — 股票 & 期权技术指标
  • signal_generator.py — SignalFeatures 组装
"""
from __future__ import annotations

import asyncio
from datetime import date
from time import perf_counter

from sqlalchemy import text

from shared.celery_app import celery_app
from shared.config import get_settings
from shared.db.session import get_postgres_session
from shared.utils import get_logger, resolve_trading_date_arg, today_trading

logger = get_logger("signal_tasks")


# ── Celery ↔ asyncio bridge ───────────────────────────────

def _run_async(coro):
    """Run an async coroutine safely — works whether or not an event loop exists."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            return pool.submit(asyncio.run, coro).result()
    return asyncio.run(coro)


# ── Public Celery task ─────────────────────────────────────

@celery_app.task(name="signal_service.tasks.compute_daily_signals", bind=True, max_retries=2)
def compute_daily_signals(
    self,
    trading_date: str | None = None,
    prev_result=None,
    symbols: list[str] | None = None,
) -> dict:
    """17:00 Celery 任务：批量计算当日所有标的的信号特征。"""
    resolved_trading_date = resolve_trading_date_arg(trading_date, prev_result)
    logger.debug(
        "signal_compute.start",
        log_event="task_start",
        stage="entry",
        task_id=getattr(self.request, "id", None),
        trading_date=trading_date,
        resolved_trading_date=resolved_trading_date,
        symbols=symbols,
        retry=getattr(self.request, "retries", 0),
    )
    return _run_async(_compute_daily_signals(resolved_trading_date, symbols=symbols))


# ── Async orchestrator ─────────────────────────────────────

async def _compute_daily_signals(
    trading_date_str: str | None = None,
    *,
    symbols: list[str] | None = None,
) -> dict:
    # Lazy imports — avoids circular dependencies at module level
    from services.signal_service.app.cross_asset import build_cross_asset_indicators
    from services.signal_service.app.data_loaders import (
        load_benchmark_returns,
        load_option_rows,
        load_stock_bars,
        load_vix_bars,
    )
    from services.signal_service.app.indicators.option_indicators import compute_option_indicators
    from services.signal_service.app.indicators.stock_indicators import compute_stock_indicators
    from services.signal_service.app.queries import delete_signal_cache, set_signal_cache
    from services.signal_service.app.signal_generator import generate_signal

    settings = get_settings()
    td = date.fromisoformat(trading_date_str) if trading_date_str else today_trading()
    target_symbols = [s.upper() for s in symbols] if symbols else settings.watchlist
    started = perf_counter()
    logger.debug(
        "signal_compute.context",
        log_event="task_context",
        stage="start",
        trading_date=str(td),
        symbols=len(target_symbols),
        custom_symbols=symbols is not None,
    )

    result: dict = {"date": str(td), "symbols_computed": 0, "errors": []}
    symbols_no_data: list[str] = []

    # ── Pre-load benchmark returns & VIX (once) ─────────────
    benchmark_returns = await load_benchmark_returns(
        settings.cross_asset_benchmarks, td,
    )
    vix_bars = await load_vix_bars(td)

    loaded_benchmarks = [k for k, v in benchmark_returns.items() if not v.empty]
    logger.debug(
        "signal_compute.benchmarks_preloaded",
        log_event="db_read",
        stage="preload",
        benchmarks_requested=settings.cross_asset_benchmarks,
        benchmarks_loaded=loaded_benchmarks,
        vix_bars=len(vix_bars),
    )
    if not loaded_benchmarks:
        logger.warning(
            "signal_compute.no_benchmark_data",
            trading_date=str(td),
            detail="All benchmark betas & correlations will default to 0.0",
        )

    # ── Per-symbol processing ──────────────────────────────
    sem = asyncio.Semaphore(4)

    async def _process_symbol(symbol: str) -> None:
        async with sem:
            try:
                t0 = perf_counter()
                bars_df, bar_type = await load_stock_bars(symbol, td)
                option_df = await load_option_rows(symbol, td)
                logger.debug(
                    "signal_compute.data_loaded",
                    log_event="symbol_context",
                    stage="after_load",
                    symbol=symbol,
                    daily_rows=len(bars_df),
                    option_rows=len(option_df),
                    bar_type=bar_type,
                )

                if bars_df.empty:
                    logger.warning("signal_compute.no_bars", symbol=symbol, date=str(td))
                    symbols_no_data.append(symbol)
                    return

                # ── Price & volume summary ─────────────────
                close_price = float(bars_df["close"].iloc[-1])
                prev_close = float(bars_df["close"].iloc[-2]) if len(bars_df) >= 2 else close_price
                daily_return = (close_price - prev_close) / prev_close if prev_close > 0 else 0.0
                total_volume = int(bars_df["volume"].iloc[-1])

                # ── Technical indicators ───────────────────
                stock_indicators = compute_stock_indicators(bars_df)
                option_indicators = await compute_option_indicators(symbol, option_df, close_price)

                # ── Cross-asset indicators ─────────────────
                bar_returns = bars_df["close"].pct_change().dropna()
                total_option_volume = float(option_df["volume"].sum()) if not option_df.empty else 0.0
                hedge_ratio = -float(option_indicators.portfolio_greeks.get("delta", 0.0))

                cross_asset = build_cross_asset_indicators(
                    symbol=symbol,
                    bars_df=bars_df,
                    bar_returns=bar_returns,
                    option_df=option_df,
                    benchmark_returns=benchmark_returns,
                    vix_bars=vix_bars,
                    total_volume=total_volume,
                    total_option_volume=total_option_volume,
                    hedge_ratio=hedge_ratio,
                )

                # ── HV-IV spread ───────────────────────────
                stock_indicators.hv_iv_spread = round(
                    stock_indicators.hv_20d - option_indicators.current_iv, 6,
                )

                # ── Assemble signal ────────────────────────
                features = generate_signal(
                    symbol=symbol,
                    trading_date=td,
                    close_price=close_price,
                    daily_return=daily_return,
                    volume=total_volume,
                    bar_type=bar_type,
                    option_indicators=option_indicators,
                    stock_indicators=stock_indicators,
                    cross_asset_indicators=cross_asset,
                    stock_bar_count=len(bars_df),
                    option_row_count=len(option_df),
                )

                # ── Persist to DB ──────────────────────────
                await _write_signal(symbol, td, features)

                # ── Refresh cache (best-effort) ────────────
                await _refresh_cache(symbol, td, features, set_signal_cache, delete_signal_cache)

                result["symbols_computed"] += 1
                logger.debug(
                    "signal_compute.symbol_completed",
                    log_event="symbol_summary",
                    stage="completed",
                    symbol=symbol,
                    trading_date=str(td),
                    bar_type=bar_type,
                    duration_ms=round((perf_counter() - t0) * 1000, 2),
                )
                logger.info(
                    "signal_compute.done",
                    symbol=symbol,
                    regime=features.volatility_regime,
                )

            except Exception as e:
                result["errors"].append(f"{symbol}: {str(e)}")
                logger.error("signal_compute.failed", symbol=symbol, error=str(e))

    await asyncio.gather(*[_process_symbol(s) for s in target_symbols])

    # ── Summarise ──────────────────────────────────────────
    if result["symbols_computed"] == 0 and not result["errors"]:
        no_data_msg = f"No stock data found for trading_date={td}. Symbols checked: {', '.join(target_symbols)}"
        result["errors"].append(no_data_msg)
        logger.error(
            "signal_compute.no_data_all_symbols",
            trading_date=str(td),
            symbols=target_symbols,
        )
    elif symbols_no_data:
        result["symbols_no_data"] = symbols_no_data

    logger.debug(
        "signal_compute.summary",
        log_event="task_summary",
        stage="completed",
        trading_date=str(td),
        symbols_total=len(settings.watchlist),
        symbols_computed=result["symbols_computed"],
        errors=len(result["errors"]),
        duration_ms=round((perf_counter() - started) * 1000, 2),
    )
    logger.info("signal_compute.batch_completed", **result)
    return result


# ── DB write helper ────────────────────────────────────────

async def _write_signal(symbol: str, td: date, features) -> None:
    async with get_postgres_session() as session:
        logger.debug(
            "signal_compute.db_write",
            log_event="db_write",
            stage="before_write",
            symbol=symbol,
            trading_date=str(td),
        )
        await session.execute(
            text(
                "INSERT INTO signal_features (symbol, date, computed_at, features_json) "
                "VALUES (:symbol, :date, :computed_at, :features_json) "
                "ON CONFLICT (symbol, date) DO UPDATE SET "
                "computed_at = :computed_at, features_json = :features_json"
            ),
            {
                "symbol": symbol,
                "date": td,
                "computed_at": features.computed_at,
                "features_json": features.model_dump_json(),
            },
        )
        logger.debug(
            "signal_compute.db_write_done",
            log_event="db_write",
            stage="after_write",
            symbol=symbol,
            trading_date=str(td),
        )


# ── Cache helper ───────────────────────────────────────────

async def _refresh_cache(symbol, td, features, set_fn, delete_fn) -> None:
    """Write-through cache refresh; falls back to delete on error."""
    try:
        await set_fn(symbol, td, features.model_dump(mode="json"))
    except Exception as cache_exc:
        logger.debug("signal_compute.cache_refresh_failed", symbol=symbol, error=str(cache_exc))
        try:
            await delete_fn(symbol, td)
        except Exception as del_exc:
            logger.debug("signal_compute.cache_delete_failed", symbol=symbol, error=str(del_exc))
