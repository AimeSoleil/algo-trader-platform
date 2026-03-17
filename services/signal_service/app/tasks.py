"""Signal Service — Celery 盘后批量计算任务"""
from __future__ import annotations

import asyncio
from datetime import date
from time import perf_counter

import pandas as pd
from sqlalchemy import text

from shared.celery_app import celery_app
from shared.config import get_settings
from shared.db.session import get_timescale_session, get_postgres_session
from shared.models.signal import CrossAssetIndicators
from shared.utils import get_logger, resolve_trading_date_arg, today_trading

logger = get_logger("signal_tasks")


def _run_async(coro):
    """Run an async coroutine safely — works whether or not an event loop exists."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        # Already inside a running loop (e.g. nested Celery / Jupyter)
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            return pool.submit(asyncio.run, coro).result()
    return asyncio.run(coro)


@celery_app.task(name="signal_service.tasks.compute_daily_signals", bind=True, max_retries=2)
def compute_daily_signals(self, trading_date: str | None = None, prev_result=None, symbols: list[str] | None = None) -> dict:
    """
    17:00 Celery 任务：批量计算当日所有标的的信号特征
    prev_result: 上游任务 (backfill) 的结果
    symbols: 可选，仅计算指定标的（默认使用完整 watchlist）
    """
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
    return _run_async(_compute_daily_signals_async(resolved_trading_date, symbols=symbols))


async def _compute_daily_signals_async(trading_date_str: str | None = None, *, symbols: list[str] | None = None) -> dict:
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
    result = {"date": str(td), "symbols_computed": 0, "errors": []}

    async def _load_stock_bars(symbol: str) -> tuple[pd.DataFrame, str]:
        """Return (daily_df, bar_type).

        * daily_df  — up to 260 daily bars; used for ALL technical indicators,
                       price, and volume.  Post-market task runs after daily bars
                       are written, so today's close & volume are already in here.
        * bar_type  — 'intraday_1min' when sufficient intraday 1-min data exists
                       in the DB (metadata only), otherwise 'daily'.
        """
        MIN_INTRADAY_ROWS = 30

        # ── 1) Load daily history ──
        logger.debug(
            "signal_compute.load_daily_bars_started",
            log_event="db_read",
            stage="before_query",
            symbol=symbol,
            trading_date=str(td),
            source="stock_daily",
        )
        async with get_timescale_session() as session:
            daily_result = await session.execute(
                text(
                    "SELECT trading_date, open, high, low, close, volume "
                    "FROM stock_daily "
                    "WHERE symbol = :symbol AND trading_date <= :date "
                    "ORDER BY trading_date DESC LIMIT 260"
                ),
                {"symbol": symbol, "date": td},
            )
            daily_rows = daily_result.fetchall()

        if not daily_rows:
            logger.debug(
                "signal_compute.load_daily_bars_empty",
                log_event="db_read",
                stage="query_result",
                symbol=symbol,
                source="stock_daily",
            )
            return pd.DataFrame(), "daily"

        daily_df = pd.DataFrame(
            daily_rows,
            columns=["timestamp", "open", "high", "low", "close", "volume"],
        )
        daily_df = daily_df.sort_values("timestamp")
        logger.debug(
            "signal_compute.load_daily_bars_done",
            log_event="db_read",
            stage="query_result",
            symbol=symbol,
            rows=len(daily_df),
            source="stock_daily",
        )

        # ── 2) Check whether intraday data exists (metadata flag only) ──
        async with get_timescale_session() as session:
            count_result = await session.execute(
                text(
                    "SELECT COUNT(*) FROM stock_1min_bars "
                    "WHERE symbol = :symbol AND timestamp::date = :date"
                ),
                {"symbol": symbol, "date": td},
            )
            intraday_count = count_result.scalar() or 0

        bar_type = "intraday_1min" if intraday_count >= MIN_INTRADAY_ROWS else "daily"
        logger.debug(
            "signal_compute.load_stock_bars_done",
            log_event="db_read",
            stage="query_result",
            symbol=symbol,
            daily_rows=len(daily_df),
            intraday_rows=intraday_count,
            bar_type=bar_type,
        )
        return daily_df, bar_type

    async def _load_option_rows(symbol: str) -> pd.DataFrame:
        logger.debug(
            "signal_compute.load_option_rows_started",
            log_event="db_read",
            stage="before_query",
            symbol=symbol,
            trading_date=str(td),
            source="option_5min_snapshots",
        )
        async with get_timescale_session() as session:
            intraday_result = await session.execute(
                text(
                    "SELECT underlying, symbol, expiry, strike, option_type, "
                    "last_price, bid, ask, volume, open_interest, iv, "
                    "delta, gamma, theta, vega, timestamp "
                    "FROM option_5min_snapshots "
                    "WHERE underlying = :symbol AND timestamp::date = :date"
                ),
                {"symbol": symbol, "date": td},
            )
            intraday_rows = intraday_result.fetchall()

        if intraday_rows:
            logger.debug(
                "signal_compute.load_option_rows_intraday",
                log_event="db_read",
                stage="query_result",
                symbol=symbol,
                rows=len(intraday_rows),
                source="option_5min_snapshots",
            )
            return pd.DataFrame(
                intraday_rows,
                columns=[
                    "underlying", "symbol", "expiry", "strike", "option_type",
                    "last_price", "bid", "ask", "volume", "open_interest",
                    "iv", "delta", "gamma", "theta", "vega", "timestamp",
                ],
            )

        logger.debug(
            "signal_compute.load_option_rows_fallback",
            log_event="db_read",
            stage="fallback",
            symbol=symbol,
            source="option_daily",
        )
        async with get_timescale_session() as session:
            daily_result = await session.execute(
                text(
                    "SELECT underlying, symbol, expiry, strike, option_type, "
                    "last_price, bid, ask, volume, open_interest, iv, "
                    "delta, gamma, theta, vega, snapshot_date "
                    "FROM option_daily "
                    "WHERE underlying = :symbol AND snapshot_date = :date"
                ),
                {"symbol": symbol, "date": td},
            )
            daily_rows = daily_result.fetchall()

        if not daily_rows:
            logger.debug(
                "signal_compute.load_option_rows_empty",
                log_event="db_read",
                stage="query_result",
                symbol=symbol,
                source="option_daily",
            )
            return pd.DataFrame()

        return pd.DataFrame(
            daily_rows,
            columns=[
                "underlying", "symbol", "expiry", "strike", "option_type",
                "last_price", "bid", "ask", "volume", "open_interest",
                "iv", "delta", "gamma", "theta", "vega", "timestamp",
            ],
        )

    sem = asyncio.Semaphore(4)  # max 4 concurrent DB sessions

    async def _process_symbol(symbol: str) -> None:
        async with sem:
            try:
                symbol_started = perf_counter()
                logger.debug(
                    "signal_compute.symbol_started",
                    log_event="symbol_start",
                    stage="compute",
                    symbol=symbol,
                    trading_date=str(td),
                )
                bars_df, bar_type = await _load_stock_bars(symbol)
                option_df = await _load_option_rows(symbol)
                logger.debug(
                    "signal_compute.symbol_data_loaded",
                    log_event="symbol_context",
                    stage="after_load",
                    symbol=symbol,
                    daily_rows=len(bars_df),
                    option_rows=len(option_df),
                    bar_type=bar_type,
                )

                if bars_df.empty:
                    logger.warning("signal_compute.no_bars", symbol=symbol, date=str(td))
                    return

                # All price/volume/indicator data comes from daily bars.
                # Post-market task runs after daily bars are written, so
                # today's close & volume are already present.
                close_price = float(bars_df["close"].iloc[-1])
                prev_close = float(bars_df["close"].iloc[-2]) if len(bars_df) >= 2 else close_price
                daily_return = (close_price - prev_close) / prev_close if prev_close > 0 else 0.0
                total_volume = int(bars_df["volume"].iloc[-1])

                stock_indicators = compute_stock_indicators(bars_df)
                option_indicators = await compute_option_indicators(symbol, option_df, close_price)

                # Cross-asset features
                bar_returns = bars_df["close"].pct_change().dropna()

                # Aggregate per-timestamp mean IV before computing changes
                # (raw option_df mixes different contracts; pct_change across
                # contracts is meaningless)
                corr = 0.0
                if not option_df.empty and "timestamp" in option_df.columns:
                    avg_iv = (
                        option_df[option_df["iv"] > 0]
                        .groupby("timestamp")["iv"]
                        .mean()
                        .sort_index()
                    )
                    iv_changes = avg_iv.pct_change().dropna()
                else:
                    iv_changes = pd.Series(dtype=float)

                if len(bar_returns) > 10 and len(iv_changes) > 10:
                    sample_size = min(len(bar_returns), len(iv_changes))
                    merged = pd.DataFrame(
                        {
                            "ret": bar_returns.tail(sample_size).reset_index(drop=True),
                            "iv": iv_changes.tail(sample_size).reset_index(drop=True),
                        }
                    )
                    if len(merged) > 5:
                        corr = float(merged["ret"].corr(merged["iv"])) if merged["ret"].std() > 0 and merged["iv"].std() > 0 else 0.0

                total_option_volume = float(option_df["volume"].sum()) if not option_df.empty else 0.0
                option_vs_stock_volume_ratio = total_option_volume / max(float(total_volume), 1.0)
                hedge_ratio = -float(option_indicators.portfolio_greeks.get("delta", 0.0))

                # SPY beta & index correlation (new cross-asset fields)
                spy_beta = 0.0
                index_correlation_20d = 0.0
                # TODO: load SPY returns and compute beta / correlation when SPY data available

                cross_asset = CrossAssetIndicators(
                    stock_iv_correlation=round(corr, 6),
                    option_vs_stock_volume_ratio=round(option_vs_stock_volume_ratio, 6),
                    delta_adjusted_hedge_ratio=round(hedge_ratio, 4),
                    spy_beta=round(spy_beta, 4),
                    index_correlation_20d=round(index_correlation_20d, 4),
                    confidence_scores={
                        "corr_quality": round(min(1.0, len(bar_returns) / 100), 4),
                        "volume_quality": 1.0 if total_volume > 0 else 0.0,
                    },
                )

                # Fill HV-IV spread in stock indicators
                stock_indicators.hv_iv_spread = round(stock_indicators.hv_20d - option_indicators.current_iv, 6)

                # 4) 生成综合信号
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
                )

                # 5) 写入 DB
                async with get_postgres_session() as session:
                    logger.debug(
                        "signal_compute.db_write_started",
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
                        "signal_compute.db_write_finished",
                        log_event="db_write",
                        stage="after_write",
                        symbol=symbol,
                        trading_date=str(td),
                    )

                # 6) Write-through cache refresh (best effort)
                try:
                    await set_signal_cache(symbol, td, features.model_dump(mode="json"))
                except Exception as cache_exc:
                    logger.debug("signal_compute.cache_refresh_failed", symbol=symbol, error=str(cache_exc))
                    try:
                        await delete_signal_cache(symbol, td)
                    except Exception as del_exc:
                        logger.debug("signal_compute.cache_delete_failed", symbol=symbol, error=str(del_exc))

                result["symbols_computed"] += 1
                logger.debug(
                    "signal_compute.symbol_completed",
                    log_event="symbol_summary",
                    stage="completed",
                    symbol=symbol,
                    trading_date=str(td),
                    bar_type=bar_type,
                    duration_ms=round((perf_counter() - symbol_started) * 1000, 2),
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
