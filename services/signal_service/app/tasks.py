"""Signal Service — Celery 盘后批量计算任务"""
from __future__ import annotations

import asyncio
from datetime import date

import pandas as pd
from sqlalchemy import text

from shared.celery_app import celery_app
from shared.config import get_settings
from shared.db.session import get_timescale_session, get_postgres_session
from shared.models.signal import CrossAssetIndicators
from shared.utils import get_logger, today_trading

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
def compute_daily_signals(self, trading_date: str | None = None, prev_result=None) -> dict:
    """
    17:00 Celery 任务：批量计算当日所有标的的信号特征
    prev_result: 上游任务 (backfill) 的结果
    """
    return _run_async(_compute_daily_signals_async(trading_date))


async def _compute_daily_signals_async(trading_date_str: str | None = None) -> dict:
    from services.signal_service.app.indicators.option_indicators import compute_option_indicators
    from services.signal_service.app.indicators.stock_indicators import compute_stock_indicators
    from services.signal_service.app.signal_generator import generate_signal

    settings = get_settings()
    td = date.fromisoformat(trading_date_str) if trading_date_str else today_trading()
    result = {"date": str(td), "symbols_computed": 0, "errors": []}

    async def _load_stock_bars(symbol: str) -> pd.DataFrame:
        # 优先使用 intraday 1min，若不存在则回退到 swing daily
        async with get_timescale_session() as session:
            bars_result = await session.execute(
                text(
                    "SELECT timestamp, open, high, low, close, volume "
                    "FROM stock_1min_bars "
                    "WHERE symbol = :symbol AND timestamp::date = :date "
                    "ORDER BY timestamp"
                ),
                {"symbol": symbol, "date": td},
            )
            bars_rows = bars_result.fetchall()

        if bars_rows:
            return pd.DataFrame(bars_rows, columns=["timestamp", "open", "high", "low", "close", "volume"])

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
            return pd.DataFrame()

        daily_df = pd.DataFrame(daily_rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
        daily_df = daily_df.sort_values("timestamp")
        return daily_df

    async def _load_option_rows(symbol: str) -> pd.DataFrame:
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
            return pd.DataFrame(
                intraday_rows,
                columns=[
                    "underlying", "symbol", "expiry", "strike", "option_type",
                    "last_price", "bid", "ask", "volume", "open_interest",
                    "iv", "delta", "gamma", "theta", "vega", "timestamp",
                ],
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
                bars_df = await _load_stock_bars(symbol)
                option_df = await _load_option_rows(symbol)

                if bars_df.empty:
                    logger.warning("signal_compute.no_bars", symbol=symbol, date=str(td))
                    return

                # Determine bar type for downstream consumers
                bar_type = "intraday_1min" if "timestamp" in bars_df.columns and len(bars_df) > 260 else "daily"

                # 3) 计算指标
                close_price = float(bars_df["close"].iloc[-1])
                prev_close = float(bars_df["close"].iloc[0])
                daily_return = (close_price - prev_close) / prev_close if prev_close > 0 else 0.0
                total_volume = int(bars_df["volume"].sum())

                stock_indicators = compute_stock_indicators(bars_df)
                option_indicators = await compute_option_indicators(symbol, option_df, close_price)

                # Cross-asset features
                bar_returns = bars_df["close"].pct_change().dropna()
                iv_changes = option_df["iv"].pct_change().dropna() if not option_df.empty else pd.Series(dtype=float)

                corr = 0.0
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

                result["symbols_computed"] += 1
                logger.info(
                    "signal_compute.done",
                    symbol=symbol,
                    regime=features.volatility_regime,
                )

            except Exception as e:
                result["errors"].append(f"{symbol}: {str(e)}")
                logger.error("signal_compute.failed", symbol=symbol, error=str(e))

    await asyncio.gather(*[_process_symbol(s) for s in settings.watchlist])

    logger.info("signal_compute.batch_completed", **result)
    return result
