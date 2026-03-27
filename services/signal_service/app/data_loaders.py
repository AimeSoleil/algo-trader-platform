"""Signal Service — 市场数据加载层

将 TimescaleDB 查询逻辑集中到可测试、可复用的 async 函数中。
每个加载器遵循 "primary + fallback" 模式（日级优先，盘中兜底）。
"""
from __future__ import annotations

from datetime import date

import pandas as pd
from sqlalchemy import text

from shared.db.session import get_timescale_session
from shared.utils import get_logger

logger = get_logger("signal_data_loaders")

# ── Constants ──────────────────────────────────────────────
STOCK_BAR_LOOKBACK = 260          # ~1 year of trading days
MIN_INTRADAY_ROWS = 30            # minimum 1-min bars to form a valid daily bar

STOCK_COLS = ["timestamp", "open", "high", "low", "close", "volume"]
OPTION_COLS = [
    "underlying", "symbol", "expiry", "strike", "option_type",
    "last_price", "bid", "ask", "volume", "open_interest",
    "iv", "delta", "gamma", "theta", "vega",
    "vanna", "charm", "is_tradeable", "timestamp",
]


# ═══════════════════════════════════════════════════════════
# Benchmark / environment preloaders
# ═══════════════════════════════════════════════════════════

async def load_benchmark_returns(
    benchmarks: list[str],
    as_of: date,
) -> dict[str, pd.Series]:
    """Pre-load daily returns for multiple benchmark symbols.

    Returns a dict of ``{symbol: pd.Series}`` where each Series is
    date-indexed ``close.pct_change().dropna()``.  Missing or failed
    benchmarks produce an empty Series (with a warning log).
    """
    result: dict[str, pd.Series] = {}
    for sym in benchmarks:
        try:
            bars_df, _ = await load_stock_bars(sym, as_of)
            if bars_df.empty:
                logger.warning(
                    "benchmark.no_data",
                    benchmark=sym,
                    trading_date=str(as_of),
                )
                result[sym] = pd.Series(dtype=float)
                continue

            returns = (
                bars_df.set_index("timestamp")["close"]
                .pct_change()
                .dropna()
            )
            result[sym] = returns
            logger.debug(
                "benchmark.loaded",
                benchmark=sym,
                return_days=len(returns),
            )
        except Exception:
            logger.exception("benchmark.load_failed", benchmark=sym)
            result[sym] = pd.Series(dtype=float)
    return result


async def load_vix_bars(as_of: date) -> pd.DataFrame:
    """Load ^VIX daily bars for VIX environment indicators.

    Thin wrapper around ``load_stock_bars`` with dedicated logging.
    Returns an empty DataFrame if VIX data is unavailable.
    """
    try:
        bars_df, bar_type = await load_stock_bars("^VIX", as_of)
        if bars_df.empty:
            logger.warning(
                "vix.no_data",
                trading_date=str(as_of),
                detail="VIX environment indicators will default to 0.0",
            )
        else:
            logger.debug(
                "vix.loaded",
                rows=len(bars_df),
                bar_type=bar_type,
            )
        return bars_df
    except Exception:
        logger.exception("vix.load_failed")
        return pd.DataFrame()


# ── Stock bars ─────────────────────────────────────────────

async def load_stock_bars(symbol: str, as_of: date) -> tuple[pd.DataFrame, str]:
    """Load up to 260 rows of daily OHLCV for *symbol*.

    Returns
    -------
    (bars_df, bar_type)
        bars_df  — DataFrame with columns: timestamp, open, high, low, close, volume
        bar_type — ``'daily'`` when sourced from ``stock_daily``,
                   ``'intraday_1min'`` when aggregated from 1-min bars.
    """
    bars_df = await _load_daily_bars(symbol, as_of)
    if not bars_df.empty:
        return bars_df, "daily"

    bars_df = await _load_aggregated_1min_bars(symbol, as_of)
    if not bars_df.empty:
        return bars_df, "intraday_1min"

    return pd.DataFrame(), "daily"


async def _load_daily_bars(symbol: str, as_of: date) -> pd.DataFrame:
    logger.debug(
        "data_loader.load_daily_bars",
        log_event="db_read",
        stage="before_query",
        symbol=symbol,
        trading_date=str(as_of),
        source="stock_daily",
    )
    async with get_timescale_session() as session:
        result = await session.execute(
            text(
                "SELECT trading_date, open, high, low, close, volume "
                "FROM stock_daily "
                "WHERE symbol = :symbol AND trading_date <= :date "
                "ORDER BY trading_date DESC LIMIT :limit"
            ),
            {"symbol": symbol, "date": as_of, "limit": STOCK_BAR_LOOKBACK},
        )
        rows = result.fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=STOCK_COLS).sort_values("timestamp")
    logger.debug(
        "data_loader.daily_bars_loaded",
        log_event="db_read",
        stage="query_result",
        symbol=symbol,
        rows=len(df),
        source="stock_daily",
    )
    return df


async def _load_aggregated_1min_bars(symbol: str, as_of: date) -> pd.DataFrame:
    logger.debug(
        "data_loader.fallback_intraday",
        log_event="db_read",
        stage="fallback",
        symbol=symbol,
        source="stock_1min_bars",
    )
    async with get_timescale_session() as session:
        result = await session.execute(
            text(
                "SELECT timestamp::date AS trading_date, "
                "       (array_agg(open ORDER BY timestamp))[1] AS open, "
                "       MAX(high) AS high, "
                "       MIN(low) AS low, "
                "       (array_agg(close ORDER BY timestamp DESC))[1] AS close, "
                "       SUM(volume) AS volume "
                "FROM stock_1min_bars "
                "WHERE symbol = :symbol AND timestamp::date <= :date "
                "GROUP BY timestamp::date "
                "HAVING COUNT(*) >= :min_rows "
                "ORDER BY trading_date DESC LIMIT :limit"
            ),
            {
                "symbol": symbol,
                "date": as_of,
                "min_rows": MIN_INTRADAY_ROWS,
                "limit": STOCK_BAR_LOOKBACK,
            },
        )
        rows = result.fetchall()

    if not rows:
        logger.debug(
            "data_loader.intraday_empty",
            log_event="db_read",
            stage="query_result",
            symbol=symbol,
            source="stock_1min_bars",
        )
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=STOCK_COLS).sort_values("timestamp")
    logger.debug(
        "data_loader.intraday_loaded",
        log_event="db_read",
        stage="query_result",
        symbol=symbol,
        rows=len(df),
        source="stock_1min_bars",
        bar_type="intraday_1min",
    )
    return df


# ── Option chain ───────────────────────────────────────────

async def load_option_rows(symbol: str, as_of: date) -> pd.DataFrame:
    """Load option chain rows for *symbol* on *as_of* date.

    Priority: ``option_daily`` → ``option_5min_snapshots``.
    """
    df = await _load_daily_options(symbol, as_of)
    if not df.empty:
        return df

    return await _load_intraday_options(symbol, as_of)


async def _load_daily_options(symbol: str, as_of: date) -> pd.DataFrame:
    logger.debug(
        "data_loader.load_options",
        log_event="db_read",
        stage="before_query",
        symbol=symbol,
        trading_date=str(as_of),
        source="option_daily",
    )
    async with get_timescale_session() as session:
        result = await session.execute(
            text(
                "SELECT underlying, symbol, expiry, strike, option_type, "
                "last_price, bid, ask, volume, open_interest, iv, "
                "delta, gamma, theta, vega, "
                "vanna, charm, is_tradeable, snapshot_date "
                "FROM option_daily "
                "WHERE underlying = :symbol AND snapshot_date = :date"
            ),
            {"symbol": symbol, "date": as_of},
        )
        rows = result.fetchall()

    if not rows:
        return pd.DataFrame()

    logger.debug(
        "data_loader.options_daily_loaded",
        log_event="db_read",
        stage="query_result",
        symbol=symbol,
        rows=len(rows),
        source="option_daily",
    )
    return pd.DataFrame(rows, columns=OPTION_COLS)


async def _load_intraday_options(symbol: str, as_of: date) -> pd.DataFrame:
    logger.debug(
        "data_loader.options_fallback_intraday",
        log_event="db_read",
        stage="fallback",
        symbol=symbol,
        source="option_5min_snapshots",
    )
    # Dedup: take only the latest snapshot per contract (symbol) to avoid
    # returning multiple intraday rows for the same contract.
    async with get_timescale_session() as session:
        result = await session.execute(
            text(
                "WITH ranked AS ( "
                "  SELECT underlying, symbol, expiry, strike, option_type, "
                "    last_price, bid, ask, volume, open_interest, iv, "
                "    delta, gamma, theta, vega, "
                "    vanna, charm, is_tradeable, timestamp, "
                "    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) AS rn "
                "  FROM option_5min_snapshots "
                "  WHERE underlying = :symbol AND timestamp::date = :date "
                ") "
                "SELECT underlying, symbol, expiry, strike, option_type, "
                "  last_price, bid, ask, volume, open_interest, iv, "
                "  delta, gamma, theta, vega, "
                "  vanna, charm, is_tradeable, timestamp "
                "FROM ranked WHERE rn = 1"
            ),
            {"symbol": symbol, "date": as_of},
        )
        rows = result.fetchall()

    if not rows:
        logger.debug(
            "data_loader.options_empty",
            log_event="db_read",
            stage="query_result",
            symbol=symbol,
            source="option_5min_snapshots",
        )
        return pd.DataFrame()

    logger.debug(
        "data_loader.options_intraday_loaded",
        log_event="db_read",
        stage="query_result",
        symbol=symbol,
        rows=len(rows),
        source="option_5min_snapshots",
    )
    return pd.DataFrame(rows, columns=OPTION_COLS)
