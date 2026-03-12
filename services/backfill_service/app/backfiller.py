"""数据回填器 — 从 Yahoo Finance 补填缺失数据

Functions:
  backfill_stock_1min  — 回填 1 分钟 K 线 → stock_1min_bars（仅最近 7 天有效）
  backfill_stock_daily — 回填日线 K 线   → stock_daily（任意日期范围）
  backfill_history     — 冷启动：新标的 90 天日线 + 7 天 1 分钟
"""
from __future__ import annotations

import asyncio
from datetime import date, timedelta

import yfinance as yf
from sqlalchemy import text

from shared.db.session import get_timescale_session
from shared.utils import get_logger, today_trading, ensure_utc

logger = get_logger("backfiller")

# yfinance 1-minute data is only available for the most recent 7 calendar days
_MAX_1MIN_LOOKBACK_DAYS = 7


# ── stock_1min_bars 回填 ───────────────────────────────────


async def backfill_stock_1min(symbol: str, start_date: date, end_date: date) -> int:
    """回填 stock_1min_bars（interval=1m, 仅 ≤7 天有效）"""
    days_diff = (today_trading() - start_date).days
    if days_diff > _MAX_1MIN_LOOKBACK_DAYS:
        logger.warning(
            "backfiller.1min_out_of_range",
            symbol=symbol,
            start=str(start_date),
            days_diff=days_diff,
        )
        # Clamp to available range
        start_date = today_trading() - timedelta(days=_MAX_1MIN_LOOKBACK_DAYS)

    def _fetch():
        ticker = yf.Ticker(symbol)
        return ticker.history(
            start=str(start_date),
            end=str(end_date + timedelta(days=1)),
            interval="1m",
        )

    hist = await asyncio.to_thread(_fetch)
    if hist.empty:
        logger.warning("backfiller.no_1min_data", symbol=symbol, start=str(start_date))
        return 0

    records = [
        {
            "symbol": symbol,
            "timestamp": ensure_utc(ts.to_pydatetime()),
            "open": float(row["Open"]),
            "high": float(row["High"]),
            "low": float(row["Low"]),
            "close": float(row["Close"]),
            "volume": int(row["Volume"]),
        }
        for ts, row in hist.iterrows()
    ]

    if records:
        async with get_timescale_session() as session:
            await session.execute(
                text(
                    "INSERT INTO stock_1min_bars "
                    "(symbol, timestamp, open, high, low, close, volume) "
                    "VALUES (:symbol, :timestamp, :open, :high, :low, :close, :volume) "
                    "ON CONFLICT (symbol, timestamp) DO NOTHING"
                ),
                records,
            )
            await session.commit()

    logger.info("backfiller.1min_done", symbol=symbol, rows=len(records))
    return len(records)


# ── stock_daily 回填 ───────────────────────────────────────


async def backfill_stock_daily(symbol: str, start_date: date, end_date: date) -> int:
    """回填 stock_daily（interval=1d, 任意日期范围）"""

    def _fetch():
        ticker = yf.Ticker(symbol)
        return ticker.history(
            start=str(start_date),
            end=str(end_date + timedelta(days=1)),
            interval="1d",
        )

    hist = await asyncio.to_thread(_fetch)
    if hist.empty:
        logger.warning("backfiller.no_daily_data", symbol=symbol, start=str(start_date))
        return 0

    records = [
        {
            "symbol": symbol,
            "trading_date": ts.to_pydatetime().date(),
            "open": float(row["Open"]),
            "high": float(row["High"]),
            "low": float(row["Low"]),
            "close": float(row["Close"]),
            "volume": int(row["Volume"]),
        }
        for ts, row in hist.iterrows()
    ]

    if records:
        async with get_timescale_session() as session:
            await session.execute(
                text(
                    "INSERT INTO stock_daily "
                    "(symbol, trading_date, open, high, low, close, volume) "
                    "VALUES (:symbol, :trading_date, :open, :high, :low, :close, :volume) "
                    "ON CONFLICT (symbol, trading_date) DO NOTHING"
                ),
                records,
            )
            await session.commit()

    logger.info("backfiller.daily_done", symbol=symbol, rows=len(records))
    return len(records)


# ── 冷启动 / 新标的回填 ───────────────────────────────────


async def backfill_history(symbol: str, days: int = 90) -> dict:
    """冷启动：回填历史数据（新标的首次加入时调用）

    - stock_daily : 过去 ``days`` 天的日线
    - stock_1min  : 最近 7 天的 1 分钟线
    - option      : 不可回填（yfinance 限制），仅记录日志
    """
    end_date = today_trading()

    daily_rows = await backfill_stock_daily(
        symbol,
        end_date - timedelta(days=days),
        end_date,
    )
    min_rows = await backfill_stock_1min(
        symbol,
        end_date - timedelta(days=_MAX_1MIN_LOOKBACK_DAYS),
        end_date,
    )

    logger.info(
        "backfiller.history_done",
        symbol=symbol,
        days=days,
        daily_rows=daily_rows,
        min_rows=min_rows,
    )
    return {
        "symbol": symbol,
        "stock_daily_rows": daily_rows,
        "stock_1min_rows": min_rows,
    }
