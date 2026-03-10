"""股票数据采集器"""
from __future__ import annotations

import asyncio
from datetime import datetime

import yfinance as yf

from shared.utils import get_logger

logger = get_logger("stock_fetcher")


def _fetch_stock_quote_sync(symbol: str) -> dict | None:
    """同步获取股票实时行情"""
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="1d", interval="1m")

        if hist.empty:
            logger.warning("stock_fetcher.no_data", symbol=symbol)
            return None

        latest = hist.iloc[-1]
        return {
            "symbol": symbol,
            "price": float(latest["Close"]),
            "open": float(hist.iloc[0]["Open"]),
            "high": float(hist["High"].max()),
            "low": float(hist["Low"].min()),
            "close": float(latest["Close"]),
            "volume": int(hist["Volume"].sum()),
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        logger.error("stock_fetcher.failed", symbol=symbol, error=str(e))
        return None


async def fetch_stock_quote(symbol: str) -> dict | None:
    """异步获取股票实时行情"""
    return await asyncio.to_thread(_fetch_stock_quote_sync, symbol)


def _fetch_stock_bars_sync(symbol: str, period: str = "1d", interval: str = "1m") -> list[dict]:
    """同步获取股票K线"""
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=period, interval=interval)
        if hist.empty:
            return []

        bars = []
        for ts, row in hist.iterrows():
            bars.append({
                "symbol": symbol,
                "timestamp": ts.isoformat(),
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": int(row["Volume"]),
            })
        return bars
    except Exception as e:
        logger.error("stock_fetcher.bars_failed", symbol=symbol, error=str(e))
        return []


async def fetch_stock_bars(symbol: str, period: str = "1d", interval: str = "1m") -> list[dict]:
    """异步获取股票K线"""
    return await asyncio.to_thread(_fetch_stock_bars_sync, symbol, period, interval)
