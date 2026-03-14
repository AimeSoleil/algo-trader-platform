"""YFinance 股票数据采集器 — StockFetcherProtocol 实现"""
from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta

import yfinance as yf

from shared.utils import ensure_utc, get_logger, now_utc, today_trading

logger = get_logger("stock_fetcher")

# yfinance 1m data only available for ~7 calendar days
_MAX_1MIN_LOOKBACK_DAYS = 7


def _fetch_stock_quote_sync(symbol: str) -> dict | None:
    """同步获取股票实时行情"""
    try:
        logger.debug("stock_fetcher.quote_fetch_start", symbol=symbol, period="1d", interval="1m")
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="1d", interval="1m")
        logger.debug("stock_fetcher.quote_fetch_done", symbol=symbol, rows=len(hist))

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
            "timestamp": now_utc().isoformat(),
        }
    except Exception as e:
        logger.error("stock_fetcher.failed", symbol=symbol, error=str(e))
        return None


def _fetch_stock_bars_sync(
    symbol: str, period: str = "1d", interval: str = "1m"
) -> list[dict]:
    """同步获取股票K线"""
    try:
        logger.debug("stock_fetcher.bars_fetch_start", symbol=symbol, period=period, interval=interval)
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=period, interval=interval)
        logger.debug("stock_fetcher.bars_fetch_done", symbol=symbol, period=period, interval=interval, rows=len(hist))
        if hist.empty:
            return []

        logger.debug("stock_fetcher.bars_transform_start", symbol=symbol, rows=len(hist))
        bars = []
        for ts, row in hist.iterrows():
            bars.append(
                {
                    "symbol": symbol,
                    "timestamp": ensure_utc(ts.to_pydatetime()),
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "close": float(row["Close"]),
                    "volume": int(row["Volume"]),
                }
            )
        logger.debug("stock_fetcher.bars_transform_done", symbol=symbol, bars_count=len(bars))
        return bars
    except Exception as e:
        logger.error("stock_fetcher.bars_failed", symbol=symbol, error=str(e))
        return []


def _fetch_stock_bars_range_sync(
    symbol: str,
    start_date: date,
    end_date: date,
    interval: str = "1d",
) -> tuple[list[dict], list[str]]:
    """Fetch bars for a specific date range. Returns (rows, warnings).

    For interval="1m", dates older than 7 days are clamped and a warning is emitted.
    """
    warnings: list[str] = []

    effective_start = start_date
    if interval == "1m":
        cutoff = today_trading() - timedelta(days=_MAX_1MIN_LOOKBACK_DAYS)
        if start_date < cutoff:
            warnings.append(
                f"{symbol}: 1m bars before {cutoff} unavailable (yfinance limit), "
                f"clamped start from {start_date} to {cutoff}"
            )
            effective_start = cutoff
        if end_date < cutoff:
            warnings.append(f"{symbol}: entire 1m range before {cutoff}, skipped")
            return [], warnings

    try:
        logger.debug(
            "stock_fetcher.range_fetch_start",
            symbol=symbol,
            interval=interval,
            start=str(effective_start),
            end=str(end_date),
        )
        ticker = yf.Ticker(symbol)
        hist = ticker.history(
            start=str(effective_start),
            end=str(end_date + timedelta(days=1)),
            interval=interval,
        )
        logger.debug(
            "stock_fetcher.range_fetch_done",
            symbol=symbol,
            interval=interval,
            rows=len(hist),
        )
        if hist.empty:
            warnings.append(
                f"{symbol}: no {interval} data for {effective_start}\u2013{end_date}"
            )
            return [], warnings

        logger.debug("stock_fetcher.range_transform_start", symbol=symbol, interval=interval, rows=len(hist))
        rows: list[dict] = []
        for ts, row in hist.iterrows():
            entry: dict = {
                "symbol": symbol,
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": int(row["Volume"]),
            }
            if interval == "1d":
                entry["trading_date"] = ts.to_pydatetime().date()
            else:
                entry["timestamp"] = ensure_utc(ts.to_pydatetime())
            rows.append(entry)
        logger.debug("stock_fetcher.range_transform_done", symbol=symbol, interval=interval, rows=len(rows))
        return rows, warnings
    except Exception as e:
        logger.error(
            "stock_fetcher.range_failed",
            symbol=symbol,
            interval=interval,
            error=str(e),
        )
        return [], [f"{symbol}: fetch error \u2013 {e}"]


class YFinanceStockFetcher:
    """yfinance-backed stock fetcher implementing StockFetcherProtocol."""

    async def fetch_quote(self, symbol: str) -> dict | None:
        """Fetch current L1 quote."""
        return await asyncio.to_thread(_fetch_stock_quote_sync, symbol)

    async def fetch_bars(
        self,
        symbol: str,
        period: str = "1d",
        interval: str = "1m",
    ) -> list[dict]:
        """Fetch bars for the given period/interval."""
        return await asyncio.to_thread(_fetch_stock_bars_sync, symbol, period, interval)

    async def fetch_bars_range(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        interval: str = "1d",
    ) -> tuple[list[dict], list[str]]:
        """Fetch bars for an explicit date range."""
        return await asyncio.to_thread(
            _fetch_stock_bars_range_sync, symbol, start_date, end_date, interval
        )


# ── Backward-compatible module-level helpers ───────────────

_default = YFinanceStockFetcher()


async def fetch_stock_quote(symbol: str) -> dict | None:
    return await _default.fetch_quote(symbol)


async def fetch_stock_bars(
    symbol: str, period: str = "1d", interval: str = "1m"
) -> list[dict]:
    return await _default.fetch_bars(symbol, period, interval)


async def fetch_stock_bars_range(
    symbol: str,
    start_date: date,
    end_date: date,
    interval: str = "1d",
) -> tuple[list[dict], list[str]]:
    return await _default.fetch_bars_range(symbol, start_date, end_date, interval)
