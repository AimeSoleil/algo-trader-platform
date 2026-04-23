"""YFinance 股票数据采集器 — StockFetcherProtocol 实现

Retry / rate-limit / concurrency 由 resilience 模块统一处理。
"""
from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta

import yfinance as yf

from shared.utils import ensure_utc, get_logger, now_utc, today_trading
from services.data_service.app.fetchers.resilience import (
    gather_with_concurrency,
    retry_sync,
)

logger = get_logger("stock_fetcher")

# yfinance 1m data only available for ~7 calendar days
_MAX_1MIN_LOOKBACK_DAYS = 7


# ── Core sync functions (run in thread pool) ───────────────


def _fetch_stock_quote_sync(symbol: str) -> dict | None:
    """同步获取股票实时行情（带重试）"""
    try:
        ticker = yf.Ticker(symbol)
        hist = retry_sync(
            lambda: ticker.history(period="1d", interval="1m"),
            label="stock.quote",
            symbol=symbol,
        )
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
        import traceback
        logger.error(
            "stock_fetcher.failed",
            symbol=symbol,
            error=str(e),
            traceback=traceback.format_exc(),
        )
        return None


def _fetch_stock_bars_sync(
    symbol: str, period: str = "1d", interval: str = "1m"
) -> list[dict]:
    """同步获取股票K线（带重试）"""
    try:
        ticker = yf.Ticker(symbol)
        hist = retry_sync(
            lambda: ticker.history(period=period, interval=interval),
            label="stock.bars",
            symbol=symbol,
        )
        if hist.empty:
            return []

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
        return bars
    except Exception as e:
        import traceback
        logger.error(
            "stock_fetcher.bars_failed",
            symbol=symbol,
            error=str(e),
            traceback=traceback.format_exc(),
        )
        return []


def _fetch_stock_bars_range_sync(
    symbol: str,
    start_date: date,
    end_date: date,
    interval: str = "1d",
) -> tuple[list[dict], list[str]]:
    """Fetch bars for a specific date range. Returns (rows, warnings)."""
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
        ticker = yf.Ticker(symbol)
        hist = retry_sync(
            lambda: ticker.history(
                start=str(effective_start),
                end=str(end_date + timedelta(days=1)),
                interval=interval,
            ),
            label="stock.bars_range",
            symbol=symbol,
        )
        if hist.empty:
            warnings.append(
                f"{symbol}: no {interval} data for {effective_start}\u2013{end_date}"
            )
            return [], warnings

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
        return rows, warnings
    except Exception as e:
        import traceback
        logger.error(
            "stock_fetcher.range_failed",
            symbol=symbol,
            interval=interval,
            error=str(e),
            traceback=traceback.format_exc(),
        )
        return [], [f"{symbol}: fetch error – {e}"]

def _fetch_next_earnings_sync(symbol: str) -> date | None:
    """Return the next earnings date for *symbol*, or ``None`` if unavailable."""
    try:
        ticker = yf.Ticker(symbol)
        cal = retry_sync(lambda: ticker.calendar, label="stock.earnings", symbol=symbol)

        if cal is None:
            return None

        # yfinance returns a DataFrame (old API) or dict (newer API)
        if isinstance(cal, dict):
            raw = cal.get("Earnings Date")
        elif hasattr(cal, "loc"):
            raw = cal.loc["Earnings Date"].iloc[0] if "Earnings Date" in cal.index else None
        else:
            raw = None

        if raw is None:
            return None

        # Normalize to date
        if isinstance(raw, list):
            raw = raw[0] if raw else None
        if raw is None:
            return None
        if hasattr(raw, "date"):
            return raw.date()
        return date.fromisoformat(str(raw)[:10])

    except Exception as e:
        import traceback
        logger.warning(
            "stock_fetcher.earnings_failed",
            symbol=symbol,
            error=str(e),
            traceback=traceback.format_exc(),
        )
        return None

# ── Async class ────────────────────────────────────────────


class YFinanceStockFetcher:
    """yfinance-backed stock fetcher implementing StockFetcherProtocol."""

    async def fetch_quote(self, symbol: str) -> dict | None:
        return await asyncio.to_thread(_fetch_stock_quote_sync, symbol)

    async def fetch_quotes_multiple(
        self, symbols: list[str]
    ) -> dict[str, dict]:
        """Fetch quotes for multiple symbols (concurrency via resilience)."""
        results: dict[str, dict] = {}

        async def _fetch(sym: str) -> None:
            quote = await self.fetch_quote(sym)
            if quote:
                results[sym] = quote

        await gather_with_concurrency([_fetch(s) for s in symbols])
        return results

    async def fetch_bars(
        self,
        symbol: str,
        period: str = "1d",
        interval: str = "1m",
    ) -> list[dict]:
        return await asyncio.to_thread(_fetch_stock_bars_sync, symbol, period, interval)

    async def fetch_bars_multiple(
        self,
        symbols: list[str],
        period: str = "1d",
        interval: str = "1m",
    ) -> dict[str, list[dict]]:
        """Fetch bars for multiple symbols (concurrency via resilience)."""
        results: dict[str, list[dict]] = {}

        async def _fetch(sym: str) -> None:
            bars = await self.fetch_bars(sym, period, interval)
            if bars:
                results[sym] = bars

        await gather_with_concurrency([_fetch(s) for s in symbols])
        return results

    async def fetch_bars_range(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        interval: str = "1d",
    ) -> tuple[list[dict], list[str]]:
        return await asyncio.to_thread(
            _fetch_stock_bars_range_sync, symbol, start_date, end_date, interval
        )

    async def fetch_next_earnings(self, symbol: str) -> date | None:
        return await asyncio.to_thread(_fetch_next_earnings_sync, symbol)
