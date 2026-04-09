"""Fetcher abstractions — Protocol definitions for data providers.

Each provider (yfinance, future CBOE/OPRA, etc.) implements these protocols.
The registry selects the active implementation based on config.
"""
from __future__ import annotations

from datetime import date
from typing import Protocol, runtime_checkable

from shared.models.option import OptionChainSnapshot


# ── Stock fetcher protocol ─────────────────────────────────


@runtime_checkable
class StockFetcherProtocol(Protocol):
    """Interface for stock data providers."""

    async def fetch_quote(self, symbol: str) -> dict | None:
        """Fetch current L1 quote for *symbol*."""
        ...

    async def fetch_bars(
        self,
        symbol: str,
        period: str = "1d",
        interval: str = "1m",
    ) -> list[dict]:
        """Fetch bars for the given period/interval."""
        ...

    async def fetch_bars_range(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        interval: str = "1d",
    ) -> tuple[list[dict], list[str]]:
        """Fetch bars for an explicit date range. Returns (rows, warnings)."""
        ...

    async def fetch_next_earnings(self, symbol: str) -> date | None:
        """Return the next earnings date for *symbol*, or ``None``."""
        ...


# ── Option fetcher protocol ───────────────────────────────


@runtime_checkable
class OptionFetcherProtocol(Protocol):
    """Interface for option chain data providers."""

    async def fetch_current(self, symbol: str) -> OptionChainSnapshot | None:
        """Fetch the current option chain snapshot for *symbol*."""
        ...

    async def fetch_current_multiple(
        self,
        symbols: list[str],
    ) -> dict[str, OptionChainSnapshot]:
        """Fetch current snapshots for multiple symbols concurrently."""
        ...

    async def fetch_historical(
        self,
        symbol: str,
        target_date: date,
    ) -> OptionChainSnapshot | None:
        """Fetch a historical option chain snapshot.

        Returns ``None`` if the provider does not support historical data.
        """
        ...
