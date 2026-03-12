"""Fetcher registry — config-driven provider selection.

Usage::

    from services.data_service.app.fetchers.registry import get_option_fetcher, get_stock_fetcher

    option_fetcher = get_option_fetcher()   # returns configured OptionFetcherProtocol
    stock_fetcher  = get_stock_fetcher()    # returns configured StockFetcherProtocol
"""
from __future__ import annotations

from functools import lru_cache

from services.data_service.app.fetchers import OptionFetcherProtocol, StockFetcherProtocol
from services.data_service.app.fetchers.option_fetcher import YFinanceOptionFetcher
from services.data_service.app.fetchers.stock_fetcher import YFinanceStockFetcher
from shared.config import get_settings
from shared.utils import get_logger

logger = get_logger("fetcher_registry")

# ── Provider maps ──────────────────────────────────────────

_STOCK_PROVIDERS: dict[str, type[StockFetcherProtocol]] = {
    "yfinance": YFinanceStockFetcher,
}

_OPTION_PROVIDERS: dict[str, type[OptionFetcherProtocol]] = {
    "yfinance": YFinanceOptionFetcher,
}


# ── Public helpers ─────────────────────────────────────────


@lru_cache
def get_stock_fetcher() -> StockFetcherProtocol:
    """Return the active stock fetcher based on config."""
    settings = get_settings()
    name = settings.data_service.providers.stock
    cls = _STOCK_PROVIDERS.get(name)
    if cls is None:
        raise ValueError(
            f"Unknown stock provider '{name}'. "
            f"Available: {list(_STOCK_PROVIDERS)}"
        )
    logger.info("fetcher_registry.stock_provider", provider=name)
    return cls()


@lru_cache
def get_option_fetcher() -> OptionFetcherProtocol:
    """Return the active option fetcher based on config."""
    settings = get_settings()
    name = settings.data_service.providers.options
    cls = _OPTION_PROVIDERS.get(name)
    if cls is None:
        raise ValueError(
            f"Unknown option provider '{name}'. "
            f"Available: {list(_OPTION_PROVIDERS)}"
        )
    logger.info("fetcher_registry.option_provider", provider=name)
    return cls()
