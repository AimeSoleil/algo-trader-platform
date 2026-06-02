"""Daily earnings-date cache — fetch next earnings dates and store in Redis."""
from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
from time import perf_counter

from shared.async_bridge import run_async
from shared.celery_app import celery_app
from shared.config import get_settings
from shared.notifier.base import EventType, NotificationEvent, Severity
from shared.notifier.helpers import notify_sync
from shared.redis_pool import get_redis
from shared.utils import get_logger, market_tz

logger = get_logger("data_tasks")

EARNINGS_HASH_KEY = "earnings:next_date"
_RETRY_COUNTDOWN_SECONDS = 120

# Symbols that never have earnings reports: ETFs, bond funds, commodity funds,
# crypto ETFs, and index tickers (^ prefix handled separately in the filter).
_NO_EARNINGS_SYMBOLS: frozenset[str] = frozenset({
    # Broad-market / equity ETFs
    "SPY", "QQQ", "IWM", "DIA", "VOO", "VTI", "IVV",
    # Fixed-income / rates
    "TLT", "IEF", "SHY", "AGG", "BND", "HYG", "LQD",
    # Commodities / REIT
    "GLD", "GDX", "SLV", "USO", "VNQ", "XLE",
    # Crypto ETFs
    "IBIT", "FBTC", "BITB",
    # Sector / factor ETFs
    "XLF", "XLK", "XLV", "XLY", "XLI", "XLP", "XLU", "XLB",
    "ARKK", "ARKW", "SMH",
})


def _seconds_until_midnight_et() -> int:
    """Return seconds remaining until 00:00 ET (next day), minimum 3600."""
    now = datetime.now(market_tz())
    midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(int((midnight - now).total_seconds()), 3600)


def _eligible_symbols() -> list[str]:
    settings = get_settings()
    return [
        symbol for symbol in settings.common.watchlist.all
        if not symbol.startswith("^")
        and symbol not in _NO_EARNINGS_SYMBOLS
    ]


def _notify_refresh_started(symbol_count: int) -> None:
    notify_sync(NotificationEvent(
        event_type=EventType.EARNINGS_CACHE_REFRESH_STARTED,
        title="Earnings Cache Refresh Started",
        message=f"Refreshing next-earnings cache for {symbol_count} symbols.",
        severity=Severity.INFO,
        payload={
            "cache_key": EARNINGS_HASH_KEY,
            "symbols": str(symbol_count),
        },
    ))


def _notify_refresh_finished(symbol_count: int, result: dict[str, int | float]) -> None:
    updated = int(result.get("updated", 0))
    failed = int(result.get("failed", 0))
    elapsed_s = result.get("elapsed_s", 0)
    notify_sync(NotificationEvent(
        event_type=EventType.EARNINGS_CACHE_REFRESH_FINISHED,
        title="Earnings Cache Refresh Finished",
        message=(
            "Next-earnings cache refresh finished: "
            f"{updated} updated, {failed} unavailable, {elapsed_s}s elapsed."
        ),
        severity=Severity.INFO,
        payload={
            "cache_key": EARNINGS_HASH_KEY,
            "symbols": str(symbol_count),
            "updated": str(updated),
            "failed": str(failed),
            "elapsed_s": str(elapsed_s),
        },
    ))


def _notify_refresh_failed(symbol_count: int, error: Exception) -> None:
    notify_sync(NotificationEvent(
        event_type=EventType.EARNINGS_CACHE_REFRESH_FAILED,
        title="Earnings Cache Refresh Failed",
        message=(
            "Next-earnings cache refresh failed: "
            f"{error}. Retry queued in {_RETRY_COUNTDOWN_SECONDS}s."
        ),
        severity=Severity.ERROR,
        payload={
            "cache_key": EARNINGS_HASH_KEY,
            "symbols": str(symbol_count),
            "error": str(error),
            "retry_in_s": str(_RETRY_COUNTDOWN_SECONDS),
        },
    ))


async def fetch_and_cache_earnings(symbols: list[str]) -> dict[str, date | None]:
    """Fetch next-earnings dates for *symbols* and update the Redis cache.

    Returns a mapping of symbol → earnings date (or ``None`` if unavailable).
    Cache TTL is set to expire at midnight ET.
    """
    from services.data_service.app.fetchers.stock_fetcher import _fetch_next_earnings_sync

    redis = get_redis()
    results: dict[str, date | None] = {}

    for symbol in symbols:
        earnings_date = await asyncio.to_thread(_fetch_next_earnings_sync, symbol)
        results[symbol] = earnings_date
        if earnings_date is not None:
            await redis.hset(EARNINGS_HASH_KEY, symbol, earnings_date.isoformat())

    await redis.expire(EARNINGS_HASH_KEY, _seconds_until_midnight_et())
    return results


@celery_app.task(
    name="data_service.tasks.refresh_earnings_cache",
    bind=True,
    max_retries=2,
    queue="data",
)
def refresh_earnings_cache(self) -> dict:
    """Refresh the Redis hash of next-earnings dates for all watchlist symbols.

    Runs daily before the signal pipeline so that
    ``earnings_proximity_days`` is populated when signals are computed.
    """
    symbols: list[str] = []

    try:
        symbols = _eligible_symbols()
        _notify_refresh_started(len(symbols))
        result = run_async(_refresh_earnings_cache_async(symbols))
        _notify_refresh_finished(len(symbols), result)
        return result
    except Exception as exc:
        _notify_refresh_failed(len(symbols), exc)
        logger.error("earnings_cache.failed", error=str(exc))
        raise self.retry(exc=exc, countdown=_RETRY_COUNTDOWN_SECONDS) from exc


async def _refresh_earnings_cache_async(symbols: list[str] | None = None) -> dict:
    symbols = symbols or _eligible_symbols()
    t0 = perf_counter()
    results = await fetch_and_cache_earnings(symbols)

    updated = sum(1 for v in results.values() if v is not None)
    failed = sum(1 for v in results.values() if v is None)
    elapsed = round(perf_counter() - t0, 2)

    logger.info(
        "earnings_cache.refreshed",
        symbols=len(symbols),
        updated=updated,
        failed=failed,
        elapsed_s=elapsed,
    )
    return {"updated": updated, "failed": failed, "elapsed_s": elapsed}
