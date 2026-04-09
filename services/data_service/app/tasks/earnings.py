"""Daily earnings-date cache — fetch next earnings dates and store in Redis."""
from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
from time import perf_counter

from shared.celery_app import celery_app
from shared.config import get_settings
from shared.redis_pool import get_redis
from shared.utils import get_logger, market_tz

logger = get_logger("data_tasks")

EARNINGS_HASH_KEY = "earnings:next_date"


def _seconds_until_midnight_et() -> int:
    """Return seconds remaining until 00:00 ET (next day), minimum 3600."""
    now = datetime.now(market_tz())
    midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(int((midnight - now).total_seconds()), 3600)


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
    try:
        return asyncio.run(_refresh_earnings_cache_async())
    except Exception as exc:
        logger.error("earnings_cache.failed", error=str(exc))
        raise self.retry(exc=exc, countdown=120) from exc


async def _refresh_earnings_cache_async() -> dict:
    settings = get_settings()
    symbols = [s for s in settings.common.watchlist.all if not s.startswith("^")]

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
