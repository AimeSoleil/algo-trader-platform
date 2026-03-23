"""Trade-service helper utilities — type coercion & Redis helpers."""
from __future__ import annotations

from typing import Any

from shared.redis_pool import RedisClient


def safe_float(value: Any, default: float = 0.0) -> float:
    """Convert *value* to float, returning *default* on failure."""
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value: Any, default: int = 0) -> int:
    """Convert *value* to int, returning *default* on failure."""
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


async def load_redis_price(redis_client: RedisClient, symbol: str) -> float | None:
    """Fetch latest quote price from Redis ``market:quote:{symbol}``."""
    quote = await redis_client.hgetall(f"market:quote:{symbol}")
    if not quote:
        return None
    price = safe_float(quote.get("price"), 0.0)
    return price if price > 0 else None
