"""Analysis Service — Redis 缓存层"""
from __future__ import annotations

import json
from datetime import date

from shared.redis_pool import get_redis
from shared.utils import get_logger

logger = get_logger("analysis_cache")

_CACHE_TTL = 3600  # 1 hour — blueprints change infrequently
_CACHE_PREFIX = "blueprint"


def _cache_key(trading_date: date) -> str:
    return f"{_CACHE_PREFIX}:{trading_date.isoformat()}"


def _get_redis():
    return get_redis()


async def get_cached_blueprint(trading_date: date) -> dict | None:
    """Try to get blueprint from Redis cache."""
    try:
        redis = _get_redis()
        cached = await redis.get(_cache_key(trading_date))
        if cached:
            logger.debug("cache.hit", date=str(trading_date))
            return json.loads(cached)
    except Exception:
        logger.debug("cache.miss", date=str(trading_date))
    return None


async def set_cached_blueprint(trading_date: date, data: dict) -> None:
    """Store blueprint in Redis cache."""
    try:
        redis = _get_redis()
        await redis.set(
            _cache_key(trading_date),
            json.dumps(data, default=str),
            ex=_CACHE_TTL,
        )
    except Exception as e:
        logger.warning("cache.set_failed", error=str(e))


async def set_cached_blueprint_strict(trading_date: date, data: dict) -> None:
    """Strict set for task write-through; raise to caller on failure."""
    redis = _get_redis()
    await redis.set(
        _cache_key(trading_date),
        json.dumps(data, default=str),
        ex=_CACHE_TTL,
    )


async def invalidate_blueprint_cache(trading_date: date) -> None:
    """Invalidate cached blueprint for a specific date."""
    try:
        redis = _get_redis()
        await redis.delete(_cache_key(trading_date))
    except Exception:
        pass


async def invalidate_blueprint_cache_strict(trading_date: date) -> None:
    """Strict delete for task fallback; raise to caller on failure."""
    redis = _get_redis()
    await redis.delete(_cache_key(trading_date))
