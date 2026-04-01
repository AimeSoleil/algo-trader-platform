"""Shared async Redis connection pool — singleton.

All services that need Redis should import ``get_redis`` from this module
instead of creating ad-hoc ``Redis.from_url()`` connections.

Usage::

    from shared.redis_pool import get_redis, close_redis_pool, RedisClient

    redis = get_redis()
    await redis.get("some_key")

    # During application shutdown:
    await close_redis_pool()
"""
from __future__ import annotations

from redis.asyncio import ConnectionPool, Redis

from shared.config.settings import get_settings

RedisClient = Redis

_pool: ConnectionPool | None = None
_redis: Redis | None = None


def get_redis() -> Redis:
    """Return a shared async ``Redis`` client.

    Lazily initialises on first call.  Task-safe for asyncio because
    ``Redis`` handles concurrency internally.
    """
    global _pool, _redis
    if _redis is not None:
        return _redis

    settings = get_settings()

    _pool = ConnectionPool.from_url(
        settings.infra.redis.url,
        decode_responses=True,
        max_connections=20,
    )
    _redis = Redis(connection_pool=_pool)

    return _redis


async def close_redis_pool() -> None:
    """Gracefully shut down the connection pool (call at app shutdown)."""
    global _pool, _redis
    if _redis is not None:
        await _redis.aclose()
        _redis = None
    if _pool is not None:
        await _pool.disconnect()
        _pool = None
