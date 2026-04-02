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

import asyncio

from redis.asyncio import ConnectionPool, Redis

from shared.config.settings import get_settings

RedisClient = Redis

_pool: ConnectionPool | None = None
_redis: Redis | None = None
_bound_loop_id: int | None = None


def _current_loop_id() -> int | None:
    """Return id of the running event loop, or None if there isn't one."""
    try:
        loop = asyncio.get_running_loop()
        return id(loop) if loop and not loop.is_closed() else None
    except RuntimeError:
        return None


def get_redis() -> Redis:
    """Return a shared async ``Redis`` client.

    Lazily initialises on first call.  If the event loop has changed
    (e.g. successive ``asyncio.run()`` calls in Celery tasks), the stale
    connection pool is discarded and a fresh one is created so that
    connections are never bound to a closed loop.
    """
    global _pool, _redis, _bound_loop_id

    current_id = _current_loop_id()

    # If we have a cached client but the loop changed, tear it down.
    if _redis is not None and current_id != _bound_loop_id:
        # Best-effort close — the old loop is likely already closed,
        # so we just discard the references.
        _pool = None
        _redis = None
        _bound_loop_id = None

    if _redis is not None:
        return _redis

    settings = get_settings()

    _pool = ConnectionPool.from_url(
        settings.infra.redis.url,
        decode_responses=True,
        max_connections=20,
    )
    _redis = Redis(connection_pool=_pool)
    _bound_loop_id = current_id

    return _redis


async def close_redis_pool() -> None:
    """Gracefully shut down the connection pool (call at app shutdown)."""
    global _pool, _redis, _bound_loop_id
    if _redis is not None:
        await _redis.aclose()
        _redis = None
    if _pool is not None:
        await _pool.disconnect()
        _pool = None
    _bound_loop_id = None
