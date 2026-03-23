"""Shared async Redis connection pool — singleton (standalone and cluster).

All services that need Redis should import ``get_redis`` from this module
instead of creating ad-hoc ``Redis.from_url()`` connections.

Usage::

    from shared.redis_pool import get_redis, close_redis_pool, RedisClient

    redis = get_redis()              # returns Redis or RedisCluster
    await redis.get("some_key")

    # During application shutdown:
    await close_redis_pool()
"""
from __future__ import annotations

from typing import Union

from redis.asyncio import ConnectionPool, Redis
from redis.asyncio.cluster import ClusterNode, RedisCluster

from shared.config.settings import get_settings

# Unified type alias — all callers should use this for type hints
RedisClient = Union[Redis, RedisCluster]

_pool: ConnectionPool | None = None
_redis: RedisClient | None = None


def get_redis() -> RedisClient:
    """Return a shared ``Redis`` or ``RedisCluster`` client.

    Lazily initialises on first call.  Thread-/task-safe for asyncio
    because both Redis and RedisCluster handle concurrency internally.

    When ``settings.redis.cluster_enabled`` is ``True``, returns a
    ``RedisCluster`` backed by the configured seed nodes.  Otherwise
    returns a plain ``Redis`` backed by a ``ConnectionPool``.
    """
    global _pool, _redis
    if _redis is not None:
        return _redis

    settings = get_settings()

    if settings.redis.cluster_enabled:
        nodes = [
            ClusterNode(host=n["host"], port=int(n["port"]))
            for n in settings.redis.cluster_nodes
        ]
        _redis = RedisCluster(
            startup_nodes=nodes,
            decode_responses=True,
            skip_full_coverage_check=True,
        )
    else:
        _pool = ConnectionPool.from_url(
            settings.redis.url,
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
