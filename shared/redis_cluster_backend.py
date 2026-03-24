"""Celery Redis result backend that works with Redis Cluster.

The built-in ``celery.backends.redis.RedisBackend`` creates a plain
``StrictRedis`` client that cannot follow ``MOVED`` redirections.
This module provides a drop-in subclass that swaps the client for a
``redis.cluster.RedisCluster`` instance.

Usage — set as Celery result backend::

    app = Celery(..., backend="shared.redis_cluster_backend.RedisClusterBackend")
"""
from __future__ import annotations

import logging

from celery.backends.redis import RedisBackend
from redis.cluster import ClusterNode, RedisCluster

from shared.config.settings import get_settings

logger = logging.getLogger(__name__)

_cluster_client: RedisCluster | None = None


def _get_cluster_client() -> RedisCluster:
    """Return a cached synchronous ``RedisCluster`` client."""
    global _cluster_client
    if _cluster_client is None:
        settings = get_settings()
        nodes = [
            ClusterNode(host=n["host"], port=int(n["port"]))
            for n in settings.redis.cluster_nodes
        ]
        _cluster_client = RedisCluster(
            startup_nodes=nodes,
            decode_responses=False,   # Celery stores binary-encoded results
            require_full_coverage=False,
        )
        logger.info(
            "redis_cluster_backend: created RedisCluster client (%d nodes)",
            len(nodes),
        )
    return _cluster_client


class RedisClusterBackend(RedisBackend):
    """Redis Cluster-aware Celery result backend."""

    @property
    def client(self):
        """Return a ``RedisCluster`` client instead of ``StrictRedis``."""
        return _get_cluster_client()
