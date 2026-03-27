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
            for n in settings.infra.redis.cluster_nodes
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
    """Redis Cluster-aware Celery result backend.

    Two adaptations on top of the standard ``RedisBackend``:

    1. ``client`` returns a ``RedisCluster`` instance so keys are
       routed to the correct shard.
    2. ``_set`` avoids putting ``PUBLISH`` inside a cluster pipeline
       (which redis-py blocks).  Instead it runs SET/SETEX first,
       then publishes separately.
    """

    @property
    def client(self):
        """Return a ``RedisCluster`` client instead of ``StrictRedis``."""
        return _get_cluster_client()

    def _set(self, key, value):
        """Store result and notify — cluster-safe (no pipelined PUBLISH)."""
        cl = self.client
        if self.expires:
            cl.setex(key, self.expires, value)
        else:
            cl.set(key, value)
        # Publish outside the pipeline so RedisCluster doesn't reject it.
        try:
            cl.publish(key, value)
        except Exception:
            # publish is best-effort notification; don't fail the result store
            pass
