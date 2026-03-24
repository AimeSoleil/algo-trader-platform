"""RedBeat scheduler with Redis Cluster support.

``redbeat.RedBeatScheduler`` internally creates a plain
``redis.StrictRedis`` client which cannot follow ``MOVED`` redirections
in a Redis Cluster.  This module provides a thin subclass that replaces
the RedBeat Redis connection with a ``redis.cluster.RedisCluster``
client when ``settings.redis.cluster_enabled`` is ``True``.

Usage — set in Celery config::

    beat_scheduler = "shared.redbeat_cluster.ClusterRedBeatScheduler"
"""
from __future__ import annotations

import logging

from redbeat import RedBeatScheduler
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
            decode_responses=True,
            require_full_coverage=False,
        )
    return _cluster_client


def _ensure_cluster_redis(app) -> None:
    """Ensure RedBeat uses a cluster-aware Redis client.

    RedBeat's ``get_redis(app)`` caches the client on ``app.redbeat_redis``.
    We replace it with a ``RedisCluster`` instance so all subsequent
    RedBeat operations go through the cluster client.
    """
    if not isinstance(getattr(app, "redbeat_redis", None), RedisCluster):
        app.redbeat_redis = _get_cluster_client()
        logger.info("RedBeat redis patched → RedisCluster")


class ClusterRedBeatScheduler(RedBeatScheduler):
    """RedBeat scheduler that works with Redis Cluster.

    Overrides ``setup_schedule`` and ``update_schedule`` to inject a
    ``RedisCluster`` client just-in-time — after ``RedBeatScheduler``
    has finished its own ``__init__`` bookkeeping but right before the
    first Redis command (``SMEMBERS``) is issued.
    """

    def setup_schedule(self):
        settings = get_settings()
        if settings.redis.cluster_enabled:
            _ensure_cluster_redis(self.app)
        super().setup_schedule()

    def update_schedule(self, schedule):
        settings = get_settings()
        if settings.redis.cluster_enabled:
            _ensure_cluster_redis(self.app)
        super().update_schedule(schedule)
