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
    """Ensure ``app.redbeat_conf._redis`` points to a cluster client.

    RedBeat may (re-)create its ``RedBeatConfig`` at various points
    during init.  This helper idempotently injects the cluster client
    right before any Redis I/O occurs.
    """
    conf = app.redbeat_conf          # may lazily create a RedBeatConfig
    if not isinstance(conf._redis, RedisCluster):
        conf._redis = _get_cluster_client()
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
