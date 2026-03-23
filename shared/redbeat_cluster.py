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
from redbeat.schedulers import RedBeatConfig
from redis.cluster import ClusterNode, RedisCluster

from shared.config.settings import get_settings

logger = logging.getLogger(__name__)


def _build_cluster_client() -> RedisCluster:
    """Build a synchronous ``RedisCluster`` client from application settings."""
    settings = get_settings()
    nodes = [
        ClusterNode(host=n["host"], port=int(n["port"]))
        for n in settings.redis.cluster_nodes
    ]
    return RedisCluster(
        startup_nodes=nodes,
        decode_responses=True,
        require_full_coverage=False,
    )


class ClusterRedBeatScheduler(RedBeatScheduler):
    """RedBeat scheduler that works with Redis Cluster.

    On initialisation it constructs a ``RedBeatConfig`` with a
    ``RedisCluster`` client and attaches it to the Celery app *before*
    the parent ``__init__`` calls ``setup_schedule`` (which hits Redis).
    """

    def __init__(self, *args, **kwargs):
        settings = get_settings()

        if settings.redis.cluster_enabled:
            # Resolve 'app' the same way Celery passes it (keyword arg).
            app = kwargs.get("app") or (args[0] if args else None)
            if app is not None:
                # Create RedBeatConfig manually and inject the cluster
                # client *before* super().__init__ triggers setup_schedule.
                conf = RedBeatConfig(app)
                conf._redis = _build_cluster_client()
                # Attach to app so RedBeat's internal code finds it.
                app.redbeat_conf = conf
                logger.info(
                    "RedBeat patched to use RedisCluster (%s)",
                    settings.redis.cluster_nodes,
                )

        super().__init__(*args, **kwargs)
