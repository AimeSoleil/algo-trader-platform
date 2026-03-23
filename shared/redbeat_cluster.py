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

    On initialisation it patches ``app.redbeat_conf`` so that the
    ``.redis`` property returns a cluster-aware client instead of the
    default ``StrictRedis`` one.  All other behaviour is inherited from
    ``RedBeatScheduler``.
    """

    def __init__(self, *args, **kwargs):
        settings = get_settings()

        if settings.redis.cluster_enabled:
            # Eagerly build the cluster client and inject it into RedBeat's
            # config object *before* the base class ``__init__`` tries to
            # read from Redis (setup_schedule → smembers).
            app = kwargs.get("app") or (args[0] if args else None)
            if app is not None:
                conf = app.redbeat_conf
                # ``RedBeatConfig`` checks ``_redis`` before falling back to
                # ``StrictRedis.from_url``.  Setting it here short-circuits
                # that path.
                conf._redis = _build_cluster_client()
                logger.info(
                    "RedBeat patched to use RedisCluster (%s)",
                    settings.redis.cluster_nodes,
                )

        super().__init__(*args, **kwargs)
