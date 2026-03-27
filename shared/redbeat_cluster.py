"""RedBeat scheduler with Redis Cluster support.

``redbeat.RedBeatScheduler`` internally creates a plain
``redis.StrictRedis`` client which cannot follow ``MOVED`` redirections
in a Redis Cluster.  This module provides a thin subclass that replaces
the RedBeat Redis connection with a ``redis.cluster.RedisCluster``
client when ``settings.infra.redis.cluster_enabled`` is ``True``.

It also makes ``tick()`` resilient to transient ``LockNotOwnedError``
exceptions — a common occurrence in Redis Cluster due to master
fail-overs or brief connectivity blips that can cause the lock key
to vanish before the next extend call.

Usage — set in Celery config::

    beat_scheduler = "shared.redbeat_cluster.ClusterRedBeatScheduler"
"""
from __future__ import annotations

import logging

from redbeat import RedBeatScheduler
from redbeat.schedulers import LUA_EXTEND_TO_SCRIPT, get_redis
from redis.cluster import ClusterNode, RedisCluster
from redis.exceptions import LockNotOwnedError

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

    Overrides ``tick()`` to catch ``LockNotOwnedError`` and transparently
    re-acquire the lock instead of crashing the beat process.  In a Redis
    Cluster environment the lock key can disappear due to master fail-over
    or brief network partitions; this makes the scheduler self-healing.
    """

    def setup_schedule(self):
        settings = get_settings()
        if settings.infra.redis.cluster_enabled:
            _ensure_cluster_redis(self.app)
        super().setup_schedule()

    def update_schedule(self, schedule):
        settings = get_settings()
        if settings.infra.redis.cluster_enabled:
            _ensure_cluster_redis(self.app)
        super().update_schedule(schedule)

    # ── Lock-resilient tick ─────────────────────────────────────
    def _reacquire_lock(self) -> bool:
        """Re-create and acquire the distributed lock.

        Mirrors the logic of ``redbeat.schedulers.acquire_distributed_beat_lock``
        but can be called at any time, not just at beat-init.
        """
        try:
            redis_client = get_redis(self.app)
            lock = redis_client.lock(
                self.lock_key,
                timeout=self.lock_timeout,
                sleep=self.max_interval,
            )
            # RedBeat replaces the default extend script so that
            # ``extend()`` *sets* (not adds) the TTL.
            lock.lua_extend = redis_client.register_script(LUA_EXTEND_TO_SCRIPT)
            if lock.acquire(blocking=False):
                self.lock = lock
                logger.warning(
                    "beat: Re-acquired lock '%s' after transient loss",
                    self.lock_key,
                )
                return True
            # Another beat instance grabbed the lock — step aside.
            logger.warning(
                "beat: Could not re-acquire lock '%s'; another instance holds it",
                self.lock_key,
            )
            return False
        except Exception:
            logger.exception("beat: Failed to re-acquire lock")
            return False

    def tick(self, **kwargs):
        try:
            return super().tick(**kwargs)
        except LockNotOwnedError:
            logger.warning(
                "beat: Lock lost (LockNotOwnedError) — attempting re-acquisition"
            )
            if self._reacquire_lock():
                # Lock re-acquired; retry the tick immediately.
                return super().tick(**kwargs)
            # Could not re-acquire — sleep for max_interval and try next cycle.
            return self.max_interval
