"""Distributed job-execution lock using Redis.

Ensures that only **one** process runs a scheduled job at a time, even when
multiple service replicas share the same APScheduler configuration.

The core primitive is ``distributed_once`` — a decorator that wraps an
async job function so it:

1. Attempts to acquire a Redis lock (``SET NX EX``).
2. If acquired → runs the job and releases the lock on completion.
3. If NOT acquired → logs a debug message and returns immediately.

Usage::

    from shared.distributed_lock import distributed_once

    @distributed_once("data:intraday_capture", ttl=240)
    async def _capture_intraday(state):
        ...  # only one replica executes this at a time

Prometheus counters are exposed so Grafana can track lock contention.
"""
from __future__ import annotations

import functools
import time as _time
from typing import Any, Callable

from prometheus_client import Counter, Histogram

from shared.redis_pool import get_redis
from shared.utils import get_logger

logger = get_logger("distributed_lock")

# ── Prometheus metrics ──────────────────────────────────────

LOCK_ACQUIRED = Counter(
    "scheduler_lock_acquired_total",
    "Number of times a distributed scheduler lock was successfully acquired",
    ["service", "job_id"],
)
LOCK_SKIPPED = Counter(
    "scheduler_lock_skipped_total",
    "Number of times a job was skipped because another instance holds the lock",
    ["service", "job_id"],
)
LOCK_HOLD_DURATION = Histogram(
    "scheduler_lock_hold_duration_seconds",
    "Time the distributed lock was held (= job execution time)",
    ["service", "job_id"],
    buckets=(1, 5, 15, 30, 60, 120, 240, 300, 600),
)

# Key prefix to namespace all scheduler locks in Redis
_KEY_PREFIX = "scheduler:lock"


def _lock_key(job_id: str) -> str:
    return f"{_KEY_PREFIX}:{job_id}"


def distributed_once(
    job_id: str,
    *,
    ttl: int = 300,
    service: str = "",
) -> Callable:
    """Decorator: run the wrapped async function only if we win the Redis lock.

    Parameters
    ----------
    job_id:
        Unique identifier for the job (used as part of the Redis key).
    ttl:
        Lock time-to-live in seconds.  Should be **longer** than the
        maximum expected job duration to prevent overlap, but short
        enough that a crashed holder doesn't block others for too long.
    service:
        Label for Prometheus metrics (e.g. ``"data_service"``).
    """

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            redis = get_redis()
            key = _lock_key(job_id)
            # Try to acquire: SET key value NX EX ttl
            acquired = await redis.set(key, "1", nx=True, ex=ttl)

            svc_label = service or job_id.split(":")[0]

            if not acquired:
                LOCK_SKIPPED.labels(service=svc_label, job_id=job_id).inc()
                logger.debug(
                    "distributed_lock.skipped",
                    job_id=job_id,
                    reason="lock_held_by_another_instance",
                )
                return None

            LOCK_ACQUIRED.labels(service=svc_label, job_id=job_id).inc()
            logger.debug("distributed_lock.acquired", job_id=job_id, ttl=ttl)
            started = _time.monotonic()
            try:
                return await fn(*args, **kwargs)
            finally:
                elapsed = _time.monotonic() - started
                LOCK_HOLD_DURATION.labels(service=svc_label, job_id=job_id).observe(elapsed)
                # Release the lock so the next tick can acquire it promptly
                # (if the job finishes well before TTL expiry).
                await redis.delete(key)
                logger.debug("distributed_lock.released", job_id=job_id)

        return wrapper

    return decorator
