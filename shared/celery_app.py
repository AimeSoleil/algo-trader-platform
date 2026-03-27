"""Celery 共享实例 — 盘后批处理流水线调度"""
from __future__ import annotations

from celery import Celery
from celery.schedules import crontab
from celery.signals import after_setup_logger

from shared.config.settings import get_settings


# ── Celery logging hook ────────────────────────────────────
# Celery workers have their own logging bootstrap.  This signal fires
# right after Celery configures its root logger, giving us a chance to
# inject our TZ-aware formatter + file handler.

@after_setup_logger.connect
def _on_celery_setup_logger(**kwargs):
    from shared.utils.logging import setup_celery_logging
    setup_celery_logging(**kwargs)


def create_celery_app() -> Celery:
    settings = get_settings()

    # ── Redis URL construction ──────────────────────────────
    # Cluster mode: use custom backend that wraps RedisCluster.
    # All DBs merge to 0; isolation via key prefix.
    # Standalone: DB 1 for results, DB 2 for RedBeat.
    if settings.infra.redis.cluster_enabled and settings.infra.redis.cluster_nodes:
        _first = settings.infra.redis.cluster_nodes[0]
        redis_base = f"redis://{_first['host']}:{_first['port']}"
        backend_url = "shared.redis_cluster_backend.RedisClusterBackend"
        redbeat_url = f"{redis_base}/0"
    else:
        redis_base = settings.infra.redis.url.rsplit("/", 1)[0]
        backend_url = f"{redis_base}/1"
        redbeat_url = f"{redis_base}/2"

    app = Celery(
        "algo_trader",
        broker=settings.infra.rabbitmq.url,
        backend=backend_url,
        include=[
            "services.data_service.app.tasks",
            "services.backfill_service.app.tasks",
            "services.signal_service.app.tasks",
            "services.analysis_service.app.tasks",
            "services.trade_service.app.execution.tasks",
            "services.trade_service.app.portfolio.tasks",
        ],
    )

    app.conf.update(
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        timezone=settings.common.timezone,
        enable_utc=True,
        task_track_started=True,
        task_acks_late=True,
        worker_prefetch_multiplier=1,
        worker_max_memory_per_child=500_000,  # 500 MB — 超出后自动重启 worker 子进程
        # Key prefix for result backend (isolates Celery keys in cluster mode)
        result_backend_transport_options={"global_keyprefix": "celery:result:"},
        # Task routes — each service handles its own tasks
        task_routes={
            "data_service.tasks.*": {"queue": "data"},
            "backfill_service.tasks.*": {"queue": "backfill"},
            "signal_service.tasks.*": {"queue": "signal"},
            "analysis_service.tasks.*": {"queue": "analysis"},
        },
        # ── RedBeat — distributed Beat scheduler ──────────────
        # Replaces the default file-based beat scheduler so that
        # multiple celery-beat replicas can co-exist safely (only
        # one holds the Redis lock at a time).
        #
        # In Redis Cluster mode we use a thin subclass that injects a
        # cluster-aware client; see shared/redbeat_cluster.py.
        beat_scheduler=(
            "shared.redbeat_cluster.ClusterRedBeatScheduler"
            if settings.infra.redis.cluster_enabled
            else "redbeat.RedBeatScheduler"
        ),
        redbeat_redis_url=redbeat_url,
        redbeat_key_prefix="redbeat:",          # namespace RedBeat keys (cluster-safe)
        redbeat_lock_timeout=300,  # seconds before a dead beat loses the lock
    )

    # ── 盘后流水线调度 (Celery Beat) ──────────────────────────
    #
    # Pipeline chain (sequential via Celery chain):
    #   16:30  capture_post_market_data  (Data — 1m bars + daily bar + option chain → DB)
    #          batch_flush_to_db         (Data — flush intraday option Parquet → DB)
    #          detect_and_backfill_gaps  (Backfill — 4-table gap check + stock backfill)
    #          compute_daily_signals     (Signal)
    #          generate_daily_blueprint  (Analysis)
    #
    # Beat only triggers the pipeline entry point; the chain handles ordering.

    # Parse schedule times from config
    _flush_h, _flush_m = map(int, settings.common.schedule.batch_flush_time.split(":"))
    _backfill_h, _backfill_m = map(int, settings.common.schedule.backfill_time.split(":"))

    app.conf.beat_schedule = {
        # ── 盘后流水线入口 ──
        "post-market-pipeline": {
            "task": "data_service.tasks.run_post_market_pipeline",
            "schedule": crontab(hour=_flush_h, minute=_flush_m, day_of_week="1-5"),
            "options": {"queue": "data"},
        },
        # ── 独立定时任务 ──
        "daily-backfill-history-check": {
            "task": "backfill_service.tasks.check_historical_gaps",
            "schedule": crontab(hour=_backfill_h, minute=_backfill_m, day_of_week="1-5"),
            "options": {"queue": "backfill"},
        },
    }

    return app


# Global instance
celery_app = create_celery_app()
