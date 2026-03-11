"""Celery 共享实例 — 盘后批处理流水线调度"""
from __future__ import annotations

from celery import Celery
from celery.schedules import crontab

from shared.config.settings import get_settings


def create_celery_app() -> Celery:
    settings = get_settings()

    # Celery result backend: Redis DB 1
    # Strip trailing db number from base URL (e.g. redis://host:6379/0 → redis://host:6379)
    # then append /1 so results go to a separate DB.
    redis_base = settings.redis.url.rsplit("/", 1)[0]

    app = Celery(
        "algo_trader",
        broker=settings.rabbitmq.url,
        backend=f"{redis_base}/1",
    )

    app.conf.update(
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        timezone=settings.trading.timezone,
        enable_utc=True,
        task_track_started=True,
        task_acks_late=True,
        worker_prefetch_multiplier=1,
        # Task routes — each service handles its own tasks
        task_routes={
            "data_service.tasks.*": {"queue": "data"},
            "backfill_service.tasks.*": {"queue": "backfill"},
            "signal_service.tasks.*": {"queue": "signal"},
            "analysis_service.tasks.*": {"queue": "analysis"},
        },
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

    app.conf.beat_schedule = {
        # ── 盘后流水线入口（16:30 触发完整链） ──
        "post-market-pipeline": {
            "task": "data_service.tasks.run_post_market_pipeline",
            "schedule": crontab(hour=16, minute=30, day_of_week="1-5"),
            "options": {"queue": "data"},
        },
        # ── 独立定时任务 ──
        "daily-backfill-history-check": {
            "task": "backfill_service.tasks.check_historical_gaps",
            "schedule": crontab(hour=18, minute=0, day_of_week="1-5"),
            "options": {"queue": "backfill"},
        },
    }

    return app


# Global instance
celery_app = create_celery_app()
