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
    # DB 1 for results, DB 2 for RedBeat.
    redis_base = settings.infra.redis.url.rsplit("/", 1)[0]
    backend_url = f"{redis_base}/1"
    redbeat_url = f"{redis_base}/2"

    app = Celery(
        "algo_trader",
        broker=settings.infra.rabbitmq.url,
        backend=backend_url,
        include=[
            # data_service
            "services.data_service.app.tasks.capture",
            "services.data_service.app.tasks.intraday",
            "services.data_service.app.tasks.aggregation",
            "services.data_service.app.tasks.pipeline",
            "services.data_service.app.tasks.options_pipeline",
            "services.data_service.app.tasks.coordination",
            "services.data_service.app.tasks.manual",
            # backfill_service
            "services.backfill_service.app.tasks.gap_detection",
            "services.backfill_service.app.tasks.maintenance",
            # signal_service
            "services.signal_service.app.tasks.signal",
            # analysis_service
            "services.analysis_service.app.tasks.blueprint",
            "services.analysis_service.app.tasks.analyze",
            # trade_service
            "services.trade_service.app.execution.tasks",
            "services.trade_service.app.portfolio.tasks",
        ],
    )

    celery_cfg = settings.common.celery
    app.conf.update(
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        timezone=settings.common.timezone,
        enable_utc=True,
        task_track_started=celery_cfg.task_track_started,
        task_acks_late=celery_cfg.task_acks_late,
        worker_prefetch_multiplier=celery_cfg.prefetch_multiplier,
        worker_max_memory_per_child=celery_cfg.max_memory_per_child,
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
        beat_scheduler="redbeat.RedBeatScheduler",
        redbeat_redis_url=redbeat_url,
        redbeat_key_prefix="redbeat:",
        redbeat_lock_timeout=settings.common.beat.redbeat_lock_timeout,
    )

    if celery_cfg.concurrency > 0:
        app.conf.worker_concurrency = celery_cfg.concurrency

    # ── Beat schedules ─────────────────────────────────────
    #
    # 1. stock-pipeline  — captures post-market stock data (1m bars / daily bars)
    #    at configured time (default 18:30 ET) weekdays.
    #
    # 2. options-post-close — aggregates intraday option snapshots into daily
    #    tables shortly after market close (default 16:10 ET) weekdays.
    #
    # 3. intraday-option-capture — captures option chain snapshots every N minutes
    #    during US market hours (09:30-16:00 ET, weekdays).  The task itself has
    #    an is_market_open() guard to handle edge cases (e.g. 9:25 crontab fire).
    #    distributed_once ensures only one orchestrator runs per tick when
    #    celery-data is scaled horizontally; chunks then fan out to all workers.
    #
    # Coordination: both stock-pipeline and options-post-close set Redis flags
    # and call check_pipelines_and_continue.  Downstream stages (backfill →
    # signals → blueprint) only dispatch when both flags are present.

    _stock_h, _stock_m = map(int, settings.data_service.worker.schedule.stock_pipeline_time.split(":"))
    intraday_interval = settings.data_service.worker.schedule.options_capture_every_minutes

    # Market hours: crontab fires at 09:30, 09:35, ... 15:55 (ET, weekdays)
    # The task's is_market_open() guard handles the 09:00-09:29 window.
    _mkt_start_h, _mkt_start_m = map(int, settings.common.market_hours.start.split(":"))
    _mkt_end_h, _mkt_end_m = map(int, settings.common.market_hours.end.split(":"))

    # Options aggregation = market close + 10 minutes
    _opt_agg_h, _opt_agg_m = _mkt_end_h, _mkt_end_m + 10
    if _opt_agg_m >= 60:
        _opt_agg_h += 1
        _opt_agg_m -= 60

    app.conf.beat_schedule = {
        "stock-pipeline": {
            "task": "data_service.tasks.run_stock_pipeline",
            "schedule": crontab(hour=_stock_h, minute=_stock_m, day_of_week="1-5"),
            "options": {"queue": "data"},
        },
        "options-post-close": {
            "task": "data_service.tasks.run_options_post_close",
            "schedule": crontab(hour=_opt_agg_h, minute=_opt_agg_m, day_of_week="1-5"),
            "options": {"queue": "data"},
        },
        "intraday-option-capture": {
            "task": "data_service.tasks.capture_intraday_options",
            "schedule": crontab(
                minute=f"*/{intraday_interval}",
                hour=f"{_mkt_start_h}-{int(settings.common.market_hours.end.split(':')[0])}",
                day_of_week="1-5",
            ),
            "options": {"queue": "data"},
        },
    }

    return app


# Global instance
celery_app = create_celery_app()
