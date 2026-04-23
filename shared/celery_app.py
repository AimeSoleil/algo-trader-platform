"""Celery 共享实例 — 盘后批处理流水线调度"""
from __future__ import annotations

from celery import Celery
from celery.schedules import crontab
from celery.signals import after_setup_logger

from shared.config.settings import get_settings
from shared.utils import get_logger


logger = get_logger("celery_app")


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
            "services.data_service.app.tasks.intraday_stock",
            "services.data_service.app.tasks.aggregation",
            "services.data_service.app.tasks.pipeline",
            "services.data_service.app.tasks.coordination",
            "services.data_service.app.tasks.earnings",
            "services.data_service.app.tasks.manual",
            # signal_service
            "services.signal_service.app.tasks.signal",
            # analysis_service
            "services.analysis_service.app.tasks.blueprint",
            "services.analysis_service.app.tasks.analyze",
            # trade_service
            "services.trade_service.app.execution.tasks",
            "services.trade_service.app.execution.tasks_intraday",
            "services.trade_service.app.portfolio.tasks",
            "services.trade_service.app.tasks.daily_report",
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
        task_soft_time_limit=celery_cfg.task_soft_time_limit,
        task_time_limit=celery_cfg.task_time_limit,
        task_reject_on_worker_lost=celery_cfg.task_reject_on_worker_lost,
        worker_prefetch_multiplier=celery_cfg.prefetch_multiplier,
        worker_max_memory_per_child=celery_cfg.max_memory_per_child,
        # Task routes — each service handles its own tasks
        task_routes={
            "data_service.tasks.*": {"queue": "data"},
            "signal_service.tasks.*": {"queue": "signal"},
            "analysis_service.tasks.*": {"queue": "analysis"},
            "trade_service.tasks.*": {"queue": "data"},
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
    # Timeline (ET, weekdays):
    #   09:30-15:55  intraday-option-capture (every 5 min)
    #   09:30-15:55  intraday-stock-capture  (every 5 min)
    #   09:31-15:56  intraday-entry-optimizer (every 5 min, +1 min offset)
    #   16:00        intraday-option-capture-close (final tick)
    #   16:00        intraday-stock-capture-close  (final tick)
    #   16:50        refresh-earnings-cache  — update Redis earnings cache
    #   17:00        post-market-pipeline    — options agg + stock capture → downstream
    #   (16:30)      daily-trading-report    — if notifier enabled

    intraday_interval = settings.data_service.worker.schedule.options_capture_every_minutes

    _mkt_start_h, _mkt_start_m = map(int, settings.common.market_hours.start.split(":"))
    _mkt_end_h, _mkt_end_m = map(int, settings.common.market_hours.end.split(":"))

    # Unified post-market pipeline
    _pm_h, _pm_m = map(int, settings.data_service.worker.schedule.post_market_time.split(":"))

    # Earnings cache refresh
    _earn_h, _earn_m = map(int, settings.data_service.worker.schedule.refresh_earnings_time.split(":"))

    pm_minutes = _pm_h * 60 + _pm_m
    earn_minutes = _earn_h * 60 + _earn_m
    earnings_before_pipeline = earn_minutes < pm_minutes

    logger.info(
        "celery.schedule.order",
        post_market_time=settings.data_service.worker.schedule.post_market_time,
        refresh_earnings_time=settings.data_service.worker.schedule.refresh_earnings_time,
        earnings_before_pipeline=earnings_before_pipeline,
    )
    if not earnings_before_pipeline:
        logger.warning(
            "celery.schedule.order_unexpected",
            post_market_time=settings.data_service.worker.schedule.post_market_time,
            refresh_earnings_time=settings.data_service.worker.schedule.refresh_earnings_time,
            reason="refresh_earnings_time should be before post_market_time",
        )

    def _build_intraday_capture_schedule_entries(schedule_name: str, task_name: str) -> dict:
        entries: dict[str, dict] = {}
        minutes_by_hour: dict[int, list[int]] = {}
        current_minute = _mkt_start_h * 60 + _mkt_start_m
        end_minute = _mkt_end_h * 60 + _mkt_end_m

        while current_minute < end_minute:
            hour, minute = divmod(current_minute, 60)
            minutes_by_hour.setdefault(hour, []).append(minute)
            current_minute += intraday_interval

        for index, (hour, minutes) in enumerate(minutes_by_hour.items()):
            entry_name = schedule_name if index == 0 else f"{schedule_name}-{hour:02d}"
            entries[entry_name] = {
                "task": task_name,
                "schedule": crontab(
                    minute=",".join(str(minute) for minute in minutes),
                    hour=hour,
                    day_of_week="1-5",
                ),
                "options": {"queue": "data"},
            }

        return entries

    app.conf.beat_schedule = {
        "post-market-pipeline": {
            "task": "data_service.tasks.run_post_market_pipeline",
            "schedule": crontab(hour=_pm_h, minute=_pm_m, day_of_week="1-5"),
            "options": {"queue": "data"},
        },
        **_build_intraday_capture_schedule_entries(
            "intraday-option-capture",
            "data_service.tasks.capture_intraday_options",
        ),
        "intraday-option-capture-close": {
            "task": "data_service.tasks.capture_intraday_options",
            "schedule": crontab(
                minute=_mkt_end_m,
                hour=_mkt_end_h,
                day_of_week="1-5",
            ),
            "options": {"queue": "data"},
        },
        **_build_intraday_capture_schedule_entries(
            "intraday-stock-capture",
            "data_service.tasks.capture_intraday_stock",
        ),
        "intraday-stock-capture-close": {
            "task": "data_service.tasks.capture_intraday_stock",
            "schedule": crontab(
                minute=_mkt_end_m,
                hour=_mkt_end_h,
                day_of_week="1-5",
            ),
            "options": {"queue": "data"},
        },
        "refresh-earnings-cache": {
            "task": "data_service.tasks.refresh_earnings_cache",
            "schedule": crontab(hour=_earn_h, minute=_earn_m, day_of_week="1-5"),
            "options": {"queue": "data"},
        },
        "intraday-entry-optimizer": {
            "task": "trade_service.tasks.evaluate_entry_windows",
            "schedule": crontab(
                minute="1,6,11,16,21,26,31,36,41,46,51,56",
                hour=f"{_mkt_start_h}-{_mkt_end_h}",
                day_of_week="1-5",
            ),
            "options": {"queue": "data"},
        },
    }

    # ── Daily trading report (if notifier enabled) ──
    if settings.common.notifier.enabled:
        _rpt_h, _rpt_m = map(int, settings.common.notifier.daily_report_time.split(":"))
        app.conf.beat_schedule["daily-trading-report"] = {
            "task": "trade_service.tasks.send_daily_report",
            "schedule": crontab(hour=_rpt_h, minute=_rpt_m, day_of_week="1-5"),
            "options": {"queue": "data"},
        }

    return app


# Global instance
celery_app = create_celery_app()
