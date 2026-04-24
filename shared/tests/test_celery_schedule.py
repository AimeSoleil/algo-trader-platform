from __future__ import annotations

from shared.celery_app import celery_app


def test_post_market_watchdog_schedule_registered() -> None:
    watchdog = celery_app.conf.beat_schedule["post-market-pipeline-watchdog"]

    assert watchdog["task"] == "data_service.tasks.ensure_post_market_pipeline_started"
    assert str(watchdog["schedule"]) == "<crontab: 10 17 * * 1-5 (m/h/dM/MY/d)>"
    assert watchdog["options"] == {"queue": "data"}


def test_redbeat_config_hardened_for_lock_refresh() -> None:
    assert celery_app.conf.redbeat_lock_timeout == 1800
    assert celery_app.conf.beat_max_loop_interval == 60