from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.data_service.app.tasks import pipeline


def test_watchdog_dispatches_pipeline_when_missing(monkeypatch):
    def _run_async_false(awaitable):
        awaitable.close()
        return False

    monkeypatch.setattr(pipeline, "run_async", _run_async_false)

    sent: dict = {}

    def _send_task(name: str, args: list[str], queue: str):
        sent["name"] = name
        sent["args"] = args
        sent["queue"] = queue
        return SimpleNamespace(id="watchdog-123")

    monkeypatch.setattr(pipeline.celery_app, "send_task", _send_task)
    monkeypatch.setattr(pipeline, "today_trading", lambda: SimpleNamespace(isoformat=lambda: "2026-04-23"))
    monkeypatch.setattr(
        pipeline,
        "notify_sync",
        lambda *args, **kwargs: None,
        raising=False,
    )

    result = pipeline.ensure_post_market_pipeline_started("2026-04-23")

    assert result == {
        "status": "watchdog_dispatched",
        "trading_date": "2026-04-23",
        "task_id": "watchdog-123",
    }
    assert sent == {
        "name": "data_service.tasks.run_post_market_pipeline",
        "args": ["2026-04-23"],
        "queue": "data",
    }


def test_watchdog_noops_when_pipeline_already_started(monkeypatch):
    def _run_async_true(awaitable):
        awaitable.close()
        return True

    monkeypatch.setattr(pipeline, "run_async", _run_async_true)

    result = pipeline.ensure_post_market_pipeline_started("2026-04-23")

    assert result == {
        "status": "already_started",
        "trading_date": "2026-04-23",
    }