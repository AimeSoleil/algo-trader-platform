from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.data_service.app import routes


def _build_client() -> TestClient:
    app = FastAPI()
    app.include_router(routes.router, prefix="/api/v1")
    return TestClient(app)


def _settings(close: str = "16:00") -> SimpleNamespace:
    return SimpleNamespace(
        common=SimpleNamespace(
            market_hours=SimpleNamespace(end=close),
        ),
    )


def test_trigger_post_market_collection_queues_collection_only_task(monkeypatch):
    client = _build_client()
    queued: dict = {}

    monkeypatch.setattr(routes, "get_settings", lambda: _settings())
    monkeypatch.setattr(routes, "now_market", lambda: datetime(2026, 4, 23, 16, 30))
    monkeypatch.setattr(routes, "today_trading", lambda: date(2026, 4, 23))
    monkeypatch.setattr(routes, "before_market_open", lambda: False)
    monkeypatch.setattr(routes, "is_market_open", lambda: False)

    def _send_task(name: str, args: list[str], queue: str):
        queued["name"] = name
        queued["args"] = args
        queued["queue"] = queue
        return SimpleNamespace(id="task-123")

    monkeypatch.setattr(routes.celery_app, "send_task", _send_task)

    response = client.post("/api/v1/data/collect/post-market", params={"trading_date": "2026-04-23"})

    assert response.status_code == 202
    assert response.json()["task_id"] == "task-123"
    assert queued == {
        "name": "data_service.tasks.run_post_market_collection_only",
        "args": ["2026-04-23"],
        "queue": "data",
    }


def test_trigger_post_market_collection_rejects_during_market_hours(monkeypatch):
    client = _build_client()

    monkeypatch.setattr(routes, "get_settings", lambda: _settings(close="16:00"))
    monkeypatch.setattr(routes, "now_market", lambda: datetime(2026, 4, 23, 15, 45))
    monkeypatch.setattr(routes, "today_trading", lambda: date(2026, 4, 23))
    monkeypatch.setattr(routes, "before_market_open", lambda: False)
    monkeypatch.setattr(routes, "is_market_open", lambda: True)

    response = client.post("/api/v1/data/collect/post-market", params={"trading_date": "2026-04-23"})

    assert response.status_code == 422
    assert "after market close (16:00)" in response.json()["detail"]["error"]


def test_trigger_post_market_collection_allows_past_date_during_market_hours(monkeypatch):
    client = _build_client()
    queued: dict = {}

    monkeypatch.setattr(routes, "get_settings", lambda: _settings())
    monkeypatch.setattr(routes, "now_market", lambda: datetime(2026, 4, 23, 10, 0))
    monkeypatch.setattr(routes, "today_trading", lambda: date(2026, 4, 23))
    monkeypatch.setattr(routes, "before_market_open", lambda: False)
    monkeypatch.setattr(routes, "is_market_open", lambda: True)

    def _send_task(name: str, args: list[str], queue: str):
        queued["name"] = name
        queued["args"] = args
        queued["queue"] = queue
        return SimpleNamespace(id="task-past-date")

    monkeypatch.setattr(routes.celery_app, "send_task", _send_task)

    response = client.post("/api/v1/data/collect/post-market", params={"trading_date": "2026-04-22"})

    assert response.status_code == 202
    assert response.json()["task_id"] == "task-past-date"
    assert queued["args"] == ["2026-04-22"]


def test_trigger_post_market_collection_requires_trading_date():
    client = _build_client()

    response = client.post("/api/v1/data/collect/post-market")

    assert response.status_code == 422
    assert any(error["loc"][-1] == "trading_date" for error in response.json()["detail"])


def test_trigger_post_market_collection_rejects_future_date(monkeypatch):
    client = _build_client()

    monkeypatch.setattr(routes, "get_settings", lambda: _settings())
    monkeypatch.setattr(routes, "now_market", lambda: datetime(2026, 4, 23, 16, 30))
    monkeypatch.setattr(routes, "today_trading", lambda: date(2026, 4, 23))

    response = client.post("/api/v1/data/collect/post-market", params={"trading_date": "2026-04-24"})

    assert response.status_code == 422
    assert response.json()["detail"]["error"] == "trading_date cannot be in the future"
