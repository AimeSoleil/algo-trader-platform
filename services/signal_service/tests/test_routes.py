from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from services.signal_service.app.routes import router


def _build_test_client() -> TestClient:
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    return TestClient(app)


def test_query_signal_by_symbol_route_uses_unified_query_with_legacy_cache_flag():
    client = _build_test_client()
    expected_payload = {
        "data": [{"symbol": "NVDA"}],
        "total": 1,
        "limit": 500,
        "offset": 0,
        "count": 1,
        "filters_applied": {"symbols": ["NVDA"]},
    }

    with patch(
        "services.signal_service.app.queries.query_signals",
        new=AsyncMock(return_value=expected_payload),
    ) as query_signals:
        response = client.get("/api/v1/signals/NVDA?by_pass_cache=true&sort_order=asc")

    assert response.status_code == 200
    assert response.json() == expected_payload
    query_signals.assert_awaited_once_with(
        symbols=["NVDA"],
        start_date=None,
        end_date=None,
        bypass_cache=True,
        volatility_regime=None,
        trend=None,
        sort_by=None,
        sort_order="asc",
        limit=500,
        offset=0,
    )


def test_query_signal_by_symbol_does_not_capture_compute_status_route():
    client = _build_test_client()

    with patch(
        "services.signal_service.app.routes.AsyncResult",
        return_value=SimpleNamespace(state="PENDING", info=None, result=None),
    ):
        response = client.get("/api/v1/signals/compute/task-123")

    assert response.status_code == 200
    assert response.json() == {"task_id": "task-123", "state": "PENDING"}


def test_trigger_signal_compute_single_date_dispatches_one_task():
    client = _build_test_client()

    with patch(
        "services.signal_service.app.routes.celery_app.send_task",
        return_value=SimpleNamespace(id="task-single"),
    ) as send_task, patch(
        "services.signal_service.app.routes.today_trading",
        return_value=date(2026, 3, 20),
    ):
        response = client.post(
            "/api/v1/signals/compute",
            json={"trading_date": "2026-03-12", "symbols": ["aapl", " MSFT "]},
        )

    assert response.status_code == 202
    payload = response.json()
    assert payload["task_id"] == "task-single"
    assert payload["task_ids"] == ["task-single"]
    assert payload["trading_dates"] == ["2026-03-12"]
    assert "trading_date=2026-03-12" in payload["message"]

    send_task.assert_called_once_with(
        "signal_service.tasks.compute_daily_signals",
        args=["2026-03-12"],
        kwargs={"symbols": ["AAPL", "MSFT"]},
        queue="signal",
    )


def test_trigger_signal_compute_supports_market_day_range_only():
    client = _build_test_client()

    with patch(
        "services.signal_service.app.routes.celery_app.send_task",
        side_effect=[
            SimpleNamespace(id="task-1"),
            SimpleNamespace(id="task-2"),
        ],
    ) as send_task, patch(
        "services.signal_service.app.routes.today_trading",
        return_value=date(2026, 3, 20),
    ):
        response = client.post(
            "/api/v1/signals/compute",
            json={"start_date": "2026-01-16", "end_date": "2026-01-20"},
        )

    assert response.status_code == 202
    payload = response.json()
    assert payload["task_id"] is None
    assert payload["task_ids"] == ["task-1", "task-2"]
    assert payload["trading_dates"] == ["2026-01-16", "2026-01-20"]
    assert "market_days=[2026-01-16, 2026-01-20]" in payload["message"]

    assert send_task.call_count == 2
    first_call = send_task.call_args_list[0]
    second_call = send_task.call_args_list[1]
    assert first_call.kwargs == {
        "args": ["2026-01-16"],
        "kwargs": {"symbols": None},
        "queue": "signal",
    }
    assert second_call.kwargs == {
        "args": ["2026-01-20"],
        "kwargs": {"symbols": None},
        "queue": "signal",
    }


def test_trigger_signal_compute_rejects_future_dates_in_range():
    client = _build_test_client()

    with patch(
        "services.signal_service.app.routes.today_trading",
        return_value=date(2026, 3, 12),
    ):
        response = client.post(
            "/api/v1/signals/compute",
            json={"start_date": "2026-03-11", "end_date": "2026-03-13"},
        )

    assert response.status_code == 422
    assert "2026-03-13" in response.json()["detail"]


def test_trigger_signal_compute_rejects_both_single_and_range_fields():
    client = _build_test_client()

    response = client.post(
        "/api/v1/signals/compute",
        json={
            "trading_date": "2026-03-10",
            "start_date": "2026-03-11",
            "end_date": "2026-03-12",
        },
    )

    assert response.status_code == 422
    assert "Provide either trading_date or start_date/end_date" in response.text


def test_trigger_signal_compute_rejects_end_date_without_start_date():
    client = _build_test_client()

    response = client.post(
        "/api/v1/signals/compute",
        json={"end_date": "2026-03-12"},
    )

    assert response.status_code == 422
    assert "start_date is required" in response.text


def test_trigger_signal_compute_rejects_market_closed_only_range():
    client = _build_test_client()

    with patch(
        "services.signal_service.app.routes.today_trading",
        return_value=date(2026, 3, 20),
    ):
        response = client.post(
            "/api/v1/signals/compute",
            json={"start_date": "2026-03-14", "end_date": "2026-03-15"},
        )

    assert response.status_code == 422
    assert "No market days found" in response.json()["detail"]