from __future__ import annotations

from types import SimpleNamespace

from fastapi.testclient import TestClient

import services.analysis_service.app.queries as queries
from services.analysis_service.app.main import app


client = TestClient(app)


def test_get_blueprint_by_id_uses_non_guid_id(monkeypatch):
    blueprint_id = "manual-09dc21fa"
    seen: dict[str, str | None] = {"by_id": None, "by_date": None}

    async def fake_query_blueprint_by_id(requested_id: str) -> dict:
        seen["by_id"] = requested_id
        return {
            "id": requested_id,
            "trading_date": "2026-03-24",
            "status": "pending",
            "blueprint": {"symbol_plans": []},
            "execution_summary": None,
            "_from_cache": False,
        }

    async def fake_query_blueprint(trading_date_str: str, bypass_cache: bool = False) -> dict:
        seen["by_date"] = trading_date_str
        return {"error": "wrong route"}

    monkeypatch.setattr(queries, "query_blueprint_by_id", fake_query_blueprint_by_id)
    monkeypatch.setattr(queries, "query_blueprint", fake_query_blueprint)

    response = client.get(f"/api/v1/analysis/blueprint/{blueprint_id}")

    assert response.status_code == 200
    assert response.json()["id"] == blueprint_id
    assert seen["by_id"] == blueprint_id
    assert seen["by_date"] is None


def test_get_blueprint_by_id_explicit_alias_route(monkeypatch):
    blueprint_id = "manual-09dc21fa"
    seen: dict[str, str | None] = {"by_id": None}

    async def fake_query_blueprint_by_id(requested_id: str) -> dict:
        seen["by_id"] = requested_id
        return {
            "id": requested_id,
            "trading_date": "2026-03-24",
            "status": "manual",
            "blueprint": {"symbol_plans": []},
            "execution_summary": None,
            "_from_cache": False,
        }

    monkeypatch.setattr(queries, "query_blueprint_by_id", fake_query_blueprint_by_id)

    response = client.get(f"/api/v1/analysis/blueprint/by-id/{blueprint_id}")

    assert response.status_code == 200
    assert response.json()["id"] == blueprint_id
    assert seen["by_id"] == blueprint_id


def test_get_blueprint_by_date_still_uses_date_route(monkeypatch):
    seen: dict[str, str | None] = {"by_id": None, "by_date": None}

    async def fake_query_blueprint_by_id(requested_id: str) -> dict:
        seen["by_id"] = requested_id
        return {"error": "wrong route"}

    async def fake_query_blueprint(trading_date_str: str, bypass_cache: bool = False) -> dict:
        seen["by_date"] = trading_date_str
        return {
            "id": "bp-123",
            "trading_date": trading_date_str,
            "status": "pending",
            "blueprint": {"symbol_plans": []},
            "execution_summary": None,
            "_from_cache": False,
        }

    monkeypatch.setattr(queries, "query_blueprint_by_id", fake_query_blueprint_by_id)
    monkeypatch.setattr(queries, "query_blueprint", fake_query_blueprint)

    response = client.get("/api/v1/analysis/blueprint/2026-03-24")

    assert response.status_code == 200
    assert response.json()["trading_date"] == "2026-03-24"
    assert seen["by_date"] == "2026-03-24"
    assert seen["by_id"] is None


def test_get_blueprint_by_id_filters_symbols(monkeypatch):
    blueprint_id = "manual-09dc21fa"

    async def fake_query_blueprint_by_id(requested_id: str) -> dict:
        return {
            "id": requested_id,
            "trading_date": "2026-03-24",
            "status": "pending",
            "blueprint": {
                "symbol_plans": [
                    {"underlying": "AAPL", "strategy_type": "single_leg"},
                    {"underlying": "MSFT", "strategy_type": "single_leg"},
                ],
            },
            "execution_summary": None,
            "_from_cache": False,
        }

    monkeypatch.setattr(queries, "query_blueprint_by_id", fake_query_blueprint_by_id)

    response = client.get(
        f"/api/v1/analysis/blueprint/{blueprint_id}",
        params={"symbols": "AAPL"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["_symbol_filter"] == ["AAPL"]
    assert body["blueprint"]["symbol_plans"] == [
        {"underlying": "AAPL", "strategy_type": "single_leg"},
    ]


def test_get_blueprint_by_id_returns_404(monkeypatch):
    blueprint_id = "manual-09dc21fa"

    async def fake_query_blueprint_by_id(requested_id: str) -> dict:
        return {"error": f"No blueprint found with id '{requested_id}'", "_from_cache": False}

    monkeypatch.setattr(queries, "query_blueprint_by_id", fake_query_blueprint_by_id)

    response = client.get(f"/api/v1/analysis/blueprint/{blueprint_id}")

    assert response.status_code == 404
    assert response.json() == {"detail": f"No blueprint found with id '{blueprint_id}'"}


def test_trigger_manual_analysis_with_provider_override(monkeypatch):
    captured: dict[str, object] = {}

    def fake_send_task(name: str, args: list[object], queue: str):
        captured["name"] = name
        captured["args"] = args
        captured["queue"] = queue
        return SimpleNamespace(id="task-123")

    monkeypatch.setattr("services.analysis_service.app.routes.celery_app.send_task", fake_send_task)

    response = client.post(
        "/api/v1/analysis",
        json={
            "symbols": ["aapl", "msft"],
            "signal_date": "2026-03-24",
            "llm_provider": "openai",
        },
    )

    assert response.status_code == 202
    assert captured["name"] == "analysis_service.tasks.manual_analyze"
    assert captured["args"] == [["AAPL", "MSFT"], "2026-03-24", "openai"]
    assert captured["queue"] == "analysis"
    assert response.json()["task_id"] == "task-123"
    assert "llm_provider=openai" in response.json()["message"]


def test_trigger_manual_analysis_rejects_unknown_provider():
    response = client.post(
        "/api/v1/analysis",
        json={
            "symbols": ["AAPL"],
            "llm_provider": "unknown_provider",
        },
    )

    assert response.status_code == 422