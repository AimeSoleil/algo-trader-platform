from __future__ import annotations

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

    async def fake_query_blueprint(trading_date_str: str, by_pass_cache: bool = False) -> dict:
        seen["by_date"] = trading_date_str
        return {"error": "wrong route"}

    monkeypatch.setattr(queries, "query_blueprint_by_id", fake_query_blueprint_by_id)
    monkeypatch.setattr(queries, "query_blueprint", fake_query_blueprint)

    response = client.get(f"/api/v1/analysis/blueprint/{blueprint_id}")

    assert response.status_code == 200
    assert response.json()["id"] == blueprint_id
    assert seen["by_id"] == blueprint_id
    assert seen["by_date"] is None


def test_get_blueprint_by_date_still_uses_date_route(monkeypatch):
    seen: dict[str, str | None] = {"by_id": None, "by_date": None}

    async def fake_query_blueprint_by_id(requested_id: str) -> dict:
        seen["by_id"] = requested_id
        return {"error": "wrong route"}

    async def fake_query_blueprint(trading_date_str: str, by_pass_cache: bool = False) -> dict:
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