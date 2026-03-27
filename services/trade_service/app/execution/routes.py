from __future__ import annotations

import json
from datetime import date
from typing import Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text

from shared.db.session import get_postgres_session
from shared.utils import now_utc

from services.trade_service.app.execution.blueprint_loader import load_blueprint_for_date
from services.trade_service.app.models import runtime_state

router = APIRouter(tags=["execution"])


class ManualOverrideRequest(BaseModel):
    action: Literal["pause", "resume"]
    reason: str | None = None


@router.get("/trade/blueprint/status")
async def blueprint_status(
    trading_date: date = Query(..., description="Target trading_date (YYYY-MM-DD)"),
):
    async with get_postgres_session() as session:
        result = await session.execute(
            text(
                """
                SELECT id, trading_date, status, updated_at, blueprint_json
                FROM llm_trading_blueprint
                WHERE trading_date = :trading_date
                ORDER BY generated_at DESC
                LIMIT 1
                """
            ),
            {"trading_date": trading_date},
        )
        row = result.mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="blueprint_not_found")

    response = {
        "id": row["id"],
        "trading_date": str(row["trading_date"]),
        "db_status": row["status"],
        "updated_at": row["updated_at"],
        "runtime": {
            "loaded_blueprint_id": runtime_state.loaded_blueprint_id,
            "loaded_trading_date": str(runtime_state.loaded_trading_date) if runtime_state.loaded_trading_date else None,
            "status": runtime_state.status,
            "paused": runtime_state.paused,
            "manual_override_reason": runtime_state.manual_override_reason,
            "loaded_at": runtime_state.loaded_at,
            "last_tick_at": runtime_state.last_tick_at,
            "last_risk_check_at": runtime_state.last_risk_check_at,
            "stoploss_events_count": len(runtime_state.stoploss_last_events),
        },
    }

    # Add data quality summary if blueprint data is available
    blueprint_data = row["blueprint_json"]
    if blueprint_data:
        bp = blueprint_data
        if isinstance(bp, str):
            try:
                bp = json.loads(bp)
            except Exception:
                bp = {}

        quality_info = {
            "min_score": bp.get("min_data_quality_score", 1.0),
            "warnings": bp.get("data_quality_summary", []),
            "per_symbol": {},
        }
        for sp in bp.get("symbol_plans", []):
            sym = sp.get("underlying", "unknown")
            quality_info["per_symbol"][sym] = {
                "score": sp.get("data_quality_score", 1.0),
                "warnings": sp.get("data_quality_warnings", []),
            }
        response["data_quality"] = quality_info

    return response


@router.post("/trade/blueprint/load")
async def load_blueprint(
    trading_date: date = Query(..., description="Target trading_date (YYYY-MM-DD)"),
):
    blueprint = await load_blueprint_for_date(trading_date)
    if not blueprint:
        raise HTTPException(status_code=404, detail="blueprint_not_found_or_not_pending")

    runtime_state.loaded_blueprint_id = str(blueprint["id"])
    runtime_state.loaded_trading_date = trading_date
    runtime_state.loaded_blueprint_json = blueprint.get("blueprint_json")
    runtime_state.status = "active"
    runtime_state.loaded_at = now_utc()
    runtime_state.manual_override_reason = None

    return {
        "status": "loaded",
        "blueprint_id": runtime_state.loaded_blueprint_id,
        "trading_date": str(trading_date),
    }


@router.post("/trade/override")
async def manual_override(payload: ManualOverrideRequest):
    runtime_state.paused = payload.action == "pause"
    runtime_state.manual_override_reason = payload.reason
    runtime_state.status = "paused" if runtime_state.paused else "active"

    return {
        "status": runtime_state.status,
        "paused": runtime_state.paused,
        "reason": runtime_state.manual_override_reason,
    }
