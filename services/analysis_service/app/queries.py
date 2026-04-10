"""Analysis Service — 查询层（Redis L1 缓存 + DB 查询）"""
from __future__ import annotations

from datetime import date

from sqlalchemy import text

from shared.db.session import get_postgres_session
from shared.utils import get_logger

from services.analysis_service.app.cache import (
    get_cached_blueprint,
    set_cached_blueprint,
)

logger = get_logger("analysis_queries")


async def query_blueprint(
    trading_date_str: str,
    by_pass_cache: bool = False,
) -> dict:
    """从 Redis / DB 查询蓝图"""
    td = date.fromisoformat(trading_date_str)

    # L1: Redis cache
    if not by_pass_cache:
        cached = await get_cached_blueprint(td)
        if cached:
            return {**cached, "_from_cache": True}

    # L2: Postgres
    async with get_postgres_session() as session:
        result = await session.execute(
            text(
                "SELECT id, trading_date, status, blueprint_json, execution_summary "
                "FROM llm_trading_blueprint WHERE trading_date = :date"
            ),
            {"date": td},
        )
        row = result.fetchone()

    if not row:
        return {"error": f"No blueprint for {td}", "_from_cache": False}

    data = {
        "id": row[0],
        "trading_date": str(row[1]),
        "status": row[2],
        "blueprint": row[3],
        "execution_summary": row[4],
    }

    # Populate cache
    await set_cached_blueprint(td, data)
    return {**data, "_from_cache": False}


async def query_reasoning(
    blueprint_id: str,
    symbol_filter: set[str] | None = None,
) -> dict:
    """从 DB 查询蓝图的 LLM 推理上下文，可选按 symbol 过滤"""
    async with get_postgres_session() as session:
        result = await session.execute(
            text(
                "SELECT id, trading_date, model_provider, model_version, "
                "       generated_at, reasoning_json "
                "FROM llm_trading_blueprint WHERE id = :id"
            ),
            {"id": blueprint_id},
        )
        row = result.fetchone()

    if not row:
        return {"error": f"No blueprint found with id '{blueprint_id}'"}

    reasoning = row[5]
    if reasoning is None:
        return {
            "blueprint_id": row[0],
            "trading_date": str(row[1]),
            "model_provider": row[2],
            "model_version": row[3],
            "generated_at": str(row[4]),
            "reasoning": None,
            "_note": "No reasoning context was stored for this blueprint. "
                     "Reasoning is captured for blueprints generated after this feature was added.",
        }

    # reasoning_json may be str or dict depending on driver
    if isinstance(reasoning, str):
        import json
        reasoning = json.loads(reasoning)

    # Apply symbol filter if requested
    if symbol_filter and isinstance(reasoning, dict):
        reasoning = _filter_reasoning_by_symbols(reasoning, symbol_filter)

    resp: dict = {
        "blueprint_id": row[0],
        "trading_date": str(row[1]),
        "model_provider": row[2],
        "model_version": row[3],
        "generated_at": str(row[4]),
        "reasoning": reasoning,
    }
    if symbol_filter:
        resp["_symbol_filter"] = sorted(symbol_filter)
    return resp


def _filter_reasoning_by_symbols(reasoning: dict, symbols: set[str]) -> dict:
    """Filter reasoning context to only include data for the requested symbols."""
    filtered = dict(reasoning)

    # Filter signals_summary
    if "signals_summary" in filtered and isinstance(filtered["signals_summary"], list):
        filtered["signals_summary"] = [
            s for s in filtered["signals_summary"]
            if s.get("symbol", "").upper() in symbols
        ]

    # Filter agent_outputs — each agent stores a dict with a "symbols" list
    if "agent_outputs" in filtered and isinstance(filtered["agent_outputs"], dict):
        filtered_outputs = {}
        for agent_name, output in filtered["agent_outputs"].items():
            if not isinstance(output, dict):
                filtered_outputs[agent_name] = output
                continue
            agent_filtered = dict(output)
            # Most agents have a "symbols" key with per-symbol analysis list
            if "symbols" in agent_filtered and isinstance(agent_filtered["symbols"], list):
                agent_filtered["symbols"] = [
                    s for s in agent_filtered["symbols"]
                    if s.get("symbol", "").upper() in symbols
                ]
            filtered_outputs[agent_name] = agent_filtered
        filtered["agent_outputs"] = filtered_outputs

    # Filter chunked contexts if present
    if "chunk_contexts" in filtered and isinstance(filtered["chunk_contexts"], list):
        filtered["chunk_contexts"] = [
            _filter_reasoning_by_symbols(ctx, symbols)
            for ctx in filtered["chunk_contexts"]
            if isinstance(ctx, dict)
        ]

    return filtered
