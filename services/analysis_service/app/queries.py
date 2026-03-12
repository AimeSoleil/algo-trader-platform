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
