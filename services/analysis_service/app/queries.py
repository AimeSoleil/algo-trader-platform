"""Analysis Service — 查询层"""
from __future__ import annotations

from datetime import date

from sqlalchemy import text

from shared.db.session import get_postgres_session
from shared.utils import get_logger

logger = get_logger("analysis_queries")


async def query_blueprint(trading_date_str: str) -> dict:
    """从 DB 查询蓝图"""
    td = date.fromisoformat(trading_date_str)

    async with get_postgres_session() as session:
        result = await session.execute(
            text(
                "SELECT id, trading_date, status, blueprint_json, execution_summary "
                "FROM llm_trading_blueprint WHERE trading_date = :date"
            ),
            {"date": td},
        )
        row = result.fetchone()
        if row:
            return {
                "id": row[0],
                "trading_date": str(row[1]),
                "status": row[2],
                "blueprint": row[3],
                "execution_summary": row[4],
            }
        return {"error": f"No blueprint for {td}"}
