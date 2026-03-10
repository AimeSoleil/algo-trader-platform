"""Signal Service — 查询层"""
from __future__ import annotations

from datetime import date

from sqlalchemy import text

from shared.db.session import get_postgres_session
from shared.utils import get_logger

logger = get_logger("signal_queries")


async def query_signal_features(symbol: str, date_str: str | None = None) -> dict:
    """从 DB 查询信号特征"""
    target_date = date.fromisoformat(date_str) if date_str else date.today()

    async with get_postgres_session() as session:
        result = await session.execute(
            text(
                "SELECT features_json FROM signal_features "
                "WHERE symbol = :symbol AND date = :date"
            ),
            {"symbol": symbol.upper(), "date": target_date},
        )
        row = result.fetchone()
        if row:
            return row[0]
        return {"error": f"No signals for {symbol} on {target_date}"}
