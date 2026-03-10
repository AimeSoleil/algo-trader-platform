from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import text

from shared.db.session import get_postgres_session


async def load_blueprint_for_date(trading_date: date) -> dict[str, Any] | None:
    async with get_postgres_session() as session:
        result = await session.execute(
            text(
                """
                SELECT id, trading_date, status, blueprint_json
                FROM llm_trading_blueprint
                WHERE trading_date = :trading_date
                  AND status IN ('pending', 'active')
                ORDER BY generated_at DESC
                LIMIT 1
                """
            ),
            {"trading_date": trading_date},
        )
        row = result.mappings().first()
        if not row:
            return None

        await session.execute(
            text(
                """
                UPDATE llm_trading_blueprint
                SET status = 'active', updated_at = :updated_at
                WHERE id = :id
                """
            ),
            {"id": row["id"], "updated_at": datetime.now(timezone.utc)},
        )

        return {
            "id": row["id"],
            "trading_date": row["trading_date"],
            "status": "active",
            "blueprint_json": row["blueprint_json"],
        }


async def complete_blueprint(trading_date: date, execution_summary: dict[str, Any]) -> int:
    async with get_postgres_session() as session:
        result = await session.execute(
            text(
                """
                UPDATE llm_trading_blueprint
                SET status = 'completed',
                    execution_summary = :execution_summary,
                    updated_at = :updated_at
                WHERE trading_date = :trading_date
                  AND status = 'active'
                """
            ),
            {
                "trading_date": trading_date,
                "execution_summary": execution_summary,
                "updated_at": datetime.now(timezone.utc),
            },
        )
        return result.rowcount or 0
