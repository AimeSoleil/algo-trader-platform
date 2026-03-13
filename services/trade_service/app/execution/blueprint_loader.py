from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import text

from shared.db.session import get_postgres_session
from shared.utils import get_logger, now_utc

logger = get_logger("execution_blueprint_loader")


async def load_blueprint_for_date(trading_date: date) -> dict[str, Any] | None:
    logger.debug(
        "blueprint_loader.load_started",
        event="load_blueprint",
        stage="query",
        trading_date=str(trading_date),
    )
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
            logger.debug(
                "blueprint_loader.load_not_found",
                event="load_blueprint",
                stage="query_result",
                trading_date=str(trading_date),
            )
            return None

        logger.debug(
            "blueprint_loader.activate_started",
            event="activate_blueprint",
            stage="db_write",
            trading_date=str(trading_date),
            blueprint_id=row["id"],
        )
        await session.execute(
            text(
                """
                UPDATE llm_trading_blueprint
                SET status = 'active', updated_at = :updated_at
                WHERE id = :id
                """
            ),
            {"id": row["id"], "updated_at": now_utc()},
        )
        logger.debug(
            "blueprint_loader.load_completed",
            event="load_blueprint",
            stage="completed",
            trading_date=str(trading_date),
            blueprint_id=row["id"],
            status="active",
        )

        return {
            "id": row["id"],
            "trading_date": row["trading_date"],
            "status": "active",
            "blueprint_json": row["blueprint_json"],
        }


async def complete_blueprint(trading_date: date, execution_summary: dict[str, Any]) -> int:
    logger.debug(
        "blueprint_loader.complete_started",
        event="complete_blueprint",
        stage="db_write",
        trading_date=str(trading_date),
        has_execution_summary=bool(execution_summary),
    )
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
                "updated_at": now_utc(),
            },
        )
        updated_rows = result.rowcount or 0
        logger.debug(
            "blueprint_loader.complete_finished",
            event="complete_blueprint",
            stage="completed",
            trading_date=str(trading_date),
            rows=updated_rows,
        )
        return updated_rows
