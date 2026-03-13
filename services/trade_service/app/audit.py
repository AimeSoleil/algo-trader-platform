"""Trade execution audit trail — persists key events to the ``execution_events`` table.

Design:
- Fire-and-forget: failures are logged but never block the trading path.
- Each call opens its own short-lived session (no shared state).
"""
from __future__ import annotations

from typing import Any
from uuid import uuid4

from shared.db.session import get_postgres_session
from shared.db.tables import ExecutionEventRecord
from shared.utils import get_logger

logger = get_logger("trade_audit")


async def log_event(
    event_type: str,
    *,
    symbol: str | None = None,
    payload: dict[str, Any] | None = None,
    order_id: str | None = None,
    blueprint_id: str | None = None,
) -> str | None:
    """Persist an execution event.  Returns the event id, or None on failure."""
    event_id = str(uuid4())
    try:
        async with get_postgres_session() as session:
            record = ExecutionEventRecord(
                id=event_id,
                event_type=event_type,
                symbol=symbol,
                blueprint_id=blueprint_id,
                order_id=order_id,
                payload=payload,
            )
            session.add(record)
        return event_id
    except Exception as exc:
        logger.warning(
            "audit.log_event_failed",
            event_type=event_type,
            symbol=symbol,
            error=str(exc),
        )
        return None
