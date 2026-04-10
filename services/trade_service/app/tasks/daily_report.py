"""Daily trading report task — summarises the day's execution events and sends a notification."""
from __future__ import annotations

from datetime import date

from sqlalchemy import text

from shared.async_bridge import run_async
from shared.celery_app import celery_app
from shared.db.session import get_postgres_session
from shared.notifier.base import EventType, NotificationEvent, Severity
from shared.notifier.helpers import notify_sync
from shared.utils import get_logger, today_trading

logger = get_logger("daily_report")


@celery_app.task(name="trade_service.tasks.send_daily_report", queue="data")
def send_daily_report(trading_date: str | None = None) -> dict:
    """Build and send a daily trading summary notification.

    Scheduled via Celery Beat at ``common.notifier.daily_report_time``.
    Queries the ``execution_events`` table for today's activity and
    dispatches a DAILY_REPORT notification.
    """
    td = trading_date or today_trading().isoformat()
    logger.info("daily_report.start", trading_date=td)

    try:
        summary = run_async(_build_report(td))
        notify_sync(NotificationEvent(
            event_type=EventType.DAILY_REPORT,
            title=f"\ud83d\udcca Daily Trading Report — {td}",
            message=summary["message"],
            severity=Severity.INFO,
            payload=summary["payload"],
        ))
        logger.info("daily_report.sent", trading_date=td)
        return {"status": "sent", "trading_date": td}
    except Exception as exc:
        logger.warning("daily_report.failed", trading_date=td, error=str(exc))
        return {"status": "error", "trading_date": td, "error": str(exc)}


async def _build_report(trading_date_str: str) -> dict:
    """Query execution_events and build a human-readable summary."""
    td = date.fromisoformat(trading_date_str)

    counts: dict[str, int] = {}
    async with get_postgres_session() as session:
        result = await session.execute(
            text(
                "SELECT event_type, COUNT(*) "
                "FROM execution_events "
                "WHERE created_at::date = :date "
                "GROUP BY event_type"
            ),
            {"date": td},
        )
        for row in result.fetchall():
            counts[row[0]] = row[1]

    entries = counts.get("entry_order_placed", 0)
    exits = counts.get("exit_order_placed", 0)
    stop_losses = counts.get("stoploss_triggered", 0)
    rejections = counts.get("order_rejected", 0) + counts.get("entry_order_rejected", 0)

    total_orders = entries + exits
    lines = [
        f"Date: {td}",
        f"Entry orders: {entries}",
        f"Exit orders: {exits}",
        f"Stop-loss triggers: {stop_losses}",
        f"Rejections: {rejections}",
        f"Total orders: {total_orders}",
    ]

    if not total_orders and not stop_losses:
        lines.append("\nNo trading activity today.")

    return {
        "message": "\n".join(lines),
        "payload": {
            "trading_date": trading_date_str,
            "entries": str(entries),
            "exits": str(exits),
            "stop_losses": str(stop_losses),
            "rejections": str(rejections),
            "total_orders": str(total_orders),
        },
    }
