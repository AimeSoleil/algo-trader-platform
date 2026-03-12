"""Portfolio Service — Celery 报告任务"""
from __future__ import annotations

import asyncio
from datetime import date

from shared.celery_app import celery_app
from shared.utils import get_logger, today_trading

from services.portfolio_service.app.service import get_portfolio_snapshot, get_performance

logger = get_logger("portfolio_tasks")


@celery_app.task(name="portfolio_service.tasks.generate_daily_report", bind=True, max_retries=2)
def generate_daily_report(self, trading_date: str | None = None) -> dict:
    return asyncio.run(_generate_daily_report_async(trading_date))


async def _generate_daily_report_async(trading_date: str | None) -> dict:
    target_date = date.fromisoformat(trading_date) if trading_date else today_trading()
    snapshot = await get_portfolio_snapshot()
    performance = await get_performance(target_date)

    report = {
        "trading_date": target_date.isoformat(),
        "snapshot": snapshot,
        "performance": performance,
    }
    logger.info("portfolio.daily_report.generated", trading_date=target_date.isoformat())
    return report
