from __future__ import annotations

import asyncio
from datetime import date

from shared.celery_app import celery_app
from shared.utils import get_logger

from services.execution_service.app.blueprint_loader import complete_blueprint, load_blueprint_for_date

logger = get_logger("execution_tasks")


@celery_app.task(name="execution_service.tasks.load_daily_blueprint", bind=True, max_retries=2)
def load_daily_blueprint(self, trading_date: str) -> dict:
    return asyncio.run(_load_daily_blueprint_async(trading_date))


async def _load_daily_blueprint_async(trading_date: str) -> dict:
    td = date.fromisoformat(trading_date)
    blueprint = await load_blueprint_for_date(td)
    if not blueprint:
        logger.warning("execution_task.load_missing", trading_date=trading_date)
        return {"status": "missing", "trading_date": trading_date}

    logger.info("execution_task.load_done", trading_date=trading_date, blueprint_id=blueprint["id"])
    return {"status": "loaded", "trading_date": trading_date, "blueprint_id": blueprint["id"]}


@celery_app.task(name="execution_service.tasks.finalize_daily_blueprint", bind=True, max_retries=2)
def finalize_daily_blueprint(self, trading_date: str) -> dict:
    return asyncio.run(_finalize_daily_blueprint_async(trading_date))


async def _finalize_daily_blueprint_async(trading_date: str) -> dict:
    td = date.fromisoformat(trading_date)
    summary = {"result": "finalized", "trading_date": trading_date}
    updated = await complete_blueprint(td, summary)
    logger.info("execution_task.finalize_done", trading_date=trading_date, updated_rows=updated)
    return {"status": "completed", "trading_date": trading_date, "updated_rows": updated}
