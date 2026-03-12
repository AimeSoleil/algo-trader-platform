from __future__ import annotations

from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from redis.asyncio import Redis

from shared.config import get_settings
from shared.utils import get_logger, now_utc

from services.execution_service.app.models import ExecutionRuntimeState
from services.execution_service.app.rule_engine import BlueprintRuleEngine

logger = get_logger("execution_scheduler")

_scheduler: AsyncIOScheduler | None = None


async def _evaluation_tick(runtime_state: ExecutionRuntimeState) -> None:
    if runtime_state.paused:
        runtime_state.last_tick_at = now_utc()
        logger.info("execution.tick_skipped", reason="paused")
        return

    settings = get_settings()
    redis_client = Redis.from_url(settings.redis.url, decode_responses=True)
    engine = BlueprintRuleEngine()

    try:
        for symbol in settings.watchlist:
            quote_key = f"market:quote:{symbol}"
            quote = await redis_client.hgetall(quote_key)
            if not quote:
                continue

            market_ctx = {
                "price": float(quote.get("price", 0) or 0),
                "bid": float(quote.get("bid", 0) or 0),
                "ask": float(quote.get("ask", 0) or 0),
            }

            plan = {"symbol": symbol, "entry_conditions": [], "exit_conditions": []}
            engine.evaluate_symbol_plan(plan, market_ctx)

        runtime_state.last_tick_at = now_utc()
        logger.info("execution.tick_completed", trading_date=str(runtime_state.loaded_trading_date))
    finally:
        await redis_client.aclose()


def start_execution_scheduler(runtime_state: ExecutionRuntimeState) -> None:
    global _scheduler
    settings = get_settings()
    _scheduler = AsyncIOScheduler(timezone=settings.trading.timezone)
    _scheduler.add_job(
        _evaluation_tick,
        trigger=IntervalTrigger(seconds=settings.trading.execution_interval),
        args=[runtime_state],
        id="execution_tick",
        name="蓝图规则执行",
        replace_existing=True,
    )
    _scheduler.start()
    logger.info("execution.scheduler_started", interval=settings.trading.execution_interval)


def stop_execution_scheduler() -> None:
    global _scheduler
    if _scheduler:
        _scheduler.shutdown(wait=False)
        logger.info("execution.scheduler_stopped")
