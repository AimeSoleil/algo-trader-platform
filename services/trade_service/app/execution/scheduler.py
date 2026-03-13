"""Execution scheduler — thin orchestrator for tick loop and lifecycle.

Heavy lifting is delegated to:
- ``risk_monitor``  — stop-loss checks & order execution
- ``position_collector`` — broker / portfolio position fetching
- ``risk_engine`` — pure evaluation functions
- ``broker`` package — broker abstraction & factory
"""
from __future__ import annotations

from time import perf_counter

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from redis.asyncio import Redis

from shared.config import get_settings
from shared.utils import get_logger, now_utc

from services.trade_service.app.broker import create_broker
from services.trade_service.app.broker.base import BrokerInterface
from services.trade_service.app.broker.paper import PaperBroker
from services.trade_service.app.execution.risk.risk_monitor import run_stop_loss_checks
from services.trade_service.app.execution.rule_engine import BlueprintRuleEngine
from services.trade_service.app.helpers import safe_float
from services.trade_service.app.models import ExecutionRuntimeState

logger = get_logger("execution_scheduler")

_scheduler: AsyncIOScheduler | None = None
_broker: BrokerInterface | None = None


def _get_broker() -> BrokerInterface:
    global _broker
    if _broker is None:
        _broker = create_broker()
    return _broker


async def _evaluation_tick(runtime_state: ExecutionRuntimeState) -> None:
    tick_started = perf_counter()
    settings = get_settings()
    broker = _get_broker()

    logger.debug(
        "execution.tick_started",
        log_event="tick_start",
        stage="scheduler",
        paused=runtime_state.paused,
        trading_date=str(runtime_state.loaded_trading_date),
        symbols=len(settings.watchlist),
    )
    if runtime_state.paused:
        runtime_state.last_tick_at = now_utc()
        logger.info("execution.tick_skipped", reason="paused")
        return

    redis_client = Redis.from_url(settings.redis.url, decode_responses=True)
    engine = BlueprintRuleEngine()
    quotes_found = 0
    symbols_evaluated = 0

    try:
        await run_stop_loss_checks(
            runtime_state=runtime_state,
            redis_client=redis_client,
            broker=broker,
        )

        for symbol in settings.watchlist:
            quote_key = f"market:quote:{symbol}"
            quote = await redis_client.hgetall(quote_key)
            if not quote:
                continue

            quote_price = safe_float(quote.get("price"), 0.0)

            if isinstance(broker, PaperBroker) and quote_price > 0:
                broker.update_price(symbol, quote_price)

            quotes_found += 1

            market_ctx = {
                "price": quote_price,
                "bid": float(quote.get("bid", 0) or 0),
                "ask": float(quote.get("ask", 0) or 0),
            }

            plan = {"symbol": symbol, "entry_conditions": [], "exit_conditions": []}
            engine.evaluate_symbol_plan(plan, market_ctx)
            symbols_evaluated += 1

        runtime_state.last_tick_at = now_utc()
        logger.debug(
            "execution.tick_context",
            log_event="tick_context",
            stage="evaluation",
            trading_date=str(runtime_state.loaded_trading_date),
            symbols_total=len(settings.watchlist),
            quotes_found=quotes_found,
            symbols_evaluated=symbols_evaluated,
            duration_ms=round((perf_counter() - tick_started) * 1000, 2),
        )
        logger.info("execution.tick_completed", trading_date=str(runtime_state.loaded_trading_date))
    except Exception as exc:
        logger.error(
            "execution.tick_failed",
            log_event="tick_failed",
            stage="evaluation",
            trading_date=str(runtime_state.loaded_trading_date),
            error=str(exc),
            duration_ms=round((perf_counter() - tick_started) * 1000, 2),
        )
        raise
    finally:
        await redis_client.aclose()


async def _startup_broker() -> None:
    global _broker
    _broker = create_broker()
    await _broker.connect()
    logger.info("execution.broker_connected", broker_type=type(_broker).__name__)


async def _shutdown_broker() -> None:
    global _broker
    if _broker is not None:
        await _broker.disconnect()
        logger.info("execution.broker_disconnected")
        _broker = None


def start_execution_scheduler(runtime_state: ExecutionRuntimeState) -> None:
    global _scheduler
    settings = get_settings()
    logger.debug(
        "execution.scheduler_starting",
        log_event="scheduler_start",
        stage="startup",
        interval_seconds=settings.trading.execution_interval,
        timezone=settings.trading.timezone,
        symbols=len(settings.watchlist),
        trading_date=str(runtime_state.loaded_trading_date),
    )
    _scheduler = AsyncIOScheduler(timezone=settings.trading.timezone)
    _scheduler.add_job(
        _evaluation_tick,
        trigger=IntervalTrigger(seconds=settings.trading.execution_interval),
        args=[runtime_state],
        id="execution_tick",
        name="blueprint_execution_tick",
        replace_existing=True,
    )
    _scheduler.start()
    logger.info("execution.scheduler_started", interval=settings.trading.execution_interval)


def stop_execution_scheduler() -> None:
    global _scheduler
    if _scheduler:
        _scheduler.shutdown(wait=False)
        logger.info("execution.scheduler_stopped")
    else:
        logger.debug("execution.scheduler_stop_skipped", log_event="scheduler_stop", reason="not_started")
