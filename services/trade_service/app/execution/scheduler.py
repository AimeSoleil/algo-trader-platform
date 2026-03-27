"""Execution scheduler — thin orchestrator for tick loop and lifecycle.

Heavy lifting is delegated to:
- ``risk_monitor``  — stop-loss checks & order execution
- ``position_collector`` — broker / portfolio position fetching
- ``risk_engine`` — pure evaluation functions
- ``broker`` package — broker abstraction & factory
"""
from __future__ import annotations

from time import perf_counter
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from shared.config import get_settings
from shared.data_quality import DataQualityConfig, apply_quality_gate
from shared.distributed_lock import distributed_once
from shared.redis_pool import get_redis
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


def _apply_quality_gate_to_plan(
    plan: dict[str, Any],
    runtime_state: ExecutionRuntimeState,
    symbol: str,
) -> dict[str, Any] | None:
    """Apply data quality gate to a symbol plan.

    Returns the (possibly modified) plan, or ``None`` if the plan should
    be skipped due to very low data quality.

    Thresholds are loaded from ``config.yaml → data_quality`` section:
    - score < skip_threshold   → skip entirely
    - score < reduce_threshold → reduce position by reduce_factor
    """
    # ── 从 blueprint JSON 中提取该标的的质量评分 ──
    bp_json = getattr(runtime_state, "loaded_blueprint_json", None)
    if not bp_json:
        return plan

    symbol_plans = bp_json.get("symbol_plans", []) if isinstance(bp_json, dict) else []
    quality_score = 1.0
    quality_warnings: list[str] = []
    for sp in symbol_plans:
        if sp.get("underlying", "").upper() == symbol.upper():
            quality_score = sp.get("data_quality_score", 1.0)
            quality_warnings = sp.get("data_quality_warnings", [])
            break

    # ── 使用可配置的门控逻辑 ──
    settings = get_settings()
    dq_cfg = DataQualityConfig.from_settings(settings)
    original_size = plan.get("max_position_size", 1)
    should_skip, adjusted_size = apply_quality_gate(
        quality_score, original_size, cfg=dq_cfg,
    )

    if should_skip:
        logger.error(
            "execution.plan_skipped_very_low_data_quality",
            symbol=symbol,
            data_quality_score=quality_score,
            skip_threshold=dq_cfg.skip_threshold,
            warnings=quality_warnings,
        )
        return None

    if adjusted_size != original_size:
        plan["max_position_size"] = adjusted_size
        logger.warning(
            "execution.position_reduced_low_data_quality",
            symbol=symbol,
            data_quality_score=quality_score,
            reduce_threshold=dq_cfg.reduce_threshold,
            original_size=original_size,
            reduced_size=adjusted_size,
            warnings=quality_warnings,
        )

    return plan


@distributed_once("trade:execution_tick", ttl=270, service="trade_service")
async def _evaluation_tick(runtime_state: ExecutionRuntimeState) -> None:
    """Main execution tick — protected by distributed lock.

    When multiple trade_service replicas run, only the instance that
    acquires the Redis lock will execute the tick; others skip silently.
    """
    tick_started = perf_counter()
    settings = get_settings()
    broker = _get_broker()

    logger.debug(
        "execution.tick_started",
        log_event="tick_start",
        stage="scheduler",
        paused=runtime_state.paused,
        trading_date=str(runtime_state.loaded_trading_date),
        symbols=len(settings.common.watchlist),
    )
    if runtime_state.paused:
        runtime_state.last_tick_at = now_utc()
        logger.info("execution.tick_skipped", reason="paused")
        return

    redis_client = get_redis()
    engine = BlueprintRuleEngine()
    quotes_found = 0
    symbols_evaluated = 0

    try:
        await run_stop_loss_checks(
            runtime_state=runtime_state,
            redis_client=redis_client,
            broker=broker,
        )

        for symbol in settings.common.watchlist:
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
            plan = _apply_quality_gate_to_plan(plan, runtime_state, symbol)
            if plan is None:
                continue  # skip due to very low data quality
            engine.evaluate_symbol_plan(plan, market_ctx)
            symbols_evaluated += 1

        runtime_state.last_tick_at = now_utc()
        logger.debug(
            "execution.tick_context",
            log_event="tick_context",
            stage="evaluation",
            trading_date=str(runtime_state.loaded_trading_date),
            symbols_total=len(settings.common.watchlist),
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
        interval_seconds=settings.trade_service.execution_interval,
        timezone=settings.common.timezone,
        symbols=len(settings.common.watchlist),
        trading_date=str(runtime_state.loaded_trading_date),
    )
    _scheduler = AsyncIOScheduler(timezone=settings.common.timezone)
    _scheduler.add_job(
        _evaluation_tick,
        trigger=IntervalTrigger(seconds=settings.trade_service.execution_interval),
        args=[runtime_state],
        id="execution_tick",
        name="blueprint_execution_tick",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )
    _scheduler.start()
    logger.info("execution.scheduler_started", interval=settings.trade_service.execution_interval)


def stop_execution_scheduler() -> None:
    global _scheduler
    if _scheduler:
        _scheduler.shutdown(wait=False)
        logger.info("execution.scheduler_stopped")
    else:
        logger.debug("execution.scheduler_stop_skipped", log_event="scheduler_stop", reason="not_started")
