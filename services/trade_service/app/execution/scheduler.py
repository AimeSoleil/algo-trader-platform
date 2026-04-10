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
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from shared.config import get_settings
from shared.data_quality import DataQualityConfig, apply_quality_gate
from shared.distributed_lock import distributed_once
from shared.redis_pool import get_redis
from shared.utils import get_logger, now_utc, now_market

from services.trade_service.app.audit import log_event
from services.trade_service.app.broker import create_broker
from services.trade_service.app.broker.base import BrokerInterface
from services.trade_service.app.broker.paper import PaperBroker
from services.trade_service.app.execution.risk.risk_monitor import run_stop_loss_checks
from services.trade_service.app.execution.rule_engine import BlueprintRuleEngine
from services.trade_service.app.helpers import safe_float
from services.trade_service.app.models import ExecutionRuntimeState

logger = get_logger("execution_scheduler")

# ── Minimum confidence to execute an entry ──
_ENTRY_CONFIDENCE_GATE = 0.3

# ── Entry cooldown: prevent re-entry within N seconds of a previous entry ──
_ENTRY_COOLDOWN_SECONDS = 300

# Track recent entries per symbol → timestamp
_entry_timestamps: dict[str, float] = {}

_scheduler: AsyncIOScheduler | None = None
_broker: BrokerInterface | None = None


def _get_broker() -> BrokerInterface:
    global _broker
    if _broker is None:
        _broker = create_broker()
    return _broker


async def _handle_entry_signal(
    *,
    symbol: str,
    plan: dict[str, Any],
    market_ctx: dict[str, Any],
    broker: BrokerInterface,
    runtime_state: ExecutionRuntimeState,
    tick_trades: list[dict[str, Any]],
    tick_errors: list[dict[str, Any]],
) -> None:
    """Place entry orders when rule engine decides 'enter'.

    Guards:
    - Confidence gate: skip if confidence < threshold.
    - Cooldown: skip if an entry was placed recently for this symbol.
    - Idempotency: key = entry:{symbol}:{trading_date}.
    """
    from time import time as _time

    confidence = plan.get("confidence", 0.0)
    if confidence < _ENTRY_CONFIDENCE_GATE:
        logger.info(
            "execution.entry_skipped_low_confidence",
            symbol=symbol,
            confidence=confidence,
            gate=_ENTRY_CONFIDENCE_GATE,
        )
        return

    # Cooldown check
    last_entry = _entry_timestamps.get(symbol, 0.0)
    if (_time() - last_entry) < _ENTRY_COOLDOWN_SECONDS:
        logger.debug("execution.entry_skipped_cooldown", symbol=symbol)
        return

    legs = plan.get("legs", [])
    if not legs:
        logger.warning("execution.entry_skipped_no_legs", symbol=symbol)
        return

    trading_date = runtime_state.loaded_trading_date
    idem_key = f"entry:{symbol}:{trading_date}"

    for i, leg in enumerate(legs):
        side = leg.get("side", "buy")
        qty = leg.get("quantity", 1)
        order_payload = {
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "type": "market",
            "reason": "blueprint_entry",
            "leg_index": i,
            "strike": leg.get("strike"),
            "expiry": str(leg.get("expiry", "")),
            "option_type": leg.get("option_type"),
        }
        leg_idem_key = f"{idem_key}:leg{i}"

        try:
            result = await broker.place_order(order_payload, idempotency_key=leg_idem_key)
            status = str(result.get("status", "")).lower()
            accepted = status in {"accepted", "submitted", "filled", "ok", "success"}

            if accepted:
                _entry_timestamps[symbol] = _time()
                logger.info(
                    "execution.entry_order_placed",
                    symbol=symbol,
                    leg_index=i,
                    side=side,
                    qty=qty,
                    status=status,
                )
                await log_event(
                    "entry_order_placed",
                    symbol=symbol,
                    order_id=str(result.get("id", "")),
                    blueprint_id=runtime_state.loaded_blueprint_id,
                    payload={
                        "leg_index": i,
                        "side": side,
                        "qty": qty,
                        "confidence": confidence,
                        "result": result,
                    },
                )
                tick_trades.append({"symbol": symbol, "action": "entry", "side": side, "qty": qty, "leg": i})
            else:
                logger.warning(
                    "execution.entry_order_rejected",
                    symbol=symbol,
                    leg_index=i,
                    result=result,
                )
        except Exception as exc:
            logger.error(
                "execution.entry_order_failed",
                symbol=symbol,
                leg_index=i,
                error=str(exc),
            )
            tick_errors.append({"symbol": symbol, "action": "entry", "leg": i, "error": str(exc)})


async def _handle_exit_signal(
    *,
    symbol: str,
    plan: dict[str, Any],
    market_ctx: dict[str, Any],
    broker: BrokerInterface,
    runtime_state: ExecutionRuntimeState,
    tick_trades: list[dict[str, Any]],
    tick_errors: list[dict[str, Any]],
) -> None:
    """Place exit orders when rule engine decides 'exit'.

    Closes all open positions for the symbol.
    Respects stop-loss cooldown to avoid conflicting with risk monitor.
    """
    from services.trade_service.app.execution.risk.risk_engine import (
        in_cooldown_redis,
    )

    # Skip if symbol is in stop-loss cooldown (risk_monitor owns the exit)
    if await in_cooldown_redis(symbol):
        logger.debug("execution.exit_skipped_stoploss_cooldown", symbol=symbol)
        return

    positions = await broker.get_positions()
    symbol_positions = [
        p for p in positions
        if str(p.get("symbol", "")).upper() == symbol.upper()
        and abs(p.get("qty", 0)) > 0
    ]

    if not symbol_positions:
        logger.debug("execution.exit_skipped_no_positions", symbol=symbol)
        return

    trading_date = runtime_state.loaded_trading_date
    idem_key = f"exit:{symbol}:{trading_date}"

    for i, pos in enumerate(symbol_positions):
        qty = abs(pos.get("qty", 0))
        if qty <= 0:
            continue
        current_side = "long" if pos.get("qty", 0) > 0 else "short"
        order_side = "sell" if current_side == "long" else "buy"

        order_payload = {
            "symbol": symbol,
            "side": order_side,
            "qty": qty,
            "type": "market",
            "reason": "blueprint_exit",
        }
        pos_idem_key = f"{idem_key}:pos{i}"

        try:
            result = await broker.place_order(order_payload, idempotency_key=pos_idem_key)
            status = str(result.get("status", "")).lower()
            accepted = status in {"accepted", "submitted", "filled", "ok", "success"}

            if accepted:
                logger.info(
                    "execution.exit_order_placed",
                    symbol=symbol,
                    side=order_side,
                    qty=qty,
                    status=status,
                )
                await log_event(
                    "exit_order_placed",
                    symbol=symbol,
                    order_id=str(result.get("id", "")),
                    blueprint_id=runtime_state.loaded_blueprint_id,
                    payload={
                        "side": order_side,
                        "qty": qty,
                        "reason": "blueprint_exit_condition",
                        "result": result,
                    },
                )
                tick_trades.append({"symbol": symbol, "action": "exit", "side": order_side, "qty": qty})
            else:
                logger.warning(
                    "execution.exit_order_rejected",
                    symbol=symbol,
                    result=result,
                )
        except Exception as exc:
            logger.error(
                "execution.exit_order_failed",
                symbol=symbol,
                error=str(exc),
            )
            tick_errors.append({"symbol": symbol, "action": "exit", "error": str(exc)})


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


async def _notify_tick_trades(
    tick_trades: list[dict[str, Any]],
    tick_errors: list[dict[str, Any]],
    *,
    trading_date: str,
) -> None:
    """Send aggregated trade notifications for one tick. Fire-and-forget."""
    from shared.notifier.base import EventType, NotificationEvent, Severity
    from shared.notifier.helpers import get_notifier

    manager = get_notifier()
    if not manager._backends:
        return

    try:
        if tick_trades:
            lines = [f"  {t['action'].upper()} {t['symbol']} {t.get('side','')} x{t.get('qty','')}" for t in tick_trades]
            await manager.notify(NotificationEvent(
                event_type=EventType.TRADE_EXECUTED,
                title=f"\ud83d\udcb9 {len(tick_trades)} Order(s) Executed",
                message=f"Trading date: {trading_date}\n" + "\n".join(lines),
                severity=Severity.INFO,
                payload={"trading_date": trading_date, "order_count": str(len(tick_trades))},
            ))
        if tick_errors:
            lines = [f"  {e['action'].upper()} {e['symbol']}: {e['error']}" for e in tick_errors]
            await manager.notify(NotificationEvent(
                event_type=EventType.TRADE_ERROR,
                title=f"\u274c {len(tick_errors)} Trade Error(s)",
                message=f"Trading date: {trading_date}\n" + "\n".join(lines),
                severity=Severity.ERROR,
                payload={"trading_date": trading_date, "error_count": str(len(tick_errors))},
            ))
    except Exception as exc:
        logger.warning("execution.notify_tick_failed", error=str(exc))


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
        symbols=len(settings.common.watchlist.for_trade),
    )
    if runtime_state.paused:
        runtime_state.last_tick_at = now_utc()
        logger.info("execution.tick_skipped", reason="paused")
        return

    redis_client = get_redis()
    engine = BlueprintRuleEngine()
    quotes_found = 0
    symbols_evaluated = 0
    tick_trades: list[dict[str, Any]] = []
    tick_errors: list[dict[str, Any]] = []

    try:
        await run_stop_loss_checks(
            runtime_state=runtime_state,
            redis_client=redis_client,
            broker=broker,
        )

        # ── Pre-build symbol → plan lookup from blueprint ──
        symbol_plan_map: dict[str, dict] = {}
        bp_json = runtime_state.loaded_blueprint_json
        if isinstance(bp_json, dict):
            for sp in bp_json.get("symbol_plans", []):
                sym = sp.get("underlying", "").upper()
                if sym:
                    symbol_plan_map[sym] = sp

        # ── Compute market time once (decimal hours in market TZ) ──
        mt = now_market()
        market_time_decimal = mt.hour + mt.minute / 60.0

        # ── Pre-load signal features for market context enrichment ──
        import json as _json

        signal_cache: dict[str, dict] = {}
        sig_date = runtime_state.loaded_trading_date
        if sig_date:
            for symbol in settings.common.watchlist.for_trade:
                sig_key = f"signal:features:{symbol.upper()}:{sig_date.isoformat()}"
                sig_data = await redis_client.get(sig_key)
                if sig_data:
                    try:
                        signal_cache[symbol.upper()] = _json.loads(sig_data)
                    except Exception:
                        pass

        for symbol in settings.common.watchlist.for_trade:
            quote_key = f"market:quote:{symbol}"
            quote = await redis_client.hgetall(quote_key)
            if not quote:
                continue

            quote_price = safe_float(quote.get("price"), 0.0)

            if isinstance(broker, PaperBroker) and quote_price > 0:
                broker.update_price(symbol, quote_price)

            quotes_found += 1

            # ── Build enriched market context ──
            market_ctx: dict[str, Any] = {
                "underlying_price": quote_price,
                "price": quote_price,
                "bid": safe_float(quote.get("bid"), 0.0),
                "ask": safe_float(quote.get("ask"), 0.0),
                "time": market_time_decimal,
            }

            # Enrich from signal features cache
            sig = signal_cache.get(symbol.upper())
            if sig:
                opt = sig.get("option_indicators", {})
                market_ctx.setdefault("iv", opt.get("current_iv", 0.0))
                market_ctx.setdefault("iv_rank", opt.get("iv_rank", 0.0))
                market_ctx.setdefault("delta", opt.get("delta_exposure_profile", {}).get("net", 0.0))
                market_ctx.setdefault("volume", sig.get("volume", 0))

            # ── Extract conditions from blueprint symbol plan ──
            bp_plan = symbol_plan_map.get(symbol.upper(), {})
            entry_conditions = bp_plan.get("entry_conditions", [])
            exit_conditions = bp_plan.get("exit_conditions", [])
            confidence = bp_plan.get("confidence", 0.0)

            plan: dict[str, Any] = {
                "symbol": symbol,
                "entry_conditions": entry_conditions,
                "exit_conditions": exit_conditions,
                "confidence": confidence,
                "max_position_size": bp_plan.get("max_position_size", 1),
                "legs": bp_plan.get("legs", []),
            }
            plan = _apply_quality_gate_to_plan(plan, runtime_state, symbol)
            if plan is None:
                continue  # skip due to very low data quality

            result = engine.evaluate_symbol_plan(plan, market_ctx)
            symbols_evaluated += 1

            if result["action"] == "enter":
                await _handle_entry_signal(
                    symbol=symbol,
                    plan=plan,
                    market_ctx=market_ctx,
                    broker=broker,
                    runtime_state=runtime_state,
                    tick_trades=tick_trades,
                    tick_errors=tick_errors,
                )
            elif result["action"] == "exit":
                await _handle_exit_signal(
                    symbol=symbol,
                    plan=plan,
                    market_ctx=market_ctx,
                    broker=broker,
                    runtime_state=runtime_state,
                    tick_trades=tick_trades,
                    tick_errors=tick_errors,
                )

        runtime_state.last_tick_at = now_utc()
        logger.debug(
            "execution.tick_context",
            log_event="tick_context",
            stage="evaluation",
            trading_date=str(runtime_state.loaded_trading_date),
            symbols_total=len(settings.common.watchlist.for_trade),
            quotes_found=quotes_found,
            symbols_evaluated=symbols_evaluated,
            duration_ms=round((perf_counter() - tick_started) * 1000, 2),
        )
        logger.info("execution.tick_completed", trading_date=str(runtime_state.loaded_trading_date))

        # ── Aggregated trade notifications (fire-and-forget) ──
        await _notify_tick_trades(
            tick_trades, tick_errors,
            trading_date=str(runtime_state.loaded_trading_date),
        )
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


async def _daily_trade_start(runtime_state: ExecutionRuntimeState) -> None:
    """Daily cron callback: load today's blueprint and start execution tick."""
    from services.trade_service.app.execution.blueprint_loader import load_blueprint_for_date
    from shared.utils import today_trading

    td = today_trading()
    logger.info("execution.daily_trade_start", trading_date=str(td))

    blueprint = await load_blueprint_for_date(td)
    if not blueprint:
        logger.warning(
            "execution.blueprint_not_found",
            trading_date=str(td),
            reason="blueprint missing or not pending — tick loop will NOT start",
        )
        return

    # Populate runtime state
    runtime_state.loaded_blueprint_id = str(blueprint["id"])
    runtime_state.loaded_trading_date = td
    runtime_state.loaded_blueprint_json = blueprint.get("blueprint_json")
    runtime_state.status = "active"
    runtime_state.loaded_at = now_utc()

    logger.info(
        "execution.blueprint_loaded",
        trading_date=str(td),
        blueprint_id=runtime_state.loaded_blueprint_id,
    )

    # Add interval tick job (if not already running)
    settings = get_settings()
    if _scheduler and not _scheduler.get_job("execution_tick"):
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
        logger.info("execution.tick_started", interval=settings.trade_service.execution_interval)


def start_execution_scheduler(runtime_state: ExecutionRuntimeState) -> None:
    """Start APScheduler with a daily cron job at trade_start_time.

    The cron job loads the blueprint and adds the interval tick job.
    No tick evaluation runs until the blueprint is successfully loaded.
    """
    global _scheduler
    settings = get_settings()
    _h, _m = map(int, settings.trade_service.trade_start_time.split(":"))

    logger.debug(
        "execution.scheduler_starting",
        log_event="scheduler_start",
        stage="startup",
        trade_start_time=settings.trade_service.trade_start_time,
        interval_seconds=settings.trade_service.execution_interval,
        timezone=settings.common.timezone,
        symbols=len(settings.common.watchlist.for_trade),
    )
    _scheduler = AsyncIOScheduler(timezone=settings.common.timezone)
    _scheduler.add_job(
        _daily_trade_start,
        trigger=CronTrigger(hour=_h, minute=_m, day_of_week="mon-fri"),
        args=[runtime_state],
        id="daily_trade_start",
        name="daily_blueprint_load_and_trade_start",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )
    _scheduler.start()
    logger.info(
        "execution.scheduler_started",
        trade_start_time=settings.trade_service.trade_start_time,
        next_fire=str(_scheduler.get_job("daily_trade_start").next_run_time),
    )


def stop_execution_scheduler() -> None:
    global _scheduler
    if _scheduler:
        _scheduler.shutdown(wait=False)
        logger.info("execution.scheduler_stopped")
    else:
        logger.debug("execution.scheduler_stop_skipped", log_event="scheduler_stop", reason="not_started")
