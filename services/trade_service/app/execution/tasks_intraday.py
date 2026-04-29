"""Intraday entry optimizer — Celery task (every 5 min during market hours)."""
from __future__ import annotations

import asyncio
import json
from datetime import date
from typing import Any

from shared.celery_app import celery_app
from shared.config import get_settings
from shared.utils import get_logger, today_trading, now_market

logger = get_logger("intraday_task")


def _coerce_positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
        return parsed if parsed > 0 else default
    except (TypeError, ValueError):
        return default


def _coerce_positive_float(value: Any, default: float) -> float:
    try:
        parsed = float(value)
        return parsed if parsed > 0 else default
    except (TypeError, ValueError):
        return default


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


async def _check_global_risk_gate(
    symbol: str,
    trading_date: date,
    blueprint_json: dict[str, Any],
    broker,
) -> tuple[bool, list[str]]:
    settings = get_settings()
    policy = settings.trade_service.risk.blueprint_limits

    max_total_positions = _coerce_positive_int(blueprint_json.get("max_total_positions"), 5)
    max_daily_loss = _coerce_positive_float(blueprint_json.get("max_daily_loss"), policy.max_daily_loss)
    max_margin_usage = _coerce_positive_float(blueprint_json.get("max_margin_usage"), policy.max_margin_usage)
    portfolio_delta_limit = _coerce_positive_float(
        blueprint_json.get("portfolio_delta_limit"), policy.portfolio_delta_limit
    )
    portfolio_gamma_limit = _coerce_positive_float(
        blueprint_json.get("portfolio_gamma_limit"), policy.portfolio_gamma_limit
    )

    reasons: list[str] = []

    from services.trade_service.app.portfolio.service import get_performance, get_positions

    positions_data = await get_positions()
    open_underlyings = {
        str(pos.get("underlying") or pos.get("symbol") or "").upper().strip()
        for pos in positions_data.get("positions", [])
        if str(pos.get("underlying") or pos.get("symbol") or "").strip()
    }
    symbol_upper = symbol.upper()
    projected_total_positions = len(open_underlyings) + (0 if symbol_upper in open_underlyings else 1)
    if projected_total_positions > max_total_positions:
        reasons.append(
            f"max_total_positions exceeded ({projected_total_positions} > {max_total_positions})"
        )

    aggregates = positions_data.get("aggregates", {})
    total_delta = abs(_coerce_float(aggregates.get("total_delta", 0.0), 0.0))
    total_gamma = abs(_coerce_float(aggregates.get("total_gamma", 0.0), 0.0))
    if total_delta > portfolio_delta_limit:
        reasons.append(f"portfolio_delta_limit exceeded ({total_delta:.4f} > {portfolio_delta_limit:.4f})")
    if total_gamma > portfolio_gamma_limit:
        reasons.append(f"portfolio_gamma_limit exceeded ({total_gamma:.4f} > {portfolio_gamma_limit:.4f})")

    perf = await get_performance(trading_date)
    net_pnl = _coerce_float(perf.get("net_pnl", 0.0), 0.0)
    if net_pnl <= -abs(max_daily_loss):
        reasons.append(f"max_daily_loss exceeded ({abs(net_pnl):.2f} >= {max_daily_loss:.2f})")

    try:
        account = await broker.get_account()
    except Exception as exc:
        logger.warning("intraday_task.account_fetch_failed", symbol=symbol, error=str(exc))
        account = {}

    cash = _coerce_float(account.get("cash", 0.0), 0.0) if isinstance(account, dict) else 0.0
    total_market_value = abs(_coerce_float(aggregates.get("total_market_value", 0.0), 0.0))
    if cash > 0:
        margin_usage = total_market_value / (total_market_value + cash)
        if margin_usage > max_margin_usage:
            reasons.append(f"max_margin_usage exceeded ({margin_usage:.4f} > {max_margin_usage:.4f})")

    return len(reasons) == 0, reasons


@celery_app.task(
    name="trade_service.tasks.evaluate_entry_windows",
    bind=True,
    max_retries=0,
    queue="data",
)
def evaluate_entry_windows(self, trading_date: str | None = None) -> dict:
    """Evaluate intraday entry quality for all pending blueprint plans.

    Scheduled by Beat every 5 min during market hours (offset from capture).
    """
    return asyncio.run(_evaluate_async(trading_date))


async def _evaluate_async(trading_date: str | None = None) -> dict:
    settings = get_settings()
    cfg = settings.trade_service.intraday_optimizer

    if not cfg.enabled:
        return {"status": "disabled"}

    td = date.fromisoformat(trading_date) if trading_date else today_trading()

    # Market hours check
    mt = now_market()
    mkt_start_h, mkt_start_m = map(int, settings.common.market_hours.start.split(":"))
    mkt_end_h, mkt_end_m = map(int, settings.common.market_hours.end.split(":"))
    market_open = mkt_start_h + mkt_start_m / 60.0
    market_close = mkt_end_h + mkt_end_m / 60.0
    current_time = mt.hour + mt.minute / 60.0

    if current_time < market_open or current_time >= market_close:
        logger.debug("intraday_task.outside_market_hours", time=f"{mt.hour}:{mt.minute:02}")
        return {"status": "outside_market_hours", "trading_date": td.isoformat()}

    # Load active blueprint
    from services.trade_service.app.execution.blueprint_loader import load_blueprint_for_date
    blueprint = await load_blueprint_for_date(td)
    if not blueprint or not blueprint.get("blueprint_json"):
        logger.info("intraday_task.no_blueprint", trading_date=str(td))
        return {"status": "no_blueprint", "trading_date": td.isoformat()}

    blueprint_id = str(blueprint["id"])
    blueprint_json = blueprint["blueprint_json"]

    # Run optimizer
    from services.trade_service.app.execution.intraday.optimizer import EntryOptimizer
    optimizer = EntryOptimizer()
    decisions = await optimizer.evaluate_all(td, blueprint_json, blueprint_id)

    enter_decisions = [d for d in decisions if d.action == "enter"]
    wait_decisions = [d for d in decisions if d.action == "wait"]
    skip_decisions = [d for d in decisions if d.action == "skip"]

    logger.info(
        "intraday_task.evaluated",
        trading_date=str(td),
        total=len(decisions),
        enter=len(enter_decisions),
        wait=len(wait_decisions),
        skip=len(skip_decisions),
    )

    # Publish scores to Redis
    await _publish_scores(decisions, td)

    # Handle enter decisions
    executed = []
    notified = []

    for decision in enter_decisions:
        symbol = decision.symbol
        # Find the matching plan
        plan = next(
            (p for p in blueprint_json.get("symbol_plans", [])
             if p.get("underlying", "").upper() == symbol.upper() and not p.get("is_entered", False)),
            None,
        )
        if not plan:
            continue

        if cfg.execution_mode == "auto":
            success = await _execute_entry(symbol, plan, td, blueprint_id, blueprint_json, decision)
            if success:
                executed.append(symbol)
        else:
            await _notify_entry(symbol, plan, decision, td)
            notified.append(symbol)

    # Also notify interesting scores below threshold if in notify mode
    if cfg.execution_mode == "notify":
        for decision in wait_decisions:
            if decision.score.total >= cfg.notify_min_score:
                plan = next(
                    (p for p in blueprint_json.get("symbol_plans", [])
                     if p.get("underlying", "").upper() == decision.symbol.upper()),
                    None,
                )
                if plan:
                    await _notify_entry(decision.symbol, plan, decision, td, below_threshold=True)
                    notified.append(decision.symbol)

    return {
        "status": "evaluated",
        "trading_date": td.isoformat(),
        "enter": len(enter_decisions),
        "wait": len(wait_decisions),
        "skip": len(skip_decisions),
        "executed": executed,
        "notified": notified,
    }


async def _execute_entry(
    symbol: str,
    plan: dict,
    trading_date: date,
    blueprint_id: str,
    blueprint_json: dict[str, Any],
    decision,
) -> bool:
    """Place entry orders via broker (auto mode)."""
    from services.trade_service.app.broker import create_broker
    from services.trade_service.app.audit import log_event

    legs = plan.get("legs", [])
    if not legs:
        return False

    broker = create_broker()
    await broker.connect()

    try:
        gate_passed, gate_reasons = await _check_global_risk_gate(
            symbol=symbol,
            trading_date=trading_date,
            blueprint_json=blueprint_json,
            broker=broker,
        )
        if not gate_passed:
            logger.warning(
                "intraday_task.global_risk_gate_blocked",
                symbol=symbol,
                reasons=gate_reasons,
            )
            return False

        for i, leg in enumerate(legs):
            order_payload = {
                "symbol": symbol,
                "side": leg.get("side", "buy"),
                "qty": leg.get("quantity", 1),
                "type": "market",
                "reason": "intraday_optimizer_entry",
                "leg_index": i,
                "strike": leg.get("strike"),
                "expiry": str(leg.get("expiry", "")),
                "option_type": leg.get("option_type"),
            }
            idem_key = f"entry:{symbol}:{trading_date}:leg{i}"
            result = await broker.place_order(order_payload, idempotency_key=idem_key)
            status = str(result.get("status", "")).lower()
            accepted = status in {"accepted", "submitted", "filled", "ok", "success"}

            if accepted:
                logger.info(
                    "intraday_task.order_placed",
                    symbol=symbol,
                    leg=i,
                    side=leg.get("side"),
                    score=round(decision.score.total, 3),
                )
                await log_event(
                    "intraday_optimizer_entry",
                    symbol=symbol,
                    order_id=str(result.get("id", "")),
                    blueprint_id=blueprint_id,
                    payload={
                        "leg_index": i,
                        "score": decision.score.total,
                        "iv_score": decision.score.iv_score,
                        "price_score": decision.score.price_score,
                        "reasons": decision.reasons[:5],
                        "result": result,
                    },
                )
            else:
                logger.warning("intraday_task.order_rejected", symbol=symbol, leg=i, result=result)
                return False
        return True
    except Exception as exc:
        logger.error("intraday_task.order_failed", symbol=symbol, error=str(exc))
        return False
    finally:
        await broker.disconnect()


async def _notify_entry(
    symbol: str,
    plan: dict,
    decision,
    trading_date: date,
    below_threshold: bool = False,
) -> None:
    """Send Discord notification for entry signal."""
    from shared.notifier.helpers import notify_sync
    from shared.notifier.base import NotificationEvent, EventType, Severity

    score = decision.score
    prefix = "📊" if below_threshold else "🎯"
    label = "Near Threshold" if below_threshold else "Entry Signal"

    notify_sync(NotificationEvent(
        event_type=EventType.TRADE_EXECUTED if not below_threshold else EventType.DAILY_REPORT,
        title=f"{prefix} {label}: {symbol}",
        message=(
            f"**{symbol}** — {plan.get('strategy_type', '?')} ({plan.get('direction', '?')})\n"
            f"Score: **{score.total:.2f}** | IV: {score.iv_score:.2f} | Price: {score.price_score:.2f} | "
            f"Liq: {score.liquidity_score:.2f} | Time: {score.time_score:.2f}\n"
            f"Conditions: {'✅ all met' if decision.conditions_met else '❌ ' + ', '.join(decision.conditions_failed[:3])}\n"
            f"Reasons: {', '.join(decision.reasons[:3])}"
        ),
        severity=Severity.INFO,
        payload={
            "trading_date": trading_date.isoformat(),
            "symbol": symbol,
            "score": str(round(score.total, 3)),
            "action": decision.action,
        },
    ))


async def _publish_scores(decisions: list, trading_date: date) -> None:
    """Publish latest scores to Redis for dashboard consumption."""
    try:
        from shared.redis_pool import get_redis
        redis = await get_redis()
        for d in decisions:
            key = f"intraday:entry_scores:{d.symbol}"
            data = {
                "symbol": d.symbol,
                "trading_date": trading_date.isoformat(),
                "score": round(d.score.total, 4),
                "iv_score": round(d.score.iv_score, 4),
                "price_score": round(d.score.price_score, 4),
                "liquidity_score": round(d.score.liquidity_score, 4),
                "time_score": round(d.score.time_score, 4),
                "action": d.action,
                "strategy_type": d.strategy_type,
                "conditions_met": d.conditions_met,
                "conditions_failed": d.conditions_failed,
                "reasons": d.reasons[:5],
            }
            await redis.set(key, json.dumps(data), ex=3600)
    except Exception as exc:
        logger.warning("intraday_task.redis_publish_failed", error=str(exc))
