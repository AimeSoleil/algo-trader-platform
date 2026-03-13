"""Stop-loss risk monitor — orchestrates risk checks and market-order exits.

Delegates pure evaluation logic to ``risk_engine`` and position fetching
to ``position_collector``. This module owns the side-effectful parts:
sending orders, managing cooldowns, and appending runtime events.
"""
from __future__ import annotations

from time import perf_counter
from typing import Any

from redis.asyncio import Redis

from shared.config import get_settings
from shared.utils import get_logger, now_utc

from services.trade_service.app.audit import log_event
from services.trade_service.app.broker.base import BrokerInterface
from services.trade_service.app.execution.risk.position_collector import collect_risk_positions
from services.trade_service.app.execution.risk.risk_engine import (
    evaluate_portfolio_stop_loss,
    evaluate_position_stop_loss,
    in_cooldown,
    mark_cooldown,
    should_run_risk_check,
)
from services.trade_service.app.helpers import safe_float, safe_int
from services.trade_service.app.models import ExecutionRuntimeState

logger = get_logger("risk_monitor")


def _append_stoploss_event(runtime_state: ExecutionRuntimeState, event: dict[str, Any]) -> None:
    runtime_state.stoploss_last_events.append(event)
    if len(runtime_state.stoploss_last_events) > 100:
        runtime_state.stoploss_last_events = runtime_state.stoploss_last_events[-100:]


async def _send_stoploss_market_order(
    broker: BrokerInterface,
    symbol: str,
    side: str,
    qty: int,
) -> tuple[bool, dict[str, Any]]:
    order_side = "sell" if side == "long" else "buy"
    payload = {
        "symbol": symbol,
        "side": order_side,
        "qty": qty,
        "type": "market",
        "reason": "stop_loss",
    }
    try:
        result = await broker.place_order(payload)
    except Exception as exc:
        return False, {"error": str(exc), "order": payload}

    status = str(result.get("status") or "").lower()
    accepted = status in {"accepted", "submitted", "filled", "ok", "success"}
    return accepted, result


async def run_stop_loss_checks(
    runtime_state: ExecutionRuntimeState,
    redis_client: Redis,
    broker: BrokerInterface,
) -> None:
    settings = get_settings()
    stop_loss = settings.risk.stop_loss
    now = now_utc()

    if not stop_loss.enabled:
        logger.info("risk_monitor.check_skipped", event="risk_check_skipped", reason="disabled")
        return

    if not should_run_risk_check(
        last_check_at=runtime_state.last_risk_check_at,
        check_interval_seconds=stop_loss.check_interval_seconds,
        now=now,
    ):
        logger.info(
            "risk_monitor.check_skipped",
            event="risk_check_skipped",
            reason="interval_not_elapsed",
            check_interval_seconds=stop_loss.check_interval_seconds,
        )
        return

    risk_started = perf_counter()
    logger.info("risk_monitor.check_start", event="risk_check_start")

    triggered_count = 0
    try:
        positions, source = await collect_risk_positions(redis_client, broker)
        total_unrealized_pnl = sum(safe_float(p.get("unrealized_pnl"), 0.0) for p in positions)

        portfolio_triggered = evaluate_portfolio_stop_loss(
            total_unrealized_pnl=total_unrealized_pnl,
            portfolio_loss_limit=stop_loss.portfolio_loss_limit,
        )

        targets: list[tuple[dict[str, Any], str]] = []
        if portfolio_triggered:
            for position in positions:
                symbol = str(position.get("symbol") or "").upper().strip()
                if not symbol:
                    continue
                if in_cooldown(symbol, runtime_state.stoploss_cooldowns, now):
                    continue
                targets.append((position, "portfolio_stop_loss"))
        else:
            for position in positions:
                symbol = str(position.get("symbol") or "").upper().strip()
                if not symbol:
                    continue
                if in_cooldown(symbol, runtime_state.stoploss_cooldowns, now):
                    continue
                if evaluate_position_stop_loss(
                    unrealized_pnl=safe_float(position.get("unrealized_pnl"), 0.0),
                    position_loss_limit=stop_loss.position_loss_limit,
                ):
                    targets.append((position, "position_stop_loss"))

        if targets:
            trigger_scope = "portfolio" if portfolio_triggered else "position"
            logger.warning(
                "risk_monitor.triggered",
                event="risk_triggered",
                source=source,
                trigger_scope=trigger_scope,
                total_unrealized_pnl=total_unrealized_pnl,
                targets=len(targets),
            )
            await log_event(
                "stoploss_triggered",
                payload={
                    "scope": trigger_scope,
                    "source": source,
                    "total_unrealized_pnl": total_unrealized_pnl,
                    "targets": len(targets),
                },
            )

        for position, trigger_type in targets:
            symbol = str(position.get("symbol") or "").upper().strip()
            side = str(position.get("side") or "long").lower()
            qty = abs(safe_int(position.get("qty", position.get("quantity")), 0))
            if not symbol or qty <= 0:
                continue

            accepted, result = await _send_stoploss_market_order(
                broker=broker,
                symbol=symbol,
                side="short" if side == "short" else "long",
                qty=qty,
            )

            event_payload = {
                "at": now_utc().isoformat(),
                "symbol": symbol,
                "trigger": trigger_type,
                "qty": qty,
                "side": side,
                "status": "sent" if accepted else "failed",
                "result": result,
            }

            if accepted:
                triggered_count += 1
                mark_cooldown(
                    symbol=symbol,
                    cooldowns=runtime_state.stoploss_cooldowns,
                    now=now,
                    cooldown_seconds=stop_loss.cooldown_seconds,
                )
                logger.info(
                    "risk_monitor.order_sent",
                    event="risk_order_sent",
                    symbol=symbol,
                    trigger=trigger_type,
                    qty=qty,
                )
                await log_event(
                    "order_created",
                    symbol=symbol,
                    order_id=str(result.get("id", "")),
                    payload={"trigger": trigger_type, "qty": qty, "side": side, "result": result},
                )
            else:
                logger.warning(
                    "risk_monitor.order_failed",
                    event="risk_order_failed",
                    symbol=symbol,
                    trigger=trigger_type,
                    qty=qty,
                    result=result,
                )
                await log_event(
                    "order_rejected",
                    symbol=symbol,
                    payload={"trigger": trigger_type, "qty": qty, "side": side, "result": result},
                )

            _append_stoploss_event(runtime_state, event_payload)
    except Exception as exc:
        logger.warning(
            "risk_monitor.check_done",
            event="risk_check_done",
            status="error",
            error=str(exc),
            duration_ms=round((perf_counter() - risk_started) * 1000, 2),
        )
    else:
        runtime_state.last_risk_check_at = now
        logger.info(
            "risk_monitor.check_done",
            event="risk_check_done",
            status="ok",
            triggered_orders=triggered_count,
            events_count=len(runtime_state.stoploss_last_events),
            duration_ms=round((perf_counter() - risk_started) * 1000, 2),
        )
