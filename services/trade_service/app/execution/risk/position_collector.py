"""Position collection — broker-first with portfolio-service fallback.

Normalises heterogeneous position dicts into a uniform schema so that
 downstream risk checks can operate on a single format.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from redis.asyncio import Redis

from shared.utils import get_logger

from services.trade_service.app.broker.base import BrokerInterface
from services.trade_service.app.helpers import load_redis_price, safe_float, safe_int
from services.trade_service.app.portfolio.service import get_positions

logger = get_logger("position_collector")


def compute_unrealized_pnl(
    side: str,
    qty: int,
    avg_entry_price: float,
    current_price: float,
    asset_type: str,
) -> float:
    multiplier = 100 if asset_type.lower() == "option" else 1
    if side == "short":
        return (avg_entry_price - current_price) * qty * multiplier
    return (current_price - avg_entry_price) * qty * multiplier


async def positions_from_broker(broker: BrokerInterface) -> list[dict[str, Any]]:
    broker_positions = await broker.get_positions()
    if not broker_positions:
        return []

    normalised: list[dict[str, Any]] = []
    for raw in broker_positions:
        symbol = str(raw.get("symbol") or "").upper().strip()
        if not symbol:
            continue

        raw_qty = safe_int(raw.get("qty", raw.get("quantity")), 0)
        if raw_qty == 0:
            continue

        side = "long" if raw_qty > 0 else "short"
        qty = abs(raw_qty)
        avg_entry_price = safe_float(raw.get("avg_price", raw.get("avg_entry_price")), 0.0)
        current_price = await broker.get_realtime_price(symbol)
        if current_price is None:
            return []

        unrealized_pnl = compute_unrealized_pnl(
            side=side,
            qty=qty,
            avg_entry_price=avg_entry_price,
            current_price=float(current_price),
            asset_type=str(raw.get("asset_type") or "stock"),
        )
        normalised.append(
            {
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "avg_entry_price": avg_entry_price,
                "current_price": float(current_price),
                "unrealized_pnl": unrealized_pnl,
                "asset_type": str(raw.get("asset_type") or "stock"),
            }
        )
    return normalised


async def positions_from_trade_portfolio(redis_client: Redis) -> list[dict[str, Any]]:
    portfolio = await get_positions()
    rows = portfolio.get("positions", []) if isinstance(portfolio, Mapping) else []

    normalised: list[dict[str, Any]] = []
    for row in rows:
        symbol = str(row.get("symbol") or "").upper().strip()
        if not symbol:
            continue

        side = str(row.get("side") or "long").lower()
        qty = abs(safe_int(row.get("quantity", row.get("qty")), 0))
        if qty <= 0:
            continue

        avg_entry_price = safe_float(row.get("avg_entry_price", row.get("avg_price")), 0.0)
        redis_price = await load_redis_price(redis_client, symbol)
        current_price = redis_price if redis_price is not None else safe_float(row.get("current_price"), 0.0)
        unrealized_pnl = safe_float(row.get("unrealized_pnl"), 0.0)
        if unrealized_pnl == 0.0 and current_price > 0 and avg_entry_price > 0:
            unrealized_pnl = compute_unrealized_pnl(
                side=side,
                qty=qty,
                avg_entry_price=avg_entry_price,
                current_price=current_price,
                asset_type=str(row.get("asset_type") or "stock"),
            )

        normalised.append(
            {
                "symbol": symbol,
                "side": "short" if side == "short" else "long",
                "qty": qty,
                "avg_entry_price": avg_entry_price,
                "current_price": current_price,
                "unrealized_pnl": unrealized_pnl,
                "asset_type": str(row.get("asset_type") or "stock"),
            }
        )

    return normalised


async def collect_risk_positions(
    redis_client: Redis,
    broker: BrokerInterface,
) -> tuple[list[dict[str, Any]], str]:
    broker_positions = await positions_from_broker(broker)
    if broker_positions:
        return broker_positions, "broker_realtime"

    portfolio_positions = await positions_from_trade_portfolio(redis_client)
    return portfolio_positions, "trade_service_portfolio"
