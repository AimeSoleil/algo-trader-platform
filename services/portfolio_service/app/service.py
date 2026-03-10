"""Portfolio Service — 持仓查询与聚合计算"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import text

from shared.db.session import get_postgres_session


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _extract_greeks(position_json: dict[str, Any]) -> tuple[float, float, float, float]:
    greeks = position_json.get("greeks")
    greek_source = greeks if isinstance(greeks, dict) else position_json
    return (
        _to_float(greek_source.get("delta")),
        _to_float(greek_source.get("gamma")),
        _to_float(greek_source.get("theta")),
        _to_float(greek_source.get("vega")),
    )


def _position_sign(side: str | None) -> int:
    return 1 if (side or "").lower() == "long" else -1


async def _load_open_positions() -> list[dict[str, Any]]:
    query = text(
        """
        SELECT
            id,
            symbol,
            underlying,
            asset_type,
            side,
            quantity,
            avg_entry_price,
            current_price,
            unrealized_pnl,
            realized_pnl,
            position_json,
            opened_at,
            closed_at,
            is_open
        FROM positions
        WHERE is_open = true
        ORDER BY underlying, symbol
        """
    )
    async with get_postgres_session() as session:
        rows = (await session.execute(query)).mappings().all()
    return [dict(row) for row in rows]


def _normalize_positions(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, float]]:
    positions: list[dict[str, Any]] = []

    total_delta = 0.0
    total_gamma = 0.0
    total_theta = 0.0
    total_vega = 0.0
    total_market_value = 0.0
    total_unrealized_pnl = 0.0
    total_realized_pnl = 0.0

    for row in rows:
        position_json = _to_dict(row.get("position_json"))
        quantity = _to_int(row.get("quantity"), 0)
        side = (row.get("side") or "long").lower()
        sign = _position_sign(side)
        asset_type = (row.get("asset_type") or "stock").lower()

        avg_entry_price = _to_float(row.get("avg_entry_price"), _to_float(position_json.get("avg_entry_price")))
        current_price = _to_float(row.get("current_price"), _to_float(position_json.get("current_price")))
        unrealized_pnl = _to_float(row.get("unrealized_pnl"), _to_float(position_json.get("unrealized_pnl")))
        realized_pnl = _to_float(row.get("realized_pnl"), _to_float(position_json.get("realized_pnl")))

        contract_multiplier = 100 if asset_type == "option" else 1
        market_value = sign * quantity * current_price * contract_multiplier

        delta, gamma, theta, vega = _extract_greeks(position_json)
        scaled_qty = quantity * contract_multiplier * sign

        total_delta += delta * scaled_qty
        total_gamma += gamma * scaled_qty
        total_theta += theta * scaled_qty
        total_vega += vega * scaled_qty

        total_market_value += market_value
        total_unrealized_pnl += unrealized_pnl
        total_realized_pnl += realized_pnl

        positions.append(
            {
                "id": row.get("id"),
                "symbol": row.get("symbol"),
                "underlying": row.get("underlying"),
                "asset_type": asset_type,
                "side": side,
                "quantity": quantity,
                "avg_entry_price": avg_entry_price,
                "current_price": current_price,
                "market_value": market_value,
                "unrealized_pnl": unrealized_pnl,
                "realized_pnl": realized_pnl,
                "delta": delta,
                "gamma": gamma,
                "theta": theta,
                "vega": vega,
                "opened_at": row.get("opened_at"),
                "closed_at": row.get("closed_at"),
                "is_open": bool(row.get("is_open")),
            }
        )

    aggregates = {
        "total_delta": total_delta,
        "total_gamma": total_gamma,
        "total_theta": total_theta,
        "total_vega": total_vega,
        "total_market_value": total_market_value,
        "total_unrealized_pnl": total_unrealized_pnl,
        "total_realized_pnl": total_realized_pnl,
        "net_pnl": total_unrealized_pnl + total_realized_pnl,
    }
    return positions, aggregates


async def get_positions() -> dict[str, Any]:
    rows = await _load_open_positions()
    positions, aggregates = _normalize_positions(rows)
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "count": len(positions),
        "positions": positions,
        "aggregates": aggregates,
    }


async def get_portfolio_snapshot() -> dict[str, Any]:
    rows = await _load_open_positions()
    positions, aggregates = _normalize_positions(rows)
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "positions_count": len(positions),
        "greeks": {
            "total_delta": aggregates["total_delta"],
            "total_gamma": aggregates["total_gamma"],
            "total_theta": aggregates["total_theta"],
            "total_vega": aggregates["total_vega"],
        },
        "pnl": {
            "unrealized": aggregates["total_unrealized_pnl"],
            "realized": aggregates["total_realized_pnl"],
            "net": aggregates["net_pnl"],
        },
        "total_market_value": aggregates["total_market_value"],
    }


async def get_performance(trading_date: date) -> dict[str, Any]:
    async with get_postgres_session() as session:
        realized = await session.execute(
            text(
                """
                SELECT COALESCE(SUM(realized_pnl), 0.0) AS realized
                FROM positions
                WHERE closed_at IS NOT NULL
                  AND DATE(closed_at) = :trading_date
                """
            ),
            {"trading_date": trading_date},
        )
        realized_pnl = _to_float(realized.scalar())

        unrealized = await session.execute(
            text(
                """
                SELECT COALESCE(SUM(unrealized_pnl), 0.0) AS unrealized
                FROM positions
                WHERE is_open = true
                """
            )
        )
        unrealized_pnl = _to_float(unrealized.scalar())

    return {
        "date": trading_date.isoformat(),
        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized_pnl,
        "net_pnl": realized_pnl + unrealized_pnl,
    }
