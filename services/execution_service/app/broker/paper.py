from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from shared.utils import now_utc
from services.execution_service.app.broker.base import BrokerInterface


class PaperBroker(BrokerInterface):
    def __init__(self) -> None:
        self.orders: list[dict[str, Any]] = []
        self.positions: list[dict[str, Any]] = []
        self.cash: float = 100000.0
        self._prices: dict[str, float] = {}

    def update_price(self, symbol: str, price: float) -> None:
        self._prices[symbol.upper()] = float(price)

    async def place_order(self, order: dict[str, Any]) -> dict[str, Any]:
        symbol = str(order.get("symbol", "")).upper()
        side = str(order.get("side", "buy")).lower()
        qty = int(order.get("qty", 0))
        if not symbol or qty <= 0:
            return {"status": "rejected", "reason": "invalid_order"}

        fill_price = await self.get_realtime_price(symbol)
        if fill_price is None:
            return {"status": "rejected", "reason": "no_price"}

        order_record = {
            "id": str(uuid4()),
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "type": order.get("type", "market"),
            "status": "filled",
            "filled_price": fill_price,
            "filled_at": now_utc(),
        }
        self.orders.append(order_record)

        signed_qty = qty if side == "buy" else -qty
        self.positions.append(
            {
                "symbol": symbol,
                "qty": signed_qty,
                "avg_price": fill_price,
                "updated_at": now_utc(),
            }
        )
        self.cash -= signed_qty * fill_price
        return order_record

    async def cancel_order(self, order_id: str) -> bool:
        for order in self.orders:
            if order.get("id") == order_id and order.get("status") == "pending":
                order["status"] = "cancelled"
                return True
        return False

    async def get_positions(self) -> list[dict[str, Any]]:
        return self.positions

    async def get_account(self) -> dict[str, Any]:
        return {"cash": self.cash, "orders": len(self.orders), "positions": len(self.positions)}

    async def get_realtime_price(self, symbol: str) -> float | None:
        return self._prices.get(symbol.upper())
