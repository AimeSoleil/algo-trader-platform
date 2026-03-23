from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

from shared.utils import get_logger, now_utc
from services.trade_service.app.broker.base import BrokerInterface

logger = get_logger("paper_broker")

# Redis key prefix & TTL for idempotency deduplication
_IDEM_PREFIX = "broker:idempotency"
_IDEM_TTL_SECONDS = 86_400  # 24 hours


class PaperBroker(BrokerInterface):
    def __init__(self, initial_cash: float = 100_000.0) -> None:
        self.orders: list[dict[str, Any]] = []
        self.positions: list[dict[str, Any]] = []
        self.cash: float = initial_cash
        self._prices: dict[str, float] = {}

    def update_price(self, symbol: str, price: float) -> None:
        self._prices[symbol.upper()] = float(price)

    async def place_order(
        self,
        order: dict[str, Any],
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        # ── Idempotency guard ──
        if idempotency_key:
            try:
                from shared.redis_pool import get_redis
                redis = get_redis()
                cache_key = f"{_IDEM_PREFIX}:{idempotency_key}"
                existing = await redis.get(cache_key)
                if existing:
                    logger.info(
                        "paper_broker.order_deduplicated",
                        idempotency_key=idempotency_key,
                    )
                    return json.loads(existing)
            except Exception:
                # If Redis is unavailable, proceed without dedup — fail-open
                logger.warning("paper_broker.idempotency_check_failed", key=idempotency_key)

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
            "filled_at": now_utc().isoformat(),
            "idempotency_key": idempotency_key,
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

        # ── Persist result for idempotency ──
        if idempotency_key:
            try:
                from shared.redis_pool import get_redis
                redis = get_redis()
                cache_key = f"{_IDEM_PREFIX}:{idempotency_key}"
                await redis.set(cache_key, json.dumps(order_record, default=str), ex=_IDEM_TTL_SECONDS)
            except Exception:
                logger.warning("paper_broker.idempotency_store_failed", key=idempotency_key)

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
