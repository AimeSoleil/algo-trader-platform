from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BrokerInterface(ABC):
    """Abstract broker adapter — all broker implementations must subclass this."""

    async def connect(self) -> None:
        """Establish connection to the broker gateway. Default: no-op."""

    async def disconnect(self) -> None:
        """Gracefully disconnect from the broker. Default: no-op."""

    @abstractmethod
    async def place_order(self, order: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def get_positions(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    async def get_account(self) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def get_realtime_price(self, symbol: str) -> float | None:
        raise NotImplementedError
