from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BrokerInterface(ABC):
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
