"""Futu broker adapter — skeleton implementation.

Requires:
- Futu OpenD gateway running locally (default ``127.0.0.1:11111``)
- ``futu-api`` package: ``pip install futu-api``

This file provides the full interface but raises ``NotImplementedError``
for every operation.  Each method docstring documents the corresponding
``futu-api`` SDK call for future implementation.
"""
from __future__ import annotations

from typing import Any

from shared.utils import get_logger

from services.trade_service.app.broker.base import BrokerInterface

logger = get_logger("broker_futu")

try:
    import futu  # noqa: F401

    _FUTU_AVAILABLE = True
except ImportError:
    _FUTU_AVAILABLE = False


class FutuBroker(BrokerInterface):
    """Futu OpenD broker adapter (skeleton).

    Parameters mirror the ``broker.futu`` config section.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 11111,
        trader_id: str = "",
        trd_env: str = "SIMULATE",
        market: str = "US",
    ) -> None:
        self.host = host
        self.port = port
        self.trader_id = trader_id
        self.trd_env = trd_env
        self.market = market
        self._connected = False

    async def connect(self) -> None:
        """Connect to Futu OpenD.

        SDK mapping::

            from futu import OpenQuoteContext, OpenSecTradeContext
            self._quote_ctx = OpenQuoteContext(host, port)
            self._trade_ctx = OpenSecTradeContext(host, port)
        """
        if not _FUTU_AVAILABLE:
            raise NotImplementedError(
                "futu-api package not installed. Run: pip install futu-api"
            )
        logger.info(
            "broker_futu.connect",
            host=self.host,
            port=self.port,
            trd_env=self.trd_env,
        )
        raise NotImplementedError("FutuBroker.connect() not yet implemented — install futu-api and run OpenD")

    async def disconnect(self) -> None:
        """Disconnect from Futu OpenD.

        SDK mapping::

            self._quote_ctx.close()
            self._trade_ctx.close()
        """
        if self._connected:
            self._connected = False
            logger.info("broker_futu.disconnected")

    async def place_order(self, order: dict[str, Any]) -> dict[str, Any]:
        """Place an order via Futu.

        SDK mapping::

            from futu import TrdSide, OrderType, TrdEnv
            ret, data = self._trade_ctx.place_order(
                price=order['price'],
                qty=order['qty'],
                code=order['symbol'],
                trd_side=TrdSide.BUY / TrdSide.SELL,
                order_type=OrderType.MARKET / OrderType.NORMAL,
                trd_env=TrdEnv.SIMULATE / TrdEnv.REAL,
            )
        """
        raise NotImplementedError("FutuBroker.place_order() not yet implemented")

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an order via Futu.

        SDK mapping::

            ret, data = self._trade_ctx.modify_order(
                modify_order_op=ModifyOrderOp.CANCEL,
                order_id=order_id,
                qty=0, price=0,
            )
        """
        raise NotImplementedError("FutuBroker.cancel_order() not yet implemented")

    async def get_positions(self) -> list[dict[str, Any]]:
        """Retrieve current positions from Futu.

        SDK mapping::

            ret, data = self._trade_ctx.position_list_query(trd_env=self.trd_env)
            # data columns: code, qty, cost_price, market_val, pl_ratio, ...
        """
        raise NotImplementedError("FutuBroker.get_positions() not yet implemented")

    async def get_account(self) -> dict[str, Any]:
        """Retrieve account summary from Futu.

        SDK mapping::

            ret, data = self._trade_ctx.accinfo_query(trd_env=self.trd_env)
            # data columns: total_assets, cash, market_val, ...
        """
        raise NotImplementedError("FutuBroker.get_account() not yet implemented")

    async def get_realtime_price(self, symbol: str) -> float | None:
        """Get realtime price for a symbol from Futu quote feed.

        SDK mapping::

            ret, data = self._quote_ctx.get_market_snapshot([symbol])
            price = data['last_price'].iloc[0]
        """
        raise NotImplementedError("FutuBroker.get_realtime_price() not yet implemented")
