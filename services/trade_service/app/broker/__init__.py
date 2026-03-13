"""Broker package — factory-based broker instantiation.

Usage::

    from services.trade_service.app.broker import create_broker

    broker = create_broker()           # uses settings.broker.type
    await broker.connect()
"""
from __future__ import annotations

from shared.config import get_settings
from shared.utils import get_logger

from services.trade_service.app.broker.base import BrokerInterface

logger = get_logger("broker_factory")


def create_broker() -> BrokerInterface:
    """Instantiate the configured broker adapter.

    Reads ``settings.broker.type`` to decide which implementation to return.
    Defaults to ``"paper"`` if not configured.
    """
    settings = get_settings()
    broker_type = settings.broker.type.lower()

    if broker_type == "futu":
        from services.trade_service.app.broker.futu import FutuBroker

        futu_cfg = settings.broker.futu
        broker = FutuBroker(
            host=futu_cfg.host,
            port=futu_cfg.port,
            trader_id=futu_cfg.trader_id,
            trd_env=futu_cfg.trd_env,
            market=futu_cfg.market,
        )
        logger.info("broker_factory.created", broker="futu", host=futu_cfg.host, port=futu_cfg.port)
        return broker

    paper_cfg = settings.broker.paper
    from services.trade_service.app.broker.paper import PaperBroker

    broker = PaperBroker(initial_cash=paper_cfg.initial_cash)
    logger.info("broker_factory.created", broker="paper", initial_cash=paper_cfg.initial_cash)
    return broker
