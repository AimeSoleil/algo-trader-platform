"""Test adapter routing for agentic-only mode."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from shared.models.blueprint import LLMTradingBlueprint, OptionLeg, SymbolPlan
from shared.models.signal import (
    CrossAssetIndicators,
    DataQuality,
    OptionIndicators,
    SignalFeatures,
    StockIndicators,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_sf(symbol: str = "AAPL") -> SignalFeatures:
    return SignalFeatures(
        symbol=symbol,
        date="2026-03-24",
        computed_at="2026-03-23T20:00:00",
        close_price=185.0,
        daily_return=0.01,
        volume=5_000_000,
        volatility_regime="normal",
        stock_indicators=StockIndicators(),
        option_indicators=OptionIndicators(),
        cross_asset_indicators=CrossAssetIndicators(),
        data_quality=DataQuality(),
    )


def _make_blueprint(**kwargs) -> LLMTradingBlueprint:
    defaults = dict(
        trading_date="2026-03-24",
        generated_at="2026-03-23T20:00:00",
        market_regime="neutral",
        symbol_plans=[SymbolPlan(
            underlying="AAPL",
            strategy_type="single_leg",
            direction="bullish",
            legs=[OptionLeg(expiry="2026-04-17", strike=150, option_type="call", side="buy")],
            max_loss_per_trade=500,
        )],
    )
    defaults.update(kwargs)
    return LLMTradingBlueprint(**defaults)


# ---------------------------------------------------------------------------
# Agentic routing
# ---------------------------------------------------------------------------


class TestAdapterRouting:
    @pytest.mark.asyncio
    async def test_routes_to_orchestrator(self):
        """Adapter delegates blueprint generation to AgentOrchestrator."""
        mock_bp = _make_blueprint()

        from services.analysis_service.app.llm.adapter import LLMAdapter

        adapter = LLMAdapter()

        mock_orch = AsyncMock()
        mock_orch.generate = AsyncMock(return_value=mock_bp)
        adapter._orchestrator = mock_orch

        result = await adapter.generate_blueprint([_make_sf()])
        mock_orch.generate.assert_awaited_once()
        assert result.symbol_plans[0].underlying == "AAPL"

    @pytest.mark.asyncio
    async def test_agentic_failure_propagates(self):
        """When agentic pipeline fails, adapter should raise the same error."""
        from services.analysis_service.app.llm.adapter import LLMAdapter

        adapter = LLMAdapter()

        mock_orch = AsyncMock()
        mock_orch.generate = AsyncMock(side_effect=RuntimeError("agents crashed"))
        adapter._orchestrator = mock_orch

        with pytest.raises(RuntimeError, match="agents crashed"):
            await adapter.generate_blueprint([_make_sf()])
