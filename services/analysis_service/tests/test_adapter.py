"""Test adapter routing: agentic mode vs legacy, fallback behavior."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

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
# Agentic vs Legacy routing
# ---------------------------------------------------------------------------


class TestAdapterRouting:
    @pytest.mark.asyncio
    async def test_agentic_mode_routes_to_orchestrator(self):
        """When agentic_mode=True, adapter delegates to AgentOrchestrator."""
        mock_bp = _make_blueprint()

        with patch("services.analysis_service.app.llm.adapter.get_settings") as mock_settings:
            settings = MagicMock()
            settings.analysis_service.llm.provider = "openai"
            settings.analysis_service.llm.chunk_size = 5
            settings.analysis_service.llm.max_concurrent_chunks = 3
            settings.analysis_service.llm.benchmark_symbols = ["SPY"]
            settings.analysis_service.llm.agentic_mode = True
            settings.analysis_service.llm.circuit_breaker_threshold = 5
            settings.analysis_service.llm.circuit_breaker_cooldown_seconds = 60
            mock_settings.return_value = settings

            from services.analysis_service.app.llm.adapter import LLMAdapter

            adapter = LLMAdapter()

            # Mock the orchestrator
            mock_orch = AsyncMock()
            mock_orch.generate = AsyncMock(return_value=mock_bp)
            adapter._orchestrator = mock_orch
            adapter._agentic_mode = True

            result = await adapter.generate_blueprint([_make_sf()])
            mock_orch.generate.assert_awaited_once()
            assert result.symbol_plans[0].underlying == "AAPL"

    @pytest.mark.asyncio
    async def test_agentic_fallback_to_legacy_on_failure(self):
        """When agentic pipeline fails, adapter falls back to legacy."""
        mock_bp = _make_blueprint()

        with patch("services.analysis_service.app.llm.adapter.get_settings") as mock_settings:
            settings = MagicMock()
            settings.analysis_service.llm.provider = "openai"
            settings.analysis_service.llm.chunk_size = 5
            settings.analysis_service.llm.max_concurrent_chunks = 3
            settings.analysis_service.llm.benchmark_symbols = ["SPY"]
            settings.analysis_service.llm.agentic_mode = True
            settings.analysis_service.llm.circuit_breaker_threshold = 5
            settings.analysis_service.llm.circuit_breaker_cooldown_seconds = 60
            mock_settings.return_value = settings

            from services.analysis_service.app.llm.adapter import LLMAdapter

            adapter = LLMAdapter()

            # Mock orchestrator to fail
            mock_orch = AsyncMock()
            mock_orch.generate = AsyncMock(side_effect=RuntimeError("agents crashed"))
            adapter._orchestrator = mock_orch
            adapter._agentic_mode = True

            # Mock legacy path to succeed
            adapter._generate_legacy = AsyncMock(return_value=mock_bp)

            result = await adapter.generate_blueprint([_make_sf()])
            adapter._generate_legacy.assert_awaited_once()
            assert result.symbol_plans[0].underlying == "AAPL"


# ---------------------------------------------------------------------------
# Circuit breaker
# ---------------------------------------------------------------------------


class TestCircuitBreaker:
    def test_circuit_opens_after_threshold(self):
        from services.analysis_service.app.llm.adapter import _CircuitBreaker

        cb = _CircuitBreaker(threshold=3, cooldown=60.0)
        assert not cb.is_open

        cb.record_failure()
        cb.record_failure()
        assert not cb.is_open

        cb.record_failure()  # 3rd failure → opens
        assert cb.is_open

    def test_circuit_resets_on_success(self):
        from services.analysis_service.app.llm.adapter import _CircuitBreaker

        cb = _CircuitBreaker(threshold=2, cooldown=60.0)
        cb.record_failure()
        cb.record_success()
        cb.record_failure()
        assert not cb.is_open  # count reset by success

    def test_circuit_halfopen_after_cooldown(self):
        from services.analysis_service.app.llm.adapter import _CircuitBreaker

        cb = _CircuitBreaker(threshold=1, cooldown=0.0)  # instant cooldown
        cb.record_failure()
        # With cooldown=0, is_open should be False (half-open)
        assert not cb.is_open
