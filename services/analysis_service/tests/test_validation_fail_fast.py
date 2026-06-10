from __future__ import annotations

from datetime import date
from types import SimpleNamespace

from pydantic import ValidationError
import pytest

from services.analysis_service.app.llm.agents.base_agent import LLMResult
from services.analysis_service.app.llm.agents.critic_agent import CriticAgent
from services.analysis_service.app.llm.agents.post_merge_portfolio_agent import PostMergePortfolioAgent
from services.analysis_service.app.llm.agents.synthesizer_agent import SynthesizerAgent


class _StaticProvider:
    name = "openai"

    def __init__(self, content: str) -> None:
        self.content = content
        self.calls = 0

    async def generate(self, **kwargs) -> LLMResult:
        self.calls += 1
        return LLMResult(
            content=self.content,
            raw_content=self.content,
            input_tokens=10,
            output_tokens=10,
            total_tokens=20,
            model="test-model",
        )


def _mock_settings() -> SimpleNamespace:
    return SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                max_output_plans=10,
                max_retries=3,
                backoff_base_seconds=0,
                backoff_max_seconds=0,
                openai=SimpleNamespace(
                    temperature=0.11,
                    max_tokens=1111,
                    model="test-model",
                ),
                precision_first=SimpleNamespace(
                    enabled=False,
                    allowed_strategy_types=[],
                ),
            )
        ),
    )


@pytest.mark.asyncio
async def test_synthesizer_validation_error_does_not_retry(monkeypatch):
    settings = _mock_settings()
    monkeypatch.setattr(
        "services.analysis_service.app.llm.agents.synthesizer_agent.get_settings",
        lambda: settings,
    )

    provider = _StaticProvider('{"symbol_plans": [{"underlying": "AAPL"}]}')

    with pytest.raises(ValidationError):
        await SynthesizerAgent().synthesize(
            agent_outputs={},
            signals_summary=[
                {
                    "symbol": "AAPL",
                    "close_price": 100.0,
                    "volume": 1_000_000,
                    "volatility_regime": "normal",
                }
            ],
            provider=provider,
            trade_symbols=["AAPL"],
        )

    assert provider.calls == 1


@pytest.mark.asyncio
async def test_synthesizer_uses_provider_model_for_blueprint_metadata(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                max_output_plans=10,
                max_retries=0,
                backoff_base_seconds=0,
                backoff_max_seconds=0,
                openai=SimpleNamespace(
                    temperature=0.11,
                    max_tokens=1111,
                    model="claude-opus-4.6",
                ),
                deepseek=SimpleNamespace(
                    temperature=0.12,
                    max_tokens=2222,
                    model="deepseek-v4-pro",
                ),
                precision_first=SimpleNamespace(
                    enabled=False,
                    allowed_strategy_types=[],
                ),
            )
        ),
    )
    monkeypatch.setattr(
        "services.analysis_service.app.llm.agents.synthesizer_agent.get_settings",
        lambda: settings,
    )

    class _DeepSeekProvider:
        name = "deepseek"

        async def generate(self, **kwargs) -> LLMResult:
            return LLMResult(
                content='{"market_regime": "neutral", "market_analysis": "ok", "max_total_positions": 10, "symbol_plans": []}',
                raw_content='{"market_regime": "neutral", "market_analysis": "ok", "max_total_positions": 10, "symbol_plans": []}',
                input_tokens=10,
                output_tokens=12,
                total_tokens=22,
                model="deepseek-v4-pro",
            )

    blueprint = await SynthesizerAgent().synthesize(
        agent_outputs={},
        signals_summary=[
            {
                "symbol": "AAPL",
                "price": {"close_price": 100.0, "volume": 1_000_000, "volatility_regime": "normal"},
            }
        ],
        provider=_DeepSeekProvider(),
        signal_date=date(2026, 3, 24),
        trade_symbols=["AAPL"],
    )

    assert blueprint.model_provider == "deepseek"
    assert blueprint.model_version == "deepseek-v4-pro"


@pytest.mark.asyncio
async def test_critic_validation_error_does_not_retry(monkeypatch):
    settings = _mock_settings()
    monkeypatch.setattr(
        "services.analysis_service.app.llm.agents.critic_agent.get_settings",
        lambda: settings,
    )

    provider = _StaticProvider('{"issues": "invalid"}')

    with pytest.raises(ValidationError):
        await CriticAgent().review(
            blueprint_json={"symbol_plans": []},
            agent_outputs={},
            signals_summary=[{"symbol": "AAPL", "close_price": 100.0}],
            provider=provider,
        )

    assert provider.calls == 1


@pytest.mark.asyncio
async def test_post_merge_validation_error_does_not_retry(monkeypatch):
    settings = _mock_settings()
    monkeypatch.setattr(
        "services.analysis_service.app.llm.agents.post_merge_portfolio_agent.get_settings",
        lambda: settings,
    )

    provider = _StaticProvider('{"selected_symbols": "invalid"}')

    with pytest.raises(ValidationError):
        await PostMergePortfolioAgent().review(
            candidate_summaries=[
                {
                    "symbol": "AAPL",
                    "confidence": 0.7,
                    "strategy_type": "single_leg",
                    "direction": "bullish",
                }
            ],
            candidate_count=1,
            provider=provider,
        )

    assert provider.calls == 1
