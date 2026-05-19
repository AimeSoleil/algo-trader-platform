from __future__ import annotations

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
        trade_service=SimpleNamespace(
            risk=SimpleNamespace(
                blueprint_limits=SimpleNamespace(
                    max_daily_loss=2_000.0,
                    max_margin_usage=0.5,
                    portfolio_delta_limit=0.5,
                    portfolio_gamma_limit=0.1,
                )
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
