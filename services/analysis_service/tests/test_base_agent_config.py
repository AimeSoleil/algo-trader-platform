from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from pydantic import BaseModel, Field, ValidationError
import pytest

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.analysis_service.app.llm.agents import base_agent
from services.analysis_service.app.llm.agents.models import TrendAnalysis, VolRegime, VolatilityAnalysis


class _DummyOutputModel:
    @classmethod
    def model_validate(cls, data: dict[str, Any]) -> dict[str, Any]:
        return data


class _DummyAgent(base_agent.AnalysisAgent):
    @property
    def name(self) -> str:
        return "dummy"

    @property
    def system_prompt(self) -> str:
        return "system"

    @property
    def output_model(self):
        return _DummyOutputModel

    def extract_signal_data(self, signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return signals


class _RepairSymbol(BaseModel):
    symbol: str
    iv_rank: float = 0.0


class _RepairOutput(BaseModel):
    symbols: list[_RepairSymbol] = Field(default_factory=list)


class _RepairAgent(base_agent.AnalysisAgent):
    @property
    def name(self) -> str:
        return "repair"

    @property
    def system_prompt(self) -> str:
        return "system"

    @property
    def output_model(self):
        return _RepairOutput

    def extract_signal_data(self, signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return signals


class _RepairProvider:
    name = "copilot"

    def __init__(self, content: str) -> None:
        self.content = content
        self.calls = 0

    async def generate(self, **kwargs) -> base_agent.LLMResult:
        self.calls += 1
        return base_agent.LLMResult(
            content=self.content,
            raw_content=self.content,
            input_tokens=10,
            output_tokens=10,
            total_tokens=20,
            model="test-model",
        )


def test_resolve_generation_config_for_closeai(monkeypatch) -> None:
    mock_settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                openai=SimpleNamespace(temperature=0.11, max_tokens=1111),
                qiniu=SimpleNamespace(temperature=0.22, max_tokens=2222),
                closeai=SimpleNamespace(temperature=0.33, max_tokens=32768),
                output_budget_ratio=0.8,
                output_truncation_threshold_ratio=0.95,
            )
        )
    )
    monkeypatch.setattr(base_agent, "get_settings", lambda: mock_settings)

    generation_config = base_agent._resolve_generation_config("closeai")

    assert generation_config.temperature == 0.33
    assert generation_config.provider_max_tokens == 32768
    assert generation_config.request_max_tokens == int(32768 * 0.8)
    assert generation_config.truncation_threshold_tokens == int(int(32768 * 0.8) * 0.95)


def test_resolve_generation_config_for_copilot_uses_fallback(monkeypatch) -> None:
    mock_settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                openai=SimpleNamespace(temperature=0.11, max_tokens=1111),
                qiniu=SimpleNamespace(temperature=0.22, max_tokens=2222),
                closeai=SimpleNamespace(temperature=0.33, max_tokens=3333),
                output_budget_ratio=0.8,
                output_truncation_threshold_ratio=0.95,
            )
        )
    )
    monkeypatch.setattr(base_agent, "get_settings", lambda: mock_settings)

    generation_config = base_agent._resolve_generation_config("copilot")

    assert generation_config.temperature is None
    assert generation_config.provider_max_tokens == 16384
    assert generation_config.request_max_tokens == int(16384 * 0.8)


def test_build_user_prompt_includes_compact_output_guidance() -> None:
    agent = _DummyAgent()

    prompt = agent._build_user_prompt(
        [{"symbol": "AAPL", "price": 100, "stock_trend": {"adx": 30}}],
        None,
    )

    assert "Keep output compact" in prompt
    assert "at most 2 short sentences" in prompt
    assert "at most 1 short sentence" in prompt
    assert "Avoid long reasoning paragraphs or chain-of-thought" in prompt


def test_trend_analysis_coerces_null_iv_rank() -> None:
    parsed = TrendAnalysis.model_validate({
        "symbols": [
            {
                "symbol": "AAPL",
                "regime": "neutral",
                "trend_direction": "neutral",
                "trend_strength": 0.2,
                "adx_zone": "transition",
                "adx_z_score": 0.0,
                "iv_rank": None,
                "divergence_detected": False,
                "divergence_type": None,
                "false_positive_risk": "medium",
                "trade_allowed": True,
                "confidence_cap": None,
                "simple_structures_only": False,
                "blocked_reasons": [],
                "strategies": [],
                "reasoning": "missing iv rank should not fail validation",
                "confidence": 0.4,
            }
        ],
        "market_trend_summary": "ok",
    })

    assert parsed.symbols[0].iv_rank == 0.0


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        ("neutral", VolRegime.NORMAL),
        ("contango", VolRegime.CONTANGO),
        ("high_vol_contango", VolRegime.HIGH_VOL_CONTANGO),
        ("low_vol_contango", VolRegime.LOW_VOL_CONTANGO),
        ("high_vol_backwardation", VolRegime.HIGH_VOL_BACKWARDATION),
        ("low_vol_backwardation", VolRegime.LOW_VOL_BACKWARDATION),
    ],
)
def test_volatility_analysis_accepts_supported_regimes(raw_value: str, expected: VolRegime) -> None:
    parsed = VolatilityAnalysis.model_validate({
        "symbols": [
            {
                "symbol": "NVDA",
                "vol_regime": raw_value,
                "iv_rank_zone": "low",
                "iv_percentile_divergence": False,
                "hv_iv_assessment": "neutral",
                "garch_divergence": False,
                "garch_divergence_direction": None,
                "surface_mispricing": False,
                "event_risk_present": False,
                "liquidity_status": "high",
                "trade_allowed": True,
                "confidence_cap": None,
                "simple_structures_only": False,
                "blocked_reasons": [],
                "strategies": [],
                "reasoning": "supported volatility regime should validate without degradation",
                "confidence": 0.4,
            }
        ],
        "market_vol_summary": "ok",
    })

    assert parsed.symbols[0].vol_regime == expected


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        ("contango high vol", VolRegime.HIGH_VOL_CONTANGO),
        ("backwardation_low_vol", VolRegime.LOW_VOL_BACKWARDATION),
    ],
)
def test_volatility_analysis_normalizes_supported_regime_token_order(raw_value: str, expected: VolRegime) -> None:
    parsed = VolatilityAnalysis.model_validate({
        "symbols": [
            {
                "symbol": "NVDA",
                "vol_regime": raw_value,
                "iv_rank_zone": "low",
                "iv_percentile_divergence": False,
                "hv_iv_assessment": "neutral",
                "garch_divergence": False,
                "garch_divergence_direction": None,
                "surface_mispricing": False,
                "event_risk_present": False,
                "liquidity_status": "high",
                "trade_allowed": True,
                "confidence_cap": None,
                "simple_structures_only": False,
                "blocked_reasons": [],
                "strategies": [],
                "reasoning": "token-order variants should normalize to the canonical supported volatility regime",
                "confidence": 0.4,
            }
        ],
        "market_vol_summary": "ok",
    })

    assert parsed.symbols[0].vol_regime == expected


@pytest.mark.asyncio
async def test_analyze_repairs_null_numeric_field_without_provider_retry(monkeypatch) -> None:
    mock_settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                max_retries=0,
                backoff_base_seconds=0,
                backoff_max_seconds=0,
                openai=SimpleNamespace(temperature=0.11, max_tokens=1111),
                qiniu=SimpleNamespace(temperature=0.22, max_tokens=2222),
                closeai=SimpleNamespace(temperature=0.33, max_tokens=3333),
                output_budget_ratio=0.8,
                output_truncation_threshold_ratio=0.95,
            )
        )
    )
    monkeypatch.setattr(base_agent, "get_settings", lambda: mock_settings)

    agent = _RepairAgent()
    provider = _RepairProvider(json.dumps({"symbols": [{"symbol": "AAPL", "iv_rank": None}]}))

    parsed = await agent.analyze(
        [{"symbol": "AAPL", "price": 100}],
        provider=provider,
    )

    assert parsed.symbols[0].iv_rank == 0.0
    assert provider.calls == 1


@pytest.mark.asyncio
async def test_analyze_unrepairable_validation_error_does_not_retry(monkeypatch) -> None:
    mock_settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                max_retries=3,
                backoff_base_seconds=0,
                backoff_max_seconds=0,
                openai=SimpleNamespace(temperature=0.11, max_tokens=1111),
                qiniu=SimpleNamespace(temperature=0.22, max_tokens=2222),
                closeai=SimpleNamespace(temperature=0.33, max_tokens=3333),
                output_budget_ratio=0.8,
                output_truncation_threshold_ratio=0.95,
            )
        )
    )
    monkeypatch.setattr(base_agent, "get_settings", lambda: mock_settings)

    agent = _RepairAgent()
    provider = _RepairProvider(json.dumps({"symbols": [{"iv_rank": 42.0}]}))

    with pytest.raises(ValidationError):
        await agent.analyze(
            [{"symbol": "AAPL", "price": 100}],
            provider=provider,
        )

    assert provider.calls == 1
