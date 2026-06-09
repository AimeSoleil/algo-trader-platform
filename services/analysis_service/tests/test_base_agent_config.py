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
from services.analysis_service.app.llm.agents.chain_agent import ChainAgent
from services.analysis_service.app.llm.agents.trend_agent import TrendAgent
from services.analysis_service.app.llm.agents.volatility_agent import VolatilityAgent
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


class _GateRepairSymbol(BaseModel):
    symbol: str
    trade_allowed: bool = True


class _GateRepairOutput(BaseModel):
    symbols: list[_GateRepairSymbol] = Field(default_factory=list)


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


class _GateRepairAgent(base_agent.AnalysisAgent):
    @property
    def name(self) -> str:
        return "gate_repair"

    @property
    def system_prompt(self) -> str:
        return "system"

    @property
    def output_model(self):
        return _GateRepairOutput

    def extract_signal_data(self, signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return signals


class _RepairProvider:
    name = "deepseek"

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
                closeai=SimpleNamespace(temperature=0.33, max_tokens=32768),
                deepseek=SimpleNamespace(temperature=0.44, max_tokens=4444),
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


def test_resolve_generation_config_for_deepseek(monkeypatch) -> None:
    mock_settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                openai=SimpleNamespace(temperature=0.11, max_tokens=1111),
                closeai=SimpleNamespace(temperature=0.33, max_tokens=3333),
                deepseek=SimpleNamespace(temperature=0.44, max_tokens=4444),
                output_budget_ratio=0.8,
                output_truncation_threshold_ratio=0.95,
            )
        )
    )
    monkeypatch.setattr(base_agent, "get_settings", lambda: mock_settings)

    generation_config = base_agent._resolve_generation_config("deepseek")

    assert generation_config.temperature == 0.44
    assert generation_config.provider_max_tokens == 4444
    assert generation_config.request_max_tokens == int(4444 * 0.8)


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


def test_chain_agent_extracts_front_expiry_dte_from_option_vol_surface() -> None:
    agent = ChainAgent()

    extracted = agent.extract_signal_data([
        {
            "symbol": "AAPL",
            "option_vol_surface": {"iv_rank": 42.0, "front_expiry_dte": 0},
        }
    ])

    assert extracted == [{"symbol": "AAPL", "iv_rank": 42.0, "front_expiry_dte": 0}]


def test_chain_agent_extracts_execution_candidates_for_structure_liquidity_context() -> None:
    agent = ChainAgent()

    extracted = agent.extract_signal_data([
        {
            "symbol": "AAPL",
            "option_spreads": {
                "vertical_spread_risk_reward": 1.15,
                "execution_candidates": {
                    "vertical": {
                        "effective_rr": 0.92,
                        "worst_leg_bid_ask_spread_ratio": 0.08,
                    }
                },
            },
        }
    ])

    assert extracted == [{
        "symbol": "AAPL",
        "option_spreads": {
            "execution_candidates": {
                "vertical": {
                    "effective_rr": 0.92,
                    "worst_leg_bid_ask_spread_ratio": 0.08,
                }
            }
        },
    }]


def test_trend_agent_preserves_unknown_iv_rank() -> None:
    agent = TrendAgent()

    extracted = agent.extract_signal_data([
        {
            "symbol": "AAPL",
            "option_vol_surface": {"iv_rank": None},
        }
    ])

    assert extracted == [{"symbol": "AAPL", "iv_rank": None}]


def test_trend_analysis_preserves_null_iv_rank() -> None:
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

    assert parsed.symbols[0].iv_rank is None


def test_volatility_agent_extracts_bollinger_band_width_for_squeeze_logic() -> None:
    agent = VolatilityAgent()

    extracted = agent.extract_signal_data([
        {
            "symbol": "AAPL",
            "option_vol_surface": {"iv_rank": 22.0, "front_expiry_dte": 12},
            "stock_trend": {"bollinger_band_width": 0.012},
        }
    ])

    assert extracted == [{
        "symbol": "AAPL",
        "option_vol_surface": {"iv_rank": 22.0, "front_expiry_dte": 12},
        "stock_trend": {"bollinger_band_width": 0.012},
    }]


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        ("high", VolRegime.HIGH_VOL),
        ("low", VolRegime.LOW_VOL),
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


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        ("high_vol_backwardation_event_risk", VolRegime.BACKWARDATION_EVENT_RISK),
        ("low_vol_backwardation_event_risk", VolRegime.BACKWARDATION_EVENT_RISK),
        ("high_vol_contango_event_risk", VolRegime.HIGH_VOL_EVENT_RISK),
    ],
)
def test_volatility_analysis_collapses_unsupported_superset_regimes(raw_value: str, expected: VolRegime) -> None:
    parsed = VolatilityAnalysis.model_validate({
        "symbols": [
            {
                "symbol": "NVDA",
                "vol_regime": raw_value,
                "iv_rank_zone": "high",
                "iv_percentile_divergence": False,
                "hv_iv_assessment": "neutral",
                "garch_divergence": False,
                "garch_divergence_direction": None,
                "surface_mispricing": False,
                "event_risk_present": True,
                "liquidity_status": "high",
                "trade_allowed": True,
                "confidence_cap": None,
                "simple_structures_only": False,
                "blocked_reasons": [],
                "strategies": [],
                "reasoning": "unsupported superset regimes should collapse to the nearest supported conservative regime",
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
                closeai=SimpleNamespace(temperature=0.33, max_tokens=3333),
                deepseek=SimpleNamespace(temperature=0.44, max_tokens=4444),
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
                closeai=SimpleNamespace(temperature=0.33, max_tokens=3333),
                deepseek=SimpleNamespace(temperature=0.44, max_tokens=4444),
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


@pytest.mark.asyncio
async def test_analyze_does_not_repair_trade_gate_defaults(monkeypatch) -> None:
    mock_settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                max_retries=3,
                backoff_base_seconds=0,
                backoff_max_seconds=0,
                openai=SimpleNamespace(temperature=0.11, max_tokens=1111),
                closeai=SimpleNamespace(temperature=0.33, max_tokens=3333),
                deepseek=SimpleNamespace(temperature=0.44, max_tokens=4444),
                output_budget_ratio=0.8,
                output_truncation_threshold_ratio=0.95,
            )
        )
    )
    monkeypatch.setattr(base_agent, "get_settings", lambda: mock_settings)

    agent = _GateRepairAgent()
    provider = _RepairProvider(json.dumps({"symbols": [{"symbol": "AAPL", "trade_allowed": None}]}))

    with pytest.raises(ValidationError):
        await agent.analyze(
            [{"symbol": "AAPL", "price": 100}],
            provider=provider,
        )

    assert provider.calls == 1
