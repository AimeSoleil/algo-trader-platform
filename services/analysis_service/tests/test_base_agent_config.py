from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.analysis_service.app.llm.agents import base_agent


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