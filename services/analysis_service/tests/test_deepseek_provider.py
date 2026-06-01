from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from services.analysis_service.app.llm.agents import _deepseek_agent_provider as deepseek_provider


class TestDeepSeekProviderHelpers:
    def test_normalize_base_url_from_v1(self) -> None:
        assert deepseek_provider._normalize_base_url("https://api.deepseek.com/v1") == "https://api.deepseek.com"

    def test_normalize_base_url_from_chat_completions(self) -> None:
        assert (
            deepseek_provider._normalize_base_url("https://api.deepseek.com/chat/completions")
            == "https://api.deepseek.com"
        )

    def test_normalize_base_url_from_anthropic(self) -> None:
        assert (
            deepseek_provider._normalize_base_url("https://api.deepseek.com/anthropic/v1/messages")
            == "https://api.deepseek.com"
        )

    def test_normalize_reasoning_effort(self) -> None:
        assert deepseek_provider._normalize_reasoning_effort("max") == "max"
        assert deepseek_provider._normalize_reasoning_effort("xhigh") == "max"
        assert deepseek_provider._normalize_reasoning_effort("medium") == "high"


@pytest.mark.asyncio
async def test_generate_uses_deepseek_chat_completions(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                deepseek=SimpleNamespace(
                    api_key="test-key",
                    base_url="https://api.deepseek.com/v1",
                    model="deepseek-v4-pro",
                    reasoning_effort="medium",
                    temperature=0.25,
                    max_tokens=2048,
                    request_timeout_seconds=30,
                )
            )
        )
    )
    monkeypatch.setattr(deepseek_provider, "get_settings", lambda: mock_settings)

    provider = deepseek_provider.DeepSeekAgentProvider()
    calls: list[dict] = []

    class _FakeCompletions:
        async def create(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(
                choices=[SimpleNamespace(message=SimpleNamespace(content="```json\n{\"ok\": true}\n```"))],
                usage=SimpleNamespace(prompt_tokens=12, completion_tokens=5, total_tokens=17),
            )

    fake_client = SimpleNamespace(chat=SimpleNamespace(completions=_FakeCompletions()))
    monkeypatch.setattr(provider, "_get_client", lambda: fake_client)

    result = await provider.generate(
        instructions="system",
        user_prompt="user",
        agent_name="trend",
        analysis_chunk_id="chunk-1",
    )

    assert json.loads(result.content) == {"ok": True}
    assert result.model == "deepseek-v4-pro"
    assert result.total_tokens == 17
    assert calls[0]["model"] == "deepseek-v4-pro"
    assert calls[0]["reasoning_effort"] == "high"
    assert calls[0]["response_format"] == {"type": "json_object"}
    assert calls[0]["extra_body"] == {"thinking": {"type": "enabled"}}