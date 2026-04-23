"""Unit tests for CloseAI provider routing helpers."""
from __future__ import annotations

from services.analysis_service.app.llm.agents._closeai_agent_provider import (
    _normalize_for_anthropic,
    _normalize_for_google,
    _normalize_to_v1,
    _provider_type_for_model,
)


class TestCloseAIProviderRouting:
    def test_provider_type_for_model(self):
        assert _provider_type_for_model("claude-sonnet-4-20250514") == "anthropic"
        assert _provider_type_for_model("gemini-2.5-flash") == "google"
        assert _provider_type_for_model("gpt-5") == "openai"

    def test_normalize_google_from_v1(self):
        assert _normalize_for_google("https://api.openai-proxy.org/v1") == "https://api.openai-proxy.org/google"

    def test_normalize_google_from_anthropic(self):
        assert _normalize_for_google("https://api.openai-proxy.org/anthropic") == "https://api.openai-proxy.org/google"

    def test_normalize_openai_from_google(self):
        assert _normalize_to_v1("https://api.openai-proxy.org/google") == "https://api.openai-proxy.org/v1"

    def test_normalize_anthropic_from_google(self):
        assert _normalize_for_anthropic("https://api.openai-proxy.org/google") == "https://api.openai-proxy.org/anthropic"