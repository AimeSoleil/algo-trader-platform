"""OpenAI provider for the agent pipeline.

Thin wrapper around ``AsyncOpenAI`` that satisfies the
``AgentLLMProvider`` protocol.  Used by the Orchestrator when
``settings.llm.provider == "openai"`` and as the default fallback
for standalone agent testing.
"""
from __future__ import annotations

from openai import AsyncOpenAI

from shared.config import get_settings

from services.analysis_service.app.llm.agents.base_agent import LLMResult


class OpenAIAgentProvider:
    """``AgentLLMProvider`` backed by the OpenAI Responses API."""

    def __init__(self) -> None:
        settings = get_settings()
        self._client = AsyncOpenAI(api_key=settings.llm.openai.api_key)
        self._model = settings.llm.openai.model
        self._timeout = settings.llm.openai.request_timeout_seconds

    @property
    def name(self) -> str:  # noqa: D401
        return "openai"

    async def generate(
        self,
        *,
        instructions: str,
        user_prompt: str,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> LLMResult:
        settings = get_settings()
        response = await self._client.responses.create(
            model=self._model,
            instructions=instructions,
            input=user_prompt,
            text={"format": {"type": "json_object"}},
            temperature=temperature if temperature is not None else settings.llm.openai.temperature,
            max_output_tokens=max_tokens or 4096,
            timeout=self._timeout,
        )

        usage = response.usage
        return LLMResult(
            content=response.output_text,
            input_tokens=usage.input_tokens if usage else 0,
            output_tokens=usage.output_tokens if usage else 0,
            total_tokens=usage.total_tokens if usage else 0,
        )
