"""OpenAI provider for the agent pipeline.

Thin wrapper around ``AsyncOpenAI`` that satisfies the
``AgentLLMProvider`` protocol.  Used by the Orchestrator when
``settings.analysis_service.llm.provider == "openai"`` and as the default fallback
for standalone agent testing.
"""
from __future__ import annotations

import asyncio

from openai import AsyncOpenAI

from shared.config import get_settings
from shared.utils import get_logger

logger = get_logger("openai_agent_provider")

from services.analysis_service.app.llm.agents.base_agent import LLMResult


class OpenAIAgentProvider:
    """``AgentLLMProvider`` backed by the OpenAI Responses API."""

    def __init__(self) -> None:
        settings = get_settings()
        self._api_key = settings.analysis_service.llm.openai.api_key
        self._client: AsyncOpenAI | None = None
        self._bound_loop_id: int | None = None
        self._model = settings.analysis_service.llm.openai.model
        self._reasoning_effort = settings.analysis_service.llm.openai.reasoning_effort
        self._timeout = settings.analysis_service.llm.openai.request_timeout_seconds

    def _get_client(self) -> AsyncOpenAI:
        """Return the AsyncOpenAI client; rebuild when the event loop changes."""
        current_loop_id = id(asyncio.get_running_loop())

        if self._client is not None and self._bound_loop_id != current_loop_id:
            logger.info("openai_agent.loop_changed, rebuilding client")
            self._client = None

        if self._client is None:
            self._client = AsyncOpenAI(api_key=self._api_key)
            self._bound_loop_id = current_loop_id
            logger.info("openai_agent.client_created")

        return self._client

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
        model: str | None = None,
        agent_name: str | None = None,
    ) -> LLMResult:
        settings = get_settings()
        client = self._get_client()
        effective_model = model or self._model
        response = await client.responses.create(
            model=effective_model,
            instructions=instructions,
            input=user_prompt,
            text={"format": {"type": "json_object"}},
            reasoning={"effort": self._reasoning_effort},
            temperature=temperature if temperature is not None else settings.analysis_service.llm.openai.temperature,
            max_output_tokens=max_tokens or 16384,
            timeout=self._timeout,
        )

        usage = response.usage
        return LLMResult(
            content=response.output_text,
            raw_content=response.output_text,
            input_tokens=usage.input_tokens if usage else 0,
            output_tokens=usage.output_tokens if usage else 0,
            total_tokens=usage.total_tokens if usage else 0,
            model=self._model,
        )
