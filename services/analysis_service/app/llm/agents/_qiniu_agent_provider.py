"""Qiniu AI provider for the agent pipeline.

Thin wrapper around Qiniu's OpenAI-compatible Chat Completions API that
satisfies the ``AgentLLMProvider`` protocol.  Used by the Orchestrator when
``settings.analysis_service.llm.provider == "qiniu"``.

API docs: https://developer.qiniu.com/aitokenapi/13390/chat-completions
Endpoint: https://api.qnaigc.com/v1/chat/completions  (OpenAI-compatible)
Auth:     Authorization: Bearer <api_key>
"""
from __future__ import annotations

import asyncio

from openai import AsyncOpenAI

from shared.config import get_settings
from shared.utils import get_logger

from services.analysis_service.app.llm.agents.base_agent import LLMResult
from services.analysis_service.app.llm.json_utils import extract_json_str

logger = get_logger("qiniu_agent_provider")


class QiniuAgentProvider:
    """``AgentLLMProvider`` backed by Qiniu's OpenAI-compatible Chat API."""

    def __init__(self) -> None:
        settings = get_settings()
        cfg = settings.analysis_service.llm.qiniu
        self._api_key = cfg.api_key
        self._base_url = cfg.base_url
        self._model = cfg.model
        self._temperature = cfg.temperature
        self._timeout = cfg.request_timeout_seconds
        self._client: AsyncOpenAI | None = None
        self._bound_loop_id: int | None = None

    def _get_client(self) -> AsyncOpenAI:
        """Return the AsyncOpenAI client pointed at Qiniu; rebuild on loop change."""
        current_loop_id = id(asyncio.get_running_loop())

        if self._client is not None and self._bound_loop_id != current_loop_id:
            logger.info("qiniu_agent.loop_changed, rebuilding client")
            self._client = None

        if self._client is None:
            self._client = AsyncOpenAI(
                api_key=self._api_key,
                base_url=self._base_url,
            )
            self._bound_loop_id = current_loop_id
            logger.info("qiniu_agent.client_created", base_url=self._base_url, model=self._model)

        return self._client

    @property
    def name(self) -> str:  # noqa: D401
        return "qiniu"

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
        effective_temp = temperature if temperature is not None else self._temperature

        # Qiniu Chat Completions uses the standard messages format
        call_kwargs: dict = {
            "model": effective_model,
            "messages": [
                {"role": "system", "content": instructions},
                {
                    "role": "user",
                    "content": (
                        f"{user_prompt}\n\n"
                        "Final output: return ONLY one valid JSON object using "
                        "double-quoted keys and string values (RFC 8259). "
                        "No single quotes, no trailing commas, no markdown fences, "
                        "no extra text."
                    ),
                },
            ],
            "temperature": effective_temp,
            "max_tokens": max_tokens if max_tokens is not None else settings.analysis_service.llm.qiniu.max_tokens,
            "timeout": self._timeout,
        }

        # Anthropic models (claude-*) do not support response_format — rely on
        # the prompt instruction above.  All other models (gemini-*, gpt-*, etc.)
        # use json_object mode for more reliable structured output.
        if not effective_model.startswith("claude-"):
            call_kwargs["response_format"] = {"type": "json_object"}

        response = await client.chat.completions.create(**call_kwargs)

        content = response.choices[0].message.content or ""
        usage = response.usage

        logger.debug(
            "qiniu_agent.response_received",
            agent=agent_name,
            model=effective_model,
            content_len=len(content),
        )

        return LLMResult(
            content=extract_json_str(content),
            raw_content=content,
            input_tokens=usage.prompt_tokens if usage else 0,
            output_tokens=usage.completion_tokens if usage else 0,
            total_tokens=usage.total_tokens if usage else 0,
            model=effective_model,
        )
