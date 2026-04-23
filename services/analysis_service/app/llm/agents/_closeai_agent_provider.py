"""CloseAI provider for the agent pipeline.

Routes requests to the appropriate SDK based on model family:
- claude-*  -> Anthropic SDK -> https://api.openai-proxy.org/anthropic
- gemini-*  -> Google SDK    -> https://api.openai-proxy.org/google
- all others -> OpenAI SDK   -> https://api.openai-proxy.org/v1

Docs: https://doc.closeai-asia.com/tutorial/library.html
"""
from __future__ import annotations

import asyncio
from importlib import import_module
from typing import Any

from anthropic import AsyncAnthropic
from openai import AsyncOpenAI

from shared.config import get_settings
from shared.utils import estimate_prompt_tokens, get_logger

from services.analysis_service.app.llm.agents.base_agent import LLMResult
from services.analysis_service.app.llm.json_utils import extract_json_str

logger = get_logger("closeai_agent_provider")

_OPENAI_SUFFIXES = (
    "/v1/chat/completions",
    "/chat/completions",
    "/v1/responses",
    "/responses",
)

_ANTHROPIC_SUFFIXES = (
    "/anthropic/v1/messages",
    "/v1/messages",
    "/messages",
)

_GOOGLE_SUFFIXES = (
    "/google",
    "/google/",
)


def _provider_type_for_model(model: str) -> str:
    if model.startswith("claude-"):
        return "anthropic"
    if model.startswith("gemini-"):
        return "google"
    return "openai"


def _normalize_to_v1(base_url: str, default: str = "https://api.openai-proxy.org/v1") -> str:
    raw = (base_url or "").strip()
    if not raw:
        return default

    normalized = raw.rstrip("/")
    for suffix in _OPENAI_SUFFIXES + _ANTHROPIC_SUFFIXES + _GOOGLE_SUFFIXES:
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
            break

    for suffix in ("/anthropic", "/google"):
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
            break

    if not normalized.endswith("/v1"):
        normalized = f"{normalized}/v1"
    return normalized


def _normalize_for_anthropic(base_url: str, default: str = "https://api.openai-proxy.org/anthropic") -> str:
    raw = (base_url or "").strip()
    if not raw:
        return default

    normalized = raw.rstrip("/")
    for suffix in _OPENAI_SUFFIXES + _ANTHROPIC_SUFFIXES + _GOOGLE_SUFFIXES:
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
            break

    for suffix in ("/v1", "/google"):
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
            break

    if not normalized.endswith("/anthropic"):
        normalized = f"{normalized}/anthropic"
    return normalized


def _normalize_for_google(base_url: str, default: str = "https://api.openai-proxy.org/google") -> str:
    raw = (base_url or "").strip()
    if not raw:
        return default

    normalized = raw.rstrip("/")
    for suffix in _OPENAI_SUFFIXES + _ANTHROPIC_SUFFIXES + _GOOGLE_SUFFIXES:
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
            break

    if normalized.endswith("/v1"):
        normalized = normalized[: -len("/v1")]
    if normalized.endswith("/anthropic"):
        normalized = normalized[: -len("/anthropic")]

    if not normalized.endswith("/google"):
        normalized = f"{normalized}/google"
    return normalized


class CloseAIAgentProvider:
    """AgentLLMProvider backed by CloseAI's gateway."""

    def __init__(self) -> None:
        settings = get_settings()
        cfg = settings.analysis_service.llm.closeai
        self._api_key = cfg.api_key
        self._raw_base_url = cfg.base_url
        self._openai_base_url = _normalize_to_v1(cfg.base_url)
        self._anthropic_base_url = _normalize_for_anthropic(cfg.base_url)
        self._google_base_url = _normalize_for_google(cfg.base_url)
        self._model = cfg.model
        self._reasoning_effort = cfg.reasoning_effort
        self._temperature = cfg.temperature
        self._timeout = cfg.request_timeout_seconds
        self._openai_client: AsyncOpenAI | None = None
        self._anthropic_client: AsyncAnthropic | None = None
        self._google_client_root: Any | None = None
        self._google_client: Any | None = None
        self._openai_loop_id: int | None = None
        self._anthropic_loop_id: int | None = None
        self._google_loop_id: int | None = None

        if self._raw_base_url:
            logger.info(
                "closeai_agent.base_url_normalized",
                configured_base_url=self._raw_base_url,
                openai_base_url=self._openai_base_url,
                anthropic_base_url=self._anthropic_base_url,
                google_base_url=self._google_base_url,
            )

    @property
    def name(self) -> str:
        return "closeai"

    def _get_openai_client(self) -> AsyncOpenAI:
        current_loop_id = id(asyncio.get_running_loop())
        if self._openai_client is not None and self._openai_loop_id != current_loop_id:
            self._openai_client = None
        if self._openai_client is None:
            self._openai_client = AsyncOpenAI(
                api_key=self._api_key,
                base_url=self._openai_base_url,
                max_retries=0,
            )
            self._openai_loop_id = current_loop_id
            logger.info(
                "closeai_agent.openai_client_created",
                base_url=self._openai_base_url,
                model=self._model,
            )
        return self._openai_client

    def _get_anthropic_client(self) -> AsyncAnthropic:
        current_loop_id = id(asyncio.get_running_loop())
        if self._anthropic_client is not None and self._anthropic_loop_id != current_loop_id:
            self._anthropic_client = None
        if self._anthropic_client is None:
            self._anthropic_client = AsyncAnthropic(
                api_key=self._api_key,
                base_url=self._anthropic_base_url,
                timeout=self._timeout,
                max_retries=0,
            )
            self._anthropic_loop_id = current_loop_id
            logger.info(
                "closeai_agent.anthropic_client_created",
                base_url=self._anthropic_base_url,
                model=self._model,
            )
        return self._anthropic_client

    def _get_google_client(self) -> Any:
        from google import genai

        current_loop_id = id(asyncio.get_running_loop())
        if self._google_client is not None and self._google_loop_id != current_loop_id:
            self._google_client_root = None
            self._google_client = None

        if self._google_client is None:
            self._google_client_root = genai.Client(
                api_key=self._api_key,
                vertexai=True,
                http_options={
                    "base_url": self._google_base_url,
                    "timeout": self._timeout,
                },
            )
            self._google_client = self._google_client_root.aio
            self._google_loop_id = current_loop_id
            logger.info(
                "closeai_agent.google_client_created",
                base_url=self._google_base_url,
                model=self._model,
            )
        return self._google_client

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
        effective_model = model or self._model
        effective_temp = temperature if temperature is not None else self._temperature
        effective_max_tokens = max_tokens if max_tokens is not None else settings.analysis_service.llm.closeai.max_tokens
        json_instruction = (
            "Final output: return ONLY one valid JSON object using "
            "double-quoted keys and string values (RFC 8259). "
            "No single quotes, no trailing commas, no markdown fences, "
            "no extra text."
        )

        provider_type = _provider_type_for_model(effective_model)

        if provider_type == "anthropic":
            content, input_tokens, output_tokens, total_tokens = await self._generate_anthropic(
                effective_model=effective_model,
                instructions=instructions,
                user_prompt=user_prompt,
                json_instruction=json_instruction,
                effective_temp=effective_temp,
                effective_max_tokens=effective_max_tokens,
                agent_name=agent_name,
            )
        elif provider_type == "google":
            content, input_tokens, output_tokens, total_tokens = await self._generate_google(
                effective_model=effective_model,
                instructions=instructions,
                user_prompt=user_prompt,
                json_instruction=json_instruction,
                effective_temp=effective_temp,
                effective_max_tokens=effective_max_tokens,
                agent_name=agent_name,
            )
        else:
            content, input_tokens, output_tokens, total_tokens = await self._generate_openai(
                effective_model=effective_model,
                instructions=instructions,
                user_prompt=user_prompt,
                json_instruction=json_instruction,
                effective_temp=effective_temp,
                effective_max_tokens=effective_max_tokens,
                agent_name=agent_name,
            )

        logger.debug(
            "closeai_agent.response_received",
            agent=agent_name,
            model=effective_model,
            content_len=len(content),
        )

        return LLMResult(
            content=extract_json_str(content),
            raw_content=content,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=total_tokens,
            model=effective_model,
        )

    async def _generate_anthropic(
        self,
        *,
        effective_model: str,
        instructions: str,
        user_prompt: str,
        json_instruction: str,
        effective_temp: float,
        effective_max_tokens: int,
        agent_name: str | None,
    ) -> tuple[str, int, int, int]:
        client = self._get_anthropic_client()
        user_content = f"{user_prompt}\n\n{json_instruction}"
        input_prompt_tokens = estimate_prompt_tokens(instructions, user_content)
        logger.info(
            "closeai_agent.request_started",
            agent=agent_name,
            client_type="anthropic",
            model=effective_model,
            input_prompt_tokens=input_prompt_tokens,
        )
        response = await client.messages.create(
            model=effective_model,
            system=instructions,
            messages=[{"role": "user", "content": user_content}],
            temperature=effective_temp,
            max_tokens=effective_max_tokens,
        )
        content = response.content[0].text if response.content else ""
        usage = response.usage
        input_tokens = usage.input_tokens if usage else 0
        output_tokens = usage.output_tokens if usage else 0
        return content, input_tokens, output_tokens, input_tokens + output_tokens

    async def _generate_google(
        self,
        *,
        effective_model: str,
        instructions: str,
        user_prompt: str,
        json_instruction: str,
        effective_temp: float,
        effective_max_tokens: int,
        agent_name: str | None,
    ) -> tuple[str, int, int, int]:
        types = import_module("google.genai.types")

        client = self._get_google_client()
        user_content = f"{user_prompt}\n\n{json_instruction}"
        input_prompt_tokens = estimate_prompt_tokens(instructions, user_content)
        logger.info(
            "closeai_agent.request_started",
            agent=agent_name,
            client_type="google",
            model=effective_model,
            input_prompt_tokens=input_prompt_tokens,
        )
        response = await client.models.generate_content(
            model=effective_model,
            contents=user_content,
            config=types.GenerateContentConfig(
                system_instruction=instructions,
                temperature=effective_temp,
                max_output_tokens=effective_max_tokens,
                response_mime_type="application/json",
            ),
        )
        content = response.text or ""
        usage = response.usage_metadata
        input_tokens = getattr(usage, "prompt_token_count", 0) if usage else 0
        output_tokens = 0
        if usage:
            output_tokens = (
                getattr(usage, "candidates_token_count", None)
                or getattr(usage, "response_token_count", 0)
            )
        total_tokens = getattr(usage, "total_token_count", 0) if usage else 0
        if not total_tokens:
            total_tokens = input_tokens + output_tokens
        return content, input_tokens, output_tokens, total_tokens

    async def _generate_openai(
        self,
        *,
        effective_model: str,
        instructions: str,
        user_prompt: str,
        json_instruction: str,
        effective_temp: float,
        effective_max_tokens: int,
        agent_name: str | None,
    ) -> tuple[str, int, int, int]:
        client = self._get_openai_client()
        user_content = f"{user_prompt}\n\n{json_instruction}"
        input_prompt_tokens = estimate_prompt_tokens(instructions, user_content)
        logger.info(
            "closeai_agent.request_started",
            agent=agent_name,
            client_type="openai",
            model=effective_model,
            input_prompt_tokens=input_prompt_tokens,
        )
        response = await client.chat.completions.create(
            model=effective_model,
            messages=[
                {"role": "system", "content": instructions},
                {"role": "user", "content": user_content},
            ],
            reasoning_effort=self._reasoning_effort,
            temperature=effective_temp,
            max_tokens=effective_max_tokens,
            response_format={"type": "json_object"},
            timeout=self._timeout,
        )
        content = response.choices[0].message.content or ""
        usage = response.usage
        return (
            content,
            usage.prompt_tokens if usage else 0,
            usage.completion_tokens if usage else 0,
            usage.total_tokens if usage else 0,
        )