"""DeepSeek provider for the agent pipeline.

Uses DeepSeek's Chat Completions API endpoint at
https://api.deepseek.com/chat/completions with official model names such as
``deepseek-v4-pro`` and ``deepseek-v4-flash``.
"""
from __future__ import annotations

import asyncio
from time import perf_counter

from openai import AsyncOpenAI

from shared.config import get_settings
from shared.utils import estimate_prompt_tokens, get_logger

from services.analysis_service.app.llm.agents.base_agent import LLMResult
from services.analysis_service.app.llm.json_utils import extract_json_str

logger = get_logger("deepseek_agent_provider")

_PATH_SUFFIXES = (
    "/anthropic/v1/messages",
    "/v1/chat/completions",
    "/chat/completions",
    "/v1/messages",
    "/messages",
    "/anthropic",
    "/v1",
)


def _normalize_base_url(base_url: str, default: str = "https://api.deepseek.com") -> str:
    raw = (base_url or "").strip()
    if not raw:
        return default

    normalized = raw.rstrip("/")
    for suffix in _PATH_SUFFIXES:
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
            break

    normalized = normalized.rstrip("/")
    return normalized or default


def _normalize_reasoning_effort(reasoning_effort: str) -> str:
    normalized = (reasoning_effort or "").strip().lower()
    if normalized in {"max", "xhigh"}:
        return "max"
    return "high"


class DeepSeekAgentProvider:
    """``AgentLLMProvider`` backed by DeepSeek chat completions."""

    def __init__(self) -> None:
        settings = get_settings()
        cfg = settings.analysis_service.llm.deepseek
        self._api_key = cfg.api_key
        self._raw_base_url = cfg.base_url
        self._base_url = _normalize_base_url(cfg.base_url)
        self._model = cfg.model
        self._reasoning_effort = _normalize_reasoning_effort(cfg.reasoning_effort)
        self._temperature = cfg.temperature
        self._max_tokens = cfg.max_tokens
        self._timeout = cfg.request_timeout_seconds
        self._client: AsyncOpenAI | None = None
        self._bound_loop_id: int | None = None

        if self._raw_base_url and self._raw_base_url.rstrip("/") != self._base_url.rstrip("/"):
            logger.info(
                "deepseek_agent.base_url_normalized",
                configured_base_url=self._raw_base_url,
                base_url=self._base_url,
            )

    @property
    def name(self) -> str:
        return "deepseek"

    def _get_client(self) -> AsyncOpenAI:
        current_loop_id = id(asyncio.get_running_loop())

        if self._client is not None and self._bound_loop_id != current_loop_id:
            self._client = None

        if self._client is None:
            self._client = AsyncOpenAI(
                api_key=self._api_key,
                base_url=self._base_url,
                max_retries=0,
            )
            self._bound_loop_id = current_loop_id
            logger.info("deepseek_agent.client_created", base_url=self._base_url, model=self._model)

        return self._client

    async def generate(
        self,
        *,
        instructions: str,
        user_prompt: str,
        temperature: float | None = None,
        max_tokens: int | None = None,
        model: str | None = None,
        agent_name: str | None = None,
        analysis_chunk_id: str | None = None,
    ) -> LLMResult:
        client = self._get_client()
        effective_model = model or self._model
        effective_temp = temperature if temperature is not None else self._temperature
        effective_max_tokens = max_tokens if max_tokens is not None else self._max_tokens
        json_instruction = (
            "Final output: return ONLY one valid JSON object using "
            "double-quoted keys and string values (RFC 8259). "
            "No single quotes, no trailing commas, no markdown fences, "
            "no extra text."
        )
        user_content = f"{user_prompt}\n\n{json_instruction}"

        input_prompt_tokens = estimate_prompt_tokens(instructions, user_content)
        logger.info(
            "deepseek_agent.request_started",
            analysis_chunk_id=analysis_chunk_id,
            agent=agent_name,
            model=effective_model,
            input_prompt_tokens=input_prompt_tokens,
        )

        started = perf_counter()
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
            extra_body={"thinking": {"type": "enabled"}},
            timeout=self._timeout,
        )
        api_latency_ms = round((perf_counter() - started) * 1000, 2)

        content = response.choices[0].message.content or ""
        logger.debug(
            "deepseek_agent.response_received",
            analysis_chunk_id=analysis_chunk_id,
            agent=agent_name,
            model=effective_model,
            content_len=len(content),
            api_latency_ms=api_latency_ms,
        )

        usage = response.usage
        return LLMResult(
            content=extract_json_str(content),
            raw_content=content,
            input_tokens=usage.prompt_tokens if usage else 0,
            output_tokens=usage.completion_tokens if usage else 0,
            total_tokens=usage.total_tokens if usage else 0,
            model=effective_model,
        )