"""Qiniu AI provider for the agent pipeline.

Routes requests to the appropriate SDK based on model family:
- claude-*  -> Anthropic SDK  -> /v1/messages
- all others -> OpenAI SDK   -> /v1/chat/completions

API docs: https://developer.qiniu.com/aitokenapi/13390/chat-completions
Auth:     Authorization: Bearer <api_key>
"""
from __future__ import annotations

import asyncio
from time import perf_counter
from typing import Any

from anthropic import AsyncAnthropic
from openai import AsyncOpenAI

from shared.config import get_settings
from shared.utils import estimate_prompt_tokens, get_logger

from services.analysis_service.app.llm.agents.base_agent import LLMResult
from services.analysis_service.app.llm.json_utils import extract_json_str

logger = get_logger("qiniu_agent_provider")

_PATH_SUFFIXES = (
    "/v1/chat/completions",
    "/chat/completions",
    "/v1/messages",
    "/messages",
)


def _strip_path_suffix(url: str) -> str:
    normalized = url.rstrip("/")
    for suffix in _PATH_SUFFIXES:
        if normalized.endswith(suffix):
            return normalized[: -len(suffix)]
    return normalized


def _normalize_to_v1(base_url: str, default: str = "https://api.qnaigc.com/v1") -> str:
    raw = (base_url or "").strip()
    if not raw:
        return default
    normalized = _strip_path_suffix(raw).rstrip("/")
    if not normalized.endswith("/v1"):
        normalized = f"{normalized}/v1"
    return normalized


def _normalize_for_anthropic(base_url: str, default: str = "https://api.qnaigc.com") -> str:
    """Normalize base URL for Anthropic SDK.

    Anthropic SDK appends `/v1/messages` internally, so base_url must not end
    with `/v1`.
    """
    raw = (base_url or "").strip()
    if not raw:
        return default

    normalized = _strip_path_suffix(raw).rstrip("/")
    if normalized.endswith("/v1"):
        normalized = normalized[: -len("/v1")]
    return normalized.rstrip("/")


def _is_route_not_found_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "404" in message and ("route not found" in message or "not found" in message)


class QiniuAgentProvider:
    """AgentLLMProvider backed by Qiniu's AI gateway.

    Uses the Anthropic SDK for claude-* models (/v1/messages) and the
    OpenAI SDK for everything else (/v1/chat/completions).
    """

    def __init__(self) -> None:
        settings = get_settings()
        cfg = settings.analysis_service.llm.qiniu
        self._api_key = cfg.api_key
        self._raw_base_url = cfg.base_url
        self._openai_base_url = _normalize_to_v1(cfg.base_url)
        self._anthropic_base_url = _normalize_for_anthropic(cfg.base_url)
        self._model = cfg.model
        self._reasoning_effort = cfg.reasoning_effort
        self._temperature = cfg.temperature
        self._timeout = cfg.request_timeout_seconds
        self._openai_client: AsyncOpenAI | None = None
        self._anthropic_client: AsyncAnthropic | None = None
        self._openai_loop_id: int | None = None
        self._anthropic_loop_id: int | None = None
        self._logged_model_lists: set[str] = set()

        if self._raw_base_url and self._raw_base_url.rstrip("/") != self._openai_base_url.rstrip("/"):
            logger.info(
                "qiniu_agent.base_url_normalized",
                configured_base_url=self._raw_base_url,
                openai_base_url=self._openai_base_url,
                anthropic_base_url=self._anthropic_base_url,
            )

    @property
    def name(self) -> str:
        return "qiniu"

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
            logger.info("qiniu_agent.openai_client_created", base_url=self._openai_base_url, model=self._model)
            self._log_model_list_once("openai", self._openai_client)
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
            logger.info("qiniu_agent.anthropic_client_created", base_url=self._anthropic_base_url, model=self._model)
            self._log_model_list_once("anthropic", self._anthropic_client)
        return self._anthropic_client

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
        settings = get_settings()
        effective_model = model or self._model
        effective_temp = temperature if temperature is not None else self._temperature
        effective_max_tokens = max_tokens if max_tokens is not None else settings.analysis_service.llm.qiniu.max_tokens
        json_instruction = (
            "Final output: return ONLY one valid JSON object using "
            "double-quoted keys and string values (RFC 8259). "
            "No single quotes, no trailing commas, no markdown fences, "
            "no extra text."
        )

        if effective_model.startswith("claude-"):
            content, input_tokens, output_tokens, total_tokens, api_latency_ms = await self._generate_anthropic(
                effective_model=effective_model,
                instructions=instructions,
                user_prompt=user_prompt,
                json_instruction=json_instruction,
                effective_temp=effective_temp,
                effective_max_tokens=effective_max_tokens,
                agent_name=agent_name,
                analysis_chunk_id=analysis_chunk_id,
            )
        else:
            content, input_tokens, output_tokens, total_tokens, api_latency_ms = await self._generate_openai(
                effective_model=effective_model,
                instructions=instructions,
                user_prompt=user_prompt,
                json_instruction=json_instruction,
                effective_temp=effective_temp,
                effective_max_tokens=effective_max_tokens,
                agent_name=agent_name,
                analysis_chunk_id=analysis_chunk_id,
            )

        logger.debug(
            "qiniu_agent.response_received",
            analysis_chunk_id=analysis_chunk_id,
            agent=agent_name,
            model=effective_model,
            content_len=len(content),
            api_latency_ms=api_latency_ms,
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
        analysis_chunk_id: str | None,
    ) -> tuple[str, int, int, int, float]:
        """Call Qiniu via the Anthropic SDK (claude-* models -> /v1/messages)."""
        client = self._get_anthropic_client()
        user_content = f"{user_prompt}\n\n{json_instruction}"
        input_prompt_tokens = estimate_prompt_tokens(instructions, user_content)
        logger.info(
            "qiniu_agent.request_started",
            analysis_chunk_id=analysis_chunk_id,
            agent=agent_name,
            client_type="anthropic",
            model=effective_model,
            input_prompt_tokens=input_prompt_tokens,
        )
        started = perf_counter()
        response = await client.messages.create(
            model=effective_model,
            system=instructions,
            messages=[{"role": "user", "content": user_content}],
            temperature=effective_temp,
            max_tokens=effective_max_tokens,
        )
        api_latency_ms = round((perf_counter() - started) * 1000, 2)
        content = response.content[0].text if response.content else ""
        usage = response.usage
        input_tokens = usage.input_tokens if usage else 0
        output_tokens = usage.output_tokens if usage else 0
        return content, input_tokens, output_tokens, input_tokens + output_tokens, api_latency_ms

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
        analysis_chunk_id: str | None,
    ) -> tuple[str, int, int, int, float]:
        """Call Qiniu via the OpenAI SDK (non-claude models -> /v1/chat/completions)."""
        client = self._get_openai_client()
        user_content = f"{user_prompt}\n\n{json_instruction}"
        input_prompt_tokens = estimate_prompt_tokens(instructions, user_content)
        logger.info(
            "qiniu_agent.request_started",
            analysis_chunk_id=analysis_chunk_id,
            agent=agent_name,
            client_type="openai",
            model=effective_model,
            input_prompt_tokens=input_prompt_tokens,
        )
        started = perf_counter()
        call_kwargs: dict = {
            "model": effective_model,
            "messages": [
                {"role": "system", "content": instructions},
                {"role": "user", "content": user_content},
            ],
            "reasoning_effort": self._reasoning_effort,
            "temperature": effective_temp,
            "max_tokens": effective_max_tokens,
            "response_format": {"type": "json_object"},
            "timeout": self._timeout,
        }

        try:
            response = await client.chat.completions.create(**call_kwargs)
        except Exception as exc:
            if not _is_route_not_found_error(exc):
                raise

            fallback_base_url = (
                self._openai_base_url[: -len("/v1")]
                if self._openai_base_url.endswith("/v1")
                else self._openai_base_url
            )
            if fallback_base_url == self._openai_base_url:
                raise

            logger.warning(
                "qiniu_agent.route_not_found_retry",
                model=effective_model,
                base_url=self._openai_base_url,
                fallback_base_url=fallback_base_url,
                error=str(exc),
            )
            fallback_client = AsyncOpenAI(api_key=self._api_key, base_url=fallback_base_url)
            response = await fallback_client.chat.completions.create(**call_kwargs)
            self._openai_base_url = fallback_base_url
            self._openai_client = fallback_client
            self._openai_loop_id = id(asyncio.get_running_loop())

        api_latency_ms = round((perf_counter() - started) * 1000, 2)
        content = response.choices[0].message.content or ""
        usage = response.usage
        return (
            content,
            usage.prompt_tokens if usage else 0,
            usage.completion_tokens if usage else 0,
            usage.total_tokens if usage else 0,
            api_latency_ms,
        )

    def _log_model_list_once(self, client_type: str, client: Any) -> None:
        if client_type in self._logged_model_lists:
            return

        self._logged_model_lists.add(client_type)
        asyncio.create_task(self._log_model_list_worker(client_type, client))

    async def _log_model_list_worker(self, client_type: str, client: Any) -> None:
        try:
            # 获取可用模型列表
            models = await client.models.list()
            model_ids = [model.id for model in models.data]
            logger.debug(
                f"qiniu_agent.{client_type}_supported_models",
                model_ids=model_ids,
                model_count=len(model_ids),
            )
        except Exception as exc:
            logger.warning(f"qiniu_agent.{client_type}_supported_models_failed", error=str(exc))
