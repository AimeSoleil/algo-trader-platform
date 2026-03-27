"""Base class for specialist analysis agents.

Each agent focuses on one dimension of analysis (trend, volatility, flow,
chain structure, spreads, cross-asset).  The agent receives:
- A focused system prompt with only its reference rules
- A subset of signal data (only relevant fields)
- Structured output schema
- A **provider** instance injected by the Orchestrator (no hardcoded LLM)

The base class handles JSON parsing, retries, and metrics.
Subclasses define the prompt, data extraction, and output model.
"""
from __future__ import annotations

import asyncio
import json
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from time import perf_counter
from typing import Any, Protocol, TypeVar, runtime_checkable

from pydantic import BaseModel, ValidationError

from shared.config import get_settings
from shared.metrics import llm_request_duration, llm_retries_total, llm_tokens_total
from shared.utils import get_logger

logger = get_logger("analysis_agent")


def _default_provider() -> "AgentLLMProvider":
    """Module-level helper: build the correct provider based on config."""
    settings = get_settings()
    provider_name = settings.analysis_service.llm.provider

    if provider_name == "copilot":
        from services.analysis_service.app.llm.agents._copilot_agent_provider import (
            CopilotAgentProvider,
        )
        return CopilotAgentProvider()

    from services.analysis_service.app.llm.agents._openai_agent_provider import (
        OpenAIAgentProvider,
    )
    return OpenAIAgentProvider()


T = TypeVar("T", bound=BaseModel)


# ── Provider protocol ─────────────────────────────────────────────────
# Any object that satisfies this interface can be used as a provider.
# The concrete implementations live in _openai_agent_provider.py and
# _copilot_agent_provider.py; they are thin wrappers around the SDK.


@dataclass
class LLMResult:
    """Standardised result envelope from any provider."""

    content: str
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0


@runtime_checkable
class AgentLLMProvider(Protocol):
    """Minimal interface that a provider must satisfy for agents."""

    @property
    def name(self) -> str:
        """Provider identifier (e.g. 'openai', 'copilot')."""
        ...

    async def generate(
        self,
        *,
        instructions: str,
        user_prompt: str,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> LLMResult:
        """Send prompt to LLM and return text + token counts."""
        ...


class AnalysisAgent(ABC):
    """Abstract base for specialist LLM analysis agents.

    Subclasses implement:
    - ``name`` — agent identifier (e.g. "trend", "volatility")
    - ``system_prompt`` — focused rules for this dimension
    - ``extract_signal_data`` — select relevant fields from full signal data
    - ``output_model`` — Pydantic model class for structured output
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Short identifier for this agent (used in logs and metrics)."""
        ...

    @property
    @abstractmethod
    def system_prompt(self) -> str:
        """System prompt containing only this agent's reference rules."""
        ...

    @property
    @abstractmethod
    def output_model(self) -> type[T]:
        """Pydantic model class for the agent's structured output."""
        ...

    @abstractmethod
    def extract_signal_data(self, signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Extract only the signal fields relevant to this agent.

        Parameters
        ----------
        signals:
            Full serialized signal data (list of per-symbol dicts).

        Returns
        -------
        list[dict]
            Filtered signal data with only the fields this agent needs.
        """
        ...

    # ------------------------------------------------------------------
    # LLM invocation — provider-agnostic
    # ------------------------------------------------------------------

    async def analyze(
        self,
        signals: list[dict[str, Any]],
        context: dict[str, Any] | None = None,
        *,
        provider: AgentLLMProvider | None = None,
    ) -> T:
        """Run analysis on the provided signal data.

        Parameters
        ----------
        signals:
            Full serialized signal data (will be filtered by extract_signal_data).
        context:
            Optional additional context (e.g. current positions, market summary).
        provider:
            LLM provider instance injected by the Orchestrator.
            If ``None``, falls back to a default OpenAI provider
            (for backward compatibility / standalone testing only).

        Returns
        -------
        T
            Structured analysis output matching ``output_model``.
        """
        if provider is None:
            provider = self._default_provider()

        # Extract relevant data subset
        filtered = self.extract_signal_data(signals)

        # Build user prompt
        user_prompt = self._build_user_prompt(filtered, context)

        settings = get_settings()
        max_retries = settings.analysis_service.llm.max_retries
        backoff_base = settings.analysis_service.llm.backoff_base_seconds
        backoff_max = settings.analysis_service.llm.backoff_max_seconds

        last_exc: Exception | None = None
        for attempt in range(max_retries):
            t0 = perf_counter()
            status = "error"
            try:
                result = await provider.generate(
                    instructions=self.system_prompt,
                    user_prompt=user_prompt,
                    temperature=settings.analysis_service.llm.openai.temperature,
                    max_tokens=4096,
                )

                data = json.loads(result.content)
                parsed = self.output_model.model_validate(data)

                status = "ok"
                llm_tokens_total.labels(
                    provider=provider.name, direction="prompt",
                ).inc(result.input_tokens)
                llm_tokens_total.labels(
                    provider=provider.name, direction="completion",
                ).inc(result.output_tokens)

                logger.info(
                    f"agent.{self.name}.completed",
                    provider=provider.name,
                    symbols=len(filtered),
                    tokens=result.total_tokens,
                )
                return parsed

            except (json.JSONDecodeError, ValidationError) as e:
                llm_retries_total.labels(
                    provider=provider.name, error_type="parse",
                ).inc()
                logger.warning(
                    f"agent.{self.name}.parse_error",
                    provider=provider.name,
                    attempt=attempt + 1,
                    error=str(e),
                )
                raise

            except Exception as e:
                last_exc = e
                error_type = type(e).__name__
                retryable = error_type in (
                    "RateLimitError", "APITimeoutError",
                    "APIConnectionError", "InternalServerError",
                ) or (hasattr(e, "status_code") and getattr(e, "status_code", 0) >= 500)

                if retryable and attempt < max_retries - 1:
                    delay = min(
                        backoff_base * (2 ** attempt) + random.uniform(0, 1),
                        backoff_max,
                    )
                    llm_retries_total.labels(
                        provider=provider.name, error_type=error_type,
                    ).inc()
                    logger.warning(
                        f"agent.{self.name}.retryable_error",
                        provider=provider.name,
                        attempt=attempt + 1,
                        error=str(e),
                        retry_delay_s=round(delay, 2),
                    )
                    await asyncio.sleep(delay)
                    continue

                logger.warning(
                    f"agent.{self.name}.failed",
                    provider=provider.name,
                    attempt=attempt + 1,
                    error=str(e),
                    retryable=retryable,
                )
                raise

            finally:
                elapsed = perf_counter() - t0
                llm_request_duration.labels(
                    provider=provider.name, agent=self.name, status=status,
                ).observe(elapsed)

        raise last_exc or RuntimeError(f"Agent {self.name} failed after {max_retries} retries")


    def _default_provider(self) -> AgentLLMProvider:
        """Build a default OpenAI provider for standalone / test usage."""
        return _default_provider()

    def _build_user_prompt(
        self,
        filtered_signals: list[dict[str, Any]],
        context: dict[str, Any] | None,
    ) -> str:
        """Build the user prompt with signal data and optional context."""
        parts: list[str] = []

        parts.append("## Signal Data\n")
        for sig in filtered_signals:
            symbol = sig.get("symbol", "UNKNOWN")
            data = {k: v for k, v in sig.items() if k != "symbol" and v}
            compact = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
            parts.append(f"### {symbol}\n{compact}")

        if context:
            parts.append("\n## Context\n")
            parts.append(json.dumps(context, indent=2, ensure_ascii=False))

        parts.append(
            "\n## Task\n"
            "Analyze each symbol using the rules in your instructions. "
            "Output ONLY valid JSON matching the expected schema. "
            "No markdown fences, no extra text."
        )

        return "\n\n".join(parts)
