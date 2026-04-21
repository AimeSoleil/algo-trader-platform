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

from services.analysis_service.app.llm.json_utils import parse_llm_json

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
    model: str = ""


@dataclass
class LLMCallRecord:
    """One LLM invocation record for usage tracking."""

    agent: str
    provider: str
    model: str
    input_tokens: int
    output_tokens: int
    total_tokens: int
    duration_s: float


class LLMUsageTracker:
    """Accumulates LLM call records across a pipeline run."""

    def __init__(self) -> None:
        self.records: list[LLMCallRecord] = []

    def record(
        self,
        *,
        agent: str,
        provider: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        total_tokens: int,
        duration_s: float,
    ) -> None:
        self.records.append(LLMCallRecord(
            agent=agent,
            provider=provider,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=total_tokens,
            duration_s=duration_s,
        ))

    def summary(self) -> dict:
        """Return per-agent breakdown and grand totals."""
        by_agent: dict[str, dict] = {}
        total_input = total_output = total_total = 0
        total_calls = 0
        total_duration = 0.0

        for r in self.records:
            entry = by_agent.setdefault(r.agent, {
                "calls": 0,
                "model": r.model,
                "provider": r.provider,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "duration_s": 0.0,
            })
            entry["calls"] += 1
            entry["input_tokens"] += r.input_tokens
            entry["output_tokens"] += r.output_tokens
            entry["total_tokens"] += r.total_tokens
            entry["duration_s"] = round(entry["duration_s"] + r.duration_s, 3)

            total_input += r.input_tokens
            total_output += r.output_tokens
            total_total += r.total_tokens
            total_calls += 1
            total_duration += r.duration_s

        return {
            "agents": by_agent,
            "total": {
                "calls": total_calls,
                "input_tokens": total_input,
                "output_tokens": total_output,
                "total_tokens": total_total,
                "duration_s": round(total_duration, 3),
            },
        }

    def merge(self, other: "LLMUsageTracker") -> None:
        """Merge records from another tracker (for chunked pipelines)."""
        self.records.extend(other.records)


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
        model: str | None = None,
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
        usage_tracker: LLMUsageTracker | None = None,
        model: str | None = None,
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

        # max_retries=0 means "one attempt, no retries"; ≥1 means up to N+1 total attempts.
        max_attempts = max_retries + 1

        last_exc: Exception | None = None
        for attempt in range(max_attempts):
            t0 = perf_counter()
            status = "error"
            try:
                _max_tokens = 16384
                result = await provider.generate(
                    instructions=self.system_prompt,
                    user_prompt=user_prompt,
                    temperature=settings.analysis_service.llm.openai.temperature,
                    max_tokens=_max_tokens,
                    model=model,
                )

                # Detect output truncation: if the model hit the token ceiling,
                # the JSON is almost certainly incomplete.  Raise immediately so
                # we retry rather than trying to parse garbage.
                if result.output_tokens >= _max_tokens * 0.97:
                    logger.error(
                        f"agent.{self.name}.output_truncated",
                        provider=provider.name,
                        model=result.model,
                        output_tokens=result.output_tokens,
                        max_tokens=_max_tokens,
                        input_symbols=len(filtered),
                        attempt=attempt + 1,
                        raw_tail=result.content[-200:] if result.content else "",
                    )
                    logger.debug(
                        f"agent.{self.name}.output_truncated_raw",
                        provider=provider.name,
                        attempt=attempt + 1,
                        raw_content=result.content,
                    )
                    raise ValueError(
                        f"agent.{self.name} output likely truncated: "
                        f"output_tokens={result.output_tokens} >= "
                        f"{_max_tokens * 0.97:.0f} (97% of max_tokens={_max_tokens}). "
                        f"input_symbols={len(filtered)}"
                    )

                data = parse_llm_json(result.content)
                # LLM sometimes returns a bare list instead of
                # {"symbols": [...], ...} — wrap it so Pydantic
                # can validate against the output model.
                if isinstance(data, list):
                    data = {"symbols": data}
                parsed = self.output_model.model_validate(data)

                # Guard against silent empty-symbols: if the model returned
                # no symbols despite receiving input, the parse was probably
                # corrupted.  Raise so the caller marks this agent as failed.
                output_symbols = getattr(parsed, "symbols", None)
                if isinstance(output_symbols, list) and len(output_symbols) == 0 and len(filtered) > 0:
                    logger.error(
                        f"agent.{self.name}.empty_symbols",
                        provider=provider.name,
                        model=result.model,
                        output_tokens=result.output_tokens,
                        input_symbols=len(filtered),
                        attempt=attempt + 1,
                        raw_tail=result.content[-200:] if result.content else "",
                    )
                    raise ValueError(
                        f"agent.{self.name} returned 0 symbols for "
                        f"{len(filtered)} input signals — likely a truncation artifact"
                    )

                status = "ok"
                elapsed = perf_counter() - t0
                llm_tokens_total.labels(
                    provider=provider.name, direction="prompt",
                ).inc(result.input_tokens)
                llm_tokens_total.labels(
                    provider=provider.name, direction="completion",
                ).inc(result.output_tokens)

                if usage_tracker is not None:
                    usage_tracker.record(
                        agent=self.name,
                        provider=provider.name,
                        model=result.model,
                        input_tokens=result.input_tokens,
                        output_tokens=result.output_tokens,
                        total_tokens=result.total_tokens,
                        duration_s=round(elapsed, 3),
                    )

                logger.info(
                    f"agent.{self.name}.completed",
                    provider=provider.name,
                    model=result.model,
                    symbols=len(filtered),
                    input_tokens=result.input_tokens,
                    output_tokens=result.output_tokens,
                    total_tokens=result.total_tokens,
                )
                return parsed

            except (json.JSONDecodeError, ValidationError, ValueError) as e:
                last_exc = e
                llm_retries_total.labels(
                    provider=provider.name, error_type="parse",
                ).inc()
                logger.warning(
                    f"agent.{self.name}.parse_error",
                    provider=provider.name,
                    attempt=attempt + 1,
                    error=str(e),
                )
                if attempt < max_attempts - 1:
                    delay = min(
                        backoff_base * (2 ** attempt) + random.uniform(0, 1),
                        backoff_max,
                    )
                    await asyncio.sleep(delay)
                    continue
                raise

            except Exception as e:
                last_exc = e
                error_type = type(e).__name__
                retryable = error_type in (
                    "RateLimitError", "APITimeoutError",
                    "APIConnectionError", "InternalServerError",
                ) or (hasattr(e, "status_code") and getattr(e, "status_code", 0) >= 500)

                if retryable and attempt < max_attempts - 1:
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

        raise last_exc or RuntimeError(f"Agent {self.name} failed after {max_attempts} attempt(s)")


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
            "Analyze each symbol per your instructions. "
            "Output ONLY valid JSON (RFC 8259). No markdown fences."
        )

        return "\n\n".join(parts)
