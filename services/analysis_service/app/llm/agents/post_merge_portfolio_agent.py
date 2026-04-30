"""PostMergePortfolioAgent — LLM-assisted ranking and explanation for merged plans.

This agent does NOT modify plan contents. It only suggests ordering,
selection preference, and portfolio-level narrative after chunk merge.
"""
from __future__ import annotations

import asyncio
import json
import random
from time import perf_counter
from typing import Any

from pydantic import ValidationError

from shared.config import get_settings
from shared.metrics import llm_request_duration, llm_retries_total, llm_tokens_total
from shared.utils import decode_escaped_unicode, get_logger

from services.analysis_service.app.llm.agents.base_agent import AgentLLMProvider, LLMUsageTracker, _default_provider
from services.analysis_service.app.llm.agents.critic_agent import _is_http_500_error
from services.analysis_service.app.llm.agents.models import PostMergeReview
from services.analysis_service.app.llm.json_utils import parse_llm_json

logger = get_logger("post_merge_portfolio_agent")


class PostMergePortfolioAgent:
    """LLM-assisted reviewer for global ranking after chunk merge."""

    async def review(
        self,
        *,
        candidate_summaries: list[dict[str, Any]],
        current_positions: dict | None = None,
        previous_execution: dict | None = None,
        chunk_limit_proposals: list[dict[str, Any]] | None = None,
        selector_metadata: dict[str, Any] | None = None,
        max_total_positions: int,
        provider: AgentLLMProvider | None = None,
        usage_tracker: LLMUsageTracker | None = None,
        model: str | None = None,
    ) -> PostMergeReview:
        if provider is None:
            provider = _default_provider()

        prompt = self._build_prompt(
            candidate_summaries=candidate_summaries,
            current_positions=current_positions,
            previous_execution=previous_execution,
            chunk_limit_proposals=chunk_limit_proposals or [],
            selector_metadata=selector_metadata or {},
            max_total_positions=max_total_positions,
        )

        settings = get_settings()
        max_retries = settings.analysis_service.llm.max_retries
        backoff_base = settings.analysis_service.llm.backoff_base_seconds
        backoff_max = settings.analysis_service.llm.backoff_max_seconds
        max_attempts = max_retries + 1
        forced_500_retry_used = False

        last_exc: Exception | None = None
        for attempt in range(max_attempts + 1):
            t0 = perf_counter()
            status = "error"
            try:
                result = await provider.generate(
                    instructions=_POST_MERGE_SYSTEM_PROMPT,
                    user_prompt=prompt,
                    temperature=0.0,
                    max_tokens=8192,
                    model=model,
                    agent_name="post_merge",
                )

                data = parse_llm_json(result.content)
                review = PostMergeReview.model_validate(data)
                status = "ok"
                elapsed = perf_counter() - t0

                llm_tokens_total.labels(provider=provider.name, direction="prompt").inc(result.input_tokens)
                llm_tokens_total.labels(provider=provider.name, direction="completion").inc(result.output_tokens)

                if usage_tracker is not None:
                    usage_tracker.record(
                        agent="post_merge",
                        provider=provider.name,
                        model=result.model,
                        input_tokens=result.input_tokens,
                        output_tokens=result.output_tokens,
                        total_tokens=result.total_tokens,
                        duration_s=round(elapsed, 3),
                    )

                logger.info(
                    "post_merge.completed",
                    provider=provider.name,
                    model=result.model,
                    selected=len(review.selected_symbols),
                    ranked=len(review.ranking),
                    input_tokens=result.input_tokens,
                    output_tokens=result.output_tokens,
                    total_tokens=result.total_tokens,
                )
                return review

            except (json.JSONDecodeError, ValidationError, ValueError, TypeError) as exc:
                last_exc = exc
                llm_retries_total.labels(provider=provider.name, error_type="parse").inc()
                logger.warning("post_merge.parse_error", provider=provider.name, attempt=attempt + 1, error=decode_escaped_unicode(exc))
                if attempt < max_attempts - 1:
                    delay = min(backoff_base * (2 ** attempt) + random.uniform(0, 1), backoff_max)
                    await asyncio.sleep(delay)
                    continue
                raise

            except Exception as exc:
                last_exc = exc
                error_type = type(exc).__name__
                is_http_500 = _is_http_500_error(exc)
                retryable = error_type in (
                    "RateLimitError", "APITimeoutError", "APIConnectionError", "InternalServerError",
                ) or (hasattr(exc, "status_code") and getattr(exc, "status_code", 0) >= 500)
                should_force_500_retry = is_http_500 and not forced_500_retry_used
                should_normal_retry = retryable and attempt < max_attempts - 1

                if should_force_500_retry or should_normal_retry:
                    if should_force_500_retry:
                        forced_500_retry_used = True
                    delay = min(backoff_base * (2 ** attempt) + random.uniform(0, 1), backoff_max)
                    llm_retries_total.labels(provider=provider.name, error_type=error_type).inc()
                    logger.warning("post_merge.retryable_error", provider=provider.name, attempt=attempt + 1, error=decode_escaped_unicode(exc), delay=round(delay, 2))
                    await asyncio.sleep(delay)
                    continue

                logger.warning("post_merge.failed", provider=provider.name, attempt=attempt + 1, error=decode_escaped_unicode(exc))
                raise

            finally:
                elapsed = perf_counter() - t0
                llm_request_duration.labels(provider=provider.name, agent="post_merge", status=status).observe(elapsed)

        raise last_exc or RuntimeError(f"Post-merge review failed after {max_attempts} attempt(s)")

    def _build_prompt(
        self,
        *,
        candidate_summaries: list[dict[str, Any]],
        current_positions: dict | None,
        previous_execution: dict | None,
        chunk_limit_proposals: list[dict[str, Any]],
        selector_metadata: dict[str, Any],
        max_total_positions: int,
    ) -> str:
        parts: list[str] = []
        parts.append("## Candidate Plans\n")
        parts.append(json.dumps(candidate_summaries, separators=(",", ":"), ensure_ascii=False))

        parts.append("\n## Selector Metadata\n")
        parts.append(json.dumps(selector_metadata, separators=(",", ":"), ensure_ascii=False))

        parts.append("\n## Chunk Limit Proposals\n")
        parts.append(json.dumps(chunk_limit_proposals, separators=(",", ":"), ensure_ascii=False))

        if current_positions:
            parts.append("\n## Current Positions\n")
            parts.append(json.dumps(current_positions, separators=(",", ":"), ensure_ascii=False))

        if previous_execution:
            parts.append("\n## Previous Execution\n")
            parts.append(json.dumps(previous_execution, separators=(",", ":"), ensure_ascii=False))

        parts.append(
            "\n## Task\n"
            f"Rank the candidate plans globally and recommend at most {max_total_positions} symbols. "
            "You may only reference symbols already present in the candidate list. "
            "Do NOT modify plan structure, legs, conditions, or risk numbers. "
            "Your job is limited to: ranking, keep/drop suggestions, conflict explanations, and a portfolio-level summary. "
            "Output JSON only."
        )
        return "\n\n".join(parts)


_POST_MERGE_SYSTEM_PROMPT = """\
Role: PostMergePortfolioAgent — global portfolio reviewer after chunk merge.

HARD RULES
- You are NOT allowed to invent new symbols.
- You are NOT allowed to modify legs, strategy_type, entry_conditions, exit_conditions, or top-level risk fields.
- You may only:
  1. rank existing candidate symbols,
  2. suggest which symbols to keep if the portfolio must be trimmed,
  3. explain duplicate/conflict choices,
  4. write a concise portfolio summary and risk notes.

SELECTION PRINCIPLES
- Favor portfolio diversification over crowded same-direction overlap when conviction is similar.
- Respect selector metadata and current position concentration.
- Prefer higher-confidence plans when no portfolio-level reason argues otherwise.
- If two plans are close, prefer the one with lower portfolio impact penalties.

OUTPUT JSON SHAPE
{
  "selected_symbols": ["MSFT", "NVDA"],
  "ranking": ["MSFT", "NVDA", "AAPL"],
  "portfolio_summary": "...",
  "risk_notes": ["..."],
  "conflict_explanations": [
    {"symbol": "AAPL", "decision": "deprioritize", "rationale": "..."}
  ]
}
"""