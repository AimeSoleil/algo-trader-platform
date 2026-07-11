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
        chunk_limit_proposals: list[dict[str, Any]] | None = None,
        selector_metadata: dict[str, Any] | None = None,
        candidate_count: int,
        provider: AgentLLMProvider | None = None,
        usage_tracker: LLMUsageTracker | None = None,
        model: str | None = None,
    ) -> PostMergeReview:
        if provider is None:
            provider = _default_provider()

        prompt = self._build_prompt(
            candidate_summaries=candidate_summaries,
            chunk_limit_proposals=chunk_limit_proposals or [],
            selector_metadata=selector_metadata or {},
            candidate_count=candidate_count,
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

            except ValidationError as exc:
                last_exc = exc
                logger.warning("post_merge.validation_error", provider=provider.name, attempt=attempt + 1, error=decode_escaped_unicode(exc))
                raise

            except (json.JSONDecodeError, ValueError, TypeError) as exc:
                last_exc = exc
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
                    logger.warning("post_merge.retryable_error", provider=provider.name, attempt=attempt + 1, error=decode_escaped_unicode(exc), delay=round(delay, 2))
                    await asyncio.sleep(delay)
                    continue

                logger.warning("post_merge.failed", provider=provider.name, attempt=attempt + 1, error=decode_escaped_unicode(exc))
                raise

            finally:
                _ = perf_counter() - t0

        raise last_exc or RuntimeError(f"Post-merge review failed after {max_attempts} attempt(s)")

    def _build_prompt(
        self,
        *,
        candidate_summaries: list[dict[str, Any]],
        chunk_limit_proposals: list[dict[str, Any]],
        selector_metadata: dict[str, Any],
        candidate_count: int,
    ) -> str:
        parts: list[str] = []
        raw_candidate_entries = len(candidate_summaries)
        parts.append("## Candidate Plans\n")
        parts.append(json.dumps(candidate_summaries, separators=(",", ":"), ensure_ascii=False))

        parts.append("\n## Selector Metadata\n")
        parts.append(json.dumps(selector_metadata, separators=(",", ":"), ensure_ascii=False))

        parts.append("\n## Chunk Limit Proposals\n")
        parts.append(json.dumps(chunk_limit_proposals, separators=(",", ":"), ensure_ascii=False))

        parts.append(
            "\n## Task\n"
            f"Rank all {candidate_count} candidate symbols globally. "
            f"The candidate list may contain {raw_candidate_entries} raw candidate entries across those symbols. "
            "Your ranking is symbol-level and must cover every candidate symbol exactly once; do not apply any portfolio-capacity trimming. "
            "If candidate_summaries contains multiple entries for the same symbol, use them only to explain conflicts and to infer that symbol's final placement. "
            "You may only reference symbols already present in the candidate list. "
            "Do NOT modify plan structure, legs, conditions, or risk numbers. "
            "The candidate list already includes deterministic selector scores, explicit ranking flags, and score breakdowns. "
            "Use selector_metadata.deterministic_sort_priority as the canonical base ordering logic, and keep your explanations aligned with those same fields. "
            "If you rank a lower-confidence symbol above a higher-confidence one, the rationale must cite better deterministic inputs already present in candidate_summaries, such as precision_first_score, selector_base_score, or portfolio_impact_score. "
            "Your job is limited to: ranking, optional strongest-conviction highlights in selected_symbols, conflict explanations, and a portfolio-level summary. "
            "Output JSON only."
        )
        return "\n\n".join(parts)


_POST_MERGE_SYSTEM_PROMPT = """\
Role: PostMergePortfolioAgent — global portfolio reviewer after chunk merge.
Mandate: Rank all valid trading plans, highlight highest-conviction opportunities, explain conflicts, and provide portfolio-level summary. YOU ARE READ-ONLY. YOU MAY NOT MODIFY ANY TRADING DETAILS.

## HARD RULES (NON-NEGOTIABLE)
- You are NOT allowed to invent new symbols.
- You are NOT allowed to modify legs, strategy_type, entry_conditions, exit_conditions, or any risk fields.
- You may ONLY perform the following actions:
  1. Rank existing candidate symbols exactly once in the correct priority order
  2. Highlight the top 5-10 strongest-conviction symbols in selected_symbols for summary purposes
  3. Explain duplicate or conflicting plans in conflict_explanations
  4. Write a concise portfolio summary and risk notes
- ranking MUST include every candidate symbol exactly once, no omissions, no additions.
- selected_symbols is ONLY for summary and highlighting purposes. It MUST NOT be used as an execution filter or portfolio-capacity cutoff. All plans in the ranking are valid.
- candidate_summaries may contain multiple candidate entries for the same symbol. Use those repeated entries only for conflict explanation and for choosing that symbol's symbol-level placement. The output ranking must still contain that symbol only once.
- Do NOT invent plan IDs, candidate IDs, or duplicate symbol rows in ranking.
- selected_symbols must be derived from the front of ranking in first-appearance order. Return the first 5-10 unique symbols from ranking when available; if fewer than 5 symbols exist, return all available symbols.

## INPUT CONTRACT
- candidate_summaries is the only candidate-level source of truth.
- Each candidate entry may include the following ranking inputs: symbol, strategy_type, direction, trade_gate_status, trade_gate_summary, machine_readable_gate_ok, confidence, data_quality_score, max_contracts, chunk_index, chunk_id, original_order, candidate_ref, selector_base_score, portfolio_impact_score, portfolio_impact_breakdown, execution_candidate_score, execution_candidate_breakdown, precision_first_score, precision_first_breakdown, master_override, effective_size_modifier, arb_opportunity, arb_priority, event_risk_present, event_risk_agents, earnings_proximity_days, signal_type, and single_indicator_agents.
- selector_metadata provides ranking_scope, deterministic_sort_priority, ranking_method, precision_first_enabled, available_ranking_signals, and trade_gate_taxonomy.
- Defaults when optional fields are missing: master_override=false, effective_size_modifier=1.0, arb_opportunity=false, arb_priority=0, event_risk_present=false, earnings_proximity_days=null, signal_type="multi_indicator".

## SORTING PIPELINE (Apply in Order)
1. Locked Head Groups:
    - Stage 1a: Master Override group. If any candidate entry for a symbol has master_override=true, that symbol belongs in the first locked group.
    - Order master-override symbols by highest effective_size_modifier first. If tied, fall back to the canonical base order.
    - Stage 1b: Arbitrage group. From the remaining symbols, if any candidate entry has arb_opportunity=true, place that symbol immediately after the master-override group.
    - Order arbitrage symbols by highest arb_priority first. If tied, fall back to the canonical base order.
    - Diversification and risk demotions MUST NOT reorder symbols inside these locked head groups.
2. Canonical Base Order For Remaining Symbols:
    - Follow selector_metadata.deterministic_sort_priority exactly as the canonical base ordering logic.
    - Interpret higher selector_base_score, higher precision_first_score, higher confidence, higher data_quality_score, and higher portfolio_impact_score as better unless a later rule explicitly demotes the symbol.
    - When precision_first_enabled=true, precision_first_score should usually dominate confidence disagreements.
3. Diversification Micro-Adjustment:
    - Only apply to non-head-group symbols after base ordering.
    - If two nearby symbols are within 0.10 confidence and one improves directional diversification versus the already-ranked prefix, you may move the more diversifying symbol ahead by at most one position.
    - For the 4th and subsequent symbols in the same direction, you may push each symbol back by one position once.
4. Risk Demotions For Non-Head-Group Symbols:
    - Apply cumulative demotion points after base ordering and diversification.
    - event_risk_present=true: +3 positions later
    - earnings_proximity_days≤3: +5 positions later
    - signal_type="single_indicator": +2 positions later
    - portfolio_impact_score<0.40: +2 positions later
    - Truncate any demotion at the end of the list.
5. Tiebreaker:
    - If two symbols are still tied after the steps above, prefer the one with better canonical base order; if still tied, preserve existing input order.

## CITATION RULES
- Do not invent hidden reasons for ranking decisions
- Explicitly cite the supplied selector_base_score, precision_first_score, portfolio_impact_score, and their breakdowns in conflict_explanations
- Always reference the specific explicit flag (master_override, arb_opportunity, event_risk_present, earnings_proximity_days, signal_type) when justifying priority adjustments
- If a symbol had multiple candidate entries, explain the conflict using the candidate-level numeric fields already present in candidate_summaries; do not claim both plans remain separately ranked.

## OUTPUT JSON SCHEMA
{
  "selected_symbols": ["MSFT", "NVDA", "AAPL", "TSLA", "META"],
  "ranking": ["MSFT", "NVDA", "AAPL", "TSLA", "META", "AMZN", "GOOGL"],
  "portfolio_summary": "Portfolio contains 7 valid trading plans: 3 bullish, 2 bearish, 2 neutral. Highest conviction opportunities are in large-cap tech with strong flow confirmation. Master override signal active for NVDA.",
  "risk_notes": [
    "2 plans have upcoming earnings within 3 days",
    "1 plan has single-indicator only confirmation",
    "Portfolio delta is moderately bullish (0.35)"
  ],
  "conflict_explanations": [
    {
      "symbol": "AAPL",
            "type": "candidate_conflict",
            "decision": "prefer higher-ranked candidate",
            "rationale": "AAPL appeared in multiple candidate entries. The symbol kept its higher placement because its strongest entry had precision_first_score=0.81, selector_base_score=0.74, and portfolio_impact_score=0.62, which beat the alternate AAPL candidate on the canonical base order."
    }
  ]
}

Output ONLY valid JSON. No markdown, no extra text.
"""