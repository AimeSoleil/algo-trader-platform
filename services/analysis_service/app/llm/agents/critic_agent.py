"""CriticAgent — Self-review loop for blueprint quality.

Reviews a synthesized blueprint against reference rules, risk constraints,
and logical consistency. Returns pass/revise verdict with specific issues.
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
from services.analysis_service.app.llm.json_utils import parse_llm_json
from services.analysis_service.app.llm.agents.models import CriticVerdict

logger = get_logger("critic_agent")


def _is_http_500_error(exc: Exception) -> bool:
    status_code = getattr(exc, "status_code", None)
    if status_code == 500:
        return True

    message = str(exc).lower()
    return (
        "error code: 500" in message
        or "status code: 500" in message
        or "internal server error" in message and "500" in message
    )


class CriticAgent:
    """Review a blueprint for rule violations, risk breaches, and logic errors.

    Returns a CriticVerdict indicating pass/revise with detailed issues.
    """

    async def review(
        self,
        blueprint_json: dict[str, Any],
        agent_outputs: dict[str, Any],
        signals_summary: list[dict[str, Any]],
        *,
        provider: AgentLLMProvider | None = None,
        usage_tracker: LLMUsageTracker | None = None,
        model: str | None = None,
    ) -> CriticVerdict:
        """Review a blueprint and return verdict.

        Parameters
        ----------
        blueprint_json:
            The synthesized blueprint as a dict.
        agent_outputs:
            Original specialist agent outputs for cross-referencing.
        signals_summary:
            Compact signal summaries for context.
        provider:
            LLM provider instance injected by the Orchestrator.
        """
        if provider is None:
            provider = _default_provider()

        settings = get_settings()

        prompt = self._build_prompt(blueprint_json, agent_outputs, signals_summary)

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
                    instructions=_CRITIC_SYSTEM_PROMPT,
                    user_prompt=prompt,
                    temperature=0.0,  # deterministic for review
                    max_tokens=16384,
                    model=model,
                    agent_name="critic",
                )

                data = parse_llm_json(result.content)
                verdict = CriticVerdict.model_validate(data)

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
                        agent="critic",
                        provider=provider.name,
                        model=result.model,
                        input_tokens=result.input_tokens,
                        output_tokens=result.output_tokens,
                        total_tokens=result.total_tokens,
                        duration_s=round(elapsed, 3),
                    )

                logger.info(
                    "critic.completed",
                    provider=provider.name,
                    model=result.model,
                    verdict=verdict.verdict,
                    issues=len(verdict.issues),
                    input_tokens=result.input_tokens,
                    output_tokens=result.output_tokens,
                    total_tokens=result.total_tokens,
                )
                return verdict

            except (json.JSONDecodeError, ValidationError, ValueError) as e:
                last_exc = e
                llm_retries_total.labels(provider=provider.name, error_type="parse").inc()
                logger.warning("critic.parse_error", provider=provider.name, attempt=attempt + 1, error=decode_escaped_unicode(e))
                if attempt < max_attempts - 1:
                    delay = min(backoff_base * (2 ** attempt) + random.uniform(0, 1), backoff_max)
                    await asyncio.sleep(delay)
                    continue
                raise

            except Exception as e:
                last_exc = e
                error_type = type(e).__name__
                is_http_500 = _is_http_500_error(e)
                retryable = error_type in (
                    "RateLimitError", "APITimeoutError",
                    "APIConnectionError", "InternalServerError",
                ) or (hasattr(e, "status_code") and getattr(e, "status_code", 0) >= 500)

                should_force_500_retry = is_http_500 and not forced_500_retry_used
                should_normal_retry = retryable and attempt < max_attempts - 1

                if should_force_500_retry or should_normal_retry:
                    if should_force_500_retry:
                        forced_500_retry_used = True
                    delay = min(backoff_base * (2 ** attempt) + random.uniform(0, 1), backoff_max)
                    llm_retries_total.labels(provider=provider.name, error_type=error_type).inc()
                    logger.warning("critic.retryable_error", provider=provider.name, attempt=attempt + 1, error=decode_escaped_unicode(e), delay=round(delay, 2))
                    await asyncio.sleep(delay)
                    continue

                logger.warning("critic.failed", provider=provider.name, attempt=attempt + 1, error=decode_escaped_unicode(e))
                raise

            finally:
                elapsed = perf_counter() - t0
                llm_request_duration.labels(provider=provider.name, agent="critic", status=status).observe(elapsed)

        raise last_exc or RuntimeError(f"Critic failed after {max_attempts} attempt(s)")

    def _build_prompt(
        self,
        blueprint_json: dict[str, Any],
        agent_outputs: dict[str, Any],
        signals_summary: list[dict[str, Any]],
    ) -> str:
        parts: list[str] = []

        parts.append("## Blueprint to Review\n")
        parts.append(json.dumps(blueprint_json, separators=(",", ":"), ensure_ascii=False))

        parts.append("\n## Specialist Agent Analyses\n")
        for name, output in agent_outputs.items():
            compact = json.dumps(output, separators=(",", ":"), ensure_ascii=False)
            parts.append(f"### {name}\n{compact}")

        parts.append("\n## Signal Context\n")
        for s in signals_summary:
            parts.append(f"- {s.get('symbol', '?')}: close={s.get('close_price', '?')}")

        parts.append(
            "\n## Task\n"
            "Review the blueprint above. Check for:\n"
            "1. Rule violations (inconsistencies with agent analyses)\n"
            "2. Risk breaches (portfolio limits, missing stop-losses)\n"
            "3. Logic errors (wrong legs count, invalid conditions)\n"
            "4. Missing justification in reasoning fields\n\n"
            "Output your verdict as JSON. No markdown fences."
        )

        return "\n\n".join(parts)


_CRITIC_SYSTEM_PROMPT = """\
Role: Critic — independent quality auditor for trading blueprints. You AUDIT, not generate.

────────────────────────────────────────────────────────
CHECK PRIORITY (highest → lowest)
────────────────────────────────────────────────────────
1. Hard Exclusion Violations (symbol should have been excluded)
2. Strategy Structure Errors (legs, strikes, DTE)
3. Risk Compliance (portfolio limits, stop-losses)
4. Agent Consistency (specialist signals vs blueprint decisions)
5. Cross-Validation (new fields: event risk, liquidity, cost, sizing)
6. Logical Completeness (conditions, reasoning)

────────────────────────────────────────────────────────
CHECKLIST
────────────────────────────────────────────────────────

1. Strategy-Legs Consistency:
- single_leg: 1 leg | vertical_spread: 2 (same expiry, diff strikes)
- iron_condor: 4 (2P+2C, same expiry) | iron_butterfly: 4 (ATM straddle + OTM wings)
- butterfly: 3-4 | calendar_spread: 2 (same strike, diff expiry)
- straddle: 2 (same strike, C+P) | strangle: 2 (diff strikes, C+P)

2. Risk Compliance:
- portfolio_delta_limit ≤ 0.5 (0.8 with justification) | portfolio_gamma_limit ≤ 0.1
- max_daily_loss ≤ $2000
- Every plan: stop_loss_amount > 0, max_loss_per_trade > 0, confidence 0-1

3. Agent Consistency:
- Chain hard_block=true OR Chain liquidity_tier="L5" → symbol must NOT appear in symbol_plans
- Strategy types should align with Trend + Volatility recommendations
- Sizes should reflect Flow position_size_modifier
- If Cross-Asset master_override=true → max_position_size must respect
  Cross-Asset effective_size_modifier (cannot exceed it)

4. Logical Completeness:
- Every leg: expiry, strike, option_type (call/put), side (buy/sell)
- Entry conditions: concrete thresholds | ≥1 exit condition per plan
- Reasoning references specific agent analyses

5. Strike Ordering Verification:
- Vertical spread (bullish): long_strike < short_strike for calls
- Vertical spread (bearish): short_strike < long_strike for puts
- Iron condor: put_long < put_short < call_short < call_long
- Iron butterfly: short legs at SAME strike (ATM), long wings further OTM
- Straddle: BOTH legs SAME strike | Strangle: call_strike > put_strike
- Calendar: SAME strike, DIFFERENT expiry | Diagonal: different strike AND different expiry

6. Direction ↔ Strategy Coherence:
- bullish direction → must NOT be a bearish-only structure (e.g., vertical_spread with
  sell_call < buy_call). Validate by checking leg structure, not strategy name.
- bearish direction → must NOT be a bullish-only structure.
- neutral direction → should use iron_condor, iron_butterfly, straddle, strangle,
  butterfly, calendar_spread.
- If direction contradicts leg structure → severity=error.

7. Greeks ↔ Direction Coherence:
- bullish strategy → net delta proxy should be positive
- bearish strategy → net delta proxy should be negative
- Proxy per leg: buy_call=+1, sell_call=−1, buy_put=−1, sell_put=+1
- If proxy delta sign contradicts stated direction → severity=warning

8. DTE Validation:
- All legs DTE ≥ 7 and ≤ 180
- Sell-premium in backwardation (from volatility agent): DTE must be > 14
- Calendar/diagonal: front leg DTE < back leg DTE

9. Exit Condition Completeness:
- Must have ≥1 stop-loss type exit (field=pnl_percent with operator <, or field=underlying_price)
- stop_loss_amount > 0 is necessary but NOT sufficient — a trigger condition must also exist
- Reasoning must explain exit logic, not just entry logic

10. Cross-Asset Confidence Guard:
- If Cross-Asset agent confidence < 0.4 → symbol_plan confidence should be ≤ 0.4.
  (The CrossAsset agent already internalizes correlation_significance and data_freshness
  into its own confidence — use it directly rather than referencing raw signal fields.)
- If Cross-Asset regime_transition=true AND regime_days < 3 → plans should be
  neutral/defensive, not aggressive directional.
- If Cross-Asset effective_size_modifier ≤ 0.5 → max_position_size should be ≤ 0.5.

11. Cost Realism Guard:
- If Spread effective_rr < 1.0 → that setup must not appear in symbol_plans.
- If Spread effective_rr is null → confidence should be ≤ 0.5.
- If Spread effective_rr and risk_reward_ratio differ by > 30%, flag as info
  (significant cost drag — reasoning should acknowledge transaction costs).

12. Event Risk Cross-Check:
- If ≥3 specialist agents flag event_risk_present=true for a symbol, the blueprint should
  reflect this: tighter stops, reduced max_position_size, or explicit acknowledgment in reasoning.
- If event risk is flagged but the plan uses earnings-sensitive strategies (calendar, butterfly)
  without acknowledging gamma crush risk → severity=warning.

13. Liquidity Consensus Check:
- Cross-reference liquidity signals: Volatility liquidity_status, Chain liquidity_tier,
  Spread liquidity_status. If ≥2 agents indicate poor liquidity (illiquid, L4+, wide) and
  the blueprint uses complex multi-leg strategies (iron_condor, butterfly, calendar) →
  severity=warning ("complex strategy in illiquid conditions").
- Simpler strategies (single_leg, vertical_spread) are acceptable in illiquid conditions.

14. Master Override Verification:
- If Cross-Asset master_override=true → every symbol_plan's max_position_size must be
  ≤ Cross-Asset effective_size_modifier. If exceeded → severity=error.
- If master_override=true AND effective_size_modifier < 0.3 → symbol should be
  skipped entirely. If it appears → severity=error.

15. Confirming Indicators vs Confidence:
- If both Flow and Chain confirming_indicators_count ≤ 1 for a symbol, but blueprint
  confidence > 0.6 → severity=warning ("high confidence with minimal confirming indicators").
- Single-indicator setups rarely justify high conviction.

────────────────────────────────────────────────────────
OUTPUT SCHEMA
────────────────────────────────────────────────────────
{"verdict":"pass|revise","issues":[{"severity":"error|warning|info","symbol":"AAPL","category":"rule_violation|risk_breach|logic_error|missing_data","description":"","suggested_fix":""}],"summary":""}

- pass: no errors (warnings/info OK) | revise: ≥1 error-severity issue

Output ONLY valid JSON. No markdown fences.
"""
