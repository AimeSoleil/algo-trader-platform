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
from shared.utils import get_logger

from services.analysis_service.app.llm.agents.base_agent import AgentLLMProvider, LLMUsageTracker, _default_provider
from services.analysis_service.app.llm.json_utils import parse_llm_json
from services.analysis_service.app.llm.agents.models import CriticVerdict

logger = get_logger("critic_agent")


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

        last_exc: Exception | None = None
        for attempt in range(max_retries):
            t0 = perf_counter()
            status = "error"
            try:
                result = await provider.generate(
                    instructions=_CRITIC_SYSTEM_PROMPT,
                    user_prompt=prompt,
                    temperature=0.0,  # deterministic for review
                    max_tokens=4096,
                    model=model,
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
                logger.warning("critic.parse_error", provider=provider.name, attempt=attempt + 1, error=str(e))
                if attempt < max_retries - 1:
                    delay = min(backoff_base * (2 ** attempt) + random.uniform(0, 1), backoff_max)
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

                if retryable and attempt < max_retries - 1:
                    delay = min(backoff_base * (2 ** attempt) + random.uniform(0, 1), backoff_max)
                    llm_retries_total.labels(provider=provider.name, error_type=error_type).inc()
                    logger.warning("critic.retryable_error", provider=provider.name, attempt=attempt + 1, error=str(e), delay=round(delay, 2))
                    await asyncio.sleep(delay)
                    continue

                logger.warning("critic.failed", provider=provider.name, attempt=attempt + 1, error=str(e))
                raise

            finally:
                elapsed = perf_counter() - t0
                llm_request_duration.labels(provider=provider.name, agent="critic", status=status).observe(elapsed)

        raise last_exc or RuntimeError("Critic failed after retries")

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

## Checklist

1. Strategy-Legs Consistency:
- single_leg:1 leg | vertical_spread:2(same expiry,diff strikes) | iron_condor:4(2P+2C,same expiry)
- iron_butterfly:4(ATM straddle+OTM wings) | butterfly:3-4 | calendar_spread:2(same strike,diff expiry)
- straddle:2(same strike,C+P) | strangle:2(diff strikes,C+P)

2. Risk Compliance:
- portfolio_delta_limit≤0.5(0.8 w/ justification) | portfolio_gamma_limit≤0.1 | max_daily_loss≤$2000
- Every plan: stop_loss_amount>0, max_loss_per_trade>0, confidence 0-1

3. Agent Consistency:
- Flow hard_block→symbol must NOT appear | Chain hard_block→must NOT appear
- Strategy types align w/ Trend+Volatility recs | Sizes reflect Flow modifiers

4. Logical Completeness:
- Every leg: expiry,strike,option_type(call/put),side(buy/sell)
- Entry conditions: concrete thresholds | ≥1 exit condition/plan
- Reasoning references specific agent analyses

5. Strike Ordering Verification (NEW):
- Vertical call spread (bullish): buy_strike < sell_strike | Vertical put spread (bearish): sell_strike < buy_strike
- Iron condor: put_long_strike < put_short_strike < call_short_strike < call_long_strike
- Iron butterfly: short legs at SAME strike (ATM), long wings further OTM
- Straddle: BOTH legs SAME strike | Strangle: call_strike > put_strike
- Calendar: SAME strike, DIFFERENT expiry | Diagonal: different strike AND different expiry

6. Direction ↔ Strategy Coherence (NEW):
- bullish direction → NOT bear_put_spread, NOT protective_put as primary strategy
- bearish direction → NOT bull_call_spread, NOT covered_call as primary strategy
- neutral direction → should use iron_condor, iron_butterfly, straddle, strangle, butterfly, calendar_spread
- If direction doesn't match strategy type → severity=error

7. Greeks ↔ Direction Coherence (NEW):
- bullish strategy → net delta should be positive (buy calls or sell puts dominate)
- bearish strategy → net delta should be negative (buy puts or sell calls dominate)
- Check by counting: buy_call=+1, sell_call=-1, buy_put=-1, sell_put=+1 proxy per leg
- If proxy delta sign contradicts stated direction → severity=warning

8. DTE Validation (NEW):
- All legs DTE ≥ 7 and ≤ 180
- Sell-premium in backwardation (from volatility agent): DTE must be > 14
- Calendar/diagonal: front leg DTE < back leg DTE

9. Exit Condition Completeness (NEW):
- Must have at least one stop-loss type exit (field=pnl_percent with operator < or field=underlying_price)
- stop_loss_amount field > 0 is necessary but NOT sufficient — a trigger condition must also exist
- Reasoning must explain exit logic, not just entry logic

10. Cross-Asset Data Quality Guards (NEW):
- If cross_asset confidence shows correlation_significance < 0.5, symbol plan confidence should be capped (<=0.4)
- If data_freshness < 0.5, aggressive directional sizing should be rejected
- If both are < 0.5, max_position_size should be defensive (no upsize)

11. Cost Realism Guard (NEW):
- If spread analysis indicates effective R:R < 1.0 after costs, that setup must not be used
- If effective R:R cannot be estimated, confidence should be capped (<=0.5)

## Output Schema
{"verdict":"pass|revise","issues":[{"severity":"error|warning|info","symbol":"AAPL","category":"rule_violation|risk_breach|logic_error|missing_data","description":"","suggested_fix":""}],"summary":""}

- pass: no errors(warnings/info OK) | revise: ≥1 error-severity issue

Output ONLY valid JSON. No markdown fences.
"""
