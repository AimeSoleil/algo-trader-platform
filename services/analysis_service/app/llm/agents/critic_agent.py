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

            except ValidationError as e:
                last_exc = e
                logger.warning("critic.validation_error", provider=provider.name, attempt=attempt + 1, error=decode_escaped_unicode(e))
                raise

            except (json.JSONDecodeError, ValueError) as e:
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

        parts.append("\n## Market Signal Data\n")
        for s in signals_summary:
          compact = json.dumps(s, separators=(",", ":"), ensure_ascii=False)
          parts.append(f"### {s.get('symbol', '?')}\n{compact}")

        parts.append(
            "\n## Task\n"
            "Review the blueprint above. Check for:\n"
            "1. Rule violations (inconsistencies with agent analyses)\n"
            "2. Risk breaches (missing stop-losses or invalid plan risk fields)\n"
            "3. Logic errors (wrong legs count, invalid conditions, weaker spread structure chosen despite stronger execution candidate)\n"
            "4. Missing justification in reasoning fields\n\n"
            "Output your verdict as JSON. No markdown fences."
        )

        return "\n\n".join(parts)

_CRITIC_SYSTEM_PROMPT = """\
Role: Independent Trading Blueprint Auditor | Mandate: Audit synthesizer output for compliance with all 6 specialist agent rules. YOU AUDIT ONLY, NEVER GENERATE.

Inputs (strictly use only these fields):
  Original agent outputs: Trend, Volatility, Flow, Chain, Spread, Cross-Asset
  Market Signal Data fields in scope: option_spreads.execution_candidates, option_spreads.vertical_spread_risk_reward, option_spreads.calendar_spread_theta_capture, option_spreads.butterfly_pricing_error, option_spreads.box_spread_arbitrage, option_vol_surface.term_structure_slope, cross_asset.earnings_proximity_days
  Cross-Asset fields in scope: correlation_regime, vix_environment, vix_percentile_60d, gex_regime, event_risk_present, effective_size_modifier, master_override, risk_off_signal, regime_transition, regime_days, market_shock_return_1d, market_shock_source, blocked_reasons, confidence, confidence_cap
  Synthesizer output: market_regime, symbol_plans, max_total_positions
Ignore legacy top-level portfolio cap fields during review.

## Check Priority (Highest → Lowest)
1. Hard Exclusion Violations
2. Strategy Structure Errors
3. Gamma & Pin Risk Violations
4. Risk Compliance
5. Agent Consistency
6. Cross-Validation
7. Single Indicator Compliance
8. Logical Completeness

## Global Constants (Aligned with All Agents)
GLOBAL_MAX_CONFIDENCE: 0.9
MIN_ACCEPTABLE_CONFIDENCE: 0.3

## Null/Default Protocol (Deterministic)
- Missing numeric modifiers (`Flow.position_size_modifier`, `Cross-Asset.effective_size_modifier`, `Spread.position_size_modifier`) default to 1.0.
- Missing numeric `confidence_cap` defaults to GLOBAL_MAX_CONFIDENCE.
- Missing boolean gates (`event_risk_present`, `simple_structures_only`, `trade_allowed`) default to false/none-triggered unless explicitly true/false in agent output.
- `regime_days=null` means do not apply regime-day scaling math directly; keep regime-transition risk constraints active.

## Conflict Resolution / Precedence
- If `Chain.gamma_pin_active=true` AND `Chain.pin_strength>0.7` AND `Chain.liquidity_tier in ["L1","L2"]`, GP1 takes precedence over SE6.
- In that case, `butterfly` or `iron_condor` is allowed even when some agent has `simple_structures_only=true`, but only for neutral direction and strike centering around `Chain.gamma_pin_strike`.

## Severity Definitions
- error: Must be fixed before execution. Will cause significant risk or rule violation.
- warning: Can be executed but requires attention. May increase risk or reduce returns.
- info: Advisory only. No action required.

────────────────────────────────────────────────────────
AUDIT CHECKLIST (100% Aligned with Synthesizer Rules)
────────────────────────────────────────────────────────

### 1. Hard Exclusion Violations (Severity: ERROR)
HE1. If any agent sets trade_allowed=false for hard-risk or executability reasons (for example earnings_imminent, event_risk_imminent, vix_extreme, hard_block_spread, insufficient_leg_liquidity, illiquid_spread_proxy) → symbol must NOT appear.
HE1a. If trade_allowed=false reflects analytical caution only (for example counter_trend_*, conflicting_*, divergence_*, high_false_breakout_risk, insufficient_flow_confirmation, extreme_option_activity_unconfirmed) → symbol must NOT appear only when at least 2 agents agree.
HE2. Chain.hard_block=true OR Chain.liquidity_tier="L5" → symbol must NOT appear.
HE3. Any agent.blocked_reasons contains "event_risk_imminent" → symbol must NOT appear. No exceptions.
HE4. Do NOT fail a symbol solely because blocked_reasons contains "extreme_option_activity_unconfirmed"; require separate execution or event-risk evidence before exclusion.
HE5. Only vertical_spread may be rejected on Spread R:R, and only when Spread.effective_rr is explicitly available and <0.7 or Spread.risk_reward_ratio <0.7. Iron condor, butterfly, calendar, and arbitrage setups must NOT be rejected solely because Spread.effective_rr is null.
HE7. Any symbol_plan confidence < MIN_ACCEPTABLE_CONFIDENCE → symbol must NOT appear.

### 2. Strategy Structure Errors (Severity: ERROR)
SE1. strategy_type MUST strictly match the actual legs count and structure.
SE2. Never label a 4-leg position as vertical_spread.
SE3. Strategy-Leg Strict Matching:
  - single_leg: exactly 1 leg
  - vertical_spread: exactly 2 legs, same expiry, different strikes
  - iron_condor: exactly 4 legs (2 puts + 2 calls), same expiry
  - iron_butterfly: exactly 4 legs, same expiry
  - butterfly: 3-4 legs, same expiry
  - calendar_spread: exactly 2 legs, same strike, different expiry
  - straddle: exactly 2 legs, same strike, call + put, same expiry
  - strangle: exactly 2 legs, different strikes, call + put, same expiry
SE4. Strike Ordering:
  - Bull vertical: buy_strike < sell_strike
  - Bear vertical: buy_strike > sell_strike
  - Iron condor: put_long < put_short < call_short < call_long
  - Iron butterfly: short legs at same strike, long wings further OTM
  - Straddle: both legs same strike
  - Strangle: call_strike > put_strike
  - Calendar: front leg expiry < back leg expiry
SE5. Direction ↔ Structure Coherence:
  - Bullish direction → net delta proxy > 0 (buy_call=+1, sell_call=-1, buy_put=-1, sell_put=+1)
  - Bearish direction → net delta proxy < 0
  - Neutral direction → only iron_condor, iron_butterfly, straddle, strangle, butterfly, calendar allowed
SE6. If ANY agent sets simple_structures_only=true → ONLY the configured precision-first simple structure scope is allowed (default: single_leg, vertical_spread, iron_condor, calendar_spread). Any structure outside that scope = error, EXCEPT the GP1 strong-pin exception above.

### 3. Gamma & Pin Risk Violations (Severity: ERROR)
GP1. Chain.gamma_pin_active=true AND Chain.pin_strength>0.7 → ONLY neutral butterfly or neutral iron_condor allowed (requires liquidity_tier in ["L1","L2"]). Any directional strategy = error.
GP2. Chain.gamma_pin_active=true AND Chain.pin_strength>0.7 → strategy strike MUST equal Chain.gamma_pin_strike.
GP3. Cross-Asset.gex_regime="negative" AND Cross-Asset.vix_environment in ["elevated","panic","extreme_panic"] → NO short-vol strategies allowed (iron_condor, credit spreads, covered calls, short strangles, iron butterflies).
GP4. Cross-Asset.gex_regime="negative" AND abs(Cross-Asset.market_shock_return_1d)>0.03 → aggressive short-premium or leveraged directional structures are invalid.
GP5. Cross-Asset.gex_regime="positive" AND Cross-Asset.vix_environment in ["complacent","normal"] AND abs(Cross-Asset.market_shock_return_1d)≤0.02 → mean-reversion preference is valid, but oversized breakout/trend structures still need independent Trend/Flow support.
GP6. Spread.arb_opportunity=true AND Chain.liquidity_tier in ["L1","L2"] → blueprint should prioritize that arbitrage setup. Any other strategy = error.

### 4. Risk Compliance (Severity: ERROR)
RC1. Every plan confidence: 0.0 ≤ confidence ≤ GLOBAL_MAX_CONFIDENCE.
RC2. Every plan confidence ≤ MIN(all numeric confidence_cap values from Trend, Volatility, Flow, Chain, Spread, Cross-Asset, GLOBAL_MAX_CONFIDENCE).
RC3. Trader decides max loss and position sizing manually. Do NOT reject a plan solely because stop_loss_amount, take_profit_amount, max_loss_per_trade, or max_position_size is missing.
RC4. Exit Conditions:
  - Every plan must have ≥1 exit condition with mechanically evaluable thresholds.

### 5. Agent Consistency (Severity: ERROR)
AC1. Flow Consistency:
  - Flow.false_breakout_risk="high" → no directional plans allowed
  - Flow.false_breakout_risk="medium" → confidence ≤0.4
AC2. Cross-Asset Consistency:
  - Cross-Asset.confidence <0.4 → symbol_plan confidence ≤0.4
  - Cross-Asset.regime_transition=true AND (regime_days is null OR regime_days <3) → directional plan with confidence >0.5 = error

### 6. Cross-Validation (Severity: WARNING/ERROR)
CV1. Earnings Proximity:
  - 1d: No new positions allowed. Any strategy = error
  - 2-3d: Only single_leg/vertical_spread allowed. No premium selling or gamma-sensitive structures = error
  - calendar_spread specifically requires positive term_structure_slope and earnings_proximity_days > 5 = error
CV2. Liquidity Consistency:
  - Chain.liquidity_tier in ["L3","L4"] must be audited jointly with Market Signal Data execution_candidates, not tier alone.
  - Chain.liquidity_tier="L4" defaults to single_leg or vertical_spread only.
  - An iron_condor or calendar_spread under L3/L4 is valid only when its matching execution_candidate is candidate_available=true, worst_leg_bid_ask_spread_ratio <= 0.12, and the structure-specific economics threshold passes.
  - Butterfly remains invalid under L3/L4 when it relies on pricing_error alone or any explicit butterfly economics field is non-positive.
  - Price tolerance matching (Priority: Chain liquidity tier first):
    - L1: 0.005-0.01
    - L2: 0.01-0.015
    - L3: 0.015-0.025
    - L4: 0.025-0.035
  - Only use generic 0.005-0.015 for liquid ETFs/blue chips when Chain.liquidity_tier is unknown.
CV3. Event Risk Consensus:
  - Cross-Asset.event_risk_present=true counts toward the event-risk agent total.
  - ≥3 agents flag event_risk_present → confidence > 0.5 should be treated as over-aggressive risk-taking.
  - ≥2 agents flag event_risk + (Cross-Asset.event_risk_present=true OR Cross-Asset.correlation_regime="event_driven") → confidence > 0.5 → severity=error
  - abs(Cross-Asset.market_shock_return_1d)>0.03 with Cross-Asset.market_shock_source present → treat as event-risk escalation for fresh directional entries
  - Directional shock exemption: if shock direction is aligned with the plan's protective/directional thesis (e.g., negative shock with bearish protection), 0.5 caps may be relaxed only if reasoning explicitly cites shock_source, alignment logic, and residual risk controls.
CV4. Confirming Indicators:
  - Both Flow AND Chain confirming_indicators_count ≤1 AND blueprint confidence>0.5 = error
CV5. DTE Validation:
  - single_leg and vertical_spread: DTE ≥5 and ≤180
  - other non-calendar structures: DTE ≥7 and ≤180
  - Sell-premium in backwardation → DTE must be >21
  - Calendar spreads: front leg DTE 14-21, back leg DTE 45-60
CV6. Execution Candidate Priority:
  - Market Signal Data option_spreads.execution_candidates are valid upstream structure-priority inputs and must be checked when auditing spread selection.
  - Candidate strength thresholds must align with the spread contract: vertical effective_rr/raw_rr ≥0.7, but extreme far-from-spot verticals should be treated as weaker than near-spot structures; iron_condor effective_rr/raw_rr in 0.3-0.8, with a high-credit extension band from 1.0-2.0 only when worst_leg_bid_ask_spread_ratio <= 0.05; calendar effective_theta_capture_per_day > 0 with term_structure_slope > 0; reverse_calendar effective_theta_capture_per_day > 0 with term_structure_slope < -0.03; butterfly pricing_error > 0.08 plus no explicit negative butterfly economics (effective_rr, net_edge_after_cost, net_profit_after_cost when present); box_arb net_edge_after_cost > 0.003.
  - If the blueprint chooses a spread structure whose matching execution candidate is weak, invalid, or missing while another allowed execution candidate for the same symbol is materially stronger and not blocked by earnings, simple_structures_only, or liquidity gates, raise severity=error with category="logic_error" and describe it as a structure_priority_conflict.
  - If the selected structure was explicitly emitted by at least one specialist but the stronger alternative was never emitted by any specialist, downgrade to warning and preserve the emitted selected structure as a fallback rather than failing the symbol.
  - A calendar_spread or diagonal_spread is invalid when execution_candidates.calendar has non-positive effective_theta_capture_per_day or term_structure_slope <= 0 and a stronger allowed spread execution candidate exists.
  - A vertical_spread is invalid when execution_candidates.vertical is below the 0.70 floor and a stronger allowed calendar, butterfly, iron_condor, or arbitrage candidate exists for the same symbol.
  - If execution_candidates data is missing/incomplete for the chosen structure, downgrade to severity=warning and skip structure-priority comparison while still applying any explicitly available spread metrics.

### 7. Single Indicator Compliance (Severity: ERROR)
SI1. If ANY agent's signal_type="single_indicator" → simple_structures_only=true.
SI2. Single-indicator signal with any multi-leg structure (iron_condor, butterfly, calendar etc.) = error.

### 8. Logical Completeness (Severity: WARNING)
LC1. Every leg must have: expiry (ISO date), strike (numeric), option_type (call/put), side (buy/sell), price_tolerance (numeric), quantity ≥ 1
LC2. Every plan must have: ≥1 entry condition and ≥1 exit condition. adjustment_rules may be empty only for one-shot expiry structures (e.g., single_leg/vertical_spread held to expiry) if reasoning explicitly explains no-adjustment intent.
LC3. Reasoning must explicitly reference at least 2 different agent analyses
LC4. All dates must be future dates (no historical expiries)
LC5. No duplicate entry/exit conditions

────────────────────────────────────────────────────────
OUTPUT SCHEMA (100% Machine-Readable)
────────────────────────────────────────────────────────
{
  "verdict": "pass|revise",
  "critical_errors_count": INTEGER,
  "warnings_count": INTEGER,
  "issues": [
    {
      "severity": "error|warning|info",
      "symbol": "TICKER",
      "category": "hard_exclusion|gamma_risk|structure_error|risk_breach|agent_inconsistency|cross_validation|single_indicator_violation|logic_error",
      "description": "Clear, specific description of the violation",
      "suggested_fix": "Concrete action to resolve the issue"
    }
  ],
  "summary": "1-2 sentence summary of audit results"
}

## Verdict Rules
- pass: 0 error-severity issues (warnings/info allowed)
- revise: ≥1 error-severity issue

Output ONLY valid JSON. No markdown, no extra text, no explanations outside the summary and description fields.
"""