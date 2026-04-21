"""SynthesizerAgent — Combine specialist analyses into a coherent trading blueprint.

Receives outputs from all 6 specialist agents, resolves conflicts,
applies risk-management rules, and produces the final LLMTradingBlueprint.
"""
from __future__ import annotations

import asyncio
import json
import random
from datetime import date
from time import perf_counter
from typing import Any

from pydantic import ValidationError

from shared.config import get_settings
from shared.metrics import llm_request_duration, llm_retries_total, llm_tokens_total
from shared.models.blueprint import LLMTradingBlueprint
from shared.utils import get_logger, now_utc, next_trading_day

from services.analysis_service.app.llm.agents.base_agent import AgentLLMProvider, LLMUsageTracker, _default_provider
from services.analysis_service.app.llm.json_utils import parse_llm_json

logger = get_logger("synthesizer_agent")


class SynthesizerAgent:
    """Synthesize specialist analyses into a complete LLMTradingBlueprint.

    Unlike specialist agents (which produce partial analysis), the
    synthesizer produces the FINAL blueprint JSON with full legs,
    conditions, and risk parameters.
    """

    async def synthesize(
        self,
        agent_outputs: dict[str, Any],
        signals_summary: list[dict[str, Any]],
        current_positions: dict | None = None,
        previous_execution: dict | None = None,
        critic_feedback: str | None = None,
        *,
        provider: AgentLLMProvider | None = None,
        signal_date: date | None = None,
        usage_tracker: LLMUsageTracker | None = None,
        trade_symbols: list[str] | None = None,
        model: str | None = None,
    ) -> LLMTradingBlueprint:
        """Produce a trading blueprint from specialist agent analyses.

        Parameters
        ----------
        agent_outputs:
            Dict mapping agent name → serialized output, e.g.
            {"trend": {...}, "volatility": {...}, ...}
        signals_summary:
            Compact signal summaries (symbol + price only) for context.
        current_positions:
            Current portfolio positions dict (or None).
        previous_execution:
            Yesterday's execution summary (or None).
        critic_feedback:
            If this is a revision pass, the Critic's feedback string.
        provider:
            LLM provider instance injected by the Orchestrator.
        """
        if provider is None:
            provider = _default_provider()

        settings = get_settings()

        prompt = self._build_prompt(
            agent_outputs, signals_summary,
            current_positions, previous_execution,
            critic_feedback,
            trade_symbols=trade_symbols,
        )

        max_retries = settings.analysis_service.llm.max_retries
        backoff_base = settings.analysis_service.llm.backoff_base_seconds
        backoff_max = settings.analysis_service.llm.backoff_max_seconds
        max_attempts = max_retries + 1

        last_exc: Exception | None = None
        for attempt in range(max_attempts):
            t0 = perf_counter()
            status = "error"
            try:
                result = await provider.generate(
                    instructions=_SYNTHESIZER_SYSTEM_PROMPT,
                    user_prompt=prompt,
                    temperature=settings.analysis_service.llm.openai.temperature,
                    max_tokens=settings.analysis_service.llm.openai.max_tokens,
                    model=model,
                )

                data = parse_llm_json(result.content)
                logger.debug("synthesizer.raw_output", provider=provider.name, output=data)

                # Normalize symbol_plans: some models (e.g. sonnet) return
                # a dict keyed by symbol instead of a list.
                sp = data.get("symbol_plans")
                if isinstance(sp, dict):
                    data["symbol_plans"] = list(sp.values())

                # Inject metadata
                data["trading_date"] = next_trading_day(from_date=signal_date).isoformat()
                data["generated_at"] = now_utc().isoformat()
                data["model_provider"] = provider.name
                data["model_version"] = settings.analysis_service.llm.openai.model

                blueprint = LLMTradingBlueprint.model_validate(data)
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
                        agent="synthesizer",
                        provider=provider.name,
                        model=result.model,
                        input_tokens=result.input_tokens,
                        output_tokens=result.output_tokens,
                        total_tokens=result.total_tokens,
                        duration_s=round(elapsed, 3),
                    )

                logger.info(
                    "synthesizer.completed",
                    provider=provider.name,
                    model=result.model,
                    plans=len(blueprint.symbol_plans),
                    input_tokens=result.input_tokens,
                    output_tokens=result.output_tokens,
                    total_tokens=result.total_tokens,
                )
                return blueprint

            except (json.JSONDecodeError, ValidationError, ValueError, TypeError) as e:
                last_exc = e
                llm_retries_total.labels(provider=provider.name, error_type="parse").inc()
                logger.warning("synthesizer.parse_error", provider=provider.name, attempt=attempt + 1, error=str(e))
                if attempt < max_attempts - 1:
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

                if retryable and attempt < max_attempts - 1:
                    delay = min(backoff_base * (2 ** attempt) + random.uniform(0, 1), backoff_max)
                    llm_retries_total.labels(provider=provider.name, error_type=error_type).inc()
                    logger.warning("synthesizer.retryable_error", provider=provider.name, attempt=attempt + 1, error=str(e), delay=round(delay, 2))
                    await asyncio.sleep(delay)
                    continue

                logger.warning("synthesizer.failed", provider=provider.name, attempt=attempt + 1, error=str(e))
                raise

            finally:
                elapsed = perf_counter() - t0
                llm_request_duration.labels(provider=provider.name, agent="synthesizer", status=status).observe(elapsed)

        raise last_exc or RuntimeError(f"Synthesizer failed after {max_attempts} attempt(s)")

    def _build_prompt(
        self,
        agent_outputs: dict[str, Any],
        signals_summary: list[dict[str, Any]],
        current_positions: dict | None,
        previous_execution: dict | None,
        critic_feedback: str | None,
        *,
        trade_symbols: list[str] | None = None,
    ) -> str:
        parts: list[str] = []

        # Agent analyses
        parts.append("## Specialist Agent Analyses\n")
        for agent_name, output in agent_outputs.items():
            compact = json.dumps(output, separators=(",", ":"), ensure_ascii=False)
            parts.append(f"### {agent_name}\n{compact}")

        # Signal summary (price context only)
        parts.append("\n## Symbol Price Context\n")
        for s in signals_summary:
            parts.append(f"- {s.get('symbol', '?')}: close={s.get('close_price', '?')}, "
                         f"volume={s.get('volume', '?')}, regime={s.get('volatility_regime', '?')}")

        # Positions
        if current_positions and current_positions.get("count", 0) > 0:
            parts.append("\n## Current Positions\n")
            parts.append(json.dumps(current_positions, separators=(",",":"), ensure_ascii=False))

        # Previous execution
        if previous_execution:
            parts.append("\n## Previous Execution Review\n")
            parts.append(json.dumps(previous_execution, separators=(",",":"), ensure_ascii=False))

        # Critic feedback (revision pass)
        if critic_feedback:
            parts.append("\n## Critic Feedback (REVISION REQUIRED)\n")
            parts.append(critic_feedback)
            parts.append(
                "\nAddress ALL issues raised by the Critic. "
                "Explain in each plan's reasoning how you resolved the feedback."
            )

        # Trade-only instruction
        if trade_symbols:
            sym_list = ", ".join(trade_symbols)
            parts.append(
                f"\n## Trade Symbols\n\n"
                f"Generate symbol_plans ONLY for these trade symbols: {sym_list}\n"
                f"Other symbols (benchmarks) are provided as cross-asset context only — "
                f"do NOT create plans for them."
            )

        # Task
        parts.append(
            "\n## Task\n\n"
            "Synthesize the specialist analyses into a complete Trading Blueprint JSON.\n"
            "Resolve any conflicts between agents (e.g. Flow rejects Trend's direction).\n"
            "Apply risk-management constraints to all plans.\n"
            "Output ONLY valid JSON conforming to the blueprint schema.\n"
            "No markdown fences, no extra text."
        )

        return "\n\n".join(parts)


_SYNTHESIZER_SYSTEM_PROMPT = """\
Role: Synthesizer — senior portfolio strategist combining 6 specialist analyses into next-day trading blueprint.

Inputs:
  Trend(regime, direction, trend_strength, divergences)
  Volatility(vol_regime, iv_rank_zone, hv_iv_assessment, event_risk_present, liquidity_status)
  Flow(flow_signal, volume_anomaly, vwap_bias, false_breakout_risk[low/medium/high], position_size_modifier, event_risk_present, liquidity_status, confirming_indicators_count)
  Chain(pcr_signal, liquidity_tier[L1-L5], hard_block, gamma_pin_active, institutional_flow, net_delta_exposure, event_risk_present, confirming_indicators_count)
  Spread(best_spread_type, risk_reward_ratio, effective_rr, theta_capture, liquidity_status, event_risk_present)
  Cross-Asset(correlation_regime, vix_environment, gex_regime, effective_size_modifier, master_override, regime_days, risk_off_signal)

Pre-computed data: _consensus (per-symbol directional agreement + confidence-weighted scoring) is provided as a reference — use it to calibrate conviction.

────────────────────────────────────────────────────────
RULE PRIORITY (highest → lowest)
────────────────────────────────────────────────────────
1. Hard Exclusions (symbol-level gates that remove a symbol entirely)
2. Conflict Resolution (agent disagreement handling)
3. Agent Agreement & Conviction Scoring
4. Confidence-Weighted Resolution & Modifier Application
5. Risk Management & Sizing
6. Entry Timing & DTE

────────────────────────────────────────────────────────
HARD EXCLUSIONS (Priority 1 — check FIRST)
────────────────────────────────────────────────────────
HE1. Chain hard_block=true OR Chain liquidity_tier="L5" → EXCLUDE symbol from symbol_plans.
HE2. Combined modifier floor: if (flow.position_size_modifier × cross_asset.effective_size_modifier) < 0.3 → SKIP symbol.
     A 0.1-0.2× position is noise. Either trade at ≥0.3× or omit.
HE3. Spread effective_rr < 1.0 after costs → exclude that spread setup.
     If effective_rr is null (cannot be estimated) → cap confidence ≤ 0.5, prefer simpler defined-risk structures.
HE4. Every symbol_plan must have confidence ≥ 0.3. If you cannot justify 0.3+, omit.
HE5. It is BETTER to output fewer high-quality plans than many low-confidence ones.

────────────────────────────────────────────────────────
CONFLICT RESOLUTION (Priority 2)
────────────────────────────────────────────────────────
CR1. Flow rejects direction → see CW4 below for confidence-scaled reduction.
CR2. Cross-Asset risk_off_signal=true → reduce size by effective_size_modifier.
CR3. Trend vs Volatility disagree on direction → prefer HIGHER confidence agent.
     If BOTH confidence < 0.5 → output neutral/SKIP.
CR4. Chain liquidity_tier in ["L4", "L5"] → simpler strategies only (single_leg, vertical_spread).
CR5. Spread arb_opportunity=true + Chain liquidity_tier in ["L1", "L2"] → prioritize arb.
CR6. If symbol has conflicting signals with no clear edge → do NOT force a trade. Omit.

────────────────────────────────────────────────────────
AGENT AGREEMENT SCORING (Priority 3)
────────────────────────────────────────────────────────
Reference the _consensus pre-computed data when available. Otherwise estimate:

AS1. Count specialist agents agreeing on directional bias per symbol:
   - 4+ agree → high conviction (confidence ≥ 0.7)
   - 2-3 agree → moderate conviction (confidence 0.4-0.6)
   - <2 agree → low conviction (confidence 0.3-0.4, PREFER neutral strategies)
AS2. Agents split evenly (3 bullish, 3 bearish) → CONFLICTING, not moderate. Use neutral or SKIP.
AS3. If both Trend and Flow confidence < 0.5 → do NOT enter directional trades.

## Event Risk Consensus
ER1. If ≥3 specialist agents flag event_risk_present=true → treat as confirmed event risk:
     tighten stops, reduce max_position_size by 20%, note in reasoning.
ER2. If ≥2 agents flag event_risk + CrossAsset correlation_regime="event_driven" → cap
     confidence ≤ 0.5 unless explicit earnings play (straddle/strangle).

## Confirming Indicators
CI1. If both Flow and Chain confirming_indicators_count ≤ 1 → cap directional confidence at 0.5.
     Single-indicator setups lack robustness.

────────────────────────────────────────────────────────
CONFIDENCE-WEIGHTED RESOLUTION (Priority 4)
────────────────────────────────────────────────────────
CW1. When two agents disagree: prefer higher confidence, BUT if BOTH < 0.5 → neutral/SKIP.
CW2. Cross-Asset regime_days < 5 (transitioning) → reduce cross-asset modifier impact by
     (regime_days / 5). regime_days ≥ 5 → full impact.
CW3. If Cross-Asset master_override=true → use effective_size_modifier as the MASTER sizing
     override for all plans. It takes precedence over Flow position_size_modifier.
     Final sizing = min(flow_modifier, cross_asset_effective_size_modifier) when master_override=true.
CW4. Flow false_breakout_risk is graduated (not boolean):
     - false_breakout_risk="low" → no adjustment
     - false_breakout_risk="medium" → reduce blueprint confidence by 15%
     - false_breakout_risk="high" → reduce blueprint confidence by 30%
     Scale by flow agent confidence: multiply the reduction by flow_confidence
     (e.g., high risk at 0.8 confidence → 30% × 0.8 = 24% reduction).

## Cross-Asset Confidence Guards
CQ1. If Cross-Asset agent confidence < 0.4 → cap symbol_plan confidence at ≤ 0.4.
     (The CrossAsset agent already internalizes correlation_significance and data_freshness
     into its own confidence score — use it directly rather than referencing raw signal fields.)
CQ2. If Cross-Asset regime_transition=true AND regime_days < 3 → prefer neutral/defensive
     structures; avoid aggressive directional plans.
CQ3. If Cross-Asset effective_size_modifier ≤ 0.5 → constrain max_position_size accordingly
     and avoid increasing exposure.

────────────────────────────────────────────────────────
RISK MANAGEMENT (Priority 5 — MANDATORY)
────────────────────────────────────────────────────────
- portfolio_delta_limit ≤ 0.5 (allow 0.8 if trend_strength > 0.7)
- portfolio_gamma_limit ≤ 0.1
- max_daily_loss = $2000
- max_margin_usage = 0.5
- Every plan: stop_loss_amount + max_loss_per_trade required
- Risk/trade ≤ 2% equity
- Correlated positions (same sector | corr > 0.7) → reduce combined 30%

────────────────────────────────────────────────────────
BLUEPRINT JSON SCHEMA
────────────────────────────────────────────────────────
market_regime:str, market_analysis:str(2-3 sentences)
symbol_plans[]: underlying, strategy_type, direction, legs[{expiry,strike,option_type,side,quantity}], entry_conditions[{field,operator,value,description}], exit_conditions[], adjustment_rules[{trigger:{field,operator,value,timeframe,description},action:hedge_delta|roll_strike|close_leg|add_leg|close_all,params:{},description}], max_position_size(FLOAT 0.0-1.5, position sizing ratio: 1.0=full, 0.5=half, 0.7=70%), max_contracts(INTEGER ≥1, number of contract sets to trade), stop_loss_amount, take_profit_amount, max_loss_per_trade, reasoning(MUST reference agents), confidence(0-1)
Top-level: max_total_positions, max_daily_loss, max_margin_usage, portfolio_delta_limit, portfolio_gamma_limit

## Enums
StrategyType: single_leg|vertical_spread|iron_condor|iron_butterfly|butterfly|calendar_spread|diagonal_spread|straddle|strangle|covered_call|protective_put|collar
Direction: bullish|bearish|neutral
AdjustmentAction: hedge_delta|roll_strike|close_leg|add_leg|close_all
ConditionField: underlying_price|iv|iv_rank|delta|gamma|theta|portfolio_delta|spread_width|time|pnl_percent|volume
ConditionOperator: >|>=|<|<=|==|between|crosses_above|crosses_below

────────────────────────────────────────────────────────
ENTRY CONDITIONS ROLE
────────────────────────────────────────────────────────
entry_conditions = HARD GATES (strategic prerequisites). The intraday optimizer evaluates ALL non-time entry_conditions as boolean AND-gates before scoring timing quality. If any condition fails, the plan is skipped entirely.
- Non-time conditions (iv_rank, underlying_price, delta, volume, etc.) → hard prerequisites.
- field=time conditions → SOFT REFERENCE only (logged for auditability). The optimizer's continuous time-of-day scoring replaces binary time gates with nuanced preferred-window weighting.

────────────────────────────────────────────────────────
ENTRY TIMING (Priority 6 — 24h decimal: 9.5=09:30, 14.25=14:15)
────────────────────────────────────────────────────────
Every plan SHOULD include field=time entry_condition as a soft reference.
- AVOID 09:30-10:00 (9.5-10.0): wide spreads, unstable IV
- Sell-premium (iron_condor, iron_butterfly, credit vertical, covered_call, short strangle): 10:00-11:00 (10.0-11.0)
- Buy-premium (debit vertical, straddle, long strangle, protective_put): 11:30-14:00 (11.5-14.0)
- Calendar/diagonal: 11:00-14:00 (11.0-14.0)
- AVOID 15:30-16:00 (15.5-16.0): gamma risk

## VIX Adjustment
- VIX < 15 (calm): Standard windows. Sell-premium can start at 10:00.
- VIX 15-25 (normal-elevated): Delay sell-premium to 10:30.
- VIX 25-35 (elevated-panic): Delay ALL entries to 11:00+.
- VIX > 35 (panic): Only enter 11:30-14:00. Avoid last 2 hours entirely.
- EARNINGS DAY OVERRIDE: If underlying reports earnings after close, avoid entries in final 90 min (15:30+ hard block).

Example: {"field":"time","operator":"between","value":[10.0,11.0],"description":"After opening vol settles"}

## Earnings Proximity (cross_asset.earnings_proximity_days or event_risk_present)
- ≤3d: IV elevated → sell-premium benefits, tighter stops. Note in reasoning.
- 1d: no NEW positions unless explicit earnings play (straddle/strangle).
- null (unknown): normal rules.
- >10d: ignore earnings effect.

## DTE Guidelines (bounds: min 7, max 180)
- Sell-premium (iron_condor, iron_butterfly, short strangle, covered_call): 30-45 DTE
- Buy-premium directional (debit vertical, protective_put): 14-30 DTE
- Straddle/strangle long: 21-35 DTE
- Calendar/diagonal: front 14-21, back 45-60 DTE
- Collar: match holding horizon/catalyst
- earnings_proximity ≤ 45 → prefer expiry INCLUDING earnings date

Output ONLY valid JSON. No markdown fences.
"""
