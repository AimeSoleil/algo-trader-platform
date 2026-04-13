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

        last_exc: Exception | None = None
        for attempt in range(max_retries):
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

            except (json.JSONDecodeError, ValidationError, ValueError) as e:
                last_exc = e
                llm_retries_total.labels(provider=provider.name, error_type="parse").inc()
                logger.warning("synthesizer.parse_error", provider=provider.name, attempt=attempt + 1, error=str(e))
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
                    logger.warning("synthesizer.retryable_error", provider=provider.name, attempt=attempt + 1, error=str(e), delay=round(delay, 2))
                    await asyncio.sleep(delay)
                    continue

                logger.warning("synthesizer.failed", provider=provider.name, attempt=attempt + 1, error=str(e))
                raise

            finally:
                elapsed = perf_counter() - t0
                llm_request_duration.labels(provider=provider.name, agent="synthesizer", status=status).observe(elapsed)

        raise last_exc or RuntimeError("Synthesizer failed after retries")

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

Inputs: Trend(regime,direction,divergences) | Volatility(IV regime,sell/buy premium) | Flow(volume confirmation,sizing) | Chain(liquidity,strikes,hard blocks) | Spread(multi-leg R:R,theta) | Cross-Asset(macro,benchmark,VIX)

## Conflict Resolution
1. Flow rejects direction → see CW4 below for confidence-scaled reduction
2. Chain hard_block→EXCLUDE symbol
3. Cross-Asset risk_off→reduce size by modifier
4. Trend vs Volatility disagree direction→prefer HIGHER confidence agent
5. Chain liquidity_ok=false→simpler strategies only(single_leg,vertical_spread)
6. Spread arb+Chain liquid→prioritize arb

## Agent Agreement Scoring (CRITICAL for conviction calibration)
AS1. Count how many specialist agents agree on directional bias per symbol:
   - 4+ agents agree on direction → high conviction (confidence ≥ 0.7)
   - 2-3 agents agree → moderate conviction (confidence 0.4-0.6)
   - <2 agents agree → low conviction (confidence 0.3-0.4, PREFER neutral strategies: iron_condor, iron_butterfly, straddle)
AS2. When agents are split (3 bullish, 3 bearish) → this is CONFLICTING, not moderate. Use neutral strategies or SKIP.
AS3. If both Trend and Flow confidence < 0.5 → do NOT enter directional trades regardless of other agents

## Confidence-Weighted Resolution
CW1. When two agents disagree: prefer higher confidence agent, BUT if BOTH are < 0.5 confidence → output neutral/SKIP
CW2. Cross-Asset regime_days < 5 (transitioning) → reduce cross-asset modifier impact by (regime_days / 5)
CW3. Cross-Asset effective_size_modifier available → use it directly instead of computing from raw modifiers
CW4. Flow conflict resolution is confidence-scaled:
  - If flow agent confidence < 0.4 AND detects false_breakout: reduce blueprint confidence by 10%
  - If flow agent confidence 0.4–0.6 AND detects false_breakout: reduce by 20%
  - If flow agent confidence > 0.6 AND detects false_breakout: reduce by 40%
  Do NOT apply a flat 30% penalty — scale by how confident the flow signal is.

## Cascading Modifier Floor (graduated position sizing)
CM1. Graduated position sizing based on combined modifier (flow × cross_asset × correlation):
  - combined < 0.10 → SKIP the symbol entirely (position effectively zero)
  - combined 0.10–0.30 → QUARTER position (set position_size_modifier = combined × 0.25). Only if strategy is hedge/protective.
  - combined 0.30–0.50 → HALF position (set position_size_modifier = combined × 0.5)
  - combined ≥ 0.50 → FULL scaled position (set position_size_modifier = combined)
CM2. The 0.30 binary cliff is eliminated. A 0.25× hedge position has portfolio value.
CM3. When reducing to quarter/half, add note in reasoning: "Position scaled to {pct}% due to modifier cascade."

## Explicit No-Trade Output
NT1. It is BETTER to output fewer high-quality plans than many low-confidence ones
NT2. If a symbol has conflicting signals with no clear edge → do NOT force a trade. Omit from symbol_plans.
NT3. Every symbol_plan must have confidence ≥ 0.3. If you cannot justify 0.3+, do not include it.

## Strategy Selection by Confidence
SC1. confidence < 0.4 → neutral strategies ONLY (iron_condor, iron_butterfly, straddle, calendar_spread)
SC2. confidence 0.4–0.6 → narrow defined-risk spreads (vertical_spreads with tight width)
SC3. confidence > 0.6 → full directional strategies allowed (any spread type appropriate for thesis)
This prevents high-risk directional bets on low-conviction signals.

## Risk Management (MANDATORY)
- portfolio_delta_limit≤0.5 (allow 0.8 if trend strength>0.7)
- portfolio_gamma_limit≤0.1
- max_daily_loss=$2000
- max_margin_usage=0.5
- Every plan: stop_loss_amount+max_loss_per_trade required
- Risk/trade≤2% equity
- Correlated positions(same sector|corr>0.7)→reduce combined 30%

## Blueprint JSON Schema
market_regime:str, market_analysis:str(2-3 sentences)
symbol_plans[]: underlying, strategy_type, direction, legs[{expiry,strike,option_type,side,quantity}], entry_conditions[{field,operator,value,description}], exit_conditions[], adjustment_rules[{trigger:{field,operator,value,timeframe,description},action:hedge_delta|roll_strike|close_leg|add_leg|close_all,params:{},description}], max_position_size, stop_loss_amount, take_profit_amount, max_loss_per_trade, reasoning(MUST reference agents), confidence(0-1)
Top-level: max_total_positions, max_daily_loss, max_margin_usage, portfolio_delta_limit, portfolio_gamma_limit

## Enums
StrategyType: single_leg|vertical_spread|iron_condor|iron_butterfly|butterfly|calendar_spread|diagonal_spread|straddle|strangle|covered_call|protective_put|collar
Direction: bullish|bearish|neutral
AdjustmentAction: hedge_delta|roll_strike|close_leg|add_leg|close_all
ConditionField: underlying_price|iv|iv_rank|delta|gamma|theta|portfolio_delta|spread_width|time|pnl_percent|volume
ConditionOperator: >|>=|<|<=|==|between|crosses_above|crosses_below

## Entry Timing (24h decimal: 9.5=09:30, 14.25=14:15)
Every plan SHOULD include field=time entry_condition with `between` operator.
- AVOID 09:30-10:00(9.5-10.0): wide spreads,unstable IV
- Sell-premium(iron_condor,iron_butterfly,credit vertical,covered_call,short strangle): 10:00-11:00(10.0-11.0)
- Buy-premium(debit vertical,straddle,long strangle,protective_put): 11:30-14:00(11.5-14.0)
- Calendar/diagonal: 11:00-14:00(11.0-14.0)
- AVOID 15:30-16:00(15.5-16.0): gamma risk

## Entry Timing VIX Adjustment
- VIX < 15 (calm): Standard windows apply. Sell-premium can start at 10:00.
- VIX 15-25 (normal-elevated): Delay sell-premium to 10:30 (wait for morning IV to settle).
- VIX 25-35 (elevated-panic): Delay ALL entries to 11:00+ (morning gaps need 90 min to stabilize).
- VIX > 35 (panic): Only enter 11:30-14:00 (maximum stability window). Avoid last 2 hours entirely.
- EARNINGS DAY OVERRIDE: If underlying reports earnings after close, avoid entries in final 90 minutes (15:30+ becomes hard block, not just caution).

Example: {"field":"time","operator":"between","value":[10.0,11.0],"description":"After opening vol settles"}

## Earnings Proximity (cross_asset.earnings_proximity_days)
- ≤3d: IV elevated→sell-premium benefits, tighter stops. Note in reasoning.
- 1d: no NEW positions unless explicit earnings play(straddle/strangle)
- -1(unknown): normal rules
- >10d: ignore earnings effect

## DTE Guidelines (bounds: min 7, max 180)
- Sell-premium(iron_condor,iron_butterfly,short strangle,covered_call): 30-45 DTE
- Buy-premium directional(debit vertical,protective_put): 14-30 DTE
- Straddle/strangle long: 21-35 DTE
- Calendar/diagonal: front 14-21, back 45-60 DTE
- Collar: match holding horizon/catalyst
- earnings_proximity≤45→prefer expiry INCLUDING earnings date

Output ONLY valid JSON. No markdown fences.
"""
