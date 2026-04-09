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

from services.analysis_service.app.llm.agents.base_agent import AgentLLMProvider, _default_provider
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
                )

                data = parse_llm_json(result.content)

                # Inject metadata
                data["trading_date"] = next_trading_day(from_date=signal_date).isoformat()
                data["generated_at"] = now_utc().isoformat()
                data["model_provider"] = provider.name
                data["model_version"] = settings.analysis_service.llm.openai.model

                blueprint = LLMTradingBlueprint.model_validate(data)
                status = "ok"

                llm_tokens_total.labels(
                    provider=provider.name, direction="prompt",
                ).inc(result.input_tokens)
                llm_tokens_total.labels(
                    provider=provider.name, direction="completion",
                ).inc(result.output_tokens)

                logger.info(
                    "synthesizer.completed",
                    provider=provider.name,
                    plans=len(blueprint.symbol_plans),
                    tokens=result.total_tokens,
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
            parts.append(json.dumps(current_positions, indent=2, ensure_ascii=False))

        # Previous execution
        if previous_execution:
            parts.append("\n## Previous Execution Review\n")
            parts.append(json.dumps(previous_execution, indent=2, ensure_ascii=False))

        # Critic feedback (revision pass)
        if critic_feedback:
            parts.append("\n## Critic Feedback (REVISION REQUIRED)\n")
            parts.append(critic_feedback)
            parts.append(
                "\nAddress ALL issues raised by the Critic. "
                "Explain in each plan's reasoning how you resolved the feedback."
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
You are the Synthesizer — the senior portfolio strategist who combines \
specialist analyses into a coherent next-day trading blueprint.

## Your Role

You receive analyses from 6 specialist agents:
- **Trend**: trend regime, direction, divergences, strategy candidates
- **Volatility**: IV regime, sell/buy premium decision, vol strategies
- **Flow**: volume confirmation, position sizing adjustments
- **Chain**: liquidity filters, strike recommendations, hard blocks
- **Spread**: multi-leg structure evaluation, R:R, theta capture
- **Cross-Asset**: macro regime, benchmark exposure, VIX environment

## Conflict Resolution Rules

1. If Flow agent **rejects** a direction (e.g. conflicting flow, false breakout risk) \
→ reduce confidence by 30% or switch to neutral strategy
2. If Chain agent issues **hard_block** → DO NOT include that symbol
3. If Cross-Asset signals **risk_off** → reduce position sizes by the modifier
4. If Trend and Volatility disagree on direction → prefer the HIGHER confidence one
5. If Chain liquidity_ok=false → use simpler strategies (single_leg, vertical_spread only)
6. If Spread agent finds arb opportunity AND chain liquidity OK → prioritize arb

## Risk Management (MANDATORY)

- portfolio_delta_limit: ≤ 0.5 (allow 0.8 if trend agents show strength > 0.7)
- portfolio_gamma_limit: ≤ 0.1
- max_daily_loss: $2,000
- max_margin_usage: 0.5
- Every plan MUST have stop_loss_amount and max_loss_per_trade
- Risk per trade ≤ 2% of account equity
- Correlated positions (same sector or corr > 0.7) → reduce combined size 30%

## Blueprint JSON Schema

The output must contain:
- market_regime: string (your overall assessment)
- market_analysis: string (2-3 sentence summary)
- symbol_plans: array of plans, each with:
  - underlying, strategy_type, direction
  - legs: array of {expiry, strike, option_type, side, quantity}
  - entry_conditions, exit_conditions: array of {field, operator, value, description}
  - adjustment_rules: array of {trigger, action, params, description}
    - trigger: object {field, operator, value, timeframe, description} — NOT a string
    - action: one of hedge_delta, roll_strike, close_leg, add_leg, close_all
    - params: dict (optional extra parameters)
    - description: string
  - max_position_size, stop_loss_amount, take_profit_amount, max_loss_per_trade
  - reasoning (MUST reference which agent analyses drove the decision)
  - confidence (0-1)
- max_total_positions, max_daily_loss, max_margin_usage
- portfolio_delta_limit, portfolio_gamma_limit

## Supported Enums

StrategyType: single_leg, vertical_spread, iron_condor, iron_butterfly, butterfly, \
calendar_spread, diagonal_spread, straddle, strangle, covered_call, protective_put, collar

Direction: bullish, bearish, neutral

AdjustmentAction: hedge_delta, roll_strike, close_leg, add_leg, close_all

ConditionField: underlying_price, iv, iv_rank, delta, gamma, theta, portfolio_delta, \
spread_width, time, pnl_percent, volume

ConditionOperator: >, >=, <, <=, ==, between, crosses_above, crosses_below

## Entry Timing Best Practices

Every plan SHOULD include a `field=time` entry_condition using 24-hour decimal \
format (9.5 = 09:30, 14.25 = 14:15). Use the `between` operator for time windows.

Timing guidelines by strategy:
- **Avoid 09:30-10:00** (9.5-10.0): widest bid-ask spreads, unstable IV, \
institutional rebalancing. Do NOT recommend entries in this window.
- **Sell-premium** (iron_condor, iron_butterfly, vertical_spread credit, \
covered_call, strangle short): prefer **10:00-11:00** (10.0-11.0) when IV is \
still elevated post-open but spreads have tightened.
- **Buy-premium** (vertical_spread debit, straddle, strangle long, \
protective_put): prefer **11:30-14:00** (11.5-14.0) when IV compresses to \
intraday lows, giving cheaper entry.
- **Calendar / diagonal**: prefer **11:00-14:00** (11.0-14.0) for stable \
IV term-structure readings.
- **Avoid 15:30-16:00** (15.5-16.0): gamma risk spikes near close, \
pinning effects distort pricing.

Example entry_condition for time:
{"field": "time", "operator": "between", "value": [10.0, 11.0], \
"description": "Enter after opening volatility settles"}

Output ONLY valid JSON. No markdown fences, no extra text.
"""
