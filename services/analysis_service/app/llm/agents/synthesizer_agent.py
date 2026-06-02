"""SynthesizerAgent — Combine specialist analyses into a coherent trading blueprint.

Receives outputs from all 6 specialist agents, resolves conflicts,
applies risk-management rules, and produces the final LLMTradingBlueprint.
"""
from __future__ import annotations

import asyncio
import json
import random
import re
from datetime import date, timedelta
from time import perf_counter
from typing import Any

from pydantic import ValidationError

from shared.config import get_settings
from shared.metrics import llm_request_duration, llm_retries_total, llm_tokens_total
from shared.models.blueprint import ConditionField, ConditionOperator, LLMTradingBlueprint
from shared.utils import decode_escaped_unicode, get_logger, now_utc, next_trading_day

from services.analysis_service.app.llm.agents.base_agent import AgentLLMProvider, LLMUsageTracker, _default_provider
from services.analysis_service.app.llm.json_utils import parse_llm_json

logger = get_logger("synthesizer_agent")

_NUMBER_RE = re.compile(r"[+-]?\d+(?:\.\d+)?")
_DTE_RE = re.compile(r"(?P<start>\d+)(?:\s*-\s*(?P<end>\d+))?\s*dte", re.I)
_SIDE_ALIASES = {
    "buy": "buy",
    "sell": "sell",
    "long": "buy",
    "short": "sell",
    "buy_to_open": "buy",
    "sell_to_open": "sell",
    "bto": "buy",
    "sto": "sell",
}

_EXPECTED_LEGS = {
    "single_leg": (1, 1),
    "vertical_spread": (2, 2),
    "iron_condor": (4, 4),
    "iron_butterfly": (4, 4),
    "butterfly": (3, 4),
    "calendar_spread": (2, 2),
    "diagonal_spread": (2, 2),
    "straddle": (2, 2),
    "strangle": (2, 2),
    "covered_call": (1, 2),
    "protective_put": (1, 2),
    "collar": (2, 3),
}


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


def _extract_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None
    match = _NUMBER_RE.search(value)
    if not match:
        return None
    return float(match.group())


def _normalize_expiry(value: Any, signal_date: date | None) -> Any:
    if not isinstance(value, str):
        return value

    minimum_expiry = next_trading_day(from_date=signal_date)
    raw = value.strip()
    try:
        parsed_date = date.fromisoformat(raw)
        if parsed_date < minimum_expiry:
            return minimum_expiry.isoformat()
        return parsed_date.isoformat()
    except ValueError:
        pass

    match = _DTE_RE.search(raw)
    if not match:
        return value

    start_days = int(match.group("start"))
    end_days = int(match.group("end") or start_days)
    target_days = round((start_days + end_days) / 2)
    base_date = signal_date or now_utc().date()
    return next_trading_day(from_date=base_date + timedelta(days=target_days)).isoformat()


def _normalize_numeric_value(value: Any) -> float | list[float] | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, list):
        normalized = []
        for item in value:
            parsed = _extract_float(item)
            if parsed is None:
                return None
            normalized.append(parsed)
        return normalized
    if isinstance(value, str):
        parsed = _extract_float(value)
        return parsed
    return None


def _normalize_leg_side(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    key = re.sub(r"[\s-]+", "_", value.strip().lower())
    return _SIDE_ALIASES.get(key, value)


def _is_valid_enum_value(enum_cls: type, value: Any) -> bool:
    try:
        enum_cls(value)
    except (ValueError, TypeError):
        return False
    return True


def _append_dropped_sample(samples: list[Any], item: Any, limit: int = 3) -> None:
    if len(samples) >= limit:
        return
    try:
        safe_item = json.loads(json.dumps(item, ensure_ascii=False, default=str))
    except (TypeError, ValueError):
        safe_item = str(item)
    samples.append(safe_item)


def _strategy_legs_match(strategy_type: str, legs: list[dict[str, Any]]) -> bool:
    rng = _EXPECTED_LEGS.get(strategy_type)
    if rng is None:
        return True
    return rng[0] <= len(legs) <= rng[1]


def _infer_strategy_type_from_legs(legs: list[dict[str, Any]]) -> str | None:
    if not legs:
        return None

    normalized_legs = [leg for leg in legs if isinstance(leg, dict)]
    if len(normalized_legs) != len(legs):
        return None

    n = len(normalized_legs)
    option_types = [leg.get("option_type") for leg in normalized_legs]
    sides = [leg.get("side") for leg in normalized_legs]
    strikes = [leg.get("strike") for leg in normalized_legs]
    expiries = [leg.get("expiry") for leg in normalized_legs]

    if any(option_type not in {"call", "put"} for option_type in option_types):
        return None
    if any(side not in {"buy", "sell"} for side in sides):
        return None
    if any(not isinstance(strike, (int, float)) for strike in strikes):
        return None

    if n == 1:
        return "single_leg"

    if n == 2:
        same_expiry = len(set(expiries)) == 1
        same_option_type = len(set(option_types)) == 1
        buy_count = sum(1 for side in sides if side == "buy")
        sell_count = sum(1 for side in sides if side == "sell")
        if same_option_type and buy_count == 1 and sell_count == 1:
            if same_expiry:
                return "vertical_spread"
            return "calendar_spread"

        if set(option_types) == {"call", "put"}:
            call_strike = next(leg.get("strike") for leg in normalized_legs if leg.get("option_type") == "call")
            put_strike = next(leg.get("strike") for leg in normalized_legs if leg.get("option_type") == "put")
            if call_strike == put_strike:
                return "straddle"
            if call_strike > put_strike:
                return "strangle"
        return None

    if n in {3, 4} and len(set(option_types)) == 1:
        buy_count = sum(1 for side in sides if side == "buy")
        sell_count = sum(1 for side in sides if side == "sell")
        unique_strikes = sorted(set(float(strike) for strike in strikes))
        if len(unique_strikes) == 3 and buy_count == 2 and sell_count in {1, 2}:
            middle_strike = unique_strikes[1]
            middle_count = sum(1 for strike in strikes if float(strike) == middle_strike)
            if (n == 3 and middle_count == 1) or (n == 4 and middle_count == 2):
                return "butterfly"

    if n == 4 and set(option_types) == {"call", "put"}:
        sorted_legs = sorted(normalized_legs, key=lambda leg: float(leg.get("strike", 0)))
        put_long, put_short, call_short, call_long = sorted_legs
        if (
            put_long.get("option_type") == "put"
            and put_short.get("option_type") == "put"
            and call_short.get("option_type") == "call"
            and call_long.get("option_type") == "call"
            and put_long.get("side") == "buy"
            and put_short.get("side") == "sell"
            and call_short.get("side") == "sell"
            and call_long.get("side") == "buy"
        ):
            short_strikes = sorted(
                float(leg.get("strike", 0))
                for leg in normalized_legs
                if leg.get("side") == "sell"
            )
            if len(short_strikes) == 2 and short_strikes[0] == short_strikes[1]:
                return "iron_butterfly"
            return "iron_condor"

    return None


def _repair_strategy_type_from_legs(normalized_plan: dict[str, Any]) -> tuple[dict[str, Any], bool, dict[str, Any] | None]:
    strategy_value = normalized_plan.get("strategy_type")
    legs = normalized_plan.get("legs")
    if not isinstance(strategy_value, str) or not isinstance(legs, list):
        return normalized_plan, False, None

    strategy_type = strategy_value.strip().lower()
    if not strategy_type or _strategy_legs_match(strategy_type, legs):
        return normalized_plan, False, None

    inferred_strategy_type = _infer_strategy_type_from_legs(legs)
    if inferred_strategy_type is None or inferred_strategy_type == strategy_type:
        return normalized_plan, False, None

    repaired_plan = dict(normalized_plan)
    repaired_plan["strategy_type"] = inferred_strategy_type
    return repaired_plan, True, {
        "underlying": repaired_plan.get("underlying"),
        "from": strategy_type,
        "to": inferred_strategy_type,
        "legs": len(legs),
    }


def _normalize_trigger_conditions(items: Any) -> tuple[list[dict[str, Any]], int, list[Any]]:
    if not isinstance(items, list):
        return [], 0, []

    normalized_items: list[dict[str, Any]] = []
    dropped = 0
    dropped_samples: list[Any] = []
    for item in items:
        if not isinstance(item, dict):
            dropped += 1
            _append_dropped_sample(dropped_samples, item)
            continue
        normalized = dict(item)
        if not _is_valid_enum_value(ConditionField, normalized.get("field")):
            dropped += 1
            _append_dropped_sample(dropped_samples, item)
            continue
        if not _is_valid_enum_value(ConditionOperator, normalized.get("operator")):
            dropped += 1
            _append_dropped_sample(dropped_samples, item)
            continue
        normalized_value = _normalize_numeric_value(normalized.get("value"))
        if normalized_value is None:
            dropped += 1
            _append_dropped_sample(dropped_samples, item)
            continue
        normalized["value"] = normalized_value
        normalized_items.append(normalized)
    return normalized_items, dropped, dropped_samples


def _normalize_adjustment_rules(items: Any) -> tuple[list[dict[str, Any]], int, list[Any]]:
    if not isinstance(items, list):
        return [], 0, []

    normalized_items: list[dict[str, Any]] = []
    dropped = 0
    dropped_samples: list[Any] = []
    for item in items:
        if not isinstance(item, dict):
            dropped += 1
            _append_dropped_sample(dropped_samples, item)
            continue
        normalized = dict(item)
        trigger = normalized.get("trigger")
        if not isinstance(trigger, dict):
            dropped += 1
            _append_dropped_sample(dropped_samples, item)
            continue
        normalized_trigger = dict(trigger)
        if not _is_valid_enum_value(ConditionField, normalized_trigger.get("field")):
            dropped += 1
            _append_dropped_sample(dropped_samples, item)
            continue
        if not _is_valid_enum_value(ConditionOperator, normalized_trigger.get("operator")):
            dropped += 1
            _append_dropped_sample(dropped_samples, item)
            continue
        normalized_value = _normalize_numeric_value(normalized_trigger.get("value"))
        if normalized_value is None:
            dropped += 1
            _append_dropped_sample(dropped_samples, item)
            continue
        normalized_trigger["value"] = normalized_value
        normalized["trigger"] = normalized_trigger
        normalized_items.append(normalized)
    return normalized_items, dropped, dropped_samples


def _plan_sort_metric(
    plan: dict[str, Any],
    key: str,
    *,
    default: float,
) -> float:
    value = plan.get(key, default)
    normalized_value = _normalize_numeric_value(value)
    if isinstance(normalized_value, list) or normalized_value is None:
        return default
    return float(normalized_value)


def _plan_output_sort_key(plan: dict[str, Any], original_index: int) -> tuple[float, float, float, int]:
    """Rank trimmed single-pass plans by explicit score or combined quality-confidence."""
    quality_score = _plan_sort_metric(plan, "data_quality_score", default=1.0)
    confidence = _plan_sort_metric(plan, "confidence", default=0.0)
    explicit_score = _plan_sort_metric(plan, "score", default=float("-inf"))
    combined_score = explicit_score if explicit_score != float("-inf") else quality_score * confidence
    return (
        combined_score,
        quality_score,
        confidence,
        -original_index,
    )


def _normalize_blueprint_payload(
    data: dict[str, Any],
    signal_date: date | None,
    *,
    max_output_plans: int | None = 10,
) -> tuple[dict[str, Any], dict[str, Any]]:
    normalized_data = dict(data)
    stats = {
        "legs_expiry_normalized": 0,
        "legs_strike_normalized": 0,
        "legs_side_normalized": 0,
        "strategy_type_repaired": 0,
        "strategy_type_repair_samples": [],
        "max_total_positions_normalized": 0,
        "symbol_plans_trimmed_to_max_output_plans": 0,
        "legacy_top_level_fields_removed": 0,
        "entry_conditions_dropped": 0,
        "entry_conditions_dropped_samples": [],
        "exit_conditions_dropped": 0,
        "exit_conditions_dropped_samples": [],
        "adjustment_rules_dropped": 0,
        "adjustment_rules_dropped_samples": [],
    }

    normalized_max_output_plans: int | None = None
    if max_output_plans is not None:
        try:
            normalized_max_output_plans = max(1, int(max_output_plans))
        except (TypeError, ValueError):
            normalized_max_output_plans = 10

    symbol_plans = normalized_data.get("symbol_plans")
    if not isinstance(symbol_plans, list):
        return normalized_data, stats

    normalized_plans: list[dict[str, Any]] = []
    for plan in symbol_plans:
        if not isinstance(plan, dict):
            continue
        normalized_plan = dict(plan)

        legs = normalized_plan.get("legs")
        if isinstance(legs, list):
            normalized_legs = []
            for leg in legs:
                if not isinstance(leg, dict):
                    continue
                normalized_leg = dict(leg)

                original_expiry = normalized_leg.get("expiry")
                updated_expiry = _normalize_expiry(original_expiry, signal_date)
                if updated_expiry != original_expiry:
                    stats["legs_expiry_normalized"] += 1
                normalized_leg["expiry"] = updated_expiry

                original_strike = normalized_leg.get("strike")
                updated_strike = _normalize_numeric_value(original_strike)
                if updated_strike is not None and updated_strike != original_strike:
                    stats["legs_strike_normalized"] += 1
                    normalized_leg["strike"] = updated_strike

                original_side = normalized_leg.get("side")
                updated_side = _normalize_leg_side(original_side)
                if updated_side != original_side:
                    stats["legs_side_normalized"] += 1
                    normalized_leg["side"] = updated_side

                normalized_legs.append(normalized_leg)
            normalized_plan["legs"] = normalized_legs

        normalized_plan, repaired, repair_sample = _repair_strategy_type_from_legs(normalized_plan)
        if repaired:
            stats["strategy_type_repaired"] += 1
            if repair_sample is not None:
                _append_dropped_sample(stats["strategy_type_repair_samples"], repair_sample)

        normalized_plan["entry_conditions"], dropped, dropped_samples = _normalize_trigger_conditions(
            normalized_plan.get("entry_conditions")
        )
        stats["entry_conditions_dropped"] += dropped
        for sample in dropped_samples:
            _append_dropped_sample(stats["entry_conditions_dropped_samples"], sample)

        normalized_plan["exit_conditions"], dropped, dropped_samples = _normalize_trigger_conditions(
            normalized_plan.get("exit_conditions")
        )
        stats["exit_conditions_dropped"] += dropped
        for sample in dropped_samples:
            _append_dropped_sample(stats["exit_conditions_dropped_samples"], sample)

        normalized_plan["adjustment_rules"], dropped, dropped_samples = _normalize_adjustment_rules(
            normalized_plan.get("adjustment_rules")
        )
        stats["adjustment_rules_dropped"] += dropped
        for sample in dropped_samples:
            _append_dropped_sample(stats["adjustment_rules_dropped_samples"], sample)

        normalized_plans.append(normalized_plan)

    if normalized_max_output_plans is not None and len(normalized_plans) > normalized_max_output_plans:
        normalized_plans = [
            plan
            for _, plan in sorted(
                enumerate(normalized_plans),
                key=lambda item: _plan_output_sort_key(item[1], item[0]),
                reverse=True,
            )
        ]
        stats["symbol_plans_trimmed_to_max_output_plans"] = len(normalized_plans) - normalized_max_output_plans
        normalized_plans = normalized_plans[:normalized_max_output_plans]

    raw_max_total_positions = normalized_data.get("max_total_positions")
    try:
        requested_max_total_positions = int(raw_max_total_positions)
    except (TypeError, ValueError):
        requested_max_total_positions = None

    normalized_max_total_positions = len(normalized_plans)
    if requested_max_total_positions != normalized_max_total_positions:
        stats["max_total_positions_normalized"] = 1

    normalized_data["max_total_positions"] = normalized_max_total_positions
    normalized_data["symbol_plans"] = normalized_plans

    for legacy_key in (
        "max_daily_loss",
        "max_margin_usage",
        "portfolio_delta_limit",
        "portfolio_gamma_limit",
    ):
        if legacy_key in normalized_data:
            stats["legacy_top_level_fields_removed"] += 1
            normalized_data.pop(legacy_key, None)

    return normalized_data, stats


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
        apply_output_cap: bool = True,
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
            signal_date=signal_date,
            trade_symbols=trade_symbols,
            apply_output_cap=apply_output_cap,
        )

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
                    instructions=_SYNTHESIZER_SYSTEM_PROMPT,
                    user_prompt=prompt,
                    temperature=settings.analysis_service.llm.openai.temperature,
                    max_tokens=settings.analysis_service.llm.openai.max_tokens,
                    model=model,
                    agent_name="synthesizer",
                )

                data = parse_llm_json(result.content)
                logger.debug("synthesizer.raw_output", provider=provider.name, output=data)

                # Normalize symbol_plans: some models (e.g. sonnet) return
                # a dict keyed by symbol instead of a list.
                sp = data.get("symbol_plans")
                if isinstance(sp, dict):
                    data["symbol_plans"] = list(sp.values())

                if apply_output_cap:
                    try:
                        max_output_plans: int | None = max(
                            1,
                            int(getattr(settings.analysis_service.llm, "max_output_plans", 10)),
                        )
                    except (TypeError, ValueError):
                        max_output_plans = 10
                else:
                    max_output_plans = None
                data, normalize_stats = _normalize_blueprint_payload(
                    data,
                    signal_date,
                    max_output_plans=max_output_plans,
                )
                if any(normalize_stats.values()):
                    logger.warning(
                        "synthesizer.output_normalized",
                        provider=provider.name,
                        **normalize_stats,
                    )

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

            except ValidationError as e:
                last_exc = e
                logger.warning("synthesizer.validation_error", provider=provider.name, attempt=attempt + 1, error=decode_escaped_unicode(e))
                raise

            except (json.JSONDecodeError, ValueError, TypeError) as e:
                last_exc = e
                llm_retries_total.labels(provider=provider.name, error_type="parse").inc()
                logger.warning("synthesizer.parse_error", provider=provider.name, attempt=attempt + 1, error=decode_escaped_unicode(e))
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
                    logger.warning("synthesizer.retryable_error", provider=provider.name, attempt=attempt + 1, error=decode_escaped_unicode(e), delay=round(delay, 2))
                    await asyncio.sleep(delay)
                    continue

                logger.warning("synthesizer.failed", provider=provider.name, attempt=attempt + 1, error=decode_escaped_unicode(e))
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
        signal_date: date | None = None,
        trade_symbols: list[str] | None = None,
        apply_output_cap: bool = True,
    ) -> str:
        parts: list[str] = []
        target_trading_date = next_trading_day(from_date=signal_date).isoformat()
        settings = get_settings()
        precision_first = settings.analysis_service.llm.precision_first
        available_symbol_count = max(1, len(trade_symbols or signals_summary))
        if apply_output_cap:
            try:
                max_output_plans = max(1, int(getattr(settings.analysis_service.llm, "max_output_plans", 10)))
            except (TypeError, ValueError):
                max_output_plans = 10
            target_max_total_positions = min(max_output_plans, available_symbol_count)
        else:
            target_max_total_positions = available_symbol_count

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

        parts.append("\n## Signal Date Context\n")
        parts.append(
            f"Input signal_date is {(signal_date.isoformat() if signal_date else 'not provided')} (ISO format). "
            f"Generate the trading blueprint for the NEXT trading day: {target_trading_date}. "
            f"Every legs.expiry must be an ISO date on or after {target_trading_date}; never output past expiry dates."
        )

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
                f"Generate at most {target_max_total_positions} symbol_plans total after applying all hard exclusions and risk rules.\n"
                f"Other symbols (benchmarks) are provided as cross-asset context only — "
                f"do NOT create plans for them."
            )

        if precision_first.enabled:
            allowed_strategy_types = ", ".join(precision_first.allowed_strategy_types)
            parts.append(
                "\n## Precision-First Strategy Scope\n\n"
                f"Precision-first mode is ENABLED. You may output symbol_plans ONLY with strategy_type in: {allowed_strategy_types}.\n"
                "If a symbol requires a more complex structure to express the thesis, omit the symbol instead of using a disallowed strategy."
            )

        parts.append("\n## Blueprint Output Targets\n")
        parts.append(
            json.dumps(
                {
                    "max_total_positions": target_max_total_positions,
                },
                separators=(",", ":"),
                ensure_ascii=False,
            )
        )

        # Task
        parts.append(
            "\n## Task\n\n"
            f"Generate the Trading Blueprint for {target_trading_date}.\n"
            "Synthesize the specialist analyses into a complete Trading Blueprint JSON.\n"
            "Resolve any conflicts between agents (e.g. Flow rejects Trend's direction).\n"
            "Apply risk-management constraints to all plans.\n"
            "Keep symbol_plans count at or below max_total_positions.\n"
            "Output ONLY valid JSON conforming to the blueprint schema.\n"
            "No markdown fences, no extra text."
        )

        return "\n\n".join(parts)


_SYNTHESIZER_SYSTEM_PROMPT = """\
Role: Synthesizer — senior portfolio strategist combining 6 specialist analyses into next-day trading blueprint.

Inputs:
    Trend(regime, direction, trend_strength, divergences, trade_allowed, confidence_cap, simple_structures_only, blocked_reasons)
    Volatility(vol_regime, iv_rank_zone, hv_iv_assessment, event_risk_present, liquidity_status, trade_allowed, confidence_cap, simple_structures_only, blocked_reasons)
    Flow(flow_signal, volume_anomaly, vwap_bias, false_breakout_risk[low/medium/high], position_size_modifier, event_risk_present, liquidity_status, trade_allowed, confidence_cap, simple_structures_only, blocked_reasons, confirming_indicators_count)
    Chain(pcr_signal, liquidity_tier[L1-L5], hard_block, gamma_pin_active, institutional_flow, net_delta_exposure, event_risk_present, trade_allowed, confidence_cap, simple_structures_only, blocked_reasons, confirming_indicators_count)
    Spread(best_spread_type, risk_reward_ratio, effective_rr, theta_capture, liquidity_status, event_risk_present, trade_allowed, confidence_cap, simple_structures_only, blocked_reasons)
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
HE0. Respect any explicit machine-readable trade gate fields from agents. If Trend, Volatility, Flow, Chain, or Spread sets
    trade_allowed=false, omit that plan instead of trying to reinterpret the reasoning text.
HE1. Chain hard_block=true OR Chain liquidity_tier="L5" → EXCLUDE symbol from symbol_plans.
HE2. Combined modifier floor: if (flow.position_size_modifier × cross_asset.effective_size_modifier) < 0.3 → SKIP symbol.
     A 0.1-0.2× position is noise. Either trade at ≥0.3× or omit.
HE3. Spread effective_rr < 1.0 after costs → exclude that spread setup.
    If effective_rr is null (cannot be estimated) → OMIT that spread setup. Do not guess cost realism.
HE4. Every symbol_plan must have confidence ≥ 0.3. If you cannot justify 0.3+, omit.
HE5. It is BETTER to output fewer high-quality plans than many low-confidence ones.
HE5a. If any relevant specialist sets simple_structures_only=true, keep that symbol INSIDE the configured
     Precision-First Strategy Scope listed above. Do not invent structures outside the configured allowlist.
HE6. Precision-first default: prefer single_leg or vertical_spread for directional theses.
    In clearly range-bound, liquid, non-event setups, iron_condor is also acceptable.
    In liquid, non-event setups with positive contango and earnings_proximity_days > 5,
    calendar_spread is also acceptable.
    Use other more complex multi-leg structures only when liquidity is clearly strong, event risk is absent,
    and a simpler defined-risk structure cannot express the thesis cleanly.
HE7. strategy_type MUST strictly match the actual legs count and structure.
    - 1 leg → single_leg only
    - 2 legs → vertical_spread | calendar_spread | diagonal_spread | straddle | strangle only
    - 3-4 legs → butterfly | iron_condor | iron_butterfly | collar only when the legs truly match that structure
    Never label a 4-leg position as vertical_spread. If the legs do not clearly match the named structure, omit the symbol instead of guessing.

────────────────────────────────────────────────────────
CONFLICT RESOLUTION (Priority 2)
────────────────────────────────────────────────────────
CR1. Flow rejects direction with confidence ≥ 0.6 → OMIT that directional plan.
    If Flow confidence < 0.6, see CW4 below for confidence-scaled reduction.
CR2. Cross-Asset risk_off_signal=true → reduce size by effective_size_modifier.
CR3. Trend vs Volatility disagree on direction → prefer HIGHER confidence agent.
     If BOTH confidence < 0.5 → output neutral/SKIP.
CR4. Chain liquidity_tier in ["L4", "L5"] → stay within the configured Precision-First Strategy Scope;
    do not escalate to structures outside that configured allowlist.
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
    reduce max_position_size to 0.8 or lower and explicitly acknowledge the event risk in reasoning.
    If confidence still ends up > 0.5, justify why the setup remains tradable.
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
    - false_breakout_risk="medium" → cap directional confidence at 0.4 and max_position_size at 0.5
    - false_breakout_risk="high" → OMIT that directional plan
    For medium risk, scale the confidence cap downward if flow_confidence is high.

## Cross-Asset Confidence Guards
CQ1. If Cross-Asset agent confidence < 0.4 → cap symbol_plan confidence at ≤ 0.4.
     (The CrossAsset agent already internalizes correlation_significance and data_freshness
     into its own confidence score — use it directly rather than referencing raw signal fields.)
CQ2. If Cross-Asset regime_transition=true AND regime_days < 3 → prefer neutral/defensive
    structures; do not keep a directional plan if confidence would exceed 0.5 or max_position_size would exceed 0.5.
CQ3. If Cross-Asset effective_size_modifier ≤ 0.5 → cap max_position_size at that exact
    effective_size_modifier and avoid increasing exposure above it.

────────────────────────────────────────────────────────
RISK MANAGEMENT (Priority 5 — PLAN-LEVEL ONLY)
────────────────────────────────────────────────────────
- Every plan: stop_loss_amount + max_loss_per_trade required, and stop_loss_amount must be < max_loss_per_trade
- Omit symbols you cannot express with defined risk, concrete exits, and a credible max_loss_per_trade.

────────────────────────────────────────────────────────
BLUEPRINT JSON SCHEMA
────────────────────────────────────────────────────────
market_regime:str, market_analysis:str(2-3 sentences)
symbol_plans[]: underlying, strategy_type, direction, legs[{expiry,strike,option_type,side=buy|sell,quantity,price_tolerance(FLOAT decimal fraction; 0.005=0.5%, 0.015=1.5%)}], entry_conditions[{field,operator,value,description}], exit_conditions[], adjustment_rules[{trigger:{field,operator,value,timeframe,description},action:hedge_delta|roll_strike|close_leg|add_leg|close_all,params:{},description}], max_position_size(FLOAT 0.0-1.5, position sizing ratio: 1.0=full, 0.5=half, 0.7=70%), max_contracts(INTEGER ≥1, number of contract sets to trade), stop_loss_amount, take_profit_amount, max_loss_per_trade, reasoning(MUST reference agents), confidence(0-1)
Top-level: max_total_positions
max_total_positions should equal symbol_plans length and must not exceed the Blueprint Output Targets cap.
Legacy portfolio-level risk cap fields are not part of the agent output contract; focus on symbol_plans plus max_total_positions.

## PRICE TOLERANCE GUIDELINES
- Every leg SHOULD include `price_tolerance` as a decimal fraction, not a percent string. Example: `0.005` = 0.5%, `0.03` = 3.0%.
- Choose a single numeric tolerance per leg using this baseline:
    - Liquid ETF / blue chip: 0.005-0.015
    - Medium liquidity: 0.015-0.03
    - Illiquid (last resort): 0.03-0.04
    - High volatility: widen the selected baseline by about 0.01 when needed
    - Buying (`side=buy`): prefer the tighter end, usually 0.005-0.02
    - Selling (`side=sell`): allow the wider end, usually 0.01-0.03
- Use tighter tolerances when spreads are narrow and liquidity is strong; use wider tolerances only when liquidity or volatility clearly justifies it.

## Enums
StrategyType: single_leg|vertical_spread|iron_condor|iron_butterfly|butterfly|calendar_spread|diagonal_spread|straddle|strangle|covered_call|protective_put|collar
Direction: bullish|bearish|neutral
AdjustmentAction: hedge_delta|roll_strike|close_leg|add_leg|close_all
ConditionField: underlying_price|vwap|iv|iv_rank|delta|gamma|theta|portfolio_delta|spread_width|time|pnl_percent|volume
ConditionOperator: >|>=|<|<=|==|between|crosses_above|crosses_below

────────────────────────────────────────────────────────
ENTRY CONDITIONS ROLE
────────────────────────────────────────────────────────
entry_conditions = HARD GATES (strategic prerequisites). The intraday optimizer evaluates ALL non-time entry_conditions as boolean AND-gates before scoring timing quality. If any condition fails, the plan is skipped entirely.
- Non-time conditions (iv_rank, underlying_price, vwap, delta, volume, etc.) → hard prerequisites.
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
- 1d: no NEW positions unless explicit earnings play (straddle/strangle).
- 2-3d: avoid new premium-selling or gamma-sensitive multi-leg structures; prefer omission or simple
    defined-risk single_leg/vertical_spread only when liquidity is strong and the thesis is unusually clear.
- calendar_spread specifically requires positive term_structure_slope and earnings_proximity_days > 5.
- null (unknown): normal rules.
- >10d: ignore earnings effect.

## DTE Guidelines (bounds: min 7, max 180)
- Sell-premium (iron_condor, iron_butterfly, short strangle, covered_call): 30-45 DTE
- Buy-premium directional (debit vertical, protective_put): 14-30 DTE
- Straddle/strangle long: 21-35 DTE
- Calendar/diagonal: front 14-21, back 45-60 DTE
- Collar: match holding horizon/catalyst
- earnings_proximity ≤ 45 → prefer expiry INCLUDING earnings date

## STRICT OUTPUT TYPING
- `legs[].expiry` MUST be a concrete ISO date string (`YYYY-MM-DD`), never `30-45 DTE` text.
- `legs[].expiry` MUST be on or after the next trading day specified in the prompt; never output historical expiry dates.
- `legs[].strike` MUST be a numeric strike price, never symbolic text like `0.30_delta`.
- `legs[].side` MUST be exactly `buy` or `sell`. Never output `long` or `short`.
- `legs[].price_tolerance` SHOULD be a numeric decimal fraction, not prose. Example: output `0.015`, not `1.5% tolerance`.
- `entry_conditions[].value`, `exit_conditions[].value`, and `adjustment_rules[].trigger.value` MUST be numeric or `[low, high]` numeric lists only.
- NEVER output symbolic references like `vwap`, `short_strike`, `long_strike`, `atm`, or field names inside any `value` field.

Output ONLY valid JSON. No markdown fences.
"""
