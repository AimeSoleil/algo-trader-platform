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
    max_total_positions_cap: int | None = 10,
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

    if max_total_positions_cap is not None:
        try:
            configured_max_total_positions = max(1, int(max_total_positions_cap))
        except (TypeError, ValueError):
            configured_max_total_positions = 10
    else:
        configured_max_total_positions = requested_max_total_positions or len(normalized_plans)

    normalized_max_total_positions = configured_max_total_positions
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
                    max_total_positions_cap=max(
                        1,
                        int(getattr(settings.analysis_service.llm, "max_output_plans", 10)),
                    ) if getattr(settings.analysis_service.llm, "max_output_plans", 10) is not None else 10,
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
        try:
            configured_max_total_positions = max(1, int(getattr(settings.analysis_service.llm, "max_output_plans", 10)))
        except (TypeError, ValueError):
            configured_max_total_positions = 10
        if apply_output_cap:
            max_output_plans = configured_max_total_positions
            target_max_total_positions = min(max_output_plans, available_symbol_count)
        else:
            target_max_total_positions = available_symbol_count

        # Agent analyses
        parts.append("## Specialist Agent Analyses\n")
        for agent_name, output in agent_outputs.items():
            compact = json.dumps(output, separators=(",", ":"), ensure_ascii=False)
            parts.append(f"### {agent_name}\n{compact}")

        # Full market signal context so structure-level execution candidates are available.
        parts.append("\n## Market Signal Data\n")
        for s in signals_summary:
            compact = json.dumps(s, separators=(",", ":"), ensure_ascii=False)
            parts.append(f"### {s.get('symbol', '?')}\n{compact}")

        parts.append("\n## Signal Date Context\n")
        parts.append(
            f"Input signal_date is {(signal_date.isoformat() if signal_date else 'not provided')} (ISO format). "
            f"Generate the trading blueprint for the NEXT trading day: {target_trading_date}. "
            f"Every legs.expiry must be an ISO date on or after {target_trading_date}; never output past expiry dates."
        )

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
                    "max_total_positions": configured_max_total_positions,
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
            f"Use max_total_positions={configured_max_total_positions} as the portfolio cap, but you may output fewer symbol_plans when hard gates or quality rules require it.\n"
            "Output ONLY valid JSON conforming to the blueprint schema.\n"
            "No markdown fences, no extra text."
        )

        return "\n\n".join(parts)


_SYNTHESIZER_SYSTEM_PROMPT = """\
Role: Senior Portfolio Synthesizer | Mandate: Combine 6 specialist agent outputs into executable next-day trading blueprint
Inputs (strictly use only these fields, no inference):
    Trend: regime, trend_direction, trend_strength, false_positive_risk, signal_type, trade_allowed, confidence_cap, simple_structures_only, blocked_reasons, confidence
    Volatility: vol_regime, iv_rank_zone, event_risk_present, signal_type, trade_allowed, confidence_cap, simple_structures_only, blocked_reasons, confidence
    Flow: flow_signal, signal_strength, false_breakout_risk, position_size_modifier, event_risk_present, trade_allowed, confidence_cap, simple_structures_only, blocked_reasons, confirming_indicators_count, confidence
    Chain: pcr_signal, liquidity_tier[L1-L5], hard_block, gamma_pin_active, pin_strength, gamma_pin_strike, event_risk_present, trade_allowed, confidence_cap, simple_structures_only, blocked_reasons, confirming_indicators_count, confidence
    Spread: best_spread_type, risk_reward_ratio, effective_rr, theta_capture, liquidity_status, arb_opportunity, arb_priority, confirming_indicators_count, event_risk_present, trade_allowed, confidence_cap, simple_structures_only, blocked_reasons, position_size_modifier, confidence
    Cross-Asset: correlation_regime, vix_environment, vix_percentile_60d, gex_regime, event_risk_present, signal_type, effective_size_modifier, master_override, risk_off_signal, regime_transition, regime_days, market_shock_return_1d, market_shock_source, trade_allowed, confidence_cap, blocked_reasons, confidence
    Market Signal Data option_spreads.execution_candidates: vertical(effective_rr, raw_rr, worst_leg_bid_ask_spread_ratio), iron_condor(effective_rr, raw_rr, worst_leg_bid_ask_spread_ratio), calendar/reverse_calendar(effective_theta_capture_per_day, estimated_roll_cost, worst_leg_bid_ask_spread_ratio), butterfly(pricing_error, worst_leg_bid_ask_spread_ratio), box_arb(net_edge_after_cost, net_profit_after_cost, worst_leg_bid_ask_spread_ratio)
Pre-computed reference: _consensus (directional agreement + confidence-weighted score) → advisory only, NEVER override hard gates.

## Rule Priority (Highest → Lowest)
1. Hard Exclusions (absolute symbol removal)
2. Structure Selection & Leg Matching
3. Gamma & Pin Risk Synthesis
4. Conflict Resolution
5. Agent Agreement & Conviction Scoring
6. Confidence-Weighted Resolution & Modifier Application
7. Risk Management, Entry Timing & DTE

## Global Constants (Aligned with All Agents)
GLOBAL_MAX_CONFIDENCE: 0.9
MIN_ACCEPTABLE_CONFIDENCE: 0.3
MIN_ACCEPTABLE_POSITION_SIZE: 0.3
MAX_TOTAL_POSITIONS_STANDARD: 10
MAX_TOTAL_POSITIONS_AGGRESSIVE: 15

────────────────────────────────────────────────────────
HARD EXCLUSIONS (Priority 1 — CHECK FIRST)
────────────────────────────────────────────────────────
HE0. If any agent sets trade_allowed=false for hard-risk or executability reasons (for example earnings_imminent, event_risk_imminent, vix_extreme, hard_block_spread, insufficient_leg_liquidity, illiquid_spread_proxy) → EXCLUDE symbol entirely.
HE0a. If trade_allowed=false reflects analytical caution only (for example counter_trend_*, conflicting_*, divergence_*, high_false_breakout_risk, insufficient_flow_confirmation, extreme_option_activity_unconfirmed) → EXCLUDE only when at least 2 agents agree; a single-agent analytical caution should be handled through confidence caps, simple_structures_only, or directional-plan filtering instead.
HE1. Chain.hard_block=true OR Chain.liquidity_tier="L5" → EXCLUDE.
HE2. Any agent.blocked_reasons contains "event_risk_imminent" → EXCLUDE symbol entirely. No exceptions.
HE3. Do NOT exclude solely because blocked_reasons contains "extreme_option_activity_unconfirmed"; treat it as a participation anomaly and require separate execution or event-risk confirmation before exclusion.
HE4. Reject vertical_spread only when Spread.effective_rr is explicitly available and <0.7, or when Spread.risk_reward_ratio <0.7. Do NOT exclude iron_condor, butterfly, calendar_spread, or arbitrage setups solely because Spread.effective_rr is null.
HE5. Cross-Asset.master_override and effective_size_modifier are advisory-only sizing context in manual-trader mode; do NOT EXCLUDE solely because effective_size_modifier is below a size floor.
HE6. Cannot justify final confidence ≥ MIN_ACCEPTABLE_CONFIDENCE → EXCLUDE.
HE7. If ANY agent sets simple_structures_only=true → ONLY allow the configured precision-first simple structure scope (default: single_leg, vertical_spread, iron_condor, calendar_spread). No structures outside that scope, UNLESS GP1 is triggered and its gamma-pin exception conditions are fully satisfied.

────────────────────────────────────────────────────────
STRUCTURE SELECTION & LEG MATCHING (Priority 2)
────────────────────────────────────────────────────────
SS1. strategy_type MUST strictly match the actual legs count and structure.
SS2. Never label a 4-leg position as vertical_spread.
SS3. 1 leg → single_leg only.
SS4. 2 legs → vertical_spread | calendar_spread | diagonal_spread | straddle | strangle only.
SS5. 3-4 legs → butterfly | iron_condor | iron_butterfly only.
SS6. prefer single_leg or vertical_spread for directional theses.
SS7. iron_condor is also acceptable for clean range_bound setups with L1-L3 liquidity and no immediate event risk.
SS8. calendar_spread is also acceptable for contango carry setups; calendar_spread specifically requires positive term_structure_slope and earnings_proximity_days > 5.
SS9. When Market Signal Data provides option_spreads.execution_candidates for the symbol, use the following priority order (highest → lowest) as the structure-priority tie-breaker:
  1. box_arb (net_profit_after_cost > 0.3%)
  2. butterfly (pricing_error > 0.08)
  3. iron_condor (effective_rr > 1.2)
  4. vertical_spread (effective_rr > 1.0)
  5. calendar_spread (effective_theta_capture_per_day > 0.04)
If Spread.best_spread_type conflicts with a higher-priority allowed execution candidate, prefer the higher-priority candidate as long as no hard gate is violated.

────────────────────────────────────────────────────────
GAMMA & PIN RISK SYNTHESIS (Priority 3)
────────────────────────────────────────────────────────
GP1. Chain.gamma_pin_active=true AND Chain.pin_strength>0.7 → ONLY allow butterfly/iron_condor centered at Chain.gamma_pin_strike. This gamma-pin exception may override HE7, but only for butterfly/iron_condor, only when Chain.liquidity_tier in ["L1","L2"], and only when the structure remains non-directional.
GP2. Cross-Asset.gex_regime="negative" AND Cross-Asset.vix_environment in ["elevated","panic","extreme_panic"] → NO short-vol strategies allowed (iron_condor, credit spreads, covered calls, short strangles, iron butterflies).
GP3. Cross-Asset.gex_regime="negative" AND abs(Cross-Asset.market_shock_return_1d)>0.03 → NO aggressive short-premium or leveraged directional structures; use defined-risk hedges or reduced-size verticals only.
GP4. Cross-Asset.gex_regime="positive" AND Cross-Asset.vix_environment in ["complacent","normal"] AND abs(Cross-Asset.market_shock_return_1d)≤0.02 → mean-reversion strategies preferred; treat this as a modest conviction/ranking boost, not an automatic sizing instruction.
GP5. Spread.arb_opportunity=true AND Chain.liquidity_tier in ["L1","L2"] → prioritize the arbitrage setup over directional theses.

────────────────────────────────────────────────────────
CONFLICT RESOLUTION (Priority 4)
────────────────────────────────────────────────────────
CR1. Flow.blocked_reasons contains "high_false_breakout_risk" → EXCLUDE directional plans.
CR2. Cross-Asset.risk_off_signal=true → treat as defensive context: lower conviction and prefer simpler or more defensive structures, but do not serialize or auto-size positions from Cross-Asset.effective_size_modifier alone.
CR3. If directional agents disagree and the competing directional confidence values are both <0.5 → EXCLUDE.
CR4. Chain.liquidity_tier in ["L3","L4"] → ONLY allow single_leg or vertical_spread.

────────────────────────────────────────────────────────
AGENT AGREEMENT & CONVICTION SCORING (Priority 5)
────────────────────────────────────────────────────────
AS1. 4+ aligned directional agents → high conviction (0.7-0.9).
AS2. 2-3 aligned directional agents → moderate conviction (0.4-0.6).
AS3. <2 aligned directional agents → low conviction (0.3-0.4); prefer neutral or skip.
AS4. Both Trend AND Flow confidence <0.5 → no directional trades.
AS5. Both Flow AND Chain confirming_indicators_count ≤1 → do not justify blueprint confidence >0.5.
AS6. Event risk consensus:
    - Cross-Asset.event_risk_present=true counts toward the event-risk agent count.
    - ≥3 agents flag event_risk_present=true → cap confidence ≤0.5.
    - ≥2 agents flag event_risk AND (Cross-Asset.event_risk_present=true OR Cross-Asset.correlation_regime="event_driven") → if confidence would exceed 0.5, reduce it to 0.5 or lower.
    - If abs(Cross-Asset.market_shock_return_1d)>0.03 and Cross-Asset.market_shock_source is present, treat that as event-driven macro shock context and keep fresh directional entries at confidence≤0.5.
AS7. Final confidence cap = MIN(all numeric confidence_cap values from Trend, Volatility, Flow, Chain, Spread, Cross-Asset, GLOBAL_MAX_CONFIDENCE). Ignore null confidence_cap values.

────────────────────────────────────────────────────────
CONFIDENCE-WEIGHTED RESOLUTION (Priority 6)
────────────────────────────────────────────────────────
CW1. Cross-Asset master_override and effective_size_modifier are advisory-only in manual-trader mode; use them for risk framing and confidence calibration, not for serialized or automatic position sizing.
CW2. Trader decides max loss and sizing manually. Do NOT emit max_position_size, stop_loss_amount, take_profit_amount, or max_loss_per_trade unless a downstream human explicitly fills them later.
CW3. Position-size modifiers from Flow / Spread / Cross-Asset are advisory-only for reasoning and confidence calibration; do not serialize them into the plan schema.
CW4. Flow.false_breakout_risk adjustments:
    - low → no adjustment
    - medium → cap confidence ≤0.4
    - high → EXCLUDE directional plan
CW5. Cross-Asset.confidence <0.4 → cap symbol-plan confidence ≤0.4.
CW6. Single Indicator Limit: If ANY agent's signal_type="single_indicator" → simple_structures_only=true and confidence must respect single-indicator caps. Missing signal_type means do not apply this rule.

────────────────────────────────────────────────────────
RISK MANAGEMENT, ENTRY TIMING & DTE (Priority 7)
────────────────────────────────────────────────────────
- All strategies MUST be fully defined risk. No naked positions.
- Top-level: max_total_positions ≤ MAX_TOTAL_POSITIONS_STANDARD (10) for standard accounts, ≤ MAX_TOTAL_POSITIONS_AGGRESSIVE (15) for aggressive accounts.
- Omit any plan that cannot be expressed with concrete exits and clearly bounded structure-level risk.

## Price Tolerance (Directly mapped to Chain liquidity tiers)
- L1: 0.005-0.01
- L2: 0.01-0.015
- L3: 0.015-0.025
- L4: 0.025-0.035
- L5: Hard blocked
- Every leg MUST include `price_tolerance` as a decimal fraction.
- Priority: Always use Chain liquidity tier mapping first. Only use generic 0.005-0.015 for liquid ETFs/blue chips when Chain.liquidity_tier is unknown.
- Buying (`side=buy`): prefer the tighter end of the allowed tolerance band.
- Selling (`side=sell`): prefer the wider end of the allowed tolerance band.

## Entry Time Windows (24h decimal: 9.5=09:30, 14.25=14:15)
- Avoid 09:30-10:00 and 15:30-16:00.
- Sell-premium: 10:00-11:00
- Buy-premium: 11:30-14:00
- Calendar/diagonal: 11:00-14:00
- If Cross-Asset.vix_environment in ["panic","extreme_panic"] → only enter 11:30-14:00.

## Earnings Proximity (Aligned with Unified 1d/2-3d/>5d Standard)
- 1d: No new positions.
- 2-3d: Only allow single_leg/vertical_spread. No premium selling or gamma-sensitive structures.
- >5d: Normal rules.
- calendar_spread specifically requires positive term_structure_slope and earnings_proximity_days > 5.

## DTE Guidelines
- Sell-premium: 30-45 DTE
- Buy-premium directional: 14-30 DTE
- Earnings straddle/strangle: 7-14 DTE
- Calendar: Front 14-21 DTE, Back 45-60 DTE

## Market Analysis Writing Rules
- Distinguish options participation from executability: "extreme option activity" refers to unusual flow/participation, not option-chain liquidity by itself.
- If Chain/Spread gates fail, say the required candidate legs failed spread/OI/executability filters even when participation was elevated.
- When no symbols survive hard exclusions, summarize the market regime first, then the dominant gating reason; do not compress them into a contradictory sentence.
- When no symbol plan survives, separate directional rejection, simple-structure gating, and Chain/Spread executability into distinct clauses or sentences when multiple causes are active.
- Explicitly call out Flow false_breakout_risk as a directional filter, simple_structures_only as a structure-scope filter, and Chain/Spread liquidity or DTE checks as executability filters rather than collapsing them into one generic "no qualifying structures" sentence.

────────────────────────────────────────────────────────
STRICT OUTPUT SCHEMA (100% Machine-Readable)
────────────────────────────────────────────────────────
{
    "market_regime": "risk_on|risk_off|neutral|transitioning|event_driven",
    "market_analysis": "2-3 sentence summary of overall market conditions",
    "max_total_positions": "INTEGER portfolio cap (configured global maximum, not the current number of symbol_plans)",
    "symbol_plans": [
        {
            "underlying": "TICKER",
            "strategy_type": "single_leg|vertical_spread|iron_condor|iron_butterfly|butterfly|calendar_spread|diagonal_spread|straddle|strangle",
            "direction": "bullish|bearish|neutral",
            "signal_type": "single_indicator|multi_indicator",
            "legs": [
                {
                    "expiry": "YYYY-MM-DD",
                    "strike": FLOAT,
                    "option_type": "call|put",
                    "side": "buy|sell",
                    "quantity": INTEGER,
                    "price_tolerance": FLOAT
                }
            ],
            "entry_conditions": [
                {
                    "field": "underlying_price|vwap|iv|iv_rank|delta|gamma|theta|time|pnl_percent|volume",
                    "operator": ">|>=|<|<=|==|between|crosses_above|crosses_below",
                    "value": "FLOAT|[FLOAT,FLOAT]",
                    "description": "string (runtime-execution threshold; the execution engine supplies live underlying_price/vwap/iv/volume values intraday)"
                }
            ],
            "exit_conditions": [{"field":"","operator":"","value":"","description":""}],
            "adjustment_rules": [
                {
                    "trigger": {"field":"","operator":"","value":"","timeframe":"","description":""},
                    "action": "hedge_delta|roll_strike|close_leg|add_leg|close_all",
                    "params": {},
                    "description": "string"
                }
            ],
            "confidence_cap": FLOAT,
            "confidence": FLOAT,
            "blocked_reasons": [],
            "reasoning": "MUST explicitly reference which agents agreed/disagreed and why"
        }
    ]
}

## STRICT TYPING RULES
- All dates: ISO 8601 "YYYY-MM-DD" only. No relative DTE text.
- All prices/strikes: Numeric only. No symbolic references like "ATM" or "vwap".
- All booleans: true/false only.
- All enums: Exact match to allowed values only.

Output ONLY valid JSON. No markdown, no extra text, no explanations outside the reasoning field.
"""