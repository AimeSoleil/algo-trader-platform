"""Blueprint rule checker — deterministic validation against agent prompt hard constraints.

Validates LLM-generated blueprints against hard constraints embedded in the
specialist agent prompts (the single source of truth).  Can complement the
CriticAgent or run standalone for backtesting.

Usage::

    from services.analysis_service.app.evaluation.rule_checker import check_blueprint
    issues = check_blueprint(blueprint_dict, signal_features_map, agent_outputs=ao)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date as _date_type
from typing import Any

from shared.config import get_settings
from services.analysis_service.app.trade_gate_semantics import (
    SOFT_TRADE_BLOCK_CONSENSUS_MIN_COUNT,
    classify_trade_block,
    normalized_blocked_reasons,
)


_DEFAULT_SIMPLE_STRATEGY_TYPES = frozenset({"single_leg", "vertical_spread", "iron_condor", "calendar_spread"})
_SPREAD_STRATEGY_TYPES = frozenset({
    "vertical_spread", "iron_condor", "iron_butterfly",
    "butterfly", "calendar_spread", "diagonal_spread",
})
_VOLATILITY_SHORT_VOL_STRATEGY_TYPES = frozenset({"iron_condor", "iron_butterfly"})
_EXECUTION_CANDIDATE_STRATEGY_KEYS = {
    "vertical_spread": ("vertical",),
    "iron_condor": ("iron_condor",),
    "iron_butterfly": ("butterfly",),
    "butterfly": ("butterfly",),
    "calendar_spread": ("calendar",),
    "diagonal_spread": ("calendar", "reverse_calendar"),
}
_SIMPLE_STRUCTURE_AGENT_NAMES = ("trend", "volatility", "flow", "chain", "spread")
_ONE_SHOT_EXPIRY_STRATEGIES = frozenset({"single_leg", "vertical_spread"})
_TRADE_GATE_AGENT_NAMES = ("trend", "volatility", "flow", "chain", "spread", "cross_asset")
_EMITTED_STRATEGY_TYPE_ALIASES = {
    "single_leg": "single_leg",
    "single_leg_call": "single_leg",
    "single_leg_put": "single_leg",
    "vertical": "vertical_spread",
    "vertical_spread": "vertical_spread",
    "call_vertical_spread": "vertical_spread",
    "put_vertical_spread": "vertical_spread",
    "bull_put_spread": "vertical_spread",
    "bear_call_spread": "vertical_spread",
    "credit_spread": "vertical_spread",
    "calendar": "calendar_spread",
    "calendar_spread": "calendar_spread",
    "reverse_calendar": "diagonal_spread",
    "diagonal_spread": "diagonal_spread",
    "iron_condor": "iron_condor",
    "iron_butterfly": "iron_butterfly",
    "butterfly": "butterfly",
    "straddle": "straddle",
    "short_straddle": "straddle",
    "long_straddle": "straddle",
    "strangle": "strangle",
    "short_strangle": "strangle",
    "long_strangle": "strangle",
    "box_arb": "box_arb",
}


@dataclass
class RuleIssue:
    """A single rule violation found by the checker."""
    severity: str  # "error", "warning", "info"
    category: str  # "risk_breach", "liquidity", "logic_error", "strategy_mismatch"
    symbol: str | None = None  # None = portfolio-level
    rule: str = ""  # rule ID / short name
    description: str = ""


@dataclass
class CheckResult:
    """Result of running all rule checks on a blueprint."""
    issues: list[RuleIssue] = field(default_factory=list)
    passed: bool = True  # True if no error-severity issues

    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "warning")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _agent_sym(
    agent_outputs: dict[str, Any],
    agent_name: str,
    symbol: str,
) -> dict[str, Any] | None:
    """Find per-symbol data for *symbol* in *agent_name*'s output."""
    agent = agent_outputs.get(agent_name)
    if not isinstance(agent, dict):
        return None
    symbols_list = agent.get("symbols")
    if not isinstance(symbols_list, list):
        return None
    sym_upper = symbol.upper()
    return next(
        (s for s in symbols_list if isinstance(s, dict) and s.get("symbol", "").upper() == sym_upper),
        None,
    )


def _flow_high_false_breakout_directional_only_cap(
    plan: dict[str, Any],
    sym_data: dict[str, Any],
) -> bool:
    direction = str(plan.get("direction") or "").strip().lower()
    if direction != "neutral":
        return False

    false_breakout_risk = str(sym_data.get("false_breakout_risk") or "").strip().lower()
    flow_signal = str(sym_data.get("flow_signal") or "").strip().lower()
    return false_breakout_risk == "high" and flow_signal in {"neutral", "conflicting", ""}


def _canonical_strategy_family(strategy_type: Any) -> str | None:
    if not isinstance(strategy_type, str):
        return None

    normalized = str(strategy_type).strip().lower().replace("-", "_").replace(" ", "_")
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    return _EMITTED_STRATEGY_TYPE_ALIASES.get(normalized)


def _append_emitted_strategy_family(emitted: list[str], strategy_type: Any) -> None:
    canonical = _canonical_strategy_family(strategy_type)
    if canonical is None or canonical in emitted:
        return
    emitted.append(canonical)


def _emitted_strategy_types(agent_outputs: dict[str, Any], symbol: str) -> list[str]:
    emitted: list[str] = []

    spread_data = _agent_sym(agent_outputs, "spread", symbol)
    if spread_data is not None:
        _append_emitted_strategy_family(emitted, spread_data.get("best_spread_type"))

    chain_data = _agent_sym(agent_outputs, "chain", symbol)
    if chain_data is not None:
        for strategy_type in chain_data.get("suggested_strategies", []) or []:
            _append_emitted_strategy_family(emitted, strategy_type)

    for agent_name in ("trend", "volatility"):
        symbol_data = _agent_sym(agent_outputs, agent_name, symbol)
        if symbol_data is None:
            continue
        for strategy in symbol_data.get("strategies", []) or []:
            if isinstance(strategy, dict):
                _append_emitted_strategy_family(emitted, strategy.get("strategy_type"))

    return emitted


def _signal_spot_price(signal: dict[str, Any]) -> float | None:
    """Best-effort spot price lookup from serialized signal features."""
    raw = signal.get("close_price", signal.get("price"))
    if not isinstance(raw, (int, float)):
        return None
    spot = float(raw)
    return spot if spot > 0 else None


def _signal_volume(signal: dict[str, Any]) -> float | None:
    """Best-effort volume lookup from raw or serialized signal structures."""
    raw = signal.get("volume")
    if isinstance(raw, (int, float)):
        volume = float(raw)
        return volume if volume >= 0 else None

    price = signal.get("price", {})
    if isinstance(price, dict):
        nested = price.get("volume")
        if isinstance(nested, (int, float)):
            volume = float(nested)
            return volume if volume >= 0 else None

    return None


def _bollinger_position(signal: dict[str, Any], stock_indicators: dict[str, Any]) -> float | None:
    spot = _signal_spot_price(signal)
    if spot is None:
        return None

    upper = stock_indicators.get("bollinger_upper")
    lower = stock_indicators.get("bollinger_lower")
    if not isinstance(upper, (int, float)) or not isinstance(lower, (int, float)):
        return None

    upper_bound = float(upper)
    lower_bound = float(lower)
    if upper_bound <= lower_bound:
        return None

    return (spot - lower_bound) / (upper_bound - lower_bound)


def _configured_simple_strategy_types() -> frozenset[str]:
    """Return the configured precision-first allowlist for simple-structure gates."""
    try:
        precision_first = get_settings().analysis_service.llm.precision_first
    except Exception:
        return _DEFAULT_SIMPLE_STRATEGY_TYPES

    if not getattr(precision_first, "enabled", False):
        return _DEFAULT_SIMPLE_STRATEGY_TYPES

    configured = frozenset(
        str(strategy_type).strip().lower()
        for strategy_type in getattr(precision_first, "allowed_strategy_types", [])
        if str(strategy_type).strip()
    )
    return configured or _DEFAULT_SIMPLE_STRATEGY_TYPES


def _trade_blocking_agents(
    agent_outputs: dict[str, Any],
    symbol: str,
    *,
    classification: str,
) -> list[str]:
    agents: list[str] = []
    for agent_name in _TRADE_GATE_AGENT_NAMES:
        sym_data = _agent_sym(agent_outputs, agent_name, symbol)
        if classify_trade_block(sym_data) == classification:
            agents.append(agent_name)
    return agents


def check_blueprint(
    blueprint: dict[str, Any],
    signal_features: dict[str, dict[str, Any]] | None = None,
    *,
    agent_outputs: dict[str, dict[str, Any]] | None = None,
    account_size: float = 100_000.0,
    context: dict[str, Any] | None = None,
) -> CheckResult:
    """Run all deterministic rule checks on a blueprint.

    Parameters
    ----------
    blueprint:
        Blueprint dict (model_dump output of LLMTradingBlueprint).
    signal_features:
        Optional mapping of symbol → serialized signal data for
        context-aware checks (e.g. ADX-based counter-trend detection).
    agent_outputs:
        Optional dict of specialist agent outputs (as returned by
        ``AgentOrchestrator._run_specialists``).  Enables additional
        checks that validate blueprint plans against agent hard
        constraints (chain hard-block, trade gates, execution-candidate
        conflicts, etc.).
    account_size:
        Account size in dollars used to compute daily-loss caps.
        Default 100_000 keeps the checker aligned with the configured 2-3% daily-loss bands.
    context:
        Optional dict with market context (e.g. ``trend_strength``).

    Returns
    -------
    CheckResult
        Aggregated issues from all checks.
    """
    result = CheckResult()
    signal_features = signal_features or {}
    agent_outputs = agent_outputs or {}
    _check_duplicate_symbols(blueprint, result)
    for plan in blueprint.get("symbol_plans", []):
        _check_plan_risk(plan, result)
        _check_strategy_legs(plan, result)
        _check_plan_reasoning(plan, result)
        _check_strike_ordering(plan, result)
        _check_greeks_direction(plan, result)
        _check_dte_bounds(plan, result)
        _check_expiry_consistency(plan, result)
        sym = plan.get("underlying", "").upper()
        if signal_features:
            sig = signal_features.get(sym, {})
            _check_counter_trend(plan, sig, result)
            _check_trend_adx_zscore_guard(plan, sig, result)
            _check_trend_numeric_liquidity_guard(plan, sig, result)
            _check_volatility_backwardation_short_dte_guard(plan, sig, result)
            _check_liquidity(plan, sig, result)
            _check_vertical_spread_moneyness(plan, sig, result)
            _check_calendar_spread_context(plan, sig, result)
            _check_earnings_proximity(plan, sig, result)
            _check_confidence_quality_gate(plan, sig, result)
            _check_cross_asset_quality_guards(plan, sig, result)
            _check_spread_execution_candidate_conflicts(plan, sig, agent_outputs, result)
        if agent_outputs:
            _check_cross_asset_agent_guards(plan, agent_outputs, result)
            _check_volatility_single_indicator_limits(plan, agent_outputs, result)
            _check_trend_trade_gate(plan, agent_outputs, result)
            _check_trend_false_positive_risk(plan, agent_outputs, result)
            _check_flow_trade_gate(plan, agent_outputs, result)
            _check_chain_hard_block(plan, agent_outputs, result)
            _check_chain_gamma_pin_exception_requirements(plan, agent_outputs, result)
            _check_chain_trade_gate(plan, agent_outputs, result)
            _check_volatility_trade_gate(plan, agent_outputs, result)
            _check_spread_trade_gate(plan, agent_outputs, result)
            _check_soft_trade_block_consensus(plan, agent_outputs, result)
            _check_spread_effective_rr(plan, agent_outputs, result)
            _check_event_risk_consensus(plan, agent_outputs, result)
            _check_confirming_indicators(plan, agent_outputs, result)

    result.passed = result.error_count == 0
    return result


# ---------------------------------------------------------------------------
# Strategy-aware confidence baselines (A6)
# ---------------------------------------------------------------------------

_CONFIDENCE_BASELINES = {
    "iron_condor": 0.35, "iron_butterfly": 0.35,
    "calendar_spread": 0.30, "diagonal": 0.30,
    "straddle": 0.30, "strangle": 0.30,
    "bull_call_spread": 0.45, "bear_put_spread": 0.45,
    "bull_put_spread": 0.45, "bear_call_spread": 0.45,
}

_DEFAULT_CONFIDENCE_BASELINE = 0.40


# ---------------------------------------------------------------------------
# Plan-level risk checks
# ---------------------------------------------------------------------------


def _check_plan_risk(plan: dict, result: CheckResult) -> None:
    """Validate per-plan confidence sanity only; sizing/loss are trader-managed."""
    conf = plan.get("confidence", 0)
    sym = plan.get("underlying", "UNKNOWN")
    strategy = plan.get("strategy_type", "")
    baseline = _CONFIDENCE_BASELINES.get(strategy, _DEFAULT_CONFIDENCE_BASELINE)
    threshold = baseline * 0.6
    if conf < threshold:
        result.issues.append(RuleIssue(
            severity="warning",
            category="risk_breach",
            symbol=sym,
            rule="low_confidence",
            description=(
                f"Plan confidence={conf:.2f} below {threshold:.2f} "
                f"(60% of {strategy or 'default'} baseline {baseline:.2f}) — consider skipping"
            ),
        ))


# ---------------------------------------------------------------------------
# Strategy ↔ legs consistency
# ---------------------------------------------------------------------------

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


def _check_strategy_legs(plan: dict, result: CheckResult) -> None:
    """Verify legs count matches strategy type."""
    sym = plan.get("underlying", "UNKNOWN")
    strategy = plan.get("strategy_type", "")
    legs = plan.get("legs", [])
    n = len(legs)

    rng = _EXPECTED_LEGS.get(strategy)
    if rng and not (rng[0] <= n <= rng[1]):
        result.issues.append(RuleIssue(
            severity="error",
            category="logic_error",
            symbol=sym,
            rule="strategy_legs_mismatch",
            description=f"{strategy} expects {rng[0]}-{rng[1]} legs, got {n}",
        ))

    # Check each leg has required fields
    for i, leg in enumerate(legs):
        for field in ("expiry", "strike", "option_type", "side"):
            if not leg.get(field):
                result.issues.append(RuleIssue(
                    severity="error",
                    category="logic_error",
                    symbol=sym,
                    rule=f"leg_{i}_missing_{field}",
                    description=f"Leg {i} missing required field: {field}",
                ))


# ---------------------------------------------------------------------------
# Reasoning check
# ---------------------------------------------------------------------------


def _check_plan_reasoning(plan: dict, result: CheckResult) -> None:
    """Verify reasoning field is substantive."""
    sym = plan.get("underlying", "UNKNOWN")
    reasoning = plan.get("reasoning", "")
    if len(reasoning) < 20:
        result.issues.append(RuleIssue(
            severity="warning",
            category="missing_data",
            symbol=sym,
            rule="reasoning_too_short",
            description=f"Reasoning is only {len(reasoning)} chars — should explain agent analyses",
        ))

    entries = plan.get("entry_conditions", [])
    if not entries:
        result.issues.append(RuleIssue(
            severity="warning",
            category="logic_error",
            symbol=sym,
            rule="no_entry_conditions",
            description="Plan has no entry conditions — should define at least one",
        ))

    exits = plan.get("exit_conditions", [])
    if not exits:
        result.issues.append(RuleIssue(
            severity="warning",
            category="logic_error",
            symbol=sym,
            rule="no_exit_conditions",
            description="Plan has no exit conditions — should define at least one",
        ))

    adjustment_rules = plan.get("adjustment_rules", [])
    if adjustment_rules:
        return

    strategy = str(plan.get("strategy_type", "")).lower()
    if strategy in _ONE_SHOT_EXPIRY_STRATEGIES:
        reasoning_lower = reasoning.lower()
        if "hold to expiry" in reasoning_lower or "one-shot" in reasoning_lower or "no adjustment" in reasoning_lower:
            return
        result.issues.append(RuleIssue(
            severity="warning",
            category="logic_error",
            symbol=sym,
            rule="adjustment_rules_missing_reasoning",
            description=(
                "One-shot structure omitted adjustment_rules but reasoning does not explain hold-to-expiry or no-adjustment intent"
            ),
        ))
        return

    result.issues.append(RuleIssue(
        severity="warning",
        category="logic_error",
        symbol=sym,
        rule="adjustment_rules_missing",
        description="Plan has no adjustment_rules for a non one-shot structure",
    ))


# ---------------------------------------------------------------------------
# Context-aware checks (require signal data)
# ---------------------------------------------------------------------------


def _check_counter_trend(plan: dict, signal: dict, result: CheckResult) -> None:
    """ADX>30 → do NOT enter counter-trend (trend-momentum.md rule 9).

    A4: downgrade to warning if 2+ confluence signals present AND stop_loss is set.
    """
    sym = plan.get("underlying", "UNKNOWN")
    trend = signal.get("stock_indicators", {})
    if not isinstance(trend, dict):
        trend = {}
    option_indicators = signal.get("option_indicators", {})
    if not isinstance(option_indicators, dict):
        option_indicators = {}
    adx = trend.get("adx_14", 0)
    trend_dir = trend.get("trend", trend.get("trend_direction", "neutral"))
    plan_dir = plan.get("direction", "neutral")

    if adx > 30 and trend_dir != "neutral" and plan_dir != "neutral":
        is_counter = (
            (trend_dir == "bullish" and plan_dir == "bearish")
            or (trend_dir == "bearish" and plan_dir == "bullish")
        )
        if is_counter:
            # A4: check confluence signals
            confluence_count = 0
            pcr = option_indicators.get("pcr_volume", 0)
            if pcr > 1.5 or pcr < 0.5:
                confluence_count += 1
            bb_pos = _bollinger_position(signal, trend)
            if bb_pos is not None and (bb_pos > 0.95 or bb_pos < 0.05):
                confluence_count += 1

            has_exit_plan = bool(plan.get("exit_conditions"))
            severity = "warning" if (confluence_count >= 2 and has_exit_plan) else "error"

            result.issues.append(RuleIssue(
                severity=severity,
                category="strategy_mismatch",
                symbol=sym,
                rule="counter_trend_adx30",
                description=(
                    f"Counter-trend entry while ADX={adx:.1f}>30. "
                    f"Trend={trend_dir}, Plan direction={plan_dir}. "
                    f"Rule: do NOT enter counter-trend when ADX>30."
                ),
            ))


def _check_trend_adx_zscore_guard(plan: dict, signal: dict, result: CheckResult) -> None:
    """Trend H6: extreme ADX z-score blocks counter-trend / reversal theses."""
    sym = plan.get("underlying", "UNKNOWN")
    trend = signal.get("stock_indicators", {})
    if not isinstance(trend, dict):
        return

    adx_z_score = trend.get("adx_z_score")
    try:
        adx_z_val = float(adx_z_score) if adx_z_score is not None else None
    except (TypeError, ValueError):
        adx_z_val = None

    if adx_z_val is None or adx_z_val <= 1.5:
        return

    trend_dir = trend.get("trend", trend.get("trend_direction", "neutral"))
    plan_dir = plan.get("direction", "neutral")
    if trend_dir == "neutral" or plan_dir == "neutral":
        return

    is_counter = (
        (trend_dir == "bullish" and plan_dir == "bearish")
        or (trend_dir == "bearish" and plan_dir == "bullish")
    )
    if not is_counter:
        return

    result.issues.append(RuleIssue(
        severity="error",
        category="strategy_mismatch",
        symbol=sym,
        rule="counter_trend_adx_zscore",
        description=(
            f"Counter-trend or reversal thesis while stock_indicators.adx_z_score={adx_z_val:.2f}>1.50. "
            f"Trend={trend_dir}, plan direction={plan_dir}. Trend H6 requires blocking counter-trend setups in extreme ADX conditions."
        ),
    ))


def _check_trend_numeric_liquidity_guard(plan: dict, signal: dict, result: CheckResult) -> None:
    """Trend H5: low-liquidity names should inherit the high false-positive guardrail."""
    sym = plan.get("underlying", "UNKNOWN")
    trend = signal.get("stock_indicators", {})
    if not isinstance(trend, dict):
        return

    liquidity_threshold = trend.get("liquidity_threshold")
    try:
        liquidity_floor = float(liquidity_threshold) if liquidity_threshold is not None else None
    except (TypeError, ValueError):
        liquidity_floor = None

    volume = _signal_volume(signal)
    if liquidity_floor is None or liquidity_floor <= 0 or volume is None or volume >= liquidity_floor:
        return

    strategy = str(plan.get("strategy_type", "")).lower()
    simple_strategy_types = _configured_simple_strategy_types()
    if strategy not in simple_strategy_types:
        result.issues.append(RuleIssue(
            severity="error",
            category="strategy_mismatch",
            symbol=sym,
            rule="trend_low_liquidity_simple_structures_only",
            description=(
                f"price.volume={volume:.0f} is below stock_indicators.liquidity_threshold={liquidity_floor:.0f}. "
                f"Trend H5 implies high false_positive_risk and simple structures only, but strategy_type={strategy or 'unknown'}"
            ),
        ))


def _check_liquidity(plan: dict, signal: dict, result: CheckResult) -> None:
    """Bid-ask spread ratio check (A5: strategy-aware thresholds)."""
    sym = plan.get("underlying", "UNKNOWN")
    chain = signal.get("option_indicators", {})
    if not isinstance(chain, dict):
        chain = {}
    bid_ask_ratio = chain.get("bid_ask_spread_ratio", 0)
    strategy = plan.get("strategy_type", "")

    # A5: relax hard block for multi-leg strategies with wider wings
    if "iron_condor" in strategy or "calendar" in strategy:
        hard_threshold = 0.45
    else:
        hard_threshold = 0.25

    if bid_ask_ratio > hard_threshold:
        result.issues.append(RuleIssue(
            severity="error",
            category="liquidity",
            symbol=sym,
            rule="bid_ask_hard_block",
            description=(
                f"bid_ask_spread_ratio={bid_ask_ratio:.4f} > {hard_threshold:.2f} — "
                f"HARD BLOCK: do not trade this symbol."
            ),
        ))
    elif bid_ask_ratio > 0.15:
        result.issues.append(RuleIssue(
            severity="warning",
            category="liquidity",
            symbol=sym,
            rule="bid_ask_illiquid",
            description=f"bid_ask_spread_ratio={bid_ask_ratio:.4f} > 0.15 — illiquid, use wider limits",
        ))


def _check_volatility_backwardation_short_dte_guard(
    plan: dict,
    signal: dict,
    result: CheckResult,
) -> None:
    """Volatility H3: backwardation plus short front DTE blocks short-vol structures."""
    strategy = str(plan.get("strategy_type", "")).lower()
    if strategy not in _VOLATILITY_SHORT_VOL_STRATEGY_TYPES:
        return

    sym = plan.get("underlying", "UNKNOWN")
    slope = _signal_term_structure_slope(signal)
    front_expiry_dte = _signal_front_expiry_dte(signal)
    if slope is None or front_expiry_dte is None:
        return

    if slope < 0.0 and front_expiry_dte < 10:
        result.issues.append(RuleIssue(
            severity="error",
            category="strategy_mismatch",
            symbol=sym,
            rule="volatility_backwardation_short_dte_short_vol",
            description=(
                f"term_structure_slope={slope:.4f} and front_expiry_dte={front_expiry_dte} < 10. "
                f"Volatility H3 blocks short-vol strategy {strategy} in near-dated backwardation."
            ),
        ))


def _check_vertical_spread_moneyness(plan: dict, signal: dict, result: CheckResult) -> None:
    """Reject fully ITM call/put verticals under precision-first validation."""
    sym = plan.get("underlying", "UNKNOWN")
    if plan.get("strategy_type") != "vertical_spread":
        return

    spot = _signal_spot_price(signal)
    if spot is None:
        return

    legs = plan.get("legs", [])
    buy_leg = next((leg for leg in legs if leg.get("side") == "buy"), None)
    sell_leg = next((leg for leg in legs if leg.get("side") == "sell"), None)
    if buy_leg is None or sell_leg is None:
        return

    buy_type = buy_leg.get("option_type")
    sell_type = sell_leg.get("option_type")
    if buy_type != sell_type:
        return

    buy_strike = buy_leg.get("strike")
    sell_strike = sell_leg.get("strike")
    if not isinstance(buy_strike, (int, float)) or not isinstance(sell_strike, (int, float)):
        return

    if buy_type == "call" and max(float(buy_strike), float(sell_strike)) < spot:
        result.issues.append(RuleIssue(
            severity="error",
            category="strategy_mismatch",
            symbol=sym,
            rule="vertical_spread_fully_itm",
            description=(
                f"Call vertical strikes ({buy_strike}, {sell_strike}) are both below spot={spot:.2f} — "
                "fully ITM call verticals are rejected in precision-first validation"
            ),
        ))
    elif buy_type == "put" and min(float(buy_strike), float(sell_strike)) > spot:
        result.issues.append(RuleIssue(
            severity="error",
            category="strategy_mismatch",
            symbol=sym,
            rule="vertical_spread_fully_itm",
            description=(
                f"Put vertical strikes ({buy_strike}, {sell_strike}) are both above spot={spot:.2f} — "
                "fully ITM put verticals are rejected in precision-first validation"
            ),
        ))


def _check_earnings_proximity(plan: dict, signal: dict, result: CheckResult) -> None:
    """Block non-event strategies immediately ahead of earnings."""
    sym = plan.get("underlying", "UNKNOWN")
    cross_asset = signal.get("cross_asset_indicators", {})
    if not isinstance(cross_asset, dict):
        return

    earnings_days = cross_asset.get("earnings_proximity_days")
    if not isinstance(earnings_days, int):
        return

    strategy = plan.get("strategy_type", "")
    if strategy == "calendar_spread" and earnings_days <= 5:
        result.issues.append(RuleIssue(
            severity="error",
            category="risk_breach",
            symbol=sym,
            rule="calendar_near_earnings",
            description=(
                f"earnings_proximity_days={earnings_days} — calendar_spread requires more than 5 days "
                "of earnings buffer in precision-first validation"
            ),
        ))
    elif earnings_days <= 1 and strategy not in {"straddle", "strangle"}:
        result.issues.append(RuleIssue(
            severity="error",
            category="risk_breach",
            symbol=sym,
            rule="earnings_imminent_non_event_strategy",
            description=(
                f"earnings_proximity_days={earnings_days} — only explicit earnings plays "
                "(straddle/strangle) are allowed this close to earnings"
            ),
        ))
    elif earnings_days <= 3 and strategy in {"butterfly", "iron_butterfly"}:
        result.issues.append(RuleIssue(
            severity="error",
            category="risk_breach",
            symbol=sym,
            rule="earnings_near_gamma_sensitive_strategy",
            description=(
                f"earnings_proximity_days={earnings_days} — {strategy} is too gamma-sensitive "
                "this close to earnings"
            ),
        ))


def _check_calendar_spread_context(plan: dict, signal: dict, result: CheckResult) -> None:
    """Require explicit contango for precision-first calendar spreads."""
    if plan.get("strategy_type") != "calendar_spread":
        return

    sym = plan.get("underlying", "UNKNOWN")
    option_indicators = signal.get("option_indicators", {})
    if not isinstance(option_indicators, dict):
        option_indicators = {}

    slope = option_indicators.get("term_structure_slope")
    try:
        slope_val = float(slope)
    except (TypeError, ValueError):
        result.issues.append(RuleIssue(
            severity="error",
            category="strategy_mismatch",
            symbol=sym,
            rule="calendar_requires_contango",
            description=(
                "calendar_spread requires a positive term_structure_slope (contango), "
                "but the signal data is missing or non-numeric"
            ),
        ))
        return

    if slope_val <= 0.0:
        result.issues.append(RuleIssue(
            severity="error",
            category="strategy_mismatch",
            symbol=sym,
            rule="calendar_requires_contango",
            description=(
                f"term_structure_slope={slope_val:.4f} — calendar_spread is allowed only in contango "
                "under precision-first validation"
            ),
        ))


def _check_spread_execution_candidate_conflicts(
    plan: dict,
    signal: dict,
    agent_outputs: dict[str, Any],
    result: CheckResult,
) -> None:
    """Flag spread plans that ignore materially stronger allowed execution candidates."""
    strategy = plan.get("strategy_type", "")
    candidate_keys = _EXECUTION_CANDIDATE_STRATEGY_KEYS.get(strategy)
    if not candidate_keys:
        return

    sym = plan.get("underlying", "UNKNOWN")
    execution_candidates = _signal_execution_candidates(signal)
    if not execution_candidates:
        result.issues.append(RuleIssue(
            severity="warning",
            category="logic_error",
            symbol=sym,
            rule="spread_execution_candidate_data_missing",
            description="execution_candidates data is missing; skipping structure-priority conflict check",
        ))
        return

    current_candidates = [
        _execution_candidate_breakdown(candidate_key, execution_candidates.get(candidate_key), signal)
        for candidate_key in candidate_keys
        if execution_candidates.get(candidate_key) is not None
    ]
    if not current_candidates:
        result.issues.append(RuleIssue(
            severity="warning",
            category="logic_error",
            symbol=sym,
            rule="spread_execution_candidate_data_missing",
            description=(
                f"execution_candidates has no usable data for selected strategy {strategy}; skipping structure-priority conflict check"
            ),
        ))
        return

    current = max(current_candidates, key=lambda item: item["score"])
    stronger_alternatives: list[dict[str, Any]] = []
    for alt_strategy, alt_candidate_keys in _EXECUTION_CANDIDATE_STRATEGY_KEYS.items():
        if alt_strategy == strategy:
            continue
        if not _is_execution_candidate_allowed(alt_strategy, signal, agent_outputs, sym):
            continue

        for candidate_key in alt_candidate_keys:
            raw_candidate = execution_candidates.get(candidate_key)
            if raw_candidate is None:
                continue
            breakdown = _execution_candidate_breakdown(candidate_key, raw_candidate, signal)
            if not breakdown["candidate_available"]:
                continue
            if breakdown["score"] < 0.55:
                continue
            if breakdown["score"] - current["score"] < 0.2:
                continue
            stronger_alternatives.append({
                "strategy": alt_strategy,
                **breakdown,
            })

    if not stronger_alternatives:
        return

    emitted_strategy_types = _emitted_strategy_types(agent_outputs, sym)
    stronger_emitted_alternatives = [
        alternative for alternative in stronger_alternatives
        if alternative["strategy"] in emitted_strategy_types
    ]
    if strategy in emitted_strategy_types and not stronger_emitted_alternatives:
        strongest_unemitted = max(stronger_alternatives, key=lambda item: item["score"])
        result.issues.append(RuleIssue(
            severity="warning",
            category="logic_error",
            symbol=sym,
            rule="spread_execution_candidate_unemitted_fallback",
            description=(
                f"Selected {strategy} remains the strongest emitted valid structure. A stronger allowed "
                f"execution candidate exists for {strongest_unemitted['strategy']} via "
                f"{strongest_unemitted['candidate_key']} with score={strongest_unemitted['score']:.2f} "
                f"({strongest_unemitted['reason']}), but no specialist emitted that strategy family; "
                "preserving the emitted structure as a fallback"
            ),
        ))
        return

    strongest_pool = stronger_emitted_alternatives or stronger_alternatives
    strongest = max(strongest_pool, key=lambda item: item["score"])
    result.issues.append(RuleIssue(
        severity="error",
        category="logic_error",
        symbol=sym,
        rule="spread_execution_candidate_conflict",
        description=(
            f"Selected {strategy} relies on execution candidate {current['candidate_key']} with "
            f"score={current['score']:.2f} ({current['reason']}). A materially stronger allowed "
            f"execution candidate exists for {strongest['strategy']} via {strongest['candidate_key']} "
            f"with score={strongest['score']:.2f} ({strongest['reason']}) — structure_priority_conflict"
        ),
    ))


def _signal_execution_candidates(signal: dict[str, Any]) -> dict[str, dict[str, Any]]:
    option_indicators = signal.get("option_indicators", {})
    if isinstance(option_indicators, dict):
        spread_inputs = option_indicators.get("spread_execution_inputs")
        if isinstance(spread_inputs, dict):
            return {
                str(name): data
                for name, data in spread_inputs.items()
                if isinstance(data, dict)
            }

    option_spreads = signal.get("option_spreads", {})
    if isinstance(option_spreads, dict):
        execution_candidates = option_spreads.get("execution_candidates")
        if isinstance(execution_candidates, dict):
            return {
                str(name): data
                for name, data in execution_candidates.items()
                if isinstance(data, dict)
            }
    return {}


def _signal_earnings_days(signal: dict[str, Any]) -> int | None:
    for key in ("cross_asset_indicators", "cross_asset"):
        cross_asset = signal.get(key, {})
        if not isinstance(cross_asset, dict):
            continue
        earnings_days = cross_asset.get("earnings_proximity_days")
        if isinstance(earnings_days, int):
            return earnings_days
    return None


def _signal_term_structure_slope(signal: dict[str, Any]) -> float | None:
    option_indicators = signal.get("option_indicators", {})
    if not isinstance(option_indicators, dict):
        return None
    slope = option_indicators.get("term_structure_slope")
    try:
        return float(slope) if slope is not None else None
    except (TypeError, ValueError):
        return None


def _signal_front_expiry_dte(signal: dict[str, Any]) -> int | None:
    option_indicators = signal.get("option_indicators", {})
    if not isinstance(option_indicators, dict):
        return None
    front_expiry_dte = option_indicators.get("front_expiry_dte")
    if isinstance(front_expiry_dte, int):
        return front_expiry_dte
    try:
        return int(front_expiry_dte) if front_expiry_dte is not None else None
    except (TypeError, ValueError):
        return None


def _signal_iv_rank(signal: dict[str, Any]) -> float | None:
    for key in ("option_indicators", "option_vol_surface"):
        option_data = signal.get(key, {})
        if not isinstance(option_data, dict):
            continue
        iv_rank = option_data.get("iv_rank")
        try:
            return float(iv_rank) if iv_rank is not None else None
        except (TypeError, ValueError):
            continue
    return None


def _has_simple_structures_only_gate(
    agent_outputs: dict[str, Any],
    symbol: str,
) -> bool:
    for agent_name in _SIMPLE_STRUCTURE_AGENT_NAMES:
        symbol_data = _agent_sym(agent_outputs, agent_name, symbol)
        if symbol_data and symbol_data.get("simple_structures_only"):
            return True
    return False


def _gamma_pin_exception_allows_complex_structure(
    plan: dict[str, Any],
    agent_outputs: dict[str, Any],
) -> bool:
    """Return True when Chain GP1 allows neutral butterfly/iron_condor exception."""
    sym = plan.get("underlying", "UNKNOWN")
    strategy = str(plan.get("strategy_type", "")).lower()
    direction = str(plan.get("direction", "")).lower()
    if strategy not in {"butterfly", "iron_condor"} or direction != "neutral":
        return False

    chain_data = _agent_sym(agent_outputs, "chain", sym)
    if chain_data is None:
        return False

    if not chain_data.get("gamma_pin_active"):
        return False

    pin_strength = _safe_float(chain_data.get("pin_strength"))
    if pin_strength is None or pin_strength <= 0.7:
        return False

    liquidity_tier = str(chain_data.get("liquidity_tier") or "").upper()
    return liquidity_tier in {"L1", "L2"}


def _is_execution_candidate_allowed(
    strategy_type: str,
    signal: dict[str, Any],
    agent_outputs: dict[str, Any],
    symbol: str,
) -> bool:
    earnings_days = _signal_earnings_days(signal)
    slope = _signal_term_structure_slope(signal)
    iv_rank = _signal_iv_rank(signal)

    if _has_simple_structures_only_gate(agent_outputs, symbol):
        if strategy_type not in _configured_simple_strategy_types():
            return False

    if isinstance(earnings_days, int):
        if earnings_days <= 1:
            return False
        if earnings_days <= 3 and strategy_type != "vertical_spread":
            return False
        if earnings_days <= 5 and strategy_type in {"calendar_spread", "diagonal_spread"}:
            return False

    if strategy_type == "calendar_spread":
        return (
            isinstance(slope, float)
            and slope > 0.0
            and isinstance(iv_rank, float)
            and 25.0 <= iv_rank <= 65.0
        )

    if strategy_type == "diagonal_spread":
        return (
            isinstance(slope, float)
            and (slope > 0.0 or slope < -0.03)
            and isinstance(iv_rank, float)
            and 25.0 <= iv_rank <= 65.0
        )

    return True


def _execution_candidate_breakdown(
    candidate_key: str,
    candidate: dict[str, Any],
    signal: dict[str, Any],
) -> dict[str, Any]:
    slope = _signal_term_structure_slope(signal)
    iv_rank = _signal_iv_rank(signal)
    candidate_available = bool(candidate.get("candidate_available", False))
    worst_leg_ratio = _safe_float(candidate.get("worst_leg_bid_ask_spread_ratio"))
    liquidity_penalty = 0.0
    if worst_leg_ratio is not None:
        if worst_leg_ratio > 0.2:
            liquidity_penalty = 0.3
        elif worst_leg_ratio > 0.1:
            liquidity_penalty = 0.1

    metric_name: str | None = None
    metric_value: float | None = None
    reason = "candidate_unavailable"
    score = 0.15

    if candidate_available:
        score = 0.25
        if candidate_key == "vertical":
            metric_name = "effective_rr"
            metric_value = _safe_float(candidate.get("effective_rr"))
            if metric_value is None:
                metric_name = "raw_rr"
                metric_value = _safe_float(candidate.get("raw_rr"))
            if metric_value is None:
                reason = "vertical_rr_missing"
            elif metric_value < 0.7:
                score = 0.1
                reason = "vertical_rr_below_floor"
            elif metric_value >= 1.2:
                score = 1.0
                reason = "vertical_rr_strong"
            else:
                score = 0.55 + ((metric_value - 0.7) / 0.5) * 0.45
                reason = "vertical_rr_acceptable"

            spot = _signal_spot_price(signal)
            strike_distance_ratio = _candidate_nearest_strike_distance_ratio(
                candidate,
                spot,
                ("long_strike", "short_strike"),
            )
            if strike_distance_ratio is not None:
                if strike_distance_ratio >= 0.35:
                    score = min(score, 0.35)
                    reason = "vertical_far_from_spot"
                elif strike_distance_ratio >= 0.2:
                    score = min(score, 0.7)
                    reason = "vertical_moderately_far_from_spot"
        elif candidate_key == "iron_condor":
            metric_name = "effective_rr"
            metric_value = _safe_float(candidate.get("effective_rr"))
            if metric_value is None:
                metric_name = "raw_rr"
                metric_value = _safe_float(candidate.get("raw_rr"))
            if metric_value is None:
                reason = "iron_condor_rr_missing"
            elif 0.4 <= metric_value <= 0.6:
                score = 1.0
                reason = "iron_condor_rr_optimal"
            elif 0.3 <= metric_value <= 0.8:
                score = 0.8
                reason = "iron_condor_rr_supported"
            elif 0.2 <= metric_value <= 1.0:
                score = 0.55
                reason = "iron_condor_rr_marginal"
            elif 1.0 < metric_value <= 2.0:
                if worst_leg_ratio is not None and worst_leg_ratio <= 0.05:
                    score = 0.75
                    reason = "iron_condor_high_credit_supported"
                else:
                    score = 0.45
                    reason = "iron_condor_high_credit_unconfirmed"
            else:
                score = 0.15
                reason = "iron_condor_rr_outside_band"
        elif candidate_key == "calendar":
            metric_name = "effective_theta_capture_per_day"
            metric_value = _safe_float(candidate.get("effective_theta_capture_per_day"))
            if metric_value is None:
                reason = "calendar_theta_missing"
            elif not isinstance(slope, float) or slope <= 0.0:
                score = 0.1
                reason = "calendar_term_structure_misaligned"
            elif not isinstance(iv_rank, float) or not (25.0 <= iv_rank <= 65.0):
                score = 0.1
                reason = "calendar_iv_rank_outside_band"
            elif metric_value <= 0.0:
                score = 0.1
                reason = "calendar_theta_not_positive"
            else:
                score = 0.55 + min(metric_value / 0.05, 1.0) * 0.45
                reason = "calendar_theta_supported"
        elif candidate_key == "reverse_calendar":
            metric_name = "effective_theta_capture_per_day"
            metric_value = _safe_float(candidate.get("effective_theta_capture_per_day"))
            if metric_value is None:
                reason = "reverse_calendar_theta_missing"
            elif not isinstance(slope, float) or slope >= -0.03:
                score = 0.1
                reason = "reverse_calendar_term_structure_misaligned"
            elif metric_value <= 0.0:
                score = 0.1
                reason = "reverse_calendar_theta_not_positive"
            else:
                score = 0.55 + min(metric_value / 0.05, 1.0) * 0.45
                reason = "reverse_calendar_theta_supported"
        elif candidate_key == "butterfly":
            metric_name = "pricing_error"
            metric_value = _safe_float(candidate.get("pricing_error"))
            butterfly_effective_rr = _safe_float(candidate.get("effective_rr"))
            butterfly_net_edge = _safe_float(candidate.get("net_edge_after_cost"))
            butterfly_net_profit = _safe_float(candidate.get("net_profit_after_cost"))
            explicit_economics = [
                value for value in (butterfly_effective_rr, butterfly_net_edge, butterfly_net_profit)
                if value is not None
            ]
            if metric_value is None:
                reason = "butterfly_pricing_missing"
            elif any(value <= 0.0 for value in explicit_economics):
                score = 0.1
                reason = "butterfly_economics_negative"
            elif metric_value > 0.12:
                if explicit_economics:
                    score = 1.0
                    reason = "butterfly_pricing_strong"
                else:
                    score = 0.75
                    reason = "butterfly_pricing_strong_economics_unconfirmed"
            elif metric_value >= 0.08:
                if explicit_economics:
                    score = 0.7
                    reason = "butterfly_pricing_supported"
                else:
                    score = 0.45
                    reason = "butterfly_pricing_supported_economics_unconfirmed"
            else:
                score = 0.2
                reason = "butterfly_pricing_below_threshold"

    final_score = max(0.0, min(1.0, round(score - liquidity_penalty, 6)))
    return {
        "candidate_key": candidate_key,
        "candidate_available": candidate_available,
        "score": final_score,
        "metric_name": metric_name,
        "metric_value": round(metric_value, 6) if metric_value is not None else None,
        "reason": reason,
    }


def _safe_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _candidate_nearest_strike_distance_ratio(
    candidate: dict[str, Any],
    spot: float | None,
    strike_keys: tuple[str, ...],
) -> float | None:
    if spot is None or spot <= 0:
        return None

    distances: list[float] = []
    for strike_key in strike_keys:
        strike = _safe_float(candidate.get(strike_key))
        if strike is None:
            continue
        distances.append(abs(strike - spot) / spot)

    if not distances:
        return None

    return min(distances)


# ---------------------------------------------------------------------------
# Strike / structure checks
# ---------------------------------------------------------------------------


def _check_strike_ordering(plan: dict, result: CheckResult) -> None:
    """Validate strike ordering for multi-leg strategies."""
    sym = plan.get("underlying", "UNKNOWN")
    strategy = plan.get("strategy_type", "")
    legs = plan.get("legs", [])
    direction = plan.get("direction", "neutral")

    if strategy == "vertical_spread" and len(legs) == 2:
        buy_legs = [leg for leg in legs if leg.get("side") == "buy"]
        sell_legs = [leg for leg in legs if leg.get("side") == "sell"]
        if len(buy_legs) != 1 or len(sell_legs) != 1:
            result.issues.append(RuleIssue(
                severity="error", category="logic_error", symbol=sym,
                rule="vertical_spread_sides",
                description="Vertical spread must have exactly one buy leg and one sell leg",
            ))
            return

        buy_leg = buy_legs[0]
        sell_leg = sell_legs[0]
        buy_type = buy_leg.get("option_type", "")
        sell_type = sell_leg.get("option_type", "")
        if buy_type != sell_type:
            result.issues.append(RuleIssue(
                severity="error", category="logic_error", symbol=sym,
                rule="vertical_spread_option_type_mismatch",
                description=(
                    f"Vertical spread legs must use the same option_type "
                    f"(got buy={buy_type}, sell={sell_type})"
                ),
            ))
            return

        buy_k = buy_leg.get("strike", 0)
        sell_k = sell_leg.get("strike", 0)
        if direction == "bullish" and buy_k >= sell_k:
            result.issues.append(RuleIssue(
                severity="error", category="logic_error", symbol=sym,
                rule="strike_ordering",
                description=(
                    f"Bullish vertical spread must buy the lower strike and sell the higher strike "
                    f"(buy={buy_k}, sell={sell_k})"
                ),
            ))
        elif direction == "bearish" and buy_k <= sell_k:
            result.issues.append(RuleIssue(
                severity="error", category="logic_error", symbol=sym,
                rule="strike_ordering",
                description=(
                    f"Bearish vertical spread must buy the higher strike and sell the lower strike "
                    f"(buy={buy_k}, sell={sell_k})"
                ),
            ))

    elif strategy == "iron_condor" and len(legs) == 4:
        sorted_legs = sorted(legs, key=lambda l: l.get("strike", 0))
        put_long, put_short, call_short, call_long = sorted_legs
        violations: list[str] = []
        if put_long.get("option_type") != "put":
            violations.append(f"lowest strike leg should be a put (got {put_long.get('option_type')})")
        if put_short.get("option_type") != "put":
            violations.append(f"second-lowest strike leg should be a put (got {put_short.get('option_type')})")
        if call_short.get("option_type") != "call":
            violations.append(f"third strike leg should be a call (got {call_short.get('option_type')})")
        if call_long.get("option_type") != "call":
            violations.append(f"highest strike leg should be a call (got {call_long.get('option_type')})")
        if put_long.get("side") != "buy":
            violations.append("lowest-strike put should be a buy (long wing)")
        if put_short.get("side") != "sell":
            violations.append("second put should be a sell (short)")
        if call_short.get("side") != "sell":
            violations.append("first call should be a sell (short)")
        if call_long.get("side") != "buy":
            violations.append("highest-strike call should be a buy (long wing)")
        for v in violations:
            result.issues.append(RuleIssue(
                severity="error", category="logic_error", symbol=sym,
                rule="strike_ordering", description=f"Iron condor: {v}",
            ))

    elif strategy == "iron_butterfly" and len(legs) == 4:
        short_legs = [l for l in legs if l.get("side") == "sell"]
        long_legs = [l for l in legs if l.get("side") == "buy"]
        if len(short_legs) == 2:
            k1 = short_legs[0].get("strike", 0)
            k2 = short_legs[1].get("strike", 0)
            if k1 != k2:
                result.issues.append(RuleIssue(
                    severity="error", category="logic_error", symbol=sym,
                    rule="strike_ordering",
                    description=(
                        f"Iron butterfly: short legs must share the same strike "
                        f"(got {k1} and {k2})"
                    ),
                ))
            atm_strike = k1
            for ll in long_legs:
                ll_k = ll.get("strike", 0)
                if ll_k == atm_strike:
                    result.issues.append(RuleIssue(
                        severity="error", category="logic_error", symbol=sym,
                        rule="strike_ordering",
                        description=(
                            f"Iron butterfly: long wing at {ll_k} should be "
                            f"further OTM than short strike {atm_strike}"
                        ),
                    ))

    elif strategy == "strangle" and len(legs) == 2:
        call_leg = next((l for l in legs if l.get("option_type") == "call"), None)
        put_leg = next((l for l in legs if l.get("option_type") == "put"), None)
        if call_leg and put_leg:
            if call_leg.get("strike", 0) <= put_leg.get("strike", 0):
                result.issues.append(RuleIssue(
                    severity="error", category="logic_error", symbol=sym,
                    rule="strike_ordering",
                    description=(
                        f"Strangle: call strike ({call_leg.get('strike')}) "
                        f"must be > put strike ({put_leg.get('strike')})"
                    ),
                ))

    elif strategy == "straddle" and len(legs) == 2:
        strikes = [l.get("strike", 0) for l in legs]
        if strikes[0] != strikes[1]:
            result.issues.append(RuleIssue(
                severity="error", category="logic_error", symbol=sym,
                rule="strike_ordering",
                description=(
                    f"Straddle: both legs must share the same strike "
                    f"(got {strikes[0]} and {strikes[1]})"
                ),
            ))


def _check_greeks_direction(plan: dict, result: CheckResult) -> None:
    """Validate that strategy direction matches expected Greeks sign."""
    sym = plan.get("underlying", "UNKNOWN")
    direction = plan.get("direction", "neutral")
    legs = plan.get("legs", [])

    if direction == "neutral" or not legs:
        return

    # Estimate net delta sign from leg side + option_type
    delta_proxy = 0.0
    for leg in legs:
        side = leg.get("side", "")
        opt_type = leg.get("option_type", "")
        if side == "buy" and opt_type == "call":
            delta_proxy += 1
        elif side == "sell" and opt_type == "call":
            delta_proxy -= 1
        elif side == "buy" and opt_type == "put":
            delta_proxy -= 1
        elif side == "sell" and opt_type == "put":
            delta_proxy += 1

    if direction == "bullish" and delta_proxy < 0:
        result.issues.append(RuleIssue(
            severity="warning", category="logic_error", symbol=sym,
            rule="greeks_direction_mismatch",
            description=(
                f"Direction is bullish but estimated net delta is negative "
                f"(proxy={delta_proxy:+.0f})"
            ),
        ))
    elif direction == "bearish" and delta_proxy > 0:
        result.issues.append(RuleIssue(
            severity="warning", category="logic_error", symbol=sym,
            rule="greeks_direction_mismatch",
            description=(
                f"Direction is bearish but estimated net delta is positive "
                f"(proxy={delta_proxy:+.0f})"
            ),
        ))


def _check_dte_bounds(plan: dict, result: CheckResult) -> None:
    """Validate all legs have DTE within strategy-appropriate bounds (A1)."""
    sym = plan.get("underlying", "UNKNOWN")
    today = _date_type.today()
    strategy = plan.get("strategy_type", "")

    # A1: strategy-specific DTE ranges
    if strategy == "gamma_scalp":
        dte_min, dte_max = 0, 5
    elif strategy in ("calendar_spread", "diagonal"):
        dte_min, dte_max = 14, 200
    elif strategy in ("leaps", "long_leaps"):
        dte_min, dte_max = 60, 365
    elif strategy in ("single_leg", "vertical_spread"):
        dte_min, dte_max = 5, 180
    else:
        dte_min, dte_max = 7, 180

    for i, leg in enumerate(plan.get("legs", [])):
        expiry_str = leg.get("expiry", "")
        if not expiry_str:
            continue
        try:
            expiry = _date_type.fromisoformat(expiry_str)
        except (ValueError, TypeError):
            continue
        dte = (expiry - today).days

        if dte < dte_min:
            result.issues.append(RuleIssue(
                severity="error", category="risk_breach", symbol=sym,
                rule="dte_bounds",
                description=f"Leg {i} expiry {expiry_str} has DTE={dte} < {dte_min} — too short for {strategy or 'default'}",
            ))
        elif dte > dte_max:
            result.issues.append(RuleIssue(
                severity="warning", category="risk_breach", symbol=sym,
                rule="dte_bounds",
                description=f"Leg {i} expiry {expiry_str} has DTE={dte} > {dte_max} — unusually long for {strategy or 'default'}",
            ))


_SAME_EXPIRY_STRATEGIES = frozenset({
    "vertical_spread", "iron_condor", "iron_butterfly",
    "straddle", "strangle", "butterfly",
})

_DIFFERENT_EXPIRY_STRATEGIES = frozenset({
    "calendar_spread", "diagonal_spread",
})


def _check_expiry_consistency(plan: dict, result: CheckResult) -> None:
    """Validate expiry dates match strategy requirements."""
    sym = plan.get("underlying", "UNKNOWN")
    strategy = plan.get("strategy_type", "")
    legs = plan.get("legs", [])
    expiries = [l.get("expiry") for l in legs if l.get("expiry")]
    if not expiries:
        return

    unique_expiries = set(expiries)

    if strategy in _SAME_EXPIRY_STRATEGIES and len(unique_expiries) > 1:
        result.issues.append(RuleIssue(
            severity="error", category="logic_error", symbol=sym,
            rule="expiry_consistency",
            description=(
                f"{strategy} requires all legs to share the same expiry "
                f"(found {sorted(unique_expiries)})"
            ),
        ))
    elif strategy in _DIFFERENT_EXPIRY_STRATEGIES and len(unique_expiries) < 2:
        result.issues.append(RuleIssue(
            severity="error", category="logic_error", symbol=sym,
            rule="expiry_consistency",
            description=(
                f"{strategy} requires legs with different expiries "
                f"(all legs expire {expiries[0]})"
            ),
        ))


# ---------------------------------------------------------------------------
# Portfolio-level duplicate check
# ---------------------------------------------------------------------------


def _check_duplicate_symbols(bp: dict, result: CheckResult) -> None:
    """Flag if the same underlying appears in multiple symbol_plans."""
    seen: dict[str, int] = {}
    for plan in bp.get("symbol_plans", []):
        sym = plan.get("underlying", "").upper()
        if not sym:
            continue
        seen[sym] = seen.get(sym, 0) + 1

    for sym, count in seen.items():
        if count > 1:
            result.issues.append(RuleIssue(
                severity="warning", category="logic_error", symbol=sym,
                rule="duplicate_symbol",
                description=(
                    f"Underlying {sym} appears in {count} symbol_plans — "
                    f"consider consolidating"
                ),
            ))


# ---------------------------------------------------------------------------
# Signal-aware quality & modifier checks
# ---------------------------------------------------------------------------


def _check_confidence_quality_gate(
    plan: dict, signal: dict, result: CheckResult,
) -> None:
    """Flag over-confidence on low-quality data."""
    sym = plan.get("underlying", "UNKNOWN")
    conf = plan.get("confidence", 0)
    dq = signal.get("data_quality", {})
    score = dq.get("score") if isinstance(dq, dict) else None
    if score is None:
        return

    if conf > 0.7 and score < 0.5:
        result.issues.append(RuleIssue(
            severity="warning", category="risk_breach", symbol=sym,
            rule="overconfident_on_bad_data",
            description=(
                f"Plan confidence={conf:.2f} but data_quality.score={score:.2f} — "
                f"overconfident on low-quality signal"
            ),
        ))


def _check_cross_asset_quality_guards(
    plan: dict, signal: dict, result: CheckResult,
) -> None:
    """Validate cross-asset confidence/freshness caps against plan aggressiveness."""
    sym = plan.get("underlying", "UNKNOWN")
    cross = signal.get("cross_asset_indicators", {})
    if not isinstance(cross, dict):
        return

    conf_scores = cross.get("confidence_scores", {})
    if not isinstance(conf_scores, dict):
        return

    corr_sig = conf_scores.get("correlation_significance")
    freshness = conf_scores.get("data_freshness")
    plan_conf = plan.get("confidence", 0)
    direction = plan.get("direction", "neutral")
    if isinstance(corr_sig, (int, float)) and corr_sig < 0.5 and plan_conf > 0.4:
        result.issues.append(RuleIssue(
            severity="warning",
            category="risk_breach",
            symbol=sym,
            rule="cross_asset_low_significance_confidence_cap",
            description=(
                f"cross-asset correlation_significance={corr_sig:.2f} but "
                f"plan confidence={plan_conf:.2f} exceeds 0.40 cap"
            ),
        ))

    if isinstance(freshness, (int, float)) and freshness < 0.5 and direction in {"bullish", "bearish"} and plan_conf > 0.5:
        result.issues.append(RuleIssue(
            severity="warning",
            category="risk_breach",
            symbol=sym,
            rule="cross_asset_stale_data_aggressive_direction",
            description=(
                f"cross-asset data_freshness={freshness:.2f} with directional "
                f"plan confidence={plan_conf:.2f} is too aggressive"
            ),
        ))


def _check_cross_asset_agent_guards(
    plan: dict, agent_outputs: dict[str, Any], result: CheckResult,
) -> None:
    """Consume downstream Cross-Asset sizing/confidence guards from agent output."""
    sym = plan.get("underlying", "UNKNOWN")
    cross_data = _agent_sym(agent_outputs, "cross_asset", sym)
    if cross_data is None:
        return

    plan_conf = plan.get("confidence", 0)
    cross_conf = cross_data.get("confidence")
    try:
        cross_conf_val = float(cross_conf) if cross_conf is not None else None
    except (TypeError, ValueError):
        cross_conf_val = None

    if cross_conf_val is not None and cross_conf_val < 0.4 and plan_conf > 0.4:
        result.issues.append(RuleIssue(
            severity="error",
            category="risk_breach",
            symbol=sym,
            rule="cross_asset_agent_confidence_cap",
            description=(
                f"Cross-Asset agent confidence={cross_conf_val:.2f} but plan confidence="
                f"{plan_conf:.2f} exceeds the 0.40 downstream cap"
            ),
        ))

    regime_days = cross_data.get("regime_days")
    regime_days_unknown = regime_days is None
    if (
        cross_data.get("regime_transition") is True
        and (regime_days_unknown or (isinstance(regime_days, int) and regime_days < 3))
        and plan.get("direction", "neutral") in {"bullish", "bearish"}
        and plan_conf > 0.5
    ):
        regime_days_desc = regime_days if isinstance(regime_days, int) else "unknown"
        result.issues.append(RuleIssue(
            severity="error",
            category="risk_breach",
            symbol=sym,
            rule="cross_asset_regime_transition_directional_aggression",
            description=(
                f"Cross-Asset regime_transition=true with regime_days={regime_days_desc} requires neutral or "
                f"defensive positioning, but plan remains directional with confidence={plan_conf:.2f}"
            ),
        ))


# ---------------------------------------------------------------------------
# Agent-output checks: Chain hard block (Chain H1, Synthesizer HE1)
# ---------------------------------------------------------------------------


def _check_chain_hard_block(
    plan: dict, agent_outputs: dict[str, Any], result: CheckResult,
) -> None:
    """Block symbol if Chain agent flagged hard_block or liquidity_tier=L5.

    Agent prompt source:
    - Chain H1: bid-ask > 0.30 → hard_block=true, confidence ≤ 0.2, NO strikes
    - Synthesizer HE1: Chain hard_block=true OR liquidity_tier="L5" → EXCLUDE symbol
    """
    sym = plan.get("underlying", "UNKNOWN")
    chain_data = _agent_sym(agent_outputs, "chain", sym)
    if chain_data is None:
        return

    hard_block = chain_data.get("hard_block", False)
    liq_tier = chain_data.get("liquidity_tier", "")

    if hard_block or liq_tier == "L5":
        reason = "hard_block=true" if hard_block else f"liquidity_tier={liq_tier}"
        result.issues.append(RuleIssue(
            severity="error",
            category="liquidity",
            symbol=sym,
            rule="chain_hard_block",
            description=(
                f"Chain agent flagged {reason} — symbol must be excluded from plans"
            ),
        ))


def _check_agent_trade_gate(
    plan: dict,
    agent_outputs: dict[str, Any],
    result: CheckResult,
    *,
    agent_name: str,
    label: str,
    apply_to_strategies: frozenset[str] | None = None,
) -> None:
    """Consume structured trade gate fields from specialist agents."""
    sym = plan.get("underlying", "UNKNOWN")
    strategy = plan.get("strategy_type", "")
    simple_strategy_types = _configured_simple_strategy_types()
    if apply_to_strategies is not None and strategy not in apply_to_strategies:
        return

    sym_data = _agent_sym(agent_outputs, agent_name, sym)
    if sym_data is None:
        return

    blocked_reasons = sym_data.get("blocked_reasons")
    blocked_clause = ""
    if isinstance(blocked_reasons, list) and blocked_reasons:
        blocked_clause = f" Reasons: {', '.join(str(reason) for reason in blocked_reasons)}."

    trade_block_classification = classify_trade_block(sym_data)
    if trade_block_classification == "hard":
        result.issues.append(RuleIssue(
            severity="error",
            category="risk_breach",
            symbol=sym,
            rule=f"{agent_name}_trade_blocked",
            description=(
                f"{label} agent set trade_allowed=false for this symbol.{blocked_clause}"
            ),
        ))
    elif trade_block_classification == "soft":
        soft_agents = _trade_blocking_agents(agent_outputs, sym, classification="soft")
        if len(soft_agents) < SOFT_TRADE_BLOCK_CONSENSUS_MIN_COUNT:
            result.issues.append(RuleIssue(
                severity="warning",
                category="risk_breach",
                symbol=sym,
                rule=f"{agent_name}_trade_block_soft",
                description=(
                    f"{label} agent set trade_allowed=false for analytical caution only; "
                    f"this remains advisory unless at least {SOFT_TRADE_BLOCK_CONSENSUS_MIN_COUNT} agents agree.{blocked_clause}"
                ),
            ))

    confidence_cap = sym_data.get("confidence_cap")
    plan_conf = plan.get("confidence", 0)
    try:
        cap_value = float(confidence_cap) if confidence_cap is not None else None
    except (TypeError, ValueError):
        cap_value = None

    if agent_name == "flow" and cap_value is not None and _flow_high_false_breakout_directional_only_cap(plan, sym_data):
        cap_value = None

    if cap_value is not None and plan_conf > cap_value:
        result.issues.append(RuleIssue(
            severity="error",
            category="risk_breach",
            symbol=sym,
            rule=f"{agent_name}_confidence_cap",
            description=(
                f"{label} agent confidence_cap={cap_value:.2f} but plan confidence={plan_conf:.2f}.{blocked_clause}"
            ),
        ))

    if sym_data.get("simple_structures_only") and strategy not in simple_strategy_types:
        if _gamma_pin_exception_allows_complex_structure(plan, agent_outputs):
            return
        result.issues.append(RuleIssue(
            severity="error",
            category="strategy_mismatch",
            symbol=sym,
            rule=f"{agent_name}_simple_structures_only",
            description=(
                f"{label} agent requires the configured precision-first strategy scope "
                f"{sorted(simple_strategy_types)}, but strategy_type={strategy}.{blocked_clause}"
            ),
        ))


def _check_volatility_trade_gate(
    plan: dict, agent_outputs: dict[str, Any], result: CheckResult,
) -> None:
    """Apply structured veto/cap fields emitted by the Volatility agent."""
    _check_agent_trade_gate(
        plan,
        agent_outputs,
        result,
        agent_name="volatility",
        label="Volatility",
    )


def _check_volatility_single_indicator_limits(
    plan: dict, agent_outputs: dict[str, Any], result: CheckResult,
) -> None:
    """Volatility H6: single-indicator regimes must cap confidence."""
    sym = plan.get("underlying", "UNKNOWN")
    vol_data = _agent_sym(agent_outputs, "volatility", sym)
    if vol_data is None:
        return

    signal_type = str(vol_data.get("signal_type") or "").strip().lower()
    if signal_type != "single_indicator":
        return

    plan_conf = plan.get("confidence", 0)
    if isinstance(plan_conf, (int, float)) and plan_conf > 0.55:
        result.issues.append(RuleIssue(
            severity="error",
            category="risk_breach",
            symbol=sym,
            rule="volatility_single_indicator_confidence_cap",
            description=(
                f"Volatility signal_type=single_indicator requires confidence <= 0.55, "
                f"but plan uses {float(plan_conf):.2f}"
            ),
        ))


def _check_trend_trade_gate(
    plan: dict, agent_outputs: dict[str, Any], result: CheckResult,
) -> None:
    """Apply structured veto/cap fields emitted by the Trend agent."""
    _check_agent_trade_gate(
        plan,
        agent_outputs,
        result,
        agent_name="trend",
        label="Trend",
    )


def _check_trend_false_positive_risk(
    plan: dict, agent_outputs: dict[str, Any], result: CheckResult,
) -> None:
    """Trend high false-positive risk must clamp structure complexity."""
    sym = plan.get("underlying", "UNKNOWN")
    trend_data = _agent_sym(agent_outputs, "trend", sym)
    if trend_data is None:
        return

    false_positive_risk = str(trend_data.get("false_positive_risk") or "").strip().lower()
    if false_positive_risk != "high":
        return

    strategy = str(plan.get("strategy_type", "")).lower()
    simple_strategy_types = _configured_simple_strategy_types()
    if strategy not in simple_strategy_types:
        result.issues.append(RuleIssue(
            severity="error",
            category="strategy_mismatch",
            symbol=sym,
            rule="trend_false_positive_risk_simple_structures_only",
            description=(
                f"Trend false_positive_risk=high requires simple structures only, but strategy_type={strategy or 'unknown'}"
            ),
        ))


def _check_flow_trade_gate(
    plan: dict, agent_outputs: dict[str, Any], result: CheckResult,
) -> None:
    """Apply structured veto/cap fields emitted by the Flow agent."""
    _check_agent_trade_gate(
        plan,
        agent_outputs,
        result,
        agent_name="flow",
        label="Flow",
    )


def _check_chain_trade_gate(
    plan: dict, agent_outputs: dict[str, Any], result: CheckResult,
) -> None:
    """Apply structured veto/cap fields emitted by the Chain agent."""
    _check_agent_trade_gate(
        plan,
        agent_outputs,
        result,
        agent_name="chain",
        label="Chain",
    )


def _check_chain_gamma_pin_exception_requirements(
    plan: dict,
    agent_outputs: dict[str, Any],
    result: CheckResult,
) -> None:
    """GP1: if gamma-pin exception is used, enforce structure and strike centering."""
    sym = plan.get("underlying", "UNKNOWN")
    strategy = str(plan.get("strategy_type", "")).lower()

    chain_data = _agent_sym(agent_outputs, "chain", sym)
    if chain_data is None:
        return
    if not chain_data.get("gamma_pin_active"):
        return

    pin_strength = _safe_float(chain_data.get("pin_strength"))
    if pin_strength is None or pin_strength <= 0.7:
        return

    liquidity_tier = str(chain_data.get("liquidity_tier") or "").upper()
    if liquidity_tier not in {"L1", "L2"}:
        result.issues.append(RuleIssue(
            severity="error",
            category="strategy_mismatch",
            symbol=sym,
            rule="chain_gamma_pin_liquidity_tier",
            description=(
                f"Chain gamma_pin_active=true with pin_strength={pin_strength:.2f} requires "
                f"liquidity_tier in [L1, L2], but got {liquidity_tier or 'unknown'}"
            ),
        ))

    if strategy not in {"butterfly", "iron_condor"}:
        result.issues.append(RuleIssue(
            severity="error",
            category="strategy_mismatch",
            symbol=sym,
            rule="chain_gamma_pin_structure_required",
            description=(
                f"Chain gamma_pin_active=true with pin_strength={pin_strength:.2f} requires "
                "butterfly or iron_condor structures"
            ),
        ))
        return

    direction = str(plan.get("direction", "")).lower()
    if direction != "neutral":
        result.issues.append(RuleIssue(
            severity="error",
            category="strategy_mismatch",
            symbol=sym,
            rule="chain_gamma_pin_neutral_direction_required",
            description=(
                f"Chain gamma-pin exception requires direction=neutral, but plan direction={direction or 'unknown'}"
            ),
        ))

    gamma_pin_strike = _safe_float(chain_data.get("gamma_pin_strike"))
    if gamma_pin_strike is None:
        return

    tolerance = 1e-6
    short_leg_strikes = []
    for leg in plan.get("legs", []):
        if not isinstance(leg, dict):
            continue
        if str(leg.get("side", "")).lower() != "sell":
            continue
        leg_strike = _safe_float(leg.get("strike"))
        if leg_strike is not None:
            short_leg_strikes.append(leg_strike)

    if not short_leg_strikes:
        result.issues.append(RuleIssue(
            severity="error",
            category="strategy_mismatch",
            symbol=sym,
            rule="chain_gamma_pin_missing_short_legs",
            description="Chain gamma-pin exception requires short center legs for strike centering",
        ))
        return

    if not any(abs(leg_strike - gamma_pin_strike) <= tolerance for leg_strike in short_leg_strikes):
        formatted_strikes = ", ".join(f"{strike:.2f}" for strike in sorted(short_leg_strikes))
        result.issues.append(RuleIssue(
            severity="error",
            category="strategy_mismatch",
            symbol=sym,
            rule="chain_gamma_pin_strike_centering",
            description=(
                f"Chain gamma-pin exception requires at least one short leg strike to match gamma_pin_strike="
                f"{gamma_pin_strike:.2f}, but short strikes are [{formatted_strikes}]"
            ),
        ))


def _check_spread_trade_gate(
    plan: dict, agent_outputs: dict[str, Any], result: CheckResult,
) -> None:
    """Apply structured veto/cap fields emitted by the Spread agent."""
    _check_agent_trade_gate(
        plan,
        agent_outputs,
        result,
        agent_name="spread",
        label="Spread",
        apply_to_strategies=_SPREAD_STRATEGY_TYPES,
    )


def _check_soft_trade_block_consensus(
    plan: dict,
    agent_outputs: dict[str, Any],
    result: CheckResult,
) -> None:
    """Escalate multi-agent analytical soft blocks to a hard symbol veto."""
    sym = plan.get("underlying", "UNKNOWN")
    if _trade_blocking_agents(agent_outputs, sym, classification="hard"):
        return

    soft_agents = _trade_blocking_agents(agent_outputs, sym, classification="soft")
    if len(soft_agents) < SOFT_TRADE_BLOCK_CONSENSUS_MIN_COUNT:
        return

    detail_parts: list[str] = []
    for agent_name in soft_agents:
        sym_data = _agent_sym(agent_outputs, agent_name, sym)
        reasons = normalized_blocked_reasons(sym_data)
        if reasons:
            detail_parts.append(f"{agent_name} ({', '.join(reasons)})")
        else:
            detail_parts.append(agent_name)

    result.issues.append(RuleIssue(
        severity="error",
        category="risk_breach",
        symbol=sym,
        rule="multi_agent_soft_trade_block",
        description=(
            "Multiple agents converged on analytical no-trade caution for this symbol: "
            f"{'; '.join(detail_parts)}."
        ),
    ))


# ---------------------------------------------------------------------------
# Agent-output checks: Spread effective R:R (Spread H1/H2, Synthesizer HE3)
# ---------------------------------------------------------------------------


def _check_spread_effective_rr(
    plan: dict, agent_outputs: dict[str, Any], result: CheckResult,
) -> None:
    """Validate the vertical spread R:R floor from Spread agent.

    Agent prompt source:
    - Spread V1: verticals require effective_rr / risk_reward_ratio >= 0.7
    - Spread contract: non-vertical spreads must not be rejected solely because
      effective_rr is null
    """
    sym = plan.get("underlying", "UNKNOWN")
    spread_data = _agent_sym(agent_outputs, "spread", sym)
    if spread_data is None:
        return

    strategy = plan.get("strategy_type", "")
    if strategy != "vertical_spread":
        return

    metrics: dict[str, float] = {}
    for metric_name in ("effective_rr", "risk_reward_ratio"):
        metric_value = spread_data.get(metric_name)
        try:
            if metric_value is not None:
                metrics[metric_name] = float(metric_value)
        except (TypeError, ValueError):
            continue

    if not metrics:
        return

    strictest_name, strictest_val = min(metrics.items(), key=lambda item: item[1])
    if strictest_val < 0.7:
        metric_desc = ", ".join(f"{name}={value:.2f}" for name, value in sorted(metrics.items()))
        result.issues.append(RuleIssue(
            severity="error",
            category="risk_breach",
            symbol=sym,
            rule="spread_vertical_rr_reject",
            description=(
                f"Vertical spread {strictest_name}={strictest_val:.2f} is below the 0.70 floor "
                f"for spread viability. Available metrics: {metric_desc}"
            ),
        ))


# ---------------------------------------------------------------------------
# Agent-output checks: Event risk consensus (Synthesizer ER1/ER2)
# ---------------------------------------------------------------------------

_EVENT_RISK_AGENTS = ("volatility", "flow", "chain", "spread", "cross_asset", "trend")


def _check_event_risk_consensus(
    plan: dict, agent_outputs: dict[str, Any], result: CheckResult,
) -> None:
    """Enforce event-risk confidence caps from the synthesizer contract.

    Agent prompt source:
    - Synthesizer ER1: ≥3 agents flag event_risk_present → max_position_size ≤ 0.8
    - Synthesizer ER2: ≥2 agents + CrossAsset="event_driven" → cap confidence ≤ 0.5
    """
    sym = plan.get("underlying", "UNKNOWN")
    event_count = 0
    for agent_name in _EVENT_RISK_AGENTS:
        sym_data = _agent_sym(agent_outputs, agent_name, sym)
        if sym_data and sym_data.get("event_risk_present"):
            event_count += 1

    cross_data = _agent_sym(agent_outputs, "cross_asset", sym)
    correlation_regime = ""
    if cross_data is not None:
        correlation_regime = str(cross_data.get("correlation_regime") or "").strip().lower()

    plan_conf = plan.get("confidence", 0)
    strategy = plan.get("strategy_type", "")
    direction = str(plan.get("direction", "")).strip().lower()

    market_shock = _safe_float(cross_data.get("market_shock_return_1d") if cross_data else None)
    market_shock_source = str(cross_data.get("market_shock_source") or "").strip() if cross_data else ""
    shock_escalation = (
        market_shock is not None
        and abs(market_shock) > 0.03
        and bool(market_shock_source)
        and direction in {"bullish", "bearish"}
        and strategy not in {"straddle", "strangle"}
    )
    shock_direction_aligned = (
        shock_escalation
        and ((market_shock < 0 and direction == "bearish") or (market_shock > 0 and direction == "bullish"))
    )

    if shock_escalation and not shock_direction_aligned:
        if plan_conf > 0.5:
            result.issues.append(RuleIssue(
                severity="error",
                category="risk_breach",
                symbol=sym,
                rule="event_risk_market_shock_confidence_cap",
                description=(
                    f"market_shock_return_1d={market_shock:.4f} ({market_shock_source}) escalates event risk, "
                    f"but confidence={plan_conf:.2f} > 0.50"
                ),
            ))
    elif shock_direction_aligned:
        reasoning = str(plan.get("reasoning") or "").lower()
        if market_shock_source.lower() not in reasoning and "shock" not in reasoning:
            result.issues.append(RuleIssue(
                severity="warning",
                category="logic_error",
                symbol=sym,
                rule="event_risk_market_shock_exemption_reasoning",
                description=(
                    "Applied directional market-shock exemption without explicit reasoning reference to shock source"
                ),
            ))

    if (
        event_count >= 2
        and correlation_regime == "event_driven"
        and strategy not in {"straddle", "strangle"}
        and plan_conf > 0.5
    ):
        result.issues.append(RuleIssue(
            severity="error",
            category="risk_breach",
            symbol=sym,
            rule="event_risk_consensus_confidence_cap",
            description=(
                f"{event_count} agents flagged event_risk_present and Cross-Asset marked event_driven, "
                f"but non-earnings strategy {strategy or 'unknown'} uses confidence={plan_conf:.2f} > 0.50"
            ),
        ))

    if event_count < 3:
        return

    if plan_conf > 0.5:
        result.issues.append(RuleIssue(
            severity="warning",
            category="risk_breach",
            symbol=sym,
            rule="event_risk_consensus",
            description=(
                f"{event_count} agents flagged event_risk_present but plan "
                f"confidence={plan_conf:.2f} > 0.50 — should reduce conviction or wait for cleaner context"
            ),
        ))


# ---------------------------------------------------------------------------
# Agent-output checks: Confirming indicators (Synthesizer CI1)
# ---------------------------------------------------------------------------


def _check_confirming_indicators(
    plan: dict, agent_outputs: dict[str, Any], result: CheckResult,
) -> None:
    """Enforce the directional confidence cap when confirmation is thin.

    Agent prompt source:
    - Synthesizer CI1: both Flow + Chain confirming_indicators_count ≤ 1
      → cap directional confidence ≤ 0.5
    """
    sym = plan.get("underlying", "UNKNOWN")
    direction = plan.get("direction", "neutral")
    if direction == "neutral":
        return

    flow_data = _agent_sym(agent_outputs, "flow", sym)
    chain_data = _agent_sym(agent_outputs, "chain", sym)
    if flow_data is None or chain_data is None:
        return

    flow_ci = flow_data.get("confirming_indicators_count")
    chain_ci = chain_data.get("confirming_indicators_count")
    if not isinstance(flow_ci, int) or not isinstance(chain_ci, int):
        return

    if flow_ci <= 1 and chain_ci <= 1:
        plan_conf = plan.get("confidence", 0)
        if plan_conf > 0.5:
            result.issues.append(RuleIssue(
                severity="error",
                category="risk_breach",
                symbol=sym,
                rule="low_confirming_indicators",
                description=(
                    f"Flow confirming_indicators={flow_ci}, Chain confirming_indicators="
                    f"{chain_ci} (both ≤1) but plan confidence={plan_conf:.2f} exceeds the 0.50 cap"
                ),
            ))
