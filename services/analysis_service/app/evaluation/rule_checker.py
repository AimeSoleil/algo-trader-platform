"""Blueprint rule checker — deterministic validation against reference rules.

Validates LLM-generated blueprints against hard constraints from reference
documents (risk-management, option-chain-structure, etc.) without using LLM.
Can complement the CriticAgent or run standalone for backtesting.

Usage::

    from services.analysis_service.app.evaluation.rule_checker import check_blueprint
    issues = check_blueprint(blueprint_dict, signal_features_map)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date as _date_type
from typing import Any


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


def check_blueprint(
    blueprint: dict[str, Any],
    signal_features: dict[str, dict[str, Any]] | None = None,
) -> CheckResult:
    """Run all deterministic rule checks on a blueprint.

    Parameters
    ----------
    blueprint:
        Blueprint dict (model_dump output of LLMTradingBlueprint).
    signal_features:
        Optional mapping of symbol → serialized signal data for
        context-aware checks (e.g. ADX-based counter-trend detection).

    Returns
    -------
    CheckResult
        Aggregated issues from all checks.
    """
    result = CheckResult()
    signal_features = signal_features or {}

    _check_portfolio_risk(blueprint, result)
    _check_duplicate_symbols(blueprint, result)
    for plan in blueprint.get("symbol_plans", []):
        _check_plan_risk(plan, result)
        _check_strategy_legs(plan, result)
        _check_plan_reasoning(plan, result)
        _check_strike_ordering(plan, result)
        _check_greeks_direction(plan, result)
        _check_dte_bounds(plan, result)
        _check_expiry_consistency(plan, result)
        if signal_features:
            sym = plan.get("underlying", "").upper()
            sig = signal_features.get(sym, {})
            _check_counter_trend(plan, sig, result)
            _check_liquidity(plan, sig, result)
            _check_confidence_quality_gate(plan, sig, result)
            _check_cross_asset_quality_guards(plan, sig, result)

    result.passed = result.error_count == 0
    return result


# ---------------------------------------------------------------------------
# Portfolio-level checks (risk-management.md)
# ---------------------------------------------------------------------------


def _check_portfolio_risk(bp: dict, result: CheckResult) -> None:
    """Validate portfolio-level risk constraints."""
    # Delta limit
    delta_limit = bp.get("portfolio_delta_limit", 0.5)
    if delta_limit > 0.8:
        result.issues.append(RuleIssue(
            severity="error",
            category="risk_breach",
            rule="portfolio_delta_limit",
            description=f"portfolio_delta_limit={delta_limit} exceeds max 0.8",
        ))
    elif delta_limit > 0.5:
        result.issues.append(RuleIssue(
            severity="warning",
            category="risk_breach",
            rule="portfolio_delta_limit_elevated",
            description=f"portfolio_delta_limit={delta_limit} > 0.5 (needs strong trend justification)",
        ))

    # Gamma limit
    gamma_limit = bp.get("portfolio_gamma_limit", 0.1)
    if gamma_limit > 0.1:
        result.issues.append(RuleIssue(
            severity="error",
            category="risk_breach",
            rule="portfolio_gamma_limit",
            description=f"portfolio_gamma_limit={gamma_limit} exceeds max 0.1",
        ))

    # Daily loss
    max_loss = bp.get("max_daily_loss", 2000.0)
    if max_loss > 2000.0:
        result.issues.append(RuleIssue(
            severity="error",
            category="risk_breach",
            rule="max_daily_loss",
            description=f"max_daily_loss=${max_loss} exceeds $2,000 hard limit",
        ))


# ---------------------------------------------------------------------------
# Plan-level risk checks
# ---------------------------------------------------------------------------


def _check_plan_risk(plan: dict, result: CheckResult) -> None:
    """Validate per-plan risk constraints."""
    sym = plan.get("underlying", "UNKNOWN")

    # Stop-loss required
    stop_loss = plan.get("stop_loss_amount")
    if stop_loss is None or stop_loss == 0:
        result.issues.append(RuleIssue(
            severity="error",
            category="risk_breach",
            symbol=sym,
            rule="stop_loss_required",
            description="Plan missing stop_loss_amount — every plan must have a stop-loss",
        ))

    # Max loss per trade required and positive
    max_loss = plan.get("max_loss_per_trade", 0)
    if max_loss <= 0:
        result.issues.append(RuleIssue(
            severity="error",
            category="risk_breach",
            symbol=sym,
            rule="max_loss_positive",
            description=f"max_loss_per_trade={max_loss} — must be > 0",
        ))

    # Stop-loss should not exceed plan max loss
    if isinstance(stop_loss, (int, float)) and isinstance(max_loss, (int, float)) and max_loss > 0:
        if stop_loss > max_loss:
            result.issues.append(RuleIssue(
                severity="error",
                category="risk_breach",
                symbol=sym,
                rule="stop_loss_exceeds_max_loss",
                description=(
                    f"stop_loss_amount={stop_loss} exceeds max_loss_per_trade={max_loss}"
                ),
            ))

    # Optional R:R sanity check when take-profit is provided
    take_profit = plan.get("take_profit_amount")
    if isinstance(take_profit, (int, float)) and take_profit > 0 and isinstance(max_loss, (int, float)) and max_loss > 0:
        if take_profit / max_loss < 1.0:
            result.issues.append(RuleIssue(
                severity="warning",
                category="risk_breach",
                symbol=sym,
                rule="risk_reward_below_one",
                description=(
                    f"take_profit_amount={take_profit} vs max_loss_per_trade={max_loss} "
                    f"(R:R<{1.0})"
                ),
            ))

    # Confidence sanity
    conf = plan.get("confidence", 0)
    if conf < 0.3:
        result.issues.append(RuleIssue(
            severity="warning",
            category="risk_breach",
            symbol=sym,
            rule="low_confidence",
            description=f"Plan confidence={conf:.2f} — very low, consider skipping",
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

    # Exit conditions
    exits = plan.get("exit_conditions", [])
    if not exits:
        result.issues.append(RuleIssue(
            severity="warning",
            category="logic_error",
            symbol=sym,
            rule="no_exit_conditions",
            description="Plan has no exit conditions — should define at least one",
        ))


# ---------------------------------------------------------------------------
# Context-aware checks (require signal data)
# ---------------------------------------------------------------------------


def _check_counter_trend(plan: dict, signal: dict, result: CheckResult) -> None:
    """ADX>30 → do NOT enter counter-trend (trend-momentum.md rule 9)."""
    sym = plan.get("underlying", "UNKNOWN")
    # Prefer SignalFeatures.model_dump schema; keep legacy fallback for tests/tools.
    trend = signal.get("stock_indicators", {})
    if not isinstance(trend, dict):
        trend = {}
    if not trend:
        legacy = signal.get("stock_trend", {})
        if isinstance(legacy, dict):
            trend = legacy
    adx = trend.get("adx_14", 0)
    trend_dir = trend.get("trend", trend.get("trend_direction", "neutral"))
    plan_dir = plan.get("direction", "neutral")

    if adx > 30 and trend_dir != "neutral" and plan_dir != "neutral":
        # Check for counter-trend
        is_counter = (
            (trend_dir == "bullish" and plan_dir == "bearish")
            or (trend_dir == "bearish" and plan_dir == "bullish")
        )
        if is_counter:
            result.issues.append(RuleIssue(
                severity="error",
                category="strategy_mismatch",
                symbol=sym,
                rule="counter_trend_adx30",
                description=(
                    f"Counter-trend entry while ADX={adx:.1f}>30. "
                    f"Trend={trend_dir}, Plan direction={plan_dir}. "
                    f"Rule: do NOT enter counter-trend when ADX>30."
                ),
            ))


def _check_liquidity(plan: dict, signal: dict, result: CheckResult) -> None:
    """bid-ask > 0.20 → HARD BLOCK (option-chain-structure.md rule 5)."""
    sym = plan.get("underlying", "UNKNOWN")
    # Prefer SignalFeatures.model_dump schema; keep legacy fallback for tests/tools.
    chain = signal.get("option_indicators", {})
    if not isinstance(chain, dict):
        chain = {}
    if not chain:
        legacy = signal.get("option_chain", {})
        if isinstance(legacy, dict):
            chain = legacy
    bid_ask_ratio = chain.get("bid_ask_spread_ratio", 0)

    if bid_ask_ratio > 0.20:
        result.issues.append(RuleIssue(
            severity="error",
            category="liquidity",
            symbol=sym,
            rule="bid_ask_hard_block",
            description=(
                f"bid_ask_spread_ratio={bid_ask_ratio:.4f} > 0.20 — "
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
        buy_leg = next((l for l in legs if l.get("side") == "buy"), None)
        sell_leg = next((l for l in legs if l.get("side") == "sell"), None)
        if buy_leg and sell_leg:
            opt_type = buy_leg.get("option_type", "")
            buy_k = buy_leg.get("strike", 0)
            sell_k = sell_leg.get("strike", 0)
            if direction == "bullish" and opt_type == "call" and buy_k >= sell_k:
                result.issues.append(RuleIssue(
                    severity="error", category="logic_error", symbol=sym,
                    rule="strike_ordering",
                    description=(
                        f"Debit call spread: buy strike ({buy_k}) must be < "
                        f"sell strike ({sell_k})"
                    ),
                ))
            if direction == "bearish" and opt_type == "put" and sell_k >= buy_k:
                result.issues.append(RuleIssue(
                    severity="error", category="logic_error", symbol=sym,
                    rule="strike_ordering",
                    description=(
                        f"Debit put spread: sell strike ({sell_k}) must be < "
                        f"buy strike ({buy_k})"
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
    """Validate all legs have DTE within [7, 180]."""
    sym = plan.get("underlying", "UNKNOWN")
    today = _date_type.today()

    for i, leg in enumerate(plan.get("legs", [])):
        expiry_str = leg.get("expiry", "")
        if not expiry_str:
            continue
        try:
            expiry = _date_type.fromisoformat(expiry_str)
        except (ValueError, TypeError):
            continue
        dte = (expiry - today).days

        if dte < 7:
            result.issues.append(RuleIssue(
                severity="error", category="risk_breach", symbol=sym,
                rule="dte_bounds",
                description=f"Leg {i} expiry {expiry_str} has DTE={dte} < 7 — too short",
            ))
        elif dte > 180:
            result.issues.append(RuleIssue(
                severity="warning", category="risk_breach", symbol=sym,
                rule="dte_bounds",
                description=f"Leg {i} expiry {expiry_str} has DTE={dte} > 180 — unusually long",
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
    max_pos = plan.get("max_position_size")

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

    if (
        isinstance(corr_sig, (int, float)) and corr_sig < 0.5
        and isinstance(freshness, (int, float)) and freshness < 0.5
        and isinstance(max_pos, (int, float)) and max_pos > 0.7
    ):
        result.issues.append(RuleIssue(
            severity="warning",
            category="risk_breach",
            symbol=sym,
            rule="cross_asset_low_quality_position_size",
            description=(
                f"cross-asset quality low (significance={corr_sig:.2f}, "
                f"freshness={freshness:.2f}) but max_position_size={max_pos:.2f} > 0.70"
            ),
        ))


def _check_cascading_modifiers(
    plan: dict, signal: dict, result: CheckResult,
) -> None:
    """Warn if stacked position-size modifiers reduce position to near-zero."""
    sym = plan.get("underlying", "UNKNOWN")

    flow_mod = signal.get("flow", {}).get("position_size_modifier") if isinstance(signal.get("flow"), dict) else None
    cross_mod = (
        signal.get("cross_asset", {}).get("position_size_modifier")
        if isinstance(signal.get("cross_asset"), dict)
        else None
    )

    if flow_mod is None or cross_mod is None:
        return

    try:
        product = float(flow_mod) * float(cross_mod)
    except (TypeError, ValueError):
        return

    if product < 0.3:
        result.issues.append(RuleIssue(
            severity="warning", category="risk_breach", symbol=sym,
            rule="cascading_size_modifiers",
            description=(
                f"Stacked modifiers (flow={flow_mod}, cross_asset={cross_mod}, "
                f"product={product:.2f}) reduce position to near-zero — "
                f"effectively a skip"
            ),
        ))
