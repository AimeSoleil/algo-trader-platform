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
    for plan in blueprint.get("symbol_plans", []):
        _check_plan_risk(plan, result)
        _check_strategy_legs(plan, result)
        _check_plan_reasoning(plan, result)
        if signal_features:
            sym = plan.get("underlying", "").upper()
            sig = signal_features.get(sym, {})
            _check_counter_trend(plan, sig, result)
            _check_liquidity(plan, sig, result)

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
    trend = signal.get("stock_trend", {})
    adx = trend.get("adx_14", 0)
    trend_dir = trend.get("trend_direction", "neutral")
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
    chain = signal.get("option_chain", {})
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
