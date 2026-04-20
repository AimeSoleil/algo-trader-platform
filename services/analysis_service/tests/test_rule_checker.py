"""Test deterministic rule checker for blueprint validation."""
from __future__ import annotations

import datetime

import pytest

from services.analysis_service.app.evaluation.rule_checker import (
    CheckResult,
    RuleIssue,
    check_blueprint,
)

# Dynamic expiry ~30 days out so DTE bounds checks always pass
_DEFAULT_EXPIRY = (datetime.date.today() + datetime.timedelta(days=30)).isoformat()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _leg(**overrides) -> dict:
    d = {"expiry": _DEFAULT_EXPIRY, "strike": 150.0, "option_type": "call", "side": "buy", "quantity": 1}
    d.update(overrides)
    return d


def _plan(**overrides) -> dict:
    d = {
        "underlying": "AAPL",
        "strategy_type": "single_leg",
        "direction": "bullish",
        "legs": [_leg()],
        "stop_loss_amount": 200.0,
        "max_loss_per_trade": 500.0,
        "confidence": 0.7,
        "reasoning": "Based on trend agent bullish regime and strong ADX with Keltner breakout.",
        "exit_conditions": [{"field": "pnl_percent", "operator": ">=", "value": 0.3}],
    }
    d.update(overrides)
    return d


def _blueprint(**overrides) -> dict:
    d = {
        "trading_date": "2026-03-24",
        "generated_at": "2026-03-23T20:00:00",
        "market_regime": "neutral",
        "symbol_plans": [_plan()],
        "portfolio_delta_limit": 0.5,
        "portfolio_gamma_limit": 0.1,
        "max_daily_loss": 2000.0,
    }
    d.update(overrides)
    return d


# ---------------------------------------------------------------------------
# Portfolio-level risk
# ---------------------------------------------------------------------------


class TestPortfolioRisk:
    def test_valid_portfolio(self):
        result = check_blueprint(_blueprint())
        assert result.passed
        assert result.error_count == 0

    def test_delta_limit_exceeds_hard_cap(self):
        result = check_blueprint(_blueprint(portfolio_delta_limit=0.9))
        assert any(i.rule == "portfolio_delta_limit" for i in result.issues)
        assert not result.passed

    def test_delta_limit_elevated_warning(self):
        result = check_blueprint(_blueprint(portfolio_delta_limit=0.6))
        warnings = [i for i in result.issues if i.rule == "portfolio_delta_limit_elevated"]
        assert len(warnings) == 1
        assert warnings[0].severity == "warning"
        assert result.passed  # warnings don't fail

    def test_gamma_limit_exceeded(self):
        result = check_blueprint(_blueprint(portfolio_gamma_limit=0.15))
        assert any(i.rule == "portfolio_gamma_limit" for i in result.issues)

    def test_daily_loss_exceeded(self):
        result = check_blueprint(_blueprint(max_daily_loss=3100.0))
        assert any(i.rule == "max_daily_loss" and i.severity == "error" for i in result.issues)


# ---------------------------------------------------------------------------
# Plan-level risk
# ---------------------------------------------------------------------------


class TestPlanRisk:
    def test_missing_stop_loss(self):
        result = check_blueprint(_blueprint(symbol_plans=[_plan(stop_loss_amount=None)]))
        assert any(i.rule == "stop_loss_required" for i in result.issues)

    def test_zero_stop_loss(self):
        result = check_blueprint(_blueprint(symbol_plans=[_plan(stop_loss_amount=0)]))
        assert any(i.rule == "stop_loss_required" for i in result.issues)

    def test_low_confidence_warning(self):
        result = check_blueprint(_blueprint(symbol_plans=[_plan(confidence=0.2)]))
        assert any(i.rule == "low_confidence" for i in result.issues)

    def test_stop_loss_exceeds_max_loss_error(self):
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(stop_loss_amount=700.0, max_loss_per_trade=500.0)])
        )
        assert any(i.rule == "stop_loss_exceeds_max_loss" for i in result.issues)

    def test_risk_reward_below_one_warning(self):
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(take_profit_amount=300.0, max_loss_per_trade=500.0)])
        )
        assert any(i.rule == "risk_reward_below_one" for i in result.issues)


# ---------------------------------------------------------------------------
# Strategy ↔ legs consistency
# ---------------------------------------------------------------------------


class TestStrategyLegs:
    def test_iron_condor_wrong_legs(self):
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            strategy_type="iron_condor",
            legs=[_leg(), _leg(strike=155)],
        )]))
        assert any(i.rule == "strategy_legs_mismatch" for i in result.issues)

    def test_leg_missing_field(self):
        bad_leg = {"expiry": _DEFAULT_EXPIRY, "strike": 150.0}  # missing option_type, side
        result = check_blueprint(_blueprint(symbol_plans=[_plan(legs=[bad_leg])]))
        assert any("missing" in i.rule for i in result.issues)


# ---------------------------------------------------------------------------
# Reasoning & exit conditions
# ---------------------------------------------------------------------------


class TestReasoningChecks:
    def test_short_reasoning_warning(self):
        result = check_blueprint(_blueprint(symbol_plans=[_plan(reasoning="buy")]))
        assert any(i.rule == "reasoning_too_short" for i in result.issues)

    def test_no_exit_conditions_warning(self):
        result = check_blueprint(_blueprint(symbol_plans=[_plan(exit_conditions=[])]))
        assert any(i.rule == "no_exit_conditions" for i in result.issues)


# ---------------------------------------------------------------------------
# Context-aware checks (signal data)
# ---------------------------------------------------------------------------


class TestContextAwareChecks:
    def test_counter_trend_adx30_error(self):
        signals = {
            "AAPL": {
                "stock_indicators": {"adx_14": 35.0, "trend": "bullish"},
                "option_indicators": {},
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(direction="bearish")]),
            signal_features=signals,
        )
        assert any(i.rule == "counter_trend_adx30" for i in result.issues)

    def test_no_counter_trend_when_same_direction(self):
        signals = {
            "AAPL": {
                "stock_indicators": {"adx_14": 35.0, "trend": "bullish"},
                "option_indicators": {},
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(direction="bullish")]),
            signal_features=signals,
        )
        assert not any(i.rule == "counter_trend_adx30" for i in result.issues)

    def test_bid_ask_hard_block(self):
        signals = {
            "AAPL": {
                "stock_indicators": {},
                "option_indicators": {"bid_ask_spread_ratio": 0.26},
            }
        }
        result = check_blueprint(
            _blueprint(),
            signal_features=signals,
        )
        assert any(i.rule == "bid_ask_hard_block" for i in result.issues)

    def test_bid_ask_illiquid_warning(self):
        signals = {
            "AAPL": {
                "stock_indicators": {},
                "option_indicators": {"bid_ask_spread_ratio": 0.17},
            }
        }
        result = check_blueprint(
            _blueprint(),
            signal_features=signals,
        )
        warns = [i for i in result.issues if i.rule == "bid_ask_illiquid"]
        assert len(warns) == 1
        assert warns[0].severity == "warning"


# ---------------------------------------------------------------------------
# Strike ordering checks
# ---------------------------------------------------------------------------


class TestStrikeOrdering:
    def test_valid_bull_call_spread(self):
        """Buy lower strike, sell higher = valid debit call spread."""
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            strategy_type="vertical_spread",
            direction="bullish",
            legs=[
                _leg(strike=150, option_type="call", side="buy"),
                _leg(strike=160, option_type="call", side="sell"),
            ],
        )]))
        assert not any(i.rule == "strike_ordering" for i in result.issues)

    def test_invalid_bull_call_spread_reversed(self):
        """Buy higher strike, sell lower = reversed for debit call spread."""
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            strategy_type="vertical_spread",
            direction="bullish",
            legs=[
                _leg(strike=160, option_type="call", side="buy"),
                _leg(strike=150, option_type="call", side="sell"),
            ],
        )]))
        assert any(i.rule == "strike_ordering" for i in result.issues)

    def test_valid_iron_condor(self):
        """Correct ordering: put_long < put_short < call_short < call_long."""
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            strategy_type="iron_condor",
            direction="neutral",
            legs=[
                _leg(strike=140, option_type="put", side="buy"),
                _leg(strike=145, option_type="put", side="sell"),
                _leg(strike=155, option_type="call", side="sell"),
                _leg(strike=160, option_type="call", side="buy"),
            ],
        )]))
        assert not any(i.rule == "strike_ordering" for i in result.issues)

    def test_straddle_same_strike(self):
        """Straddle legs must have same strike."""
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            strategy_type="straddle",
            direction="neutral",
            legs=[
                _leg(strike=150, option_type="call", side="buy"),
                _leg(strike=150, option_type="put", side="buy"),
            ],
        )]))
        assert not any(i.rule == "strike_ordering" for i in result.issues)

    def test_straddle_different_strikes_error(self):
        """Straddle with different strikes should error."""
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            strategy_type="straddle",
            direction="neutral",
            legs=[
                _leg(strike=150, option_type="call", side="buy"),
                _leg(strike=155, option_type="put", side="buy"),
            ],
        )]))
        assert any(i.rule == "strike_ordering" for i in result.issues)

    def test_strangle_call_above_put(self):
        """Strangle: call strike should be > put strike."""
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            strategy_type="strangle",
            direction="neutral",
            legs=[
                _leg(strike=145, option_type="put", side="buy"),
                _leg(strike=155, option_type="call", side="buy"),
            ],
        )]))
        assert not any(i.rule == "strike_ordering" for i in result.issues)


# ---------------------------------------------------------------------------
# Greeks direction checks
# ---------------------------------------------------------------------------


class TestGreeksDirection:
    def test_bullish_with_all_bearish_legs(self):
        """Direction=bullish but all legs are sell calls = bearish delta → warning."""
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            strategy_type="vertical_spread",
            direction="bullish",
            legs=[
                _leg(strike=150, option_type="call", side="sell"),
                _leg(strike=160, option_type="call", side="sell"),
            ],
        )]))
        assert any(i.rule == "greeks_direction_mismatch" for i in result.issues)

    def test_bearish_with_buy_calls_warning(self):
        """Direction=bearish but net delta is positive → warning."""
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            strategy_type="vertical_spread",
            direction="bearish",
            legs=[
                _leg(strike=150, option_type="call", side="buy"),
                _leg(strike=160, option_type="call", side="buy"),
            ],
        )]))
        assert any(i.rule == "greeks_direction_mismatch" for i in result.issues)

    def test_neutral_no_warning(self):
        """Neutral direction should not trigger regardless of legs."""
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            strategy_type="iron_condor",
            direction="neutral",
            legs=[
                _leg(strike=140, option_type="put", side="buy"),
                _leg(strike=145, option_type="put", side="sell"),
                _leg(strike=155, option_type="call", side="sell"),
                _leg(strike=160, option_type="call", side="buy"),
            ],
        )]))
        assert not any(i.rule == "greeks_direction_mismatch" for i in result.issues)


# ---------------------------------------------------------------------------
# DTE bounds checks
# ---------------------------------------------------------------------------


class TestDTEBounds:
    def test_dte_too_short_error(self):
        """Legs expiring within 7 days should error."""
        from datetime import date, timedelta
        short_expiry = (date.today() + timedelta(days=3)).isoformat()
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            legs=[_leg(expiry=short_expiry)],
        )]))
        assert any(i.rule == "dte_bounds" and i.severity == "error" for i in result.issues)

    def test_dte_too_long_warning(self):
        """Legs expiring beyond 180 days should warn."""
        from datetime import date, timedelta
        long_expiry = (date.today() + timedelta(days=200)).isoformat()
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            legs=[_leg(expiry=long_expiry)],
        )]))
        assert any(i.rule == "dte_bounds" and i.severity == "warning" for i in result.issues)

    def test_dte_within_bounds_ok(self):
        """Legs expiring in 30 days should be fine."""
        from datetime import date, timedelta
        ok_expiry = (date.today() + timedelta(days=30)).isoformat()
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            legs=[_leg(expiry=ok_expiry)],
        )]))
        assert not any(i.rule == "dte_bounds" for i in result.issues)


# ---------------------------------------------------------------------------
# Expiry consistency checks
# ---------------------------------------------------------------------------


class TestExpiryConsistency:
    def test_vertical_spread_different_expiries_error(self):
        """Vertical spread legs must have same expiry."""
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            strategy_type="vertical_spread",
            legs=[
                _leg(expiry="2026-05-15", strike=150, option_type="call", side="buy"),
                _leg(expiry="2026-06-15", strike=160, option_type="call", side="sell"),
            ],
        )]))
        assert any(i.rule == "expiry_consistency" for i in result.issues)

    def test_calendar_same_expiry_error(self):
        """Calendar spread legs must have different expiries."""
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            strategy_type="calendar_spread",
            legs=[
                _leg(expiry="2026-05-15", strike=150, side="sell"),
                _leg(expiry="2026-05-15", strike=150, side="buy"),
            ],
        )]))
        assert any(i.rule == "expiry_consistency" for i in result.issues)

    def test_calendar_different_expiries_ok(self):
        """Calendar spread with different expiries is correct."""
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            strategy_type="calendar_spread",
            legs=[
                _leg(expiry="2026-05-15", strike=150, side="sell"),
                _leg(expiry="2026-06-15", strike=150, side="buy"),
            ],
        )]))
        assert not any(i.rule == "expiry_consistency" for i in result.issues)


# ---------------------------------------------------------------------------
# Duplicate symbols check
# ---------------------------------------------------------------------------


class TestDuplicateSymbols:
    def test_duplicate_underlying_warning(self):
        """Same underlying in multiple plans should warn."""
        result = check_blueprint(_blueprint(symbol_plans=[
            _plan(underlying="AAPL"),
            _plan(underlying="AAPL", strategy_type="straddle", direction="neutral",
                  legs=[_leg(option_type="call"), _leg(option_type="put")]),
        ]))
        assert any(i.rule == "duplicate_symbol" for i in result.issues)

    def test_different_underlyings_no_warning(self):
        """Different underlyings should not warn."""
        result = check_blueprint(_blueprint(symbol_plans=[
            _plan(underlying="AAPL"),
            _plan(underlying="MSFT"),
        ]))
        assert not any(i.rule == "duplicate_symbol" for i in result.issues)


# ---------------------------------------------------------------------------
# Confidence × quality gate check
# ---------------------------------------------------------------------------


class TestConfidenceQualityGate:
    def test_overconfident_on_bad_data(self):
        """High confidence + low data quality should warn."""
        signals = {
            "AAPL": {
                "stock_indicators": {},
                "option_indicators": {},
                "data_quality": {"score": 0.3},
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(confidence=0.8)]),
            signal_features=signals,
        )
        assert any(i.rule == "overconfident_on_bad_data" for i in result.issues)

    def test_appropriate_confidence_ok(self):
        """Moderate confidence + low data quality is fine."""
        signals = {
            "AAPL": {
                "stock_indicators": {},
                "option_indicators": {},
                "data_quality": {"score": 0.3},
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(confidence=0.5)]),
            signal_features=signals,
        )
        assert not any(i.rule == "overconfident_on_bad_data" for i in result.issues)


# ---------------------------------------------------------------------------
# Agent-output checks (require agent_outputs dict)
# ---------------------------------------------------------------------------

# Helper to build agent_outputs dict with per-symbol data
def _agent_outputs(**agent_dicts) -> dict:
    """Build agent_outputs dict. Each kwarg is agent_name=list_of_sym_dicts."""
    return {
        agent_name: {"symbols": sym_list}
        for agent_name, sym_list in agent_dicts.items()
    }


class TestCascadingModifiers:
    def test_product_below_threshold_error(self):
        ao = _agent_outputs(
            flow=[{"symbol": "AAPL", "position_size_modifier": 0.4}],
            cross_asset=[{"symbol": "AAPL", "effective_size_modifier": 0.5}],
        )
        result = check_blueprint(
            _blueprint(),
            agent_outputs=ao,
        )
        # 0.4 × 0.5 = 0.2 < 0.3 → error
        assert any(i.rule == "cascading_size_modifiers" and i.severity == "error" for i in result.issues)

    def test_product_near_zero_error(self):
        ao = _agent_outputs(
            flow=[{"symbol": "AAPL", "position_size_modifier": 0.1}],
            cross_asset=[{"symbol": "AAPL", "effective_size_modifier": 0.2}],
        )
        result = check_blueprint(
            _blueprint(),
            agent_outputs=ao,
        )
        # 0.1 × 0.2 = 0.02 < 0.1 → error (near-zero)
        assert any(i.rule == "cascading_size_modifiers" for i in result.issues)

    def test_product_above_threshold_ok(self):
        ao = _agent_outputs(
            flow=[{"symbol": "AAPL", "position_size_modifier": 0.8}],
            cross_asset=[{"symbol": "AAPL", "effective_size_modifier": 0.9}],
        )
        result = check_blueprint(
            _blueprint(),
            agent_outputs=ao,
        )
        # 0.8 × 0.9 = 0.72 ≥ 0.3 → ok
        assert not any(i.rule == "cascading_size_modifiers" for i in result.issues)

    def test_missing_agent_no_issue(self):
        ao = _agent_outputs(
            flow=[{"symbol": "AAPL", "position_size_modifier": 0.1}],
        )
        result = check_blueprint(
            _blueprint(),
            agent_outputs=ao,
        )
        assert not any(i.rule == "cascading_size_modifiers" for i in result.issues)


class TestChainHardBlock:
    def test_hard_block_true_error(self):
        ao = _agent_outputs(
            chain=[{"symbol": "AAPL", "hard_block": True, "liquidity_tier": "L5"}],
        )
        result = check_blueprint(
            _blueprint(),
            agent_outputs=ao,
        )
        assert any(i.rule == "chain_hard_block" and i.severity == "error" for i in result.issues)

    def test_l5_liquidity_tier_error(self):
        ao = _agent_outputs(
            chain=[{"symbol": "AAPL", "hard_block": False, "liquidity_tier": "L5"}],
        )
        result = check_blueprint(
            _blueprint(),
            agent_outputs=ao,
        )
        assert any(i.rule == "chain_hard_block" for i in result.issues)

    def test_l3_no_block(self):
        ao = _agent_outputs(
            chain=[{"symbol": "AAPL", "hard_block": False, "liquidity_tier": "L3"}],
        )
        result = check_blueprint(
            _blueprint(),
            agent_outputs=ao,
        )
        assert not any(i.rule == "chain_hard_block" for i in result.issues)

    def test_no_chain_agent_no_issue(self):
        ao = _agent_outputs(flow=[{"symbol": "AAPL"}])
        result = check_blueprint(
            _blueprint(),
            agent_outputs=ao,
        )
        assert not any(i.rule == "chain_hard_block" for i in result.issues)


class TestMasterOverride:
    def test_override_exceeded_error(self):
        ao = _agent_outputs(
            cross_asset=[{
                "symbol": "AAPL",
                "master_override": True,
                "effective_size_modifier": 0.5,
            }],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(max_position_size=0.8)]),
            agent_outputs=ao,
        )
        assert any(i.rule == "master_override_exceeded" and i.severity == "error" for i in result.issues)

    def test_override_skip_when_below_floor(self):
        ao = _agent_outputs(
            cross_asset=[{
                "symbol": "AAPL",
                "master_override": True,
                "effective_size_modifier": 0.2,
            }],
        )
        result = check_blueprint(
            _blueprint(),
            agent_outputs=ao,
        )
        assert any(i.rule == "master_override_skip" and i.severity == "error" for i in result.issues)

    def test_override_within_limit_ok(self):
        ao = _agent_outputs(
            cross_asset=[{
                "symbol": "AAPL",
                "master_override": True,
                "effective_size_modifier": 0.8,
            }],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(max_position_size=0.7)]),
            agent_outputs=ao,
        )
        assert not any(i.rule.startswith("master_override") for i in result.issues)

    def test_no_override_no_check(self):
        ao = _agent_outputs(
            cross_asset=[{
                "symbol": "AAPL",
                "master_override": False,
                "effective_size_modifier": 0.3,
            }],
        )
        result = check_blueprint(
            _blueprint(),
            agent_outputs=ao,
        )
        assert not any(i.rule.startswith("master_override") for i in result.issues)


class TestSpreadEffectiveRR:
    def test_effective_rr_below_one_error(self):
        ao = _agent_outputs(
            spread=[{"symbol": "AAPL", "effective_rr": 0.7}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="vertical_spread",
                legs=[_leg(strike=150, side="buy"), _leg(strike=160, side="sell")],
            )]),
            agent_outputs=ao,
        )
        assert any(i.rule == "spread_effective_rr_reject" and i.severity == "error" for i in result.issues)

    def test_effective_rr_null_high_confidence_warning(self):
        ao = _agent_outputs(
            spread=[{"symbol": "AAPL", "effective_rr": None}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="iron_condor",
                direction="neutral",
                confidence=0.7,
                legs=[
                    _leg(strike=140, option_type="put", side="buy"),
                    _leg(strike=145, option_type="put", side="sell"),
                    _leg(strike=155, option_type="call", side="sell"),
                    _leg(strike=160, option_type="call", side="buy"),
                ],
            )]),
            agent_outputs=ao,
        )
        assert any(i.rule == "spread_effective_rr_unknown" and i.severity == "warning" for i in result.issues)

    def test_effective_rr_ok_for_single_leg(self):
        """Single leg is not a spread strategy — skip check."""
        ao = _agent_outputs(
            spread=[{"symbol": "AAPL", "effective_rr": 0.5}],
        )
        result = check_blueprint(
            _blueprint(),
            agent_outputs=ao,
        )
        assert not any(i.rule.startswith("spread_effective_rr") for i in result.issues)


class TestEventRiskConsensus:
    def test_three_agents_flag_event_risk_warning(self):
        ao = _agent_outputs(
            volatility=[{"symbol": "AAPL", "event_risk_present": True}],
            flow=[{"symbol": "AAPL", "event_risk_present": True}],
            chain=[{"symbol": "AAPL", "event_risk_present": True}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(confidence=0.7)]),
            agent_outputs=ao,
        )
        assert any(i.rule == "event_risk_consensus" and i.severity == "warning" for i in result.issues)

    def test_two_agents_flag_no_warning(self):
        ao = _agent_outputs(
            volatility=[{"symbol": "AAPL", "event_risk_present": True}],
            flow=[{"symbol": "AAPL", "event_risk_present": True}],
            chain=[{"symbol": "AAPL", "event_risk_present": False}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(confidence=0.7)]),
            agent_outputs=ao,
        )
        assert not any(i.rule == "event_risk_consensus" for i in result.issues)

    def test_three_agents_low_confidence_ok(self):
        ao = _agent_outputs(
            volatility=[{"symbol": "AAPL", "event_risk_present": True}],
            flow=[{"symbol": "AAPL", "event_risk_present": True}],
            chain=[{"symbol": "AAPL", "event_risk_present": True}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(confidence=0.4)]),
            agent_outputs=ao,
        )
        assert not any(i.rule == "event_risk_consensus" for i in result.issues)


class TestConfirmingIndicators:
    def test_low_indicators_high_confidence_warning(self):
        ao = _agent_outputs(
            flow=[{"symbol": "AAPL", "confirming_indicators_count": 1}],
            chain=[{"symbol": "AAPL", "confirming_indicators_count": 0}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(confidence=0.8, direction="bullish")]),
            agent_outputs=ao,
        )
        assert any(i.rule == "low_confirming_indicators" and i.severity == "warning" for i in result.issues)

    def test_enough_indicators_no_warning(self):
        ao = _agent_outputs(
            flow=[{"symbol": "AAPL", "confirming_indicators_count": 3}],
            chain=[{"symbol": "AAPL", "confirming_indicators_count": 2}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(confidence=0.8, direction="bullish")]),
            agent_outputs=ao,
        )
        assert not any(i.rule == "low_confirming_indicators" for i in result.issues)

    def test_neutral_direction_skipped(self):
        ao = _agent_outputs(
            flow=[{"symbol": "AAPL", "confirming_indicators_count": 0}],
            chain=[{"symbol": "AAPL", "confirming_indicators_count": 0}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(confidence=0.8, direction="neutral")]),
            agent_outputs=ao,
        )
        assert not any(i.rule == "low_confirming_indicators" for i in result.issues)


class TestBackwardCompat:
    def test_no_agent_outputs_still_works(self):
        """check_blueprint without agent_outputs should work as before."""
        result = check_blueprint(_blueprint())
        assert isinstance(result, CheckResult)
        assert result.error_count == 0


# ---------------------------------------------------------------------------
# Cross-asset quality guard checks
# ---------------------------------------------------------------------------


class TestCrossAssetQualityGuards:
    def test_low_significance_caps_confidence(self):
        signals = {
            "AAPL": {
                "stock_indicators": {},
                "option_indicators": {},
                "cross_asset_indicators": {
                    "confidence_scores": {
                        "correlation_significance": 0.4,
                        "data_freshness": 0.9,
                    }
                },
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(confidence=0.7)]),
            signal_features=signals,
        )
        assert any(i.rule == "cross_asset_low_significance_confidence_cap" for i in result.issues)

    def test_stale_data_blocks_aggressive_directional(self):
        signals = {
            "AAPL": {
                "stock_indicators": {},
                "option_indicators": {},
                "cross_asset_indicators": {
                    "confidence_scores": {
                        "correlation_significance": 0.8,
                        "data_freshness": 0.3,
                    }
                },
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(direction="bullish", confidence=0.7)]),
            signal_features=signals,
        )
        assert any(i.rule == "cross_asset_stale_data_aggressive_direction" for i in result.issues)

    def test_low_quality_caps_position_size(self):
        signals = {
            "AAPL": {
                "stock_indicators": {},
                "option_indicators": {},
                "cross_asset_indicators": {
                    "confidence_scores": {
                        "correlation_significance": 0.4,
                        "data_freshness": 0.3,
                    }
                },
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(max_position_size=0.9)]),
            signal_features=signals,
        )
        assert any(i.rule == "cross_asset_low_quality_position_size" for i in result.issues)
