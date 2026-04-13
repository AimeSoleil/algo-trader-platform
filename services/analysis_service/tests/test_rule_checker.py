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
                "stock_trend": {"adx_14": 35.0, "trend_direction": "bullish"},
                "option_chain": {},
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
                "stock_trend": {"adx_14": 35.0, "trend_direction": "bullish"},
                "option_chain": {},
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
                "stock_trend": {},
                "option_chain": {"bid_ask_spread_ratio": 0.30},
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
                "stock_trend": {},
                "option_chain": {"bid_ask_spread_ratio": 0.17},
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
                "stock_trend": {},
                "option_chain": {},
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
                "stock_trend": {},
                "option_chain": {},
                "data_quality": {"score": 0.3},
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(confidence=0.5)]),
            signal_features=signals,
        )
        assert not any(i.rule == "overconfident_on_bad_data" for i in result.issues)


# ---------------------------------------------------------------------------
# Cascading modifiers check
# ---------------------------------------------------------------------------


class TestCascadingModifiers:
    def test_cascading_modifiers_near_zero(self):
        """Stacked modifiers producing <0.3 effective size should warn."""
        signals = {
            "AAPL": {
                "stock_trend": {},
                "option_chain": {},
                "flow": {"position_size_modifier": 0.5},
                "cross_asset": {"position_size_modifier": 0.4},
            }
        }
        result = check_blueprint(
            _blueprint(),
            signal_features=signals,
        )
        assert any(i.rule == "cascading_size_modifiers" for i in result.issues)

    def test_reasonable_modifiers_ok(self):
        """Normal modifiers should not warn."""
        signals = {
            "AAPL": {
                "stock_trend": {},
                "option_chain": {},
                "flow": {"position_size_modifier": 0.8},
                "cross_asset": {"position_size_modifier": 0.9},
            }
        }
        result = check_blueprint(
            _blueprint(),
            signal_features=signals,
        )
        assert not any(i.rule == "cascading_size_modifiers" for i in result.issues)


# ---------------------------------------------------------------------------
# A1: Context-Aware DTE Bounds
# ---------------------------------------------------------------------------


class TestDTEBoundsContextAware:
    def test_gamma_scalp_allows_short_dte(self):
        """gamma_scalp allows DTE=2 (min=0)."""
        from datetime import date, timedelta
        short_expiry = (date.today() + timedelta(days=2)).isoformat()
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            strategy_type="gamma_scalp",
            legs=[_leg(expiry=short_expiry)],
        )]))
        assert not any(i.rule == "dte_bounds" for i in result.issues)

    def test_gamma_scalp_warns_long_dte(self):
        """gamma_scalp warns if DTE > 5."""
        from datetime import date, timedelta
        long_expiry = (date.today() + timedelta(days=10)).isoformat()
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            strategy_type="gamma_scalp",
            legs=[_leg(expiry=long_expiry)],
        )]))
        assert any(i.rule == "dte_bounds" and i.severity == "warning" for i in result.issues)

    def test_calendar_spread_errors_short_dte(self):
        """calendar_spread errors if DTE < 14."""
        from datetime import date, timedelta
        short_expiry = (date.today() + timedelta(days=10)).isoformat()
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            strategy_type="calendar_spread",
            legs=[
                _leg(expiry=short_expiry, side="sell"),
                _leg(expiry=(date.today() + timedelta(days=50)).isoformat(), side="buy"),
            ],
        )]))
        assert any(i.rule == "dte_bounds" and i.severity == "error" for i in result.issues)

    def test_leaps_allows_long_dte(self):
        """leaps allows DTE=300."""
        from datetime import date, timedelta
        long_expiry = (date.today() + timedelta(days=300)).isoformat()
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            strategy_type="leaps",
            legs=[_leg(expiry=long_expiry)],
        )]))
        assert not any(i.rule == "dte_bounds" for i in result.issues)

    def test_leaps_errors_short_dte(self):
        """leaps errors if DTE < 60."""
        from datetime import date, timedelta
        short_expiry = (date.today() + timedelta(days=30)).isoformat()
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            strategy_type="leaps",
            legs=[_leg(expiry=short_expiry)],
        )]))
        assert any(i.rule == "dte_bounds" and i.severity == "error" for i in result.issues)


# ---------------------------------------------------------------------------
# A2: Account-Scaled Daily Loss Cap
# ---------------------------------------------------------------------------


class TestAccountScaledLossCap:
    def test_small_account_lower_hard_limit(self):
        """50k account → hard limit $1500. $1600 should error."""
        result = check_blueprint(_blueprint(max_daily_loss=1600.0), account_size=50_000.0)
        assert any(i.rule == "max_daily_loss" and i.severity == "error" for i in result.issues)

    def test_small_account_soft_limit_warning(self):
        """50k account → soft limit $1000. $1100 should warn."""
        result = check_blueprint(_blueprint(max_daily_loss=1100.0), account_size=50_000.0)
        warns = [i for i in result.issues if i.rule == "max_daily_loss" and i.severity == "warning"]
        assert len(warns) == 1

    def test_large_account_allows_higher_loss(self):
        """500k account → hard limit $15000. $3000 should pass."""
        result = check_blueprint(_blueprint(max_daily_loss=3000.0), account_size=500_000.0)
        assert not any(i.rule == "max_daily_loss" for i in result.issues)

    def test_default_account_preserves_behaviour(self):
        """Default 100k → hard limit $3000, soft limit $2000."""
        result = check_blueprint(_blueprint(max_daily_loss=2000.0))
        assert not any(i.rule == "max_daily_loss" for i in result.issues)


# ---------------------------------------------------------------------------
# A3: Adaptive Delta/Gamma Limits
# ---------------------------------------------------------------------------


class TestAdaptiveDeltaGamma:
    def test_strong_trend_allows_high_delta(self):
        """trend_strength > 0.7 → delta 0.85 only warns (no error)."""
        result = check_blueprint(
            _blueprint(portfolio_delta_limit=0.85),
            context={"trend_strength": 0.8},
        )
        warns = [i for i in result.issues if i.rule == "portfolio_delta_limit_elevated"]
        errs = [i for i in result.issues if i.rule == "portfolio_delta_limit"]
        assert len(warns) == 1
        assert len(errs) == 0

    def test_weak_trend_errors_lower_delta(self):
        """trend_strength < 0.3 → delta 0.75 errors."""
        result = check_blueprint(
            _blueprint(portfolio_delta_limit=0.75),
            context={"trend_strength": 0.2},
        )
        assert any(i.rule == "portfolio_delta_limit" and i.severity == "error"
                    for i in result.issues)

    def test_weak_trend_warns_moderate_delta(self):
        """trend_strength < 0.3 → delta 0.45 warns."""
        result = check_blueprint(
            _blueprint(portfolio_delta_limit=0.45),
            context={"trend_strength": 0.2},
        )
        warns = [i for i in result.issues if i.rule == "portfolio_delta_limit_elevated"]
        assert len(warns) == 1

    def test_gamma_warning_without_short_dte(self):
        """Gamma > 0.1 but no short gamma + short DTE → warning only."""
        result = check_blueprint(_blueprint(portfolio_gamma_limit=0.15))
        issues = [i for i in result.issues if i.rule == "portfolio_gamma_limit"]
        assert len(issues) == 1
        assert issues[0].severity == "warning"

    def test_gamma_error_with_short_gamma_short_dte(self):
        """Gamma > 0.1 with short gamma + DTE ≤ 7 → error."""
        from datetime import date, timedelta
        short_expiry = (date.today() + timedelta(days=3)).isoformat()
        result = check_blueprint(_blueprint(
            portfolio_gamma_limit=0.15,
            symbol_plans=[_plan(
                strategy_type="gamma_scalp",
                legs=[_leg(expiry=short_expiry, side="sell")],
            )],
        ))
        gamma_issues = [i for i in result.issues if i.rule == "portfolio_gamma_limit"]
        assert len(gamma_issues) == 1
        assert gamma_issues[0].severity == "error"


# ---------------------------------------------------------------------------
# A4: Counter-Trend Confluence Gate
# ---------------------------------------------------------------------------


class TestCounterTrendConfluence:
    def test_confluence_downgrades_to_warning(self):
        """2+ confluence signals + stop_loss → warning instead of error."""
        signals = {
            "AAPL": {
                "stock_trend": {"adx_14": 35.0, "trend_direction": "bullish"},
                "option_chain": {"pcr_volume": 1.8},
                "trend": {"bb_position": 0.98},
                "flow": {"volume_ratio": 0.7},
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(direction="bearish", stop_loss_amount=200.0)]),
            signal_features=signals,
        )
        issues = [i for i in result.issues if i.rule == "counter_trend_adx30"]
        assert len(issues) == 1
        assert issues[0].severity == "warning"

    def test_no_confluence_stays_error(self):
        """No confluence signals → stays error."""
        signals = {
            "AAPL": {
                "stock_trend": {"adx_14": 35.0, "trend_direction": "bullish"},
                "option_chain": {},
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(direction="bearish")]),
            signal_features=signals,
        )
        issues = [i for i in result.issues if i.rule == "counter_trend_adx30"]
        assert len(issues) == 1
        assert issues[0].severity == "error"

    def test_confluence_without_stop_loss_stays_error(self):
        """Confluence signals but no stop_loss → stays error."""
        signals = {
            "AAPL": {
                "stock_trend": {"adx_14": 35.0, "trend_direction": "bullish"},
                "option_chain": {"pcr_volume": 1.8},
                "trend": {"bb_position": 0.02},
                "flow": {},
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(direction="bearish", stop_loss_amount=None)]),
            signal_features=signals,
        )
        issues = [i for i in result.issues if i.rule == "counter_trend_adx30"]
        assert len(issues) == 1
        assert issues[0].severity == "error"


# ---------------------------------------------------------------------------
# A5: Relative Bid-Ask Spread Check
# ---------------------------------------------------------------------------


class TestBidAskRelative:
    def test_iron_condor_relaxed_threshold(self):
        """iron_condor has hard block at 0.30 — 0.27 should not block."""
        signals = {
            "AAPL": {
                "stock_trend": {},
                "option_chain": {"bid_ask_spread_ratio": 0.27},
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="iron_condor",
                direction="neutral",
                legs=[
                    _leg(strike=140, option_type="put", side="buy"),
                    _leg(strike=145, option_type="put", side="sell"),
                    _leg(strike=155, option_type="call", side="sell"),
                    _leg(strike=160, option_type="call", side="buy"),
                ],
            )]),
            signal_features=signals,
        )
        assert not any(i.rule == "bid_ask_hard_block" for i in result.issues)
        assert any(i.rule == "bid_ask_illiquid" for i in result.issues)

    def test_regular_strategy_blocks_at_new_threshold(self):
        """Non-condor/calendar: hard block at 0.25."""
        signals = {
            "AAPL": {
                "stock_trend": {},
                "option_chain": {"bid_ask_spread_ratio": 0.26},
            }
        }
        result = check_blueprint(
            _blueprint(),
            signal_features=signals,
        )
        assert any(i.rule == "bid_ask_hard_block" for i in result.issues)

    def test_between_old_and_new_threshold_no_hard_block(self):
        """0.21 no longer hard-blocks (was 0.20), should only warn."""
        signals = {
            "AAPL": {
                "stock_trend": {},
                "option_chain": {"bid_ask_spread_ratio": 0.21},
            }
        }
        result = check_blueprint(
            _blueprint(),
            signal_features=signals,
        )
        assert not any(i.rule == "bid_ask_hard_block" for i in result.issues)
        assert any(i.rule == "bid_ask_illiquid" for i in result.issues)


# ---------------------------------------------------------------------------
# A6: Strategy-Aware Confidence Floor
# ---------------------------------------------------------------------------


class TestStrategyAwareConfidence:
    def test_iron_condor_low_confidence_warns(self):
        """iron_condor baseline=0.35, threshold=0.21. confidence=0.15 should warn."""
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            strategy_type="iron_condor",
            direction="neutral",
            confidence=0.15,
            legs=[
                _leg(strike=140, option_type="put", side="buy"),
                _leg(strike=145, option_type="put", side="sell"),
                _leg(strike=155, option_type="call", side="sell"),
                _leg(strike=160, option_type="call", side="buy"),
            ],
        )]))
        assert any(i.rule == "low_confidence" for i in result.issues)

    def test_bull_call_spread_higher_threshold(self):
        """bull_call_spread baseline=0.45, threshold=0.27. confidence=0.25 should warn."""
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            strategy_type="bull_call_spread",
            confidence=0.25,
        )]))
        assert any(i.rule == "low_confidence" for i in result.issues)

    def test_iron_condor_above_threshold_no_warn(self):
        """iron_condor with confidence=0.25 (above 0.21) should not warn."""
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            strategy_type="iron_condor",
            direction="neutral",
            confidence=0.25,
            legs=[
                _leg(strike=140, option_type="put", side="buy"),
                _leg(strike=145, option_type="put", side="sell"),
                _leg(strike=155, option_type="call", side="sell"),
                _leg(strike=160, option_type="call", side="buy"),
            ],
        )]))
        assert not any(i.rule == "low_confidence" for i in result.issues)

    def test_default_strategy_uses_0_40_baseline(self):
        """Unknown strategy uses 0.40 baseline → threshold 0.24."""
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            strategy_type="custom_exotic",
            confidence=0.2,
        )]))
        assert any(i.rule == "low_confidence" for i in result.issues)


# ---------------------------------------------------------------------------
# A7: Smarter Cascading Modifier Check
# ---------------------------------------------------------------------------


class TestSmartCascadingModifiers:
    def test_product_below_0_1_errors(self):
        """Product < 0.1 → error (effectively zero)."""
        signals = {
            "AAPL": {
                "stock_trend": {},
                "option_chain": {},
                "flow": {"position_size_modifier": 0.2},
                "cross_asset": {"position_size_modifier": 0.3},
            }
        }
        result = check_blueprint(_blueprint(), signal_features=signals)
        issues = [i for i in result.issues if i.rule == "cascading_size_modifiers"]
        assert len(issues) == 1
        assert issues[0].severity == "error"

    def test_both_reducing_is_info(self):
        """Both modifiers < 0.5 with product 0.1–0.3 → info (intentional)."""
        signals = {
            "AAPL": {
                "stock_trend": {},
                "option_chain": {},
                "flow": {"position_size_modifier": 0.4},
                "cross_asset": {"position_size_modifier": 0.4},
            }
        }
        result = check_blueprint(_blueprint(), signal_features=signals)
        issues = [i for i in result.issues if i.rule == "cascading_size_modifiers"]
        assert len(issues) == 1
        assert issues[0].severity == "info"

    def test_conflicting_modifiers_warns(self):
        """One > 0.75, other < 0.5 → warning (unclear intent)."""
        signals = {
            "AAPL": {
                "stock_trend": {},
                "option_chain": {},
                "flow": {"position_size_modifier": 0.8},
                "cross_asset": {"position_size_modifier": 0.3},
            }
        }
        result = check_blueprint(_blueprint(), signal_features=signals)
        issues = [i for i in result.issues if i.rule == "cascading_size_modifiers"]
        assert len(issues) == 1
        assert issues[0].severity == "warning"

    def test_hedge_strategy_is_info(self):
        """Strategy containing 'hedge' with product 0.1–0.3 → info."""
        signals = {
            "AAPL": {
                "stock_trend": {},
                "option_chain": {},
                "flow": {"position_size_modifier": 0.5},
                "cross_asset": {"position_size_modifier": 0.4},
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(strategy_type="delta_hedge")]),
            signal_features=signals,
        )
        issues = [i for i in result.issues if i.rule == "cascading_size_modifiers"]
        assert len(issues) == 1
        assert issues[0].severity == "info"
