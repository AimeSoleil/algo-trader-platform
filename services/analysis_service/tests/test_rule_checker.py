"""Test deterministic rule checker for blueprint validation."""
from __future__ import annotations

import datetime
from types import SimpleNamespace

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
# Plan-level risk
# ---------------------------------------------------------------------------


class TestPlanRisk:
    def test_missing_stop_loss_is_allowed_in_manual_trader_mode(self):
        result = check_blueprint(_blueprint(symbol_plans=[_plan(stop_loss_amount=None)]))
        assert not any(i.rule == "stop_loss_required" for i in result.issues)

    def test_zero_stop_loss_is_allowed_in_manual_trader_mode(self):
        result = check_blueprint(_blueprint(symbol_plans=[_plan(stop_loss_amount=0)]))
        assert not any(i.rule == "stop_loss_required" for i in result.issues)

    def test_low_confidence_warning(self):
        result = check_blueprint(_blueprint(symbol_plans=[_plan(confidence=0.2)]))
        assert any(i.rule == "low_confidence" for i in result.issues)

    def test_stop_loss_exceeds_max_loss_not_checked_in_manual_trader_mode(self):
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(stop_loss_amount=700.0, max_loss_per_trade=500.0)])
        )
        assert not any(i.rule == "stop_loss_exceeds_max_loss" for i in result.issues)

    def test_stop_loss_equal_max_loss_not_checked_in_manual_trader_mode(self):
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(stop_loss_amount=500.0, max_loss_per_trade=500.0)])
        )
        assert not any(i.rule == "stop_loss_equals_max_loss" for i in result.issues)

    def test_risk_reward_below_one_not_checked_in_manual_trader_mode(self):
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(take_profit_amount=300.0, max_loss_per_trade=500.0)])
        )
        assert not any(i.rule == "risk_reward_below_one" for i in result.issues)


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

    def test_counter_trend_adx_zscore_error(self):
        signals = {
            "AAPL": {
                "volume": 1_500_000,
                "stock_indicators": {
                    "adx_14": 32.0,
                    "adx_z_score": 1.8,
                    "trend": "bullish",
                    "liquidity_threshold": 750_000.0,
                },
                "option_indicators": {},
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(direction="bearish")]),
            signal_features=signals,
        )
        assert any(i.rule == "counter_trend_adx_zscore" and i.severity == "error" for i in result.issues)

    def test_trend_low_liquidity_no_longer_caps_size(self):
        signals = {
            "AAPL": {
                "volume": 400_000,
                "stock_indicators": {
                    "liquidity_threshold": 750_000.0,
                    "trend": "bullish",
                },
                "option_indicators": {},
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(max_position_size=0.5)]),
            signal_features=signals,
        )
        assert not any(i.rule == "trend_low_liquidity_size_cap" for i in result.issues)

    def test_trend_low_liquidity_requires_simple_structure(self):
        signals = {
            "AAPL": {
                "volume": 400_000,
                "stock_indicators": {
                    "liquidity_threshold": 750_000.0,
                    "trend": "neutral",
                },
                "option_indicators": {},
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="butterfly",
                direction="neutral",
                max_position_size=0.2,
                legs=[
                    _leg(strike=145, option_type="call", side="buy"),
                    _leg(strike=150, option_type="call", side="sell"),
                    _leg(strike=155, option_type="call", side="buy"),
                ],
            )]),
            signal_features=signals,
        )
        assert any(i.rule == "trend_low_liquidity_simple_structures_only" and i.severity == "error" for i in result.issues)

    def test_bid_ask_hard_block(self):
        signals = {
            "AAPL": {
                "stock_indicators": {},
                "option_indicators": {"bid_ask_spread_ratio": 0.31},
            }
        }
        result = check_blueprint(
            _blueprint(),
            signal_features=signals,
        )
        assert any(i.rule == "bid_ask_hard_block" for i in result.issues)

    def test_bid_ask_hard_block_for_condor_uses_relaxed_threshold(self):
        signals = {
            "AAPL": {
                "stock_indicators": {},
                "option_indicators": {"bid_ask_spread_ratio": 0.46},
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

    def test_volatility_backwardation_short_dte_blocks_short_vol_structures(self):
        signals = {
            "AAPL": {
                "close_price": 150.0,
                "option_indicators": {
                    "term_structure_slope": -0.02,
                    "front_expiry_dte": 7,
                },
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
        assert any(i.rule == "volatility_backwardation_short_dte_short_vol" and i.severity == "error" for i in result.issues)

    def test_fully_itm_vertical_spread_error(self):
        signals = {
            "AAPL": {
                "close_price": 100.0,
                "stock_indicators": {},
                "option_indicators": {"current_iv": 0.3},
                "cross_asset_indicators": {},
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="vertical_spread",
                direction="bullish",
                legs=[
                    _leg(strike=90, option_type="call", side="buy"),
                    _leg(strike=95, option_type="call", side="sell"),
                ],
            )]),
            signal_features=signals,
        )
        assert any(i.rule == "vertical_spread_fully_itm" and i.severity == "error" for i in result.issues)

    def test_earnings_imminent_non_event_strategy_error(self):
        signals = {
            "AAPL": {
                "close_price": 100.0,
                "stock_indicators": {},
                "option_indicators": {"current_iv": 0.3},
                "cross_asset_indicators": {"earnings_proximity_days": 1},
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="vertical_spread",
                direction="bullish",
                legs=[
                    _leg(strike=100, option_type="call", side="buy"),
                    _leg(strike=110, option_type="call", side="sell"),
                ],
            )]),
            signal_features=signals,
        )
        assert any(i.rule == "earnings_imminent_non_event_strategy" for i in result.issues)


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
        """Default structures still reject very short DTE legs."""
        from datetime import date, timedelta
        short_expiry = (date.today() + timedelta(days=3)).isoformat()
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            legs=[_leg(expiry=short_expiry)],
        )]))
        assert any(i.rule == "dte_bounds" and i.severity == "error" for i in result.issues)

    def test_single_leg_dte_five_days_is_allowed(self):
        """single_leg now allows DTE >= 5."""
        from datetime import date, timedelta
        short_expiry = (date.today() + timedelta(days=5)).isoformat()
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            strategy_type="single_leg",
            legs=[_leg(expiry=short_expiry)],
        )]))
        assert not any(i.rule == "dte_bounds" and i.severity == "error" for i in result.issues)

    def test_vertical_spread_dte_five_days_is_allowed(self):
        """vertical_spread now allows DTE >= 5."""
        from datetime import date, timedelta
        short_expiry = (date.today() + timedelta(days=5)).isoformat()
        result = check_blueprint(_blueprint(symbol_plans=[_plan(
            strategy_type="vertical_spread",
            legs=[
                _leg(expiry=short_expiry, strike=150, option_type="call", side="buy"),
                _leg(expiry=short_expiry, strike=155, option_type="call", side="sell"),
            ],
        )]))
        assert not any(i.rule == "dte_bounds" and i.severity == "error" for i in result.issues)

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


class TestCalendarPrecisionFirstGates:
    def test_calendar_requires_positive_contango(self):
        signals = {
            "AAPL": {
                "close_price": 150.0,
                "option_indicators": {"term_structure_slope": 0.0},
                "cross_asset_indicators": {"earnings_proximity_days": 10},
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="calendar_spread",
                direction="neutral",
                legs=[
                    _leg(expiry="2026-05-15", strike=150, side="sell"),
                    _leg(expiry="2026-06-19", strike=150, side="buy"),
                ],
            )]),
            signal_features=signals,
        )
        assert any(i.rule == "calendar_requires_contango" and i.severity == "error" for i in result.issues)

    def test_calendar_near_earnings_is_rejected_inside_five_days(self):
        signals = {
            "AAPL": {
                "close_price": 150.0,
                "option_indicators": {"term_structure_slope": 0.02},
                "cross_asset_indicators": {"earnings_proximity_days": 5},
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="calendar_spread",
                direction="neutral",
                legs=[
                    _leg(expiry="2026-05-15", strike=150, side="sell"),
                    _leg(expiry="2026-06-19", strike=150, side="buy"),
                ],
            )]),
            signal_features=signals,
        )
        assert any(i.rule == "calendar_near_earnings" and i.severity == "error" for i in result.issues)

    def test_calendar_with_contango_and_earnings_buffer_passes_context_checks(self):
        signals = {
            "AAPL": {
                "close_price": 150.0,
                "option_indicators": {"term_structure_slope": 0.02},
                "cross_asset_indicators": {"earnings_proximity_days": 8},
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="calendar_spread",
                direction="neutral",
                legs=[
                    _leg(expiry="2026-05-15", strike=150, side="sell"),
                    _leg(expiry="2026-06-19", strike=150, side="buy"),
                ],
            )]),
            signal_features=signals,
        )
        assert not any(i.rule in {"calendar_requires_contango", "calendar_near_earnings"} for i in result.issues)


class TestSpreadExecutionCandidateConflicts:
    def test_calendar_conflict_when_vertical_candidate_is_materially_stronger(self):
        signals = {
            "AAPL": {
                "close_price": 150.0,
                "option_indicators": {
                    "term_structure_slope": 0.02,
                    "spread_execution_inputs": {
                        "calendar": {
                            "candidate_available": True,
                            "effective_theta_capture_per_day": -0.01,
                            "worst_leg_bid_ask_spread_ratio": 0.05,
                        },
                        "vertical": {
                            "candidate_available": True,
                            "effective_rr": 1.0,
                            "raw_rr": 1.05,
                            "worst_leg_bid_ask_spread_ratio": 0.04,
                        },
                    },
                },
                "cross_asset_indicators": {"earnings_proximity_days": 8},
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="calendar_spread",
                direction="neutral",
                legs=[
                    _leg(expiry="2026-05-15", strike=150, side="sell"),
                    _leg(expiry="2026-06-19", strike=150, side="buy"),
                ],
            )]),
            signal_features=signals,
        )
        assert any(i.rule == "spread_execution_candidate_conflict" and i.severity == "error" for i in result.issues)

    def test_vertical_conflict_when_calendar_candidate_is_materially_stronger(self):
        signals = {
            "AAPL": {
                "close_price": 150.0,
                "option_indicators": {
                    "iv_rank": 50.0,
                    "term_structure_slope": 0.05,
                    "spread_execution_inputs": {
                        "vertical": {
                            "candidate_available": True,
                            "effective_rr": 0.62,
                            "raw_rr": 0.65,
                            "worst_leg_bid_ask_spread_ratio": 0.04,
                        },
                        "calendar": {
                            "candidate_available": True,
                            "effective_theta_capture_per_day": 0.05,
                            "worst_leg_bid_ask_spread_ratio": 0.03,
                        },
                    },
                },
                "cross_asset_indicators": {"earnings_proximity_days": 10},
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="vertical_spread",
                legs=[
                    _leg(expiry="2026-05-15", strike=150, side="buy"),
                    _leg(expiry="2026-05-15", strike=155, side="sell"),
                ],
            )]),
            signal_features=signals,
        )
        assert any(i.rule == "spread_execution_candidate_conflict" and i.severity == "error" for i in result.issues)

    def test_no_conflict_when_stronger_calendar_candidate_is_blocked_by_earnings(self):
        signals = {
            "AAPL": {
                "close_price": 150.0,
                "option_indicators": {
                    "iv_rank": 50.0,
                    "term_structure_slope": 0.05,
                    "spread_execution_inputs": {
                        "vertical": {
                            "candidate_available": True,
                            "effective_rr": 0.72,
                            "raw_rr": 0.75,
                            "worst_leg_bid_ask_spread_ratio": 0.04,
                        },
                        "calendar": {
                            "candidate_available": True,
                            "effective_theta_capture_per_day": 0.05,
                            "worst_leg_bid_ask_spread_ratio": 0.03,
                        },
                    },
                },
                "cross_asset_indicators": {"earnings_proximity_days": 3},
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="vertical_spread",
                legs=[
                    _leg(expiry="2026-05-15", strike=150, side="buy"),
                    _leg(expiry="2026-05-15", strike=155, side="sell"),
                ],
            )]),
            signal_features=signals,
        )
        assert not any(i.rule == "spread_execution_candidate_conflict" for i in result.issues)

    def test_conflict_when_simple_structure_only_still_allows_configured_calendar_alternative(self):
        signals = {
            "AAPL": {
                "close_price": 150.0,
                "option_indicators": {
                    "iv_rank": 50.0,
                    "term_structure_slope": 0.05,
                    "spread_execution_inputs": {
                        "vertical": {
                            "candidate_available": True,
                            "effective_rr": 0.72,
                            "raw_rr": 0.75,
                            "worst_leg_bid_ask_spread_ratio": 0.04,
                        },
                        "calendar": {
                            "candidate_available": True,
                            "effective_theta_capture_per_day": 0.05,
                            "worst_leg_bid_ask_spread_ratio": 0.03,
                        },
                    },
                },
                "cross_asset_indicators": {"earnings_proximity_days": 10},
            }
        }
        agent_outputs = _agent_outputs(
            flow=[{"symbol": "AAPL", "simple_structures_only": True}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="vertical_spread",
                legs=[
                    _leg(expiry="2026-05-15", strike=150, side="buy"),
                    _leg(expiry="2026-05-15", strike=155, side="sell"),
                ],
            )]),
            signal_features=signals,
            agent_outputs=agent_outputs,
        )
        assert any(i.rule == "spread_execution_candidate_conflict" and i.severity == "error" for i in result.issues)

    def test_missing_execution_candidates_downgrades_to_warning(self):
        signals = {
            "AAPL": {
                "close_price": 150.0,
                "option_indicators": {
                    "term_structure_slope": 0.02,
                },
                "cross_asset_indicators": {"earnings_proximity_days": 8},
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="calendar_spread",
                direction="neutral",
                legs=[
                    _leg(expiry="2026-05-15", strike=150, side="sell"),
                    _leg(expiry="2026-06-19", strike=150, side="buy"),
                ],
            )]),
            signal_features=signals,
        )
        assert any(i.rule == "spread_execution_candidate_data_missing" and i.severity == "warning" for i in result.issues)

    def test_butterfly_negative_explicit_economics_does_not_outrank_stronger_vertical(self):
        signals = {
            "AAPL": {
                "close_price": 150.0,
                "option_indicators": {
                    "term_structure_slope": 0.01,
                    "spread_execution_inputs": {
                        "butterfly": {
                            "candidate_available": True,
                            "pricing_error": 0.14,
                            "effective_rr": -0.2,
                            "net_edge_after_cost": -0.01,
                            "net_profit_after_cost": -8.0,
                            "worst_leg_bid_ask_spread_ratio": 0.03,
                        },
                        "vertical": {
                            "candidate_available": True,
                            "effective_rr": 1.05,
                            "raw_rr": 1.1,
                            "worst_leg_bid_ask_spread_ratio": 0.03,
                        },
                    },
                },
                "cross_asset_indicators": {"earnings_proximity_days": 10},
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="butterfly",
                direction="neutral",
                legs=[
                    _leg(strike=145, option_type="call", side="buy"),
                    _leg(strike=150, option_type="call", side="sell"),
                    _leg(strike=155, option_type="call", side="buy"),
                ],
            )]),
            signal_features=signals,
        )
        assert any(i.rule == "spread_execution_candidate_conflict" and i.severity == "error" for i in result.issues)

    def test_no_conflict_when_far_otm_vertical_is_less_actionable_than_tight_high_credit_condor(self):
        signals = {
            "TSLA": {
                "close_price": 396.68,
                "option_indicators": {
                    "iv_rank": 50.0,
                    "term_structure_slope": 0.1191,
                    "spread_execution_inputs": {
                        "iron_condor": {
                            "candidate_available": True,
                            "effective_rr": 1.6596,
                            "raw_rr": 9.0,
                            "worst_leg_bid_ask_spread_ratio": 0.012848,
                        },
                        "vertical": {
                            "candidate_available": True,
                            "effective_rr": 10.236,
                            "raw_rr": 19.5128,
                            "long_strike": 580,
                            "short_strike": 600,
                            "worst_leg_bid_ask_spread_ratio": 0.033333,
                        },
                    },
                },
                "cross_asset_indicators": {"earnings_proximity_days": 43},
            }
        }
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                underlying="TSLA",
                strategy_type="iron_condor",
                direction="neutral",
                confidence=0.35,
                legs=[
                    _leg(strike=390, option_type="put", side="buy"),
                    _leg(strike=392.5, option_type="put", side="sell"),
                    _leg(strike=402.5, option_type="call", side="sell"),
                    _leg(strike=405, option_type="call", side="buy"),
                ],
            )]),
            signal_features=signals,
        )
        assert not any(i.rule == "spread_execution_candidate_conflict" for i in result.issues)

    def test_conflict_downgrades_to_warning_when_stronger_calendar_candidate_was_never_emitted(self):
        signals = {
            "TSLA": {
                "close_price": 396.68,
                "option_indicators": {
                    "iv_rank": 50.0,
                    "term_structure_slope": 0.1191,
                    "spread_execution_inputs": {
                        "iron_condor": {
                            "candidate_available": True,
                            "effective_rr": 1.6596,
                            "raw_rr": 9.0,
                            "worst_leg_bid_ask_spread_ratio": 0.012848,
                        },
                        "calendar": {
                            "candidate_available": True,
                            "effective_theta_capture_per_day": 0.341456,
                            "worst_leg_bid_ask_spread_ratio": 0.025157,
                        },
                    },
                },
                "cross_asset_indicators": {"earnings_proximity_days": 43},
            }
        }
        agent_outputs = _agent_outputs(
            trend=[{
                "symbol": "TSLA",
                "strategies": [{"strategy_type": "Iron Condor"}],
                "simple_structures_only": True,
            }],
            volatility=[{
                "symbol": "TSLA",
                "strategies": [{"strategy_type": "Iron Condor"}],
                "simple_structures_only": True,
            }],
            chain=[{
                "symbol": "TSLA",
                "suggested_strategies": ["iron_condor", "call_vertical_spread"],
                "simple_structures_only": True,
            }],
            spread=[{
                "symbol": "TSLA",
                "best_spread_type": "iron_condor",
            }],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                underlying="TSLA",
                strategy_type="iron_condor",
                direction="neutral",
                confidence=0.35,
                legs=[
                    _leg(strike=390, option_type="put", side="buy"),
                    _leg(strike=392.5, option_type="put", side="sell"),
                    _leg(strike=402.5, option_type="call", side="sell"),
                    _leg(strike=405, option_type="call", side="buy"),
                ],
            )]),
            signal_features=signals,
            agent_outputs=agent_outputs,
        )
        assert not any(i.rule == "spread_execution_candidate_conflict" and i.severity == "error" for i in result.issues)
        assert any(i.rule == "spread_execution_candidate_unemitted_fallback" and i.severity == "warning" for i in result.issues)

    def test_conflict_remains_error_when_stronger_calendar_candidate_was_emitted(self):
        signals = {
            "TSLA": {
                "close_price": 396.68,
                "option_indicators": {
                    "iv_rank": 50.0,
                    "term_structure_slope": 0.1191,
                    "spread_execution_inputs": {
                        "iron_condor": {
                            "candidate_available": True,
                            "effective_rr": 1.6596,
                            "raw_rr": 9.0,
                            "worst_leg_bid_ask_spread_ratio": 0.012848,
                        },
                        "calendar": {
                            "candidate_available": True,
                            "effective_theta_capture_per_day": 0.341456,
                            "worst_leg_bid_ask_spread_ratio": 0.025157,
                        },
                    },
                },
                "cross_asset_indicators": {"earnings_proximity_days": 43},
            }
        }
        agent_outputs = _agent_outputs(
            trend=[{
                "symbol": "TSLA",
                "strategies": [{"strategy_type": "Iron Condor"}, {"strategy_type": "Calendar Spread"}],
                "simple_structures_only": True,
            }],
            spread=[{
                "symbol": "TSLA",
                "best_spread_type": "iron_condor",
            }],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                underlying="TSLA",
                strategy_type="iron_condor",
                direction="neutral",
                confidence=0.35,
                legs=[
                    _leg(strike=390, option_type="put", side="buy"),
                    _leg(strike=392.5, option_type="put", side="sell"),
                    _leg(strike=402.5, option_type="call", side="sell"),
                    _leg(strike=405, option_type="call", side="buy"),
                ],
            )]),
            signal_features=signals,
            agent_outputs=agent_outputs,
        )
        assert any(i.rule == "spread_execution_candidate_conflict" and i.severity == "error" for i in result.issues)

    def test_no_calendar_fallback_warning_when_calendar_violates_iv_rank_contract(self):
        signals = {
            "TSLA": {
                "close_price": 396.68,
                "option_indicators": {
                    "iv_rank": 94.72,
                    "term_structure_slope": 0.1191,
                    "spread_execution_inputs": {
                        "iron_condor": {
                            "candidate_available": True,
                            "effective_rr": 1.6596,
                            "raw_rr": 9.0,
                            "worst_leg_bid_ask_spread_ratio": 0.012848,
                        },
                        "calendar": {
                            "candidate_available": True,
                            "effective_theta_capture_per_day": 0.341456,
                            "worst_leg_bid_ask_spread_ratio": 0.025157,
                        },
                    },
                },
                "cross_asset_indicators": {"earnings_proximity_days": 43},
            }
        }
        agent_outputs = _agent_outputs(
            trend=[{
                "symbol": "TSLA",
                "strategies": [{"strategy_type": "Iron Condor"}],
                "simple_structures_only": True,
            }],
            spread=[{
                "symbol": "TSLA",
                "best_spread_type": "iron_condor",
            }],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                underlying="TSLA",
                strategy_type="iron_condor",
                direction="neutral",
                confidence=0.35,
                legs=[
                    _leg(strike=390, option_type="put", side="buy"),
                    _leg(strike=392.5, option_type="put", side="sell"),
                    _leg(strike=402.5, option_type="call", side="sell"),
                    _leg(strike=405, option_type="call", side="buy"),
                ],
            )]),
            signal_features=signals,
            agent_outputs=agent_outputs,
        )
        assert not any(i.rule == "spread_execution_candidate_unemitted_fallback" for i in result.issues)
        assert not any(i.rule == "spread_execution_candidate_conflict" for i in result.issues)


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


class TestManualTraderSizingGuards:
    def test_cascading_size_modifiers_no_longer_blocks_trade(self):
        ao = _agent_outputs(
            flow=[{"symbol": "AAPL", "position_size_modifier": 0.1}],
            cross_asset=[{"symbol": "AAPL", "effective_size_modifier": 0.2}],
        )
        result = check_blueprint(
            _blueprint(),
            agent_outputs=ao,
        )
        assert not any(i.rule == "cascading_size_modifiers" for i in result.issues)


class TestFlowPositionSizeModifierCaps:
    def test_flow_modifier_no_longer_caps_blueprint_max_position_size(self):
        ao = _agent_outputs(
            flow=[{"symbol": "AAPL", "position_size_modifier": 0.3}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(max_position_size=0.5)]),
            agent_outputs=ao,
        )
        assert not any(i.rule == "flow_position_size_modifier_cap" for i in result.issues)

    def test_flow_modifier_allows_equal_or_smaller_blueprint_size(self):
        ao = _agent_outputs(
            flow=[{"symbol": "AAPL", "position_size_modifier": 0.5}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(max_position_size=0.5)]),
            agent_outputs=ao,
        )
        assert not any(i.rule == "flow_position_size_modifier_cap" for i in result.issues)

    def test_flow_modifier_skips_when_blueprint_has_no_max_position_size(self):
        ao = _agent_outputs(
            flow=[{"symbol": "AAPL", "position_size_modifier": 0.3}],
        )
        result = check_blueprint(
            _blueprint(),
            agent_outputs=ao,
        )
        assert not any(i.rule == "flow_position_size_modifier_cap" for i in result.issues)


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
    def test_override_exceeded_is_not_enforced_in_manual_trader_mode(self):
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
        assert not any(i.rule == "master_override_exceeded" for i in result.issues)

    def test_override_skip_when_below_floor_is_not_enforced_in_manual_trader_mode(self):
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
        assert not any(i.rule == "master_override_skip" for i in result.issues)

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


class TestGlobalPositionSizeCap:
    def test_default_global_cap_is_not_enforced_in_manual_trader_mode(self):
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(max_position_size=2.1)]),
        )
        assert not any(i.rule == "max_position_size_global_cap" for i in result.issues)

    def test_configured_global_cap_is_respected(self, monkeypatch):
        settings = SimpleNamespace(
            analysis_service=SimpleNamespace(
                llm=SimpleNamespace(
                    max_position_size_cap=2.2,
                    precision_first=SimpleNamespace(enabled=True, allowed_strategy_types=["single_leg", "vertical_spread"]),
                )
            )
        )
        monkeypatch.setattr(
            "services.analysis_service.app.evaluation.rule_checker.get_settings",
            lambda: settings,
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(max_position_size=2.1)]),
        )
        assert not any(i.rule == "max_position_size_global_cap" for i in result.issues)


class TestSpreadEffectiveRR:
    def test_vertical_rr_below_floor_error(self):
        ao = _agent_outputs(
            spread=[{"symbol": "AAPL", "effective_rr": 0.6}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="vertical_spread",
                legs=[_leg(strike=150, side="buy"), _leg(strike=160, side="sell")],
            )]),
            agent_outputs=ao,
        )
        assert any(i.rule == "spread_vertical_rr_reject" and i.severity == "error" for i in result.issues)

    def test_vertical_falls_back_to_raw_risk_reward_when_effective_rr_missing(self):
        ao = _agent_outputs(
            spread=[{"symbol": "AAPL", "effective_rr": None, "risk_reward_ratio": 0.6}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="vertical_spread",
                legs=[_leg(strike=150, option_type="call", side="buy"), _leg(strike=160, option_type="call", side="sell")],
            )]),
            agent_outputs=ao,
        )
        assert any(i.rule == "spread_vertical_rr_reject" and i.severity == "error" for i in result.issues)

    def test_iron_condor_effective_rr_null_is_allowed(self):
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
        assert not any(i.rule.startswith("spread_vertical_rr") for i in result.issues)

    def test_effective_rr_ok_for_single_leg(self):
        """Single leg is not a spread strategy — skip check."""
        ao = _agent_outputs(
            spread=[{"symbol": "AAPL", "effective_rr": 0.5}],
        )
        result = check_blueprint(
            _blueprint(),
            agent_outputs=ao,
        )
        assert not any(i.rule.startswith("spread_vertical_rr") for i in result.issues)


class TestStructuredAgentTradeGates:
    def test_trend_trade_allowed_false_error(self):
        ao = _agent_outputs(
            trend=[{
                "symbol": "AAPL",
                "trade_allowed": False,
                "blocked_reasons": ["earnings_imminent"],
            }],
        )
        result = check_blueprint(_blueprint(), agent_outputs=ao)
        assert any(i.rule == "trend_trade_blocked" and i.severity == "error" for i in result.issues)

    def test_trend_trade_allowed_false_soft_reason_warns_only(self):
        ao = _agent_outputs(
            trend=[{
                "symbol": "AAPL",
                "trade_allowed": False,
                "blocked_reasons": ["counter_trend_strong_adx", "divergence_reversal_warning"],
            }],
        )
        result = check_blueprint(_blueprint(), agent_outputs=ao)
        assert not any(i.rule == "trend_trade_blocked" and i.severity == "error" for i in result.issues)
        assert any(i.rule == "trend_trade_block_soft" and i.severity == "warning" for i in result.issues)

    def test_trend_confidence_cap_error(self):
        ao = _agent_outputs(
            trend=[{
                "symbol": "AAPL",
                "confidence_cap": 0.2,
                "blocked_reasons": ["counter_trend_strong_adx"],
            }],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(confidence=0.7)]),
            agent_outputs=ao,
        )
        assert any(i.rule == "trend_confidence_cap" and i.severity == "error" for i in result.issues)

    def test_trend_simple_structures_only_error(self):
        ao = _agent_outputs(
            trend=[{
                "symbol": "AAPL",
                "simple_structures_only": True,
                "blocked_reasons": ["divergence_reversal_warning"],
            }],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="butterfly",
                direction="neutral",
                legs=[
                    _leg(strike=145, option_type="call", side="buy"),
                    _leg(strike=150, option_type="call", side="sell"),
                    _leg(strike=155, option_type="call", side="buy"),
                ],
            )]),
            agent_outputs=ao,
        )
        assert any(i.rule == "trend_simple_structures_only" for i in result.issues)

    def test_trend_simple_structures_only_allows_configured_precision_first_strategy(self, monkeypatch):
        settings = SimpleNamespace(
            analysis_service=SimpleNamespace(
                llm=SimpleNamespace(
                    precision_first=SimpleNamespace(
                        enabled=True,
                        allowed_strategy_types=["single_leg", "vertical_spread", "iron_condor", "calendar_spread"],
                    )
                )
            )
        )
        monkeypatch.setattr(
            "services.analysis_service.app.evaluation.rule_checker.get_settings",
            lambda: settings,
        )

        ao = _agent_outputs(
            trend=[{
                "symbol": "AAPL",
                "simple_structures_only": True,
                "blocked_reasons": ["divergence_reversal_warning"],
            }],
        )
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
            agent_outputs=ao,
        )
        assert not any(i.rule == "trend_simple_structures_only" for i in result.issues)

    def test_trend_false_positive_risk_high_no_longer_caps_size(self):
        ao = _agent_outputs(
            trend=[{
                "symbol": "AAPL",
                "false_positive_risk": "high",
            }],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(max_position_size=0.5)]),
            agent_outputs=ao,
        )
        assert not any(i.rule == "trend_false_positive_risk_size_cap" for i in result.issues)

    def test_trend_false_positive_risk_high_requires_simple_structures(self):
        ao = _agent_outputs(
            trend=[{
                "symbol": "AAPL",
                "false_positive_risk": "high",
            }],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="butterfly",
                direction="neutral",
                max_position_size=0.2,
                legs=[
                    _leg(strike=145, option_type="call", side="buy"),
                    _leg(strike=150, option_type="call", side="sell"),
                    _leg(strike=155, option_type="call", side="buy"),
                ],
            )]),
            agent_outputs=ao,
        )
        assert any(i.rule == "trend_false_positive_risk_simple_structures_only" and i.severity == "error" for i in result.issues)

    def test_flow_trade_allowed_false_error(self):
        ao = _agent_outputs(
            flow=[{
                "symbol": "AAPL",
                "trade_allowed": False,
                "blocked_reasons": ["event_risk_imminent"],
            }],
        )
        result = check_blueprint(_blueprint(), agent_outputs=ao)
        assert any(i.rule == "flow_trade_blocked" and i.severity == "error" for i in result.issues)

    def test_soft_trade_block_consensus_escalates_to_error(self):
        ao = _agent_outputs(
            trend=[{
                "symbol": "AAPL",
                "trade_allowed": False,
                "blocked_reasons": ["counter_trend_strong_adx"],
            }],
            flow=[{
                "symbol": "AAPL",
                "trade_allowed": False,
                "blocked_reasons": ["conflicting_flow"],
            }],
        )
        result = check_blueprint(_blueprint(), agent_outputs=ao)
        assert any(i.rule == "multi_agent_soft_trade_block" and i.severity == "error" for i in result.issues)

    def test_flow_confidence_cap_error(self):
        ao = _agent_outputs(
            flow=[{
                "symbol": "AAPL",
                "confidence_cap": 0.3,
                "blocked_reasons": ["single_indicator_only"],
            }],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(confidence=0.7)]),
            agent_outputs=ao,
        )
        assert any(i.rule == "flow_confidence_cap" and i.severity == "error" for i in result.issues)

    def test_flow_high_false_breakout_cap_does_not_apply_to_neutral_condor(self):
        ao = _agent_outputs(
            flow=[{
                "symbol": "AAPL",
                "flow_signal": "neutral",
                "false_breakout_risk": "high",
                "confidence_cap": 0.3,
                "blocked_reasons": ["high_false_breakout_risk"],
            }],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="iron_condor",
                direction="neutral",
                confidence=0.35,
                legs=[
                    _leg(strike=140, option_type="put", side="buy"),
                    _leg(strike=145, option_type="put", side="sell"),
                    _leg(strike=155, option_type="call", side="sell"),
                    _leg(strike=160, option_type="call", side="buy"),
                ],
            )]),
            agent_outputs=ao,
        )
        assert not any(i.rule == "flow_confidence_cap" for i in result.issues)

    def test_flow_high_false_breakout_cap_still_applies_to_directional_plan(self):
        ao = _agent_outputs(
            flow=[{
                "symbol": "AAPL",
                "flow_signal": "neutral",
                "false_breakout_risk": "high",
                "confidence_cap": 0.3,
                "blocked_reasons": ["high_false_breakout_risk"],
            }],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(confidence=0.35, direction="bullish")]),
            agent_outputs=ao,
        )
        assert any(i.rule == "flow_confidence_cap" and i.severity == "error" for i in result.issues)

    def test_chain_single_indicator_trade_allowed_false_is_soft_warning(self):
        ao = _agent_outputs(
            chain=[{
                "symbol": "AAPL",
                "trade_allowed": False,
                "blocked_reasons": ["single_indicator_only"],
            }],
        )
        result = check_blueprint(_blueprint(), agent_outputs=ao)
        assert any(i.rule == "chain_trade_block_soft" and i.severity == "warning" for i in result.issues)
        assert not any(i.rule == "chain_trade_blocked" and i.severity == "error" for i in result.issues)

    def test_single_indicator_only_counts_toward_soft_trade_block_consensus(self):
        ao = _agent_outputs(
            chain=[{
                "symbol": "AAPL",
                "trade_allowed": False,
                "blocked_reasons": ["single_indicator_only"],
            }],
            flow=[{
                "symbol": "AAPL",
                "trade_allowed": False,
                "blocked_reasons": ["conflicting_flow"],
            }],
        )
        result = check_blueprint(_blueprint(), agent_outputs=ao)
        assert any(i.rule == "multi_agent_soft_trade_block" and i.severity == "error" for i in result.issues)

    def test_chain_trade_allowed_false_error(self):
        ao = _agent_outputs(
            chain=[{
                "symbol": "AAPL",
                "trade_allowed": False,
                "blocked_reasons": ["insufficient_leg_liquidity"],
            }],
        )
        result = check_blueprint(_blueprint(), agent_outputs=ao)
        assert any(i.rule == "chain_trade_blocked" and i.severity == "error" for i in result.issues)

    def test_simple_structures_only_execution_candidate_allows_configured_precision_first_strategy(self, monkeypatch):
        from services.analysis_service.app.evaluation.rule_checker import _is_execution_candidate_allowed

        settings = SimpleNamespace(
            analysis_service=SimpleNamespace(
                llm=SimpleNamespace(
                    precision_first=SimpleNamespace(
                        enabled=True,
                        allowed_strategy_types=["single_leg", "vertical_spread", "iron_condor", "calendar_spread"],
                    )
                )
            )
        )
        monkeypatch.setattr(
            "services.analysis_service.app.evaluation.rule_checker.get_settings",
            lambda: settings,
        )

        signal = {
            "option_indicators": {"term_structure_slope": 0.05},
            "cross_asset_indicators": {"earnings_proximity_days": 10},
        }
        agent_outputs = _agent_outputs(
            flow=[{"symbol": "AAPL", "simple_structures_only": True}],
        )

        assert _is_execution_candidate_allowed("iron_condor", signal, agent_outputs, "AAPL") is True

    def test_chain_simple_structures_only_error(self):
        ao = _agent_outputs(
            chain=[{
                "symbol": "AAPL",
                "simple_structures_only": True,
                "blocked_reasons": ["low_liquidity"],
            }],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="butterfly",
                direction="neutral",
                legs=[
                    _leg(strike=145, option_type="call", side="buy"),
                    _leg(strike=150, option_type="call", side="sell"),
                    _leg(strike=155, option_type="call", side="buy"),
                ],
            )]),
            agent_outputs=ao,
        )
        assert any(i.rule == "chain_simple_structures_only" for i in result.issues)

    def test_chain_gamma_pin_exception_allows_butterfly_when_centered_and_liquid(self):
        ao = _agent_outputs(
            chain=[{
                "symbol": "AAPL",
                "simple_structures_only": True,
                "gamma_pin_active": True,
                "pin_strength": 0.82,
                "gamma_pin_strike": 150.0,
                "liquidity_tier": "L1",
            }],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="butterfly",
                direction="neutral",
                legs=[
                    _leg(strike=145, option_type="call", side="buy"),
                    _leg(strike=150, option_type="call", side="sell"),
                    _leg(strike=155, option_type="call", side="buy"),
                ],
            )]),
            agent_outputs=ao,
        )
        assert not any(i.rule == "chain_simple_structures_only" for i in result.issues)
        assert not any(i.rule.startswith("chain_gamma_pin_") for i in result.issues)

    def test_chain_gamma_pin_requires_allowed_structure(self):
        ao = _agent_outputs(
            chain=[{
                "symbol": "AAPL",
                "simple_structures_only": True,
                "gamma_pin_active": True,
                "pin_strength": 0.88,
                "gamma_pin_strike": 150.0,
                "liquidity_tier": "L1",
            }],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="vertical_spread",
                direction="bullish",
                legs=[
                    _leg(strike=145, option_type="call", side="buy"),
                    _leg(strike=150, option_type="call", side="sell"),
                ],
            )]),
            agent_outputs=ao,
        )
        assert any(i.rule == "chain_gamma_pin_structure_required" for i in result.issues)

    def test_chain_gamma_pin_requires_centered_short_strike(self):
        ao = _agent_outputs(
            chain=[{
                "symbol": "AAPL",
                "simple_structures_only": True,
                "gamma_pin_active": True,
                "pin_strength": 0.9,
                "gamma_pin_strike": 150.0,
                "liquidity_tier": "L2",
            }],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="butterfly",
                direction="neutral",
                legs=[
                    _leg(strike=147, option_type="call", side="buy"),
                    _leg(strike=152, option_type="call", side="sell"),
                    _leg(strike=157, option_type="call", side="buy"),
                ],
            )]),
            agent_outputs=ao,
        )
        assert any(i.rule == "chain_gamma_pin_strike_centering" for i in result.issues)

    def test_volatility_trade_allowed_false_error(self):
        ao = _agent_outputs(
            volatility=[{
                "symbol": "AAPL",
                "trade_allowed": False,
                "blocked_reasons": ["earnings_imminent"],
            }],
        )
        result = check_blueprint(_blueprint(), agent_outputs=ao)
        assert any(i.rule == "volatility_trade_blocked" and i.severity == "error" for i in result.issues)

    def test_volatility_simple_structures_only_error(self):
        ao = _agent_outputs(
            volatility=[{
                "symbol": "AAPL",
                "simple_structures_only": True,
                "blocked_reasons": ["low_liquidity"],
            }],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="butterfly",
                direction="neutral",
                legs=[
                    _leg(strike=145, option_type="call", side="buy"),
                    _leg(strike=150, option_type="call", side="sell"),
                    _leg(strike=155, option_type="call", side="buy"),
                ],
            )]),
            agent_outputs=ao,
        )
        assert any(i.rule == "volatility_simple_structures_only" for i in result.issues)

    def test_volatility_single_indicator_caps_confidence(self):
        ao = _agent_outputs(
            volatility=[{
                "symbol": "AAPL",
                "signal_type": "single_indicator",
            }],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(confidence=0.7)]),
            agent_outputs=ao,
        )
        assert any(i.rule == "volatility_single_indicator_confidence_cap" and i.severity == "error" for i in result.issues)

    def test_volatility_single_indicator_no_longer_caps_size(self):
        ao = _agent_outputs(
            volatility=[{
                "symbol": "AAPL",
                "signal_type": "single_indicator",
            }],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(max_position_size=0.5)]),
            agent_outputs=ao,
        )
        assert not any(i.rule == "volatility_single_indicator_size_cap" for i in result.issues)

    def test_spread_confidence_cap_error(self):
        ao = _agent_outputs(
            spread=[{
                "symbol": "AAPL",
                "confidence_cap": 0.4,
                "blocked_reasons": ["effective_rr_unknown"],
            }],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="vertical_spread",
                confidence=0.7,
                legs=[_leg(strike=150, option_type="call", side="buy"), _leg(strike=160, option_type="call", side="sell")],
            )]),
            agent_outputs=ao,
        )
        assert any(i.rule == "spread_confidence_cap" and i.severity == "error" for i in result.issues)

    def test_spread_trade_gate_ignored_for_single_leg(self):
        ao = _agent_outputs(
            spread=[{"symbol": "AAPL", "trade_allowed": False, "blocked_reasons": ["effective_rr_below_one"]}],
        )
        result = check_blueprint(_blueprint(), agent_outputs=ao)
        assert not any(i.rule.startswith("spread_") and i.rule.endswith("trade_blocked") for i in result.issues)


class TestEventRiskConsensus:
    def test_three_agents_no_longer_require_reduced_position_size(self):
        ao = _agent_outputs(
            volatility=[{"symbol": "AAPL", "event_risk_present": True}],
            flow=[{"symbol": "AAPL", "event_risk_present": True}],
            chain=[{"symbol": "AAPL", "event_risk_present": True}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(confidence=0.7, max_position_size=1.0)]),
            agent_outputs=ao,
        )
        assert not any(i.rule == "event_risk_consensus_position_size" for i in result.issues)

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
        assert not any(i.rule.startswith("event_risk_consensus") for i in result.issues)

    def test_cross_asset_event_risk_no_longer_imposes_position_cap(self):
        ao = _agent_outputs(
            volatility=[{"symbol": "AAPL", "event_risk_present": True}],
            flow=[{"symbol": "AAPL", "event_risk_present": True}],
            cross_asset=[{"symbol": "AAPL", "event_risk_present": True, "correlation_regime": "event_driven"}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(confidence=0.7, max_position_size=1.0)]),
            agent_outputs=ao,
        )
        assert not any(i.rule == "event_risk_consensus_position_size" for i in result.issues)

    def test_three_agents_with_reduced_size_pass_event_risk_check(self):
        ao = _agent_outputs(
            volatility=[{"symbol": "AAPL", "event_risk_present": True}],
            flow=[{"symbol": "AAPL", "event_risk_present": True}],
            chain=[{"symbol": "AAPL", "event_risk_present": True}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(confidence=0.4, max_position_size=0.8)]),
            agent_outputs=ao,
        )
        assert not any(i.rule.startswith("event_risk_consensus") for i in result.issues)

    def test_two_agents_plus_event_driven_caps_confidence_for_non_earnings_play(self):
        ao = _agent_outputs(
            volatility=[{"symbol": "AAPL", "event_risk_present": True}],
            flow=[{"symbol": "AAPL", "event_risk_present": True}],
            cross_asset=[{"symbol": "AAPL", "correlation_regime": "event_driven"}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(confidence=0.6, strategy_type="single_leg")]),
            agent_outputs=ao,
        )
        assert any(i.rule == "event_risk_consensus_confidence_cap" and i.severity == "error" for i in result.issues)

    def test_two_agents_plus_event_driven_allows_explicit_earnings_play(self):
        ao = _agent_outputs(
            volatility=[{"symbol": "AAPL", "event_risk_present": True}],
            flow=[{"symbol": "AAPL", "event_risk_present": True}],
            cross_asset=[{"symbol": "AAPL", "correlation_regime": "event_driven"}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                strategy_type="straddle",
                direction="neutral",
                confidence=0.7,
                legs=[
                    _leg(strike=150, option_type="call", side="buy"),
                    _leg(strike=150, option_type="put", side="buy"),
                ],
            )]),
            agent_outputs=ao,
        )
        assert not any(i.rule == "event_risk_consensus_confidence_cap" for i in result.issues)

    def test_cross_asset_event_risk_can_be_the_second_event_flag_in_event_driven_consensus(self):
        ao = _agent_outputs(
            flow=[{"symbol": "AAPL", "event_risk_present": True}],
            cross_asset=[{"symbol": "AAPL", "event_risk_present": True, "correlation_regime": "event_driven"}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(confidence=0.6, strategy_type="single_leg")]),
            agent_outputs=ao,
        )
        assert any(i.rule == "event_risk_consensus_confidence_cap" and i.severity == "error" for i in result.issues)

    def test_market_shock_escalation_caps_directional_plan_when_not_aligned(self):
        ao = _agent_outputs(
            cross_asset=[{
                "symbol": "AAPL",
                "market_shock_return_1d": -0.05,
                "market_shock_source": "macro_gap_down",
            }],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                direction="bullish",
                confidence=0.7,
                max_position_size=0.8,
                reasoning="Bullish continuation despite recent macro stress.",
            )]),
            agent_outputs=ao,
        )
        assert any(i.rule == "event_risk_market_shock_confidence_cap" and i.severity == "error" for i in result.issues)
        assert not any(i.rule == "event_risk_market_shock_position_size_cap" for i in result.issues)

    def test_market_shock_directional_alignment_allows_exemption_with_reasoning(self):
        ao = _agent_outputs(
            cross_asset=[{
                "symbol": "AAPL",
                "market_shock_return_1d": -0.05,
                "market_shock_source": "macro_gap_down",
            }],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(
                direction="bearish",
                confidence=0.7,
                max_position_size=0.8,
                reasoning="Macro_gap_down shock supports bearish protection; keep strict stop-loss and defined max loss.",
            )]),
            agent_outputs=ao,
        )
        assert not any(i.rule == "event_risk_market_shock_confidence_cap" for i in result.issues)
        assert not any(i.rule == "event_risk_market_shock_position_size_cap" for i in result.issues)
        assert not any(i.rule == "event_risk_market_shock_exemption_reasoning" for i in result.issues)


class TestConfirmingIndicators:
    def test_low_indicators_above_half_confidence_error(self):
        ao = _agent_outputs(
            flow=[{"symbol": "AAPL", "confirming_indicators_count": 1}],
            chain=[{"symbol": "AAPL", "confirming_indicators_count": 0}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(confidence=0.55, direction="bullish")]),
            agent_outputs=ao,
        )
        assert any(i.rule == "low_confirming_indicators" and i.severity == "error" for i in result.issues)

    def test_low_indicators_at_half_confidence_ok(self):
        ao = _agent_outputs(
            flow=[{"symbol": "AAPL", "confirming_indicators_count": 1}],
            chain=[{"symbol": "AAPL", "confirming_indicators_count": 0}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(confidence=0.5, direction="bullish")]),
            agent_outputs=ao,
        )
        assert not any(i.rule == "low_confirming_indicators" for i in result.issues)

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


class TestBaselineBehavior:
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

    def test_low_quality_no_longer_caps_position_size(self):
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
        assert not any(i.rule == "cross_asset_low_quality_position_size" for i in result.issues)


class TestCrossAssetAgentGuards:
    def test_cross_asset_agent_confidence_caps_plan_confidence(self):
        ao = _agent_outputs(
            cross_asset=[{"symbol": "AAPL", "confidence": 0.35}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(confidence=0.55)]),
            agent_outputs=ao,
        )
        assert any(i.rule == "cross_asset_agent_confidence_cap" and i.severity == "error" for i in result.issues)

    def test_cross_asset_effective_size_modifier_no_longer_caps_position_size(self):
        ao = _agent_outputs(
            cross_asset=[{"symbol": "AAPL", "effective_size_modifier": 0.4, "master_override": False}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(max_position_size=0.6)]),
            agent_outputs=ao,
        )
        assert not any(i.rule == "cross_asset_effective_size_modifier_cap" for i in result.issues)

    def test_cross_asset_regime_transition_blocks_aggressive_directional_plan(self):
        ao = _agent_outputs(
            cross_asset=[{"symbol": "AAPL", "regime_transition": True, "regime_days": 1}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(direction="bullish", confidence=0.7, max_position_size=0.8)]),
            agent_outputs=ao,
        )
        assert any(
            i.rule == "cross_asset_regime_transition_directional_aggression" and i.severity == "error"
            for i in result.issues
        )

    def test_cross_asset_regime_transition_without_regime_days_still_blocks_aggressive_directional_plan(self):
        ao = _agent_outputs(
            cross_asset=[{"symbol": "AAPL", "regime_transition": True, "regime_days": None}],
        )
        result = check_blueprint(
            _blueprint(symbol_plans=[_plan(direction="bullish", confidence=0.7, max_position_size=0.8)]),
            agent_outputs=ao,
        )
        assert any(
            i.rule == "cross_asset_regime_transition_directional_aggression" and i.severity == "error"
            for i in result.issues
        )


def test_portfolio_limit_fields_do_not_trigger_deterministic_failure():
    result = check_blueprint(
        _blueprint(
            portfolio_delta_limit=0.95,
            portfolio_gamma_limit=0.3,
            max_daily_loss=10_000.0,
        )
    )

    assert result.passed
    assert not any(
        issue.rule in {
            "portfolio_delta_limit",
            "portfolio_delta_limit_elevated",
            "portfolio_gamma_limit",
            "max_daily_loss",
            "computed_portfolio_delta_limit",
        }
        for issue in result.issues
    )
