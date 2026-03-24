"""Test deterministic rule checker for blueprint validation."""
from __future__ import annotations

import pytest

from services.analysis_service.app.evaluation.rule_checker import (
    CheckResult,
    RuleIssue,
    check_blueprint,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _leg(**overrides) -> dict:
    d = {"expiry": "2026-04-17", "strike": 150.0, "option_type": "call", "side": "buy", "quantity": 1}
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
        result = check_blueprint(_blueprint(max_daily_loss=3000.0))
        assert any(i.rule == "max_daily_loss" for i in result.issues)


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
        bad_leg = {"expiry": "2026-04-17", "strike": 150.0}  # missing option_type, side
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
                "option_chain": {"bid_ask_spread_ratio": 0.25},
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
