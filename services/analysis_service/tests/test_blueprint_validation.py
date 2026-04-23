"""Test Pydantic blueprint model validation rules.

Covers strict field constraints, strategy-legs consistency,
and enum enforcement added during the P1 harness hardening.
"""
from __future__ import annotations

from datetime import date, datetime

import pytest
from pydantic import ValidationError

from shared.models.blueprint import (
    AdjustmentAction,
    ConditionField,
    Direction,
    LLMTradingBlueprint,
    OptionLeg,
    StrategyType,
    SymbolPlan,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_leg(**overrides) -> dict:
    defaults = {
        "expiry": "2026-04-17",
        "strike": 150.0,
        "option_type": "call",
        "side": "buy",
        "quantity": 1,
    }
    defaults.update(overrides)
    return defaults


def _make_plan(**overrides) -> dict:
    defaults = {
        "underlying": "AAPL",
        "strategy_type": "single_leg",
        "direction": "bullish",
        "legs": [_make_leg()],
        "max_loss_per_trade": 500.0,
        "confidence": 0.7,
    }
    defaults.update(overrides)
    return defaults


def _make_blueprint(**overrides) -> dict:
    defaults = {
        "trading_date": "2026-03-24",
        "generated_at": "2026-03-23T20:00:00",
        "market_regime": "neutral",
        "symbol_plans": [_make_plan()],
    }
    defaults.update(overrides)
    return defaults


# ---------------------------------------------------------------------------
# OptionLeg enum strictness
# ---------------------------------------------------------------------------

class TestOptionLeg:
    def test_valid_call_buy(self):
        leg = OptionLeg(**_make_leg(option_type="call", side="buy"))
        assert leg.option_type == "call"
        assert leg.side == "buy"
        assert leg.is_long is True

    def test_valid_put_sell(self):
        leg = OptionLeg(**_make_leg(option_type="put", side="sell"))
        assert leg.option_type == "put"
        assert leg.side == "sell"
        assert leg.is_short is True

    def test_invalid_option_type(self):
        with pytest.raises(ValidationError, match="option_type"):
            OptionLeg(**_make_leg(option_type="Call"))  # must be lowercase

    def test_invalid_side(self):
        with pytest.raises(ValidationError, match="side"):
            OptionLeg(**_make_leg(side="long"))  # must be buy/sell


# ---------------------------------------------------------------------------
# SymbolPlan validation
# ---------------------------------------------------------------------------

class TestSymbolPlan:
    def test_valid_plan(self):
        plan = SymbolPlan(**_make_plan())
        assert plan.underlying == "AAPL"
        assert plan.confidence == 0.7

    def test_confidence_out_of_range_high(self):
        with pytest.raises(ValidationError, match="confidence"):
            SymbolPlan(**_make_plan(confidence=1.5))

    def test_confidence_out_of_range_low(self):
        with pytest.raises(ValidationError, match="confidence"):
            SymbolPlan(**_make_plan(confidence=-0.1))

    def test_max_loss_per_trade_must_be_positive(self):
        with pytest.raises(ValidationError, match="max_loss_per_trade"):
            SymbolPlan(**_make_plan(max_loss_per_trade=0))

    def test_max_loss_per_trade_negative(self):
        with pytest.raises(ValidationError, match="max_loss_per_trade"):
            SymbolPlan(**_make_plan(max_loss_per_trade=-100))

    def test_strategy_type_enum_enforced(self):
        plan = SymbolPlan(**_make_plan(strategy_type="iron_condor", legs=[
            _make_leg(strike=140, option_type="put", side="buy"),
            _make_leg(strike=145, option_type="put", side="sell"),
            _make_leg(strike=155, option_type="call", side="sell"),
            _make_leg(strike=160, option_type="call", side="buy"),
        ]))
        assert plan.strategy_type == StrategyType.IRON_CONDOR

    def test_invalid_strategy_type(self):
        with pytest.raises(ValidationError):
            SymbolPlan(**_make_plan(strategy_type="magic_spread"))

    def test_direction_enum_enforced(self):
        plan = SymbolPlan(**_make_plan(direction="bearish"))
        assert plan.direction == Direction.BEARISH

    def test_invalid_direction(self):
        with pytest.raises(ValidationError):
            SymbolPlan(**_make_plan(direction="sideways"))

    def test_vwap_entry_condition_is_allowed(self):
        plan = SymbolPlan(**_make_plan(entry_conditions=[
            {
                "field": "vwap",
                "operator": ">",
                "value": 185.5,
                "description": "Price above VWAP confirms bullish bias",
            },
        ]))

        assert plan.entry_conditions[0].field.value == "vwap"

    def test_invalid_entry_condition_field_is_ignored(self):
        plan = SymbolPlan(**_make_plan(entry_conditions=[
            {
                "field": "unsupported_metric",
                "operator": ">",
                "value": 1,
                "description": "Bad field should be dropped",
            },
            {
                "field": "vwap",
                "operator": ">",
                "value": 185.5,
                "description": "Valid field should remain",
            },
        ]))

        assert len(plan.entry_conditions) == 1
        assert plan.entry_conditions[0].field == ConditionField.VWAP

    def test_invalid_adjustment_rule_trigger_is_ignored(self):
        plan = SymbolPlan(**_make_plan(adjustment_rules=[
            {
                "trigger": {
                    "field": "unsupported_metric",
                    "operator": ">",
                    "value": 1,
                    "description": "Bad trigger should be dropped",
                },
                "action": "close_all",
                "description": "drop me",
            },
            {
                "trigger": {
                    "field": "underlying_price",
                    "operator": "<=",
                    "value": 170,
                    "description": "Valid trigger should remain",
                },
                "action": "close_all",
                "description": "keep me",
            },
        ]))

        assert len(plan.adjustment_rules) == 1
        assert plan.adjustment_rules[0].action == AdjustmentAction.CLOSE_ALL
        assert plan.adjustment_rules[0].trigger.field == ConditionField.UNDERLYING_PRICE


# ---------------------------------------------------------------------------
# Strategy ↔ legs count validation
# ---------------------------------------------------------------------------

class TestStrategyLegsConsistency:
    def test_single_leg_with_1_leg(self):
        plan = SymbolPlan(**_make_plan(strategy_type="single_leg", legs=[_make_leg()]))
        assert len(plan.legs) == 1

    def test_single_leg_with_2_legs_fails(self):
        with pytest.raises(ValidationError, match="expects 1-1 legs"):
            SymbolPlan(**_make_plan(
                strategy_type="single_leg",
                legs=[_make_leg(), _make_leg(strike=160)],
            ))

    def test_vertical_spread_with_2_legs(self):
        plan = SymbolPlan(**_make_plan(
            strategy_type="vertical_spread",
            legs=[_make_leg(strike=150, side="buy"), _make_leg(strike=155, side="sell")],
        ))
        assert len(plan.legs) == 2

    def test_vertical_spread_with_1_leg_fails(self):
        with pytest.raises(ValidationError, match="expects 2-2 legs"):
            SymbolPlan(**_make_plan(strategy_type="vertical_spread", legs=[_make_leg()]))

    def test_iron_condor_with_4_legs(self):
        plan = SymbolPlan(**_make_plan(
            strategy_type="iron_condor",
            direction="neutral",
            legs=[
                _make_leg(strike=140, option_type="put", side="buy"),
                _make_leg(strike=145, option_type="put", side="sell"),
                _make_leg(strike=155, option_type="call", side="sell"),
                _make_leg(strike=160, option_type="call", side="buy"),
            ],
        ))
        assert len(plan.legs) == 4

    def test_iron_condor_with_3_legs_fails(self):
        with pytest.raises(ValidationError, match="expects 4-4 legs"):
            SymbolPlan(**_make_plan(
                strategy_type="iron_condor",
                legs=[_make_leg(), _make_leg(strike=155), _make_leg(strike=160)],
            ))

    def test_straddle_with_2_legs(self):
        plan = SymbolPlan(**_make_plan(
            strategy_type="straddle",
            direction="neutral",
            legs=[
                _make_leg(strike=150, option_type="call", side="buy"),
                _make_leg(strike=150, option_type="put", side="buy"),
            ],
        ))
        assert len(plan.legs) == 2

    def test_butterfly_accepts_3_or_4_legs(self):
        plan3 = SymbolPlan(**_make_plan(
            strategy_type="butterfly",
            direction="neutral",
            legs=[
                _make_leg(strike=145, side="buy"),
                _make_leg(strike=150, side="sell"),
                _make_leg(strike=155, side="buy"),
            ],
        ))
        assert len(plan3.legs) == 3

        plan4 = SymbolPlan(**_make_plan(
            strategy_type="butterfly",
            direction="neutral",
            legs=[
                _make_leg(strike=145, side="buy"),
                _make_leg(strike=150, side="sell"),
                _make_leg(strike=150, side="sell"),
                _make_leg(strike=155, side="buy"),
            ],
        ))
        assert len(plan4.legs) == 4


# ---------------------------------------------------------------------------
# Blueprint top-level validation
# ---------------------------------------------------------------------------

class TestLLMTradingBlueprint:
    def test_valid_blueprint(self):
        bp = LLMTradingBlueprint(**_make_blueprint())
        assert len(bp.symbol_plans) == 1
        assert bp.market_regime == "neutral"

    def test_missing_symbols_default_empty(self):
        bp = LLMTradingBlueprint(**_make_blueprint())
        assert bp.missing_symbols == []

    def test_missing_symbols_populated(self):
        bp = LLMTradingBlueprint(**_make_blueprint(missing_symbols=["TSLA", "NVDA"]))
        assert bp.missing_symbols == ["TSLA", "NVDA"]

    def test_min_data_quality_score_bounds(self):
        with pytest.raises(ValidationError, match="min_data_quality_score"):
            LLMTradingBlueprint(**_make_blueprint(min_data_quality_score=1.5))

    def test_data_quality_score_plan_bounds(self):
        plan = _make_plan()
        plan["data_quality_score"] = -0.1
        with pytest.raises(ValidationError, match="data_quality_score"):
            LLMTradingBlueprint(**_make_blueprint(symbol_plans=[plan]))
