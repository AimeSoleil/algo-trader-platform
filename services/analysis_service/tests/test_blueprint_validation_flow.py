from __future__ import annotations

from datetime import date, timedelta
from types import SimpleNamespace

from shared.models.blueprint import Direction, LLMTradingBlueprint, OptionLeg, StrategyType, SymbolPlan
import services.analysis_service.app.tasks.blueprint as blueprint_task
from services.analysis_service.app.tasks.blueprint import (
    _apply_deterministic_validation,
    _is_blueprint_soft_blocked,
)


def _make_plan(symbol: str) -> SymbolPlan:
    return SymbolPlan(
        underlying=symbol,
        strategy_type="single_leg",
        direction="bullish",
        legs=[OptionLeg(expiry="2026-05-15", strike=100, option_type="call", side="buy")],
        stop_loss_amount=200.0,
        max_loss_per_trade=500.0,
        exit_conditions=[{"field": "pnl_percent", "operator": ">=", "value": 0.3}],
        reasoning="Trend, flow, and liquidity all align for a defined-risk breakout setup.",
        confidence=0.6,
    )


def _make_blueprint(symbols: list[str]) -> LLMTradingBlueprint:
    return LLMTradingBlueprint(
        trading_date="2026-04-30",
        generated_at="2026-04-29T20:00:00",
        market_regime="neutral",
        symbol_plans=[_make_plan(symbol) for symbol in symbols],
        portfolio_delta_limit=0.8,
    )


def _signal_map() -> dict[str, dict[str, object]]:
    return {
        "AAPL": {
            "close_price": 100.0,
            "stock_indicators": {},
            "option_indicators": {"current_iv": 0.3},
            "cross_asset_indicators": {"earnings_proximity_days": 1},
        },
        "MSFT": {
            "close_price": 100.0,
            "stock_indicators": {},
            "option_indicators": {"current_iv": 0.3},
            "cross_asset_indicators": {"earnings_proximity_days": 10},
        },
    }


def test_apply_deterministic_validation_prunes_offending_symbols_only():
    blueprint, summary = _apply_deterministic_validation(
        _make_blueprint(["AAPL", "MSFT"]),
        _signal_map(),
        agent_outputs=None,
    )

    assert [plan.underlying for plan in blueprint.symbol_plans] == ["MSFT"]
    assert summary["pruned_symbols"] == ["AAPL"]
    assert summary["pruned_plan_count"] == 1
    assert summary["error_count"] == 0
    assert summary["passed"] is True


def test_empty_blueprint_after_pruning_is_soft_blocked():
    blueprint, summary = _apply_deterministic_validation(
        _make_blueprint(["AAPL"]),
        _signal_map(),
        agent_outputs=None,
    )

    blueprint = blueprint.model_copy(update={"reasoning_context": {"deterministic_validation": summary}})
    assert blueprint.symbol_plans == []
    assert summary["empty_after_pruning"] is True
    assert summary["passed"] is False
    assert _is_blueprint_soft_blocked(blueprint) is True


def test_precision_first_strategy_scope_prunes_complex_strategies(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                precision_first=SimpleNamespace(
                    enabled=True,
                    allowed_strategy_types=["single_leg", "vertical_spread"],
                )
            )
        )
    )
    monkeypatch.setattr(blueprint_task, "get_settings", lambda: settings)

    complex_plan = _make_plan("MSFT").model_copy(update={
        "strategy_type": "iron_condor",
        "direction": "neutral",
        "legs": [
            OptionLeg(expiry="2026-05-15", strike=90, option_type="put", side="buy"),
            OptionLeg(expiry="2026-05-15", strike=95, option_type="put", side="sell"),
            OptionLeg(expiry="2026-05-15", strike=105, option_type="call", side="sell"),
            OptionLeg(expiry="2026-05-15", strike=110, option_type="call", side="buy"),
        ],
    })
    blueprint = _make_blueprint(["AAPL"]).model_copy(update={"symbol_plans": [complex_plan]})

    blueprint, summary = _apply_deterministic_validation(
        blueprint,
        _signal_map(),
        agent_outputs=None,
    )

    assert blueprint.symbol_plans == []
    assert summary["precision_first_enabled"] is True
    assert summary["allowed_strategy_types"] == ["single_leg", "vertical_spread"]
    assert summary["strategy_scope_pruned_symbols"] == ["MSFT"]
    assert summary["strategy_scope_pruned_plan_count"] == 1


def test_precision_first_keeps_calendar_when_allowed_and_context_is_clean(monkeypatch):
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
    monkeypatch.setattr(blueprint_task, "get_settings", lambda: settings)

    front_expiry = (date.today() + timedelta(days=21)).isoformat()
    back_expiry = (date.today() + timedelta(days=56)).isoformat()

    calendar_plan = _make_plan("MSFT").model_copy(update={
        "strategy_type": StrategyType.CALENDAR_SPREAD,
        "direction": Direction.NEUTRAL,
        "legs": [
            OptionLeg(expiry=front_expiry, strike=100, option_type="call", side="sell"),
            OptionLeg(expiry=back_expiry, strike=100, option_type="call", side="buy"),
        ],
    })
    blueprint = _make_blueprint(["AAPL"]).model_copy(update={"symbol_plans": [calendar_plan]})
    signal_map = {
        "MSFT": {
            "close_price": 100.0,
            "stock_indicators": {},
            "option_indicators": {"current_iv": 0.3, "term_structure_slope": 0.02},
            "cross_asset_indicators": {"earnings_proximity_days": 8},
        }
    }

    blueprint, summary = _apply_deterministic_validation(
        blueprint,
        signal_map,
        agent_outputs=None,
    )

    assert [plan.underlying for plan in blueprint.symbol_plans] == ["MSFT"]
    assert summary["allowed_strategy_types"] == ["single_leg", "vertical_spread", "iron_condor", "calendar_spread"]
    assert summary["error_count"] == 0
    assert summary["passed"] is True