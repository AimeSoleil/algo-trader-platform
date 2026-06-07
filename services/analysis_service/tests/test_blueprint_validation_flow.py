from __future__ import annotations

from datetime import date, timedelta
from types import SimpleNamespace

from shared.models.blueprint import Direction, LLMTradingBlueprint, OptionLeg, StrategyType, SymbolPlan
import services.analysis_service.app.tasks.blueprint as blueprint_task
from services.analysis_service.app.tasks.blueprint import (
    _apply_deterministic_validation,
    _apply_and_log_deterministic_validation,
    _format_pre_synthesis_summary_text,
    _is_blueprint_soft_blocked,
    _resolve_validation_agent_outputs,
    _summarize_pre_synthesis_outcome,
)


def _make_plan(symbol: str) -> SymbolPlan:
    future_expiry = (date.today() + timedelta(days=21)).isoformat()
    return SymbolPlan(
        underlying=symbol,
        strategy_type="single_leg",
        direction="bullish",
        legs=[OptionLeg(expiry=future_expiry, strike=100, option_type="call", side="buy")],
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


def test_resolve_validation_agent_outputs_aggregates_chunk_contexts():
    resolved = _resolve_validation_agent_outputs({
        "chunk_contexts": [
            {
                "agent_outputs": {
                    "trend": {
                        "symbols": [
                            {"symbol": "AAPL", "trade_allowed": False},
                        ]
                    }
                }
            },
            {
                "agent_outputs": {
                    "trend": {
                        "symbols": [
                            {"symbol": "MSFT", "trade_allowed": True},
                        ]
                    },
                    "flow": {
                        "symbols": [
                            {"symbol": "MSFT", "confidence_cap": 0.4},
                        ]
                    },
                }
            },
        ]
    })

    assert resolved == {
        "trend": {
            "symbols": [
                {"symbol": "AAPL", "trade_allowed": False},
                {"symbol": "MSFT", "trade_allowed": True},
            ]
        },
        "flow": {
            "symbols": [
                {"symbol": "MSFT", "confidence_cap": 0.4},
            ]
        },
    }


def test_apply_and_log_deterministic_validation_uses_chunk_agent_outputs(monkeypatch):
    captured: dict[str, object] = {}

    class _FakeSignal:
        def __init__(self, symbol: str):
            self.symbol = symbol

        def model_dump(self, mode: str = "json") -> dict[str, object]:
            return {"symbol": self.symbol}

    def _fake_apply(blueprint, signal_map, agent_outputs):
        captured["signal_map"] = signal_map
        captured["agent_outputs"] = agent_outputs
        return blueprint, {
            "passed": True,
            "error_count": 0,
            "warning_count": 0,
            "issues": [],
            "initial_error_count": 0,
            "initial_warning_count": 0,
            "precision_first_enabled": False,
            "allowed_strategy_types": [],
            "emitted_strategy_scope_pruned_symbols": [],
            "emitted_strategy_scope_pruned_plan_count": 0,
            "pruned_symbols": [],
            "pruned_plan_count": 0,
            "pruned_symbol_errors": [],
            "empty_after_pruning": False,
        }

    monkeypatch.setattr(blueprint_task, "_apply_deterministic_validation", _fake_apply)

    blueprint = _make_blueprint(["MSFT"]).model_copy(update={
        "reasoning_context": {
            "chunk_contexts": [
                {
                    "agent_outputs": {
                        "trend": {
                            "symbols": [
                                {"symbol": "MSFT", "trade_allowed": False},
                            ]
                        }
                    }
                }
            ]
        }
    })

    validated = _apply_and_log_deterministic_validation(
        blueprint,
        [_FakeSignal("MSFT")],
        td=date(2026, 4, 30),
    )

    assert captured["signal_map"] == {"MSFT": {"symbol": "MSFT"}}
    assert captured["agent_outputs"] == {
        "trend": {
            "symbols": [
                {"symbol": "MSFT", "trade_allowed": False},
            ]
        }
    }
    assert validated.reasoning_context["deterministic_validation"]["passed"] is True


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


def test_emitted_strategy_scope_guard_prunes_complex_strategies(monkeypatch):
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
    assert summary["emitted_strategy_scope_pruned_symbols"] == ["MSFT"]
    assert summary["emitted_strategy_scope_pruned_plan_count"] == 1


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


def test_pre_synthesis_summary_includes_analysis_priority_order():
    blueprint = _make_blueprint(["MSFT"]).model_copy(update={
        "reasoning_context": {
            "pre_synthesis_filter": {
                "dropped_symbol_count": 2,
            },
            "pre_synthesis_triage": {
                "analysis_symbol_count": 6,
                "analysis_order": ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META"],
                "ranked_symbols": [
                    {
                        "symbol": "AAPL",
                        "rank": 1,
                        "action": "analyze",
                        "coarse_score": 0.9123,
                        "decision_reason": "priority rank 1; strongest data_quality=1.00, option_coverage=1.00; weakest earnings_buffer=0.75",
                    },
                    {
                        "symbol": "MSFT",
                        "rank": 2,
                        "action": "analyze",
                        "coarse_score": 0.8423,
                        "decision_reason": "priority rank 2; strongest data_quality=0.95, option_coverage=0.90; weakest earnings_buffer=0.75",
                    },
                    {
                        "symbol": "NVDA",
                        "rank": 3,
                        "action": "analyze",
                        "coarse_score": 0.7923,
                        "decision_reason": "priority rank 3; strongest data_quality=0.90, option_coverage=0.80; weakest earnings_buffer=0.75",
                    },
                    {
                        "symbol": "TSLA",
                        "rank": 4,
                        "action": "analyze",
                        "coarse_score": 0.7423,
                        "decision_reason": "priority rank 4; strongest data_quality=0.85, option_coverage=0.70; weakest earnings_buffer=0.75",
                    },
                    {
                        "symbol": "AMZN",
                        "rank": 5,
                        "action": "analyze",
                        "coarse_score": 0.7023,
                        "decision_reason": "priority rank 5; strongest data_quality=0.80, option_coverage=0.60; weakest earnings_buffer=0.75",
                    },
                    {
                        "symbol": "META",
                        "rank": 6,
                        "action": "analyze",
                        "coarse_score": 0.4123,
                        "decision_reason": "priority rank 6; strongest liquidity=0.30, earnings_buffer=0.35; weakest option_coverage=0.20",
                    },
                ],
            },
        }
    })

    summary = _summarize_pre_synthesis_outcome(blueprint)

    assert summary["dropped_symbol_count"] == 2
    assert summary["analysis_symbol_count"] == 6
    assert summary["analysis_order"] == ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META"]
    assert summary["top_ranked_symbols"] == [
        {
            "symbol": "AAPL",
            "rank": 1,
            "coarse_score": 0.9123,
            "reason": "priority rank 1; strongest data_quality=1.00, option_coverage=1.00; weakest earnings_buffer=0.75",
        },
        {
            "symbol": "MSFT",
            "rank": 2,
            "coarse_score": 0.8423,
            "reason": "priority rank 2; strongest data_quality=0.95, option_coverage=0.90; weakest earnings_buffer=0.75",
        },
        {
            "symbol": "NVDA",
            "rank": 3,
            "coarse_score": 0.7923,
            "reason": "priority rank 3; strongest data_quality=0.90, option_coverage=0.80; weakest earnings_buffer=0.75",
        },
        {
            "symbol": "TSLA",
            "rank": 4,
            "coarse_score": 0.7423,
            "reason": "priority rank 4; strongest data_quality=0.85, option_coverage=0.70; weakest earnings_buffer=0.75",
        },
        {
            "symbol": "AMZN",
            "rank": 5,
            "coarse_score": 0.7023,
            "reason": "priority rank 5; strongest data_quality=0.80, option_coverage=0.60; weakest earnings_buffer=0.75",
        },
    ]


def test_pre_synthesis_summary_text_lists_priority_preview():
    text = _format_pre_synthesis_summary_text({
        "analysis_order": ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META"],
    })

    assert text == (
        "Pre-synthesis analysis priority: "
        "AAPL, MSFT, NVDA, TSLA, AMZN, +1 more."
    )