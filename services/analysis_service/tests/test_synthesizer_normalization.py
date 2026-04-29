from __future__ import annotations

from shared.models.blueprint import LLMTradingBlueprint

from services.analysis_service.app.llm.agents.synthesizer_agent import _normalize_blueprint_payload


def test_normalize_blueprint_payload_records_dropped_condition_samples():
    payload = {
        "symbol_plans": [
            {
                "underlying": "AAPL",
                "strategy_type": "single_leg",
                "direction": "bullish",
                "legs": [
                    {
                        "expiry": "2026-05-08",
                        "strike": 185,
                        "option_type": "call",
                        "side": "buy",
                        "quantity": 1,
                    },
                ],
                "entry_conditions": [
                    {
                        "field": "unsupported_metric",
                        "operator": ">",
                        "value": 1,
                        "description": "drop me",
                    },
                    {
                        "field": "vwap",
                        "operator": ">",
                        "value": 185.5,
                        "description": "keep me",
                    },
                ],
                "adjustment_rules": [
                    {
                        "trigger": {
                            "field": "bad_trigger",
                            "operator": "<=",
                            "value": 170,
                            "description": "drop me too",
                        },
                        "action": "close_all",
                    },
                ],
            },
        ],
    }

    normalized, stats = _normalize_blueprint_payload(payload, signal_date=None)

    assert len(normalized["symbol_plans"][0]["entry_conditions"]) == 1
    assert stats["entry_conditions_dropped"] == 1
    assert stats["entry_conditions_dropped_samples"] == [
        {
            "field": "unsupported_metric",
            "operator": ">",
            "value": 1,
            "description": "drop me",
        },
    ]
    assert stats["adjustment_rules_dropped"] == 1
    assert stats["adjustment_rules_dropped_samples"] == [
        {
            "trigger": {
                "field": "bad_trigger",
                "operator": "<=",
                "value": 170,
                "description": "drop me too",
            },
            "action": "close_all",
        },
    ]


def test_normalize_blueprint_payload_normalizes_leg_side_aliases():
    payload = {
        "trading_date": "2026-04-29",
        "generated_at": "2026-04-28T03:39:57",
        "model_provider": "closeai",
        "model_version": "claude-sonnet-4-20250514",
        "market_regime": "trending_calm",
        "symbol_plans": [
            {
                "underlying": "SPY",
                "strategy_type": "vertical_spread",
                "direction": "bullish",
                "legs": [
                    {
                        "expiry": "2026-06-05",
                        "strike": 720,
                        "option_type": "call",
                        "side": "long",
                        "quantity": 1,
                    },
                    {
                        "expiry": "2026-06-05",
                        "strike": 730,
                        "option_type": "call",
                        "side": "short",
                        "quantity": 1,
                    },
                ],
                "max_loss_per_trade": 500,
                "confidence": 0.7,
            },
        ],
    }

    normalized, stats = _normalize_blueprint_payload(payload, signal_date=None)

    assert normalized["symbol_plans"][0]["legs"][0]["side"] == "buy"
    assert normalized["symbol_plans"][0]["legs"][1]["side"] == "sell"
    assert stats["legs_side_normalized"] == 2

    blueprint = LLMTradingBlueprint.model_validate(normalized)

    assert blueprint.symbol_plans[0].legs[0].side == "buy"
    assert blueprint.symbol_plans[0].legs[1].side == "sell"


def test_normalize_blueprint_payload_trims_symbol_plans_to_max_total_positions():
    payload = {
        "trading_date": "2026-04-29",
        "generated_at": "2026-04-28T03:39:57",
        "model_provider": "closeai",
        "model_version": "claude-sonnet-4-20250514",
        "market_regime": "trending_calm",
        "max_total_positions": 2,
        "symbol_plans": [
            {
                "underlying": "SPY",
                "strategy_type": "single_leg",
                "direction": "bullish",
                "legs": [{"expiry": "2026-06-05", "strike": 720, "option_type": "call", "side": "buy", "quantity": 1}],
                "max_loss_per_trade": 500,
                "confidence": 0.7,
            },
            {
                "underlying": "QQQ",
                "strategy_type": "single_leg",
                "direction": "bullish",
                "legs": [{"expiry": "2026-06-05", "strike": 670, "option_type": "call", "side": "buy", "quantity": 1}],
                "max_loss_per_trade": 500,
                "confidence": 0.7,
            },
            {
                "underlying": "IWM",
                "strategy_type": "single_leg",
                "direction": "bullish",
                "legs": [{"expiry": "2026-06-05", "strike": 280, "option_type": "call", "side": "buy", "quantity": 1}],
                "max_loss_per_trade": 500,
                "confidence": 0.7,
            },
        ],
    }

    normalized, stats = _normalize_blueprint_payload(payload, signal_date=None)

    assert normalized["max_total_positions"] == 2
    assert len(normalized["symbol_plans"]) == 2
    assert [p["underlying"] for p in normalized["symbol_plans"]] == ["SPY", "QQQ"]
    assert stats["symbol_plans_trimmed"] == 1

    blueprint = LLMTradingBlueprint.model_validate(normalized)

    assert blueprint.max_total_positions == 2
    assert len(blueprint.symbol_plans) == 2


def test_normalize_blueprint_payload_clamps_global_risk_limits_to_policy_caps():
    payload = {
        "trading_date": "2026-04-29",
        "generated_at": "2026-04-28T03:39:57",
        "model_provider": "closeai",
        "model_version": "claude-sonnet-4-20250514",
        "market_regime": "trending_calm",
        "max_daily_loss": 5000,
        "max_margin_usage": 0.9,
        "portfolio_delta_limit": 0.9,
        "portfolio_gamma_limit": 0.3,
        "symbol_plans": [
            {
                "underlying": "SPY",
                "strategy_type": "single_leg",
                "direction": "bullish",
                "legs": [{"expiry": "2026-06-05", "strike": 720, "option_type": "call", "side": "buy", "quantity": 1}],
                "max_loss_per_trade": 500,
                "confidence": 0.7,
            },
        ],
    }

    normalized, stats = _normalize_blueprint_payload(payload, signal_date=None)

    assert normalized["max_daily_loss"] == 2000.0
    assert normalized["max_margin_usage"] == 0.5
    assert normalized["portfolio_delta_limit"] == 0.5
    assert normalized["portfolio_gamma_limit"] == 0.1
    assert stats["max_daily_loss_clamped"] == 1
    assert stats["max_margin_usage_clamped"] == 1
    assert stats["portfolio_delta_limit_clamped"] == 1
    assert stats["portfolio_gamma_limit_clamped"] == 1