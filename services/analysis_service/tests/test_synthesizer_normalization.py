from __future__ import annotations

from types import SimpleNamespace

from shared.models.blueprint import LLMTradingBlueprint

from services.analysis_service.app.llm.agents.synthesizer_agent import (
    SynthesizerAgent,
    _normalize_blueprint_payload,
    _SYNTHESIZER_SYSTEM_PROMPT,
)


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


def test_blueprint_option_leg_price_tolerance_accepts_percent_strings():
    blueprint = LLMTradingBlueprint.model_validate(
        {
            "trading_date": "2026-04-29",
            "generated_at": "2026-04-28T03:39:57",
            "market_regime": "trending_calm",
            "symbol_plans": [
                {
                    "underlying": "SPY",
                    "strategy_type": "single_leg",
                    "direction": "bullish",
                    "legs": [
                        {
                            "expiry": "2026-06-05",
                            "strike": 720,
                            "option_type": "call",
                            "side": "buy",
                            "quantity": 1,
                            "price_tolerance": "1.5%",
                        },
                    ],
                    "max_loss_per_trade": 500,
                    "confidence": 0.7,
                },
            ],
        }
    )

    assert blueprint.symbol_plans[0].legs[0].price_tolerance == 0.015


def test_normalize_blueprint_payload_uses_global_cap_not_current_plan_count():
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

    assert normalized["max_total_positions"] == 10
    assert len(normalized["symbol_plans"]) == 3
    assert [p["underlying"] for p in normalized["symbol_plans"]] == ["SPY", "QQQ", "IWM"]
    assert stats["max_total_positions_normalized"] == 1

    blueprint = LLMTradingBlueprint.model_validate(normalized)

    assert blueprint.max_total_positions == 10
    assert len(blueprint.symbol_plans) == 3


def test_normalize_blueprint_payload_trims_symbol_plans_to_configured_cap():
    payload = {
        "trading_date": "2026-04-29",
        "generated_at": "2026-04-28T03:39:57",
        "model_provider": "closeai",
        "model_version": "claude-sonnet-4-20250514",
        "market_regime": "trending_calm",
        "max_total_positions": 20,
        "symbol_plans": [
            {
                "underlying": f"SYM{i:02d}",
                "strategy_type": "single_leg",
                "direction": "bullish",
                "legs": [{"expiry": "2026-06-05", "strike": 100 + i, "option_type": "call", "side": "buy", "quantity": 1}],
                "max_loss_per_trade": 500,
                "confidence": 0.7,
            }
            for i in range(12)
        ],
    }

    normalized, stats = _normalize_blueprint_payload(payload, signal_date=None, max_output_plans=10)

    assert normalized["max_total_positions"] == 10
    assert len(normalized["symbol_plans"]) == 10
    assert [plan["underlying"] for plan in normalized["symbol_plans"]] == [
        "SYM00", "SYM01", "SYM02", "SYM03", "SYM04", "SYM05", "SYM06", "SYM07", "SYM08", "SYM09",
    ]
    assert stats["symbol_plans_trimmed_to_max_output_plans"] == 2
    assert stats["max_total_positions_normalized"] == 1


def test_normalize_blueprint_payload_sorts_trimmed_plans_by_score_then_quality_confidence():
    payload = {
        "trading_date": "2026-04-29",
        "generated_at": "2026-04-28T03:39:57",
        "market_regime": "trending_calm",
        "symbol_plans": [
            {
                "underlying": "AAPL",
                "strategy_type": "single_leg",
                "direction": "bullish",
                "legs": [{"expiry": "2026-06-05", "strike": 185, "option_type": "call", "side": "buy", "quantity": 1}],
                "max_loss_per_trade": 500,
                "confidence": 0.82,
                "data_quality_score": 0.70,
            },
            {
                "underlying": "MSFT",
                "strategy_type": "single_leg",
                "direction": "bullish",
                "legs": [{"expiry": "2026-06-05", "strike": 430, "option_type": "call", "side": "buy", "quantity": 1}],
                "max_loss_per_trade": 500,
                "confidence": 0.68,
                "data_quality_score": 0.95,
            },
            {
                "underlying": "NVDA",
                "strategy_type": "single_leg",
                "direction": "bullish",
                "legs": [{"expiry": "2026-06-05", "strike": 1200, "option_type": "call", "side": "buy", "quantity": 1}],
                "max_loss_per_trade": 500,
                "confidence": 0.74,
                "data_quality_score": 0.90,
                "score": 0.91,
            },
            {
                "underlying": "TSLA",
                "strategy_type": "single_leg",
                "direction": "bullish",
                "legs": [{"expiry": "2026-06-05", "strike": 210, "option_type": "call", "side": "buy", "quantity": 1}],
                "max_loss_per_trade": 500,
                "confidence": 0.93,
                "data_quality_score": 0.92,
            },
        ],
    }

    normalized, stats = _normalize_blueprint_payload(payload, signal_date=None, max_output_plans=2)

    assert [plan["underlying"] for plan in normalized["symbol_plans"]] == ["NVDA", "TSLA"]
    assert normalized["max_total_positions"] == 10
    assert stats["symbol_plans_trimmed_to_max_output_plans"] == 2


def test_normalize_blueprint_payload_does_not_trim_when_output_cap_disabled():
    payload = {
        "trading_date": "2026-04-29",
        "generated_at": "2026-04-28T03:39:57",
        "market_regime": "trending_calm",
        "symbol_plans": [
            {
                "underlying": f"SYM{i:02d}",
                "strategy_type": "single_leg",
                "direction": "bullish",
                "legs": [{"expiry": "2026-06-05", "strike": 100 + i, "option_type": "call", "side": "buy", "quantity": 1}],
                "max_loss_per_trade": 500,
                "confidence": 0.7,
            }
            for i in range(12)
        ],
    }

    normalized, stats = _normalize_blueprint_payload(payload, signal_date=None, max_output_plans=None)

    assert normalized["max_total_positions"] == 10
    assert len(normalized["symbol_plans"]) == 12
    assert stats["symbol_plans_trimmed_to_max_output_plans"] == 0


def test_synthesizer_prompt_caps_max_total_positions_to_configured_limit(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                max_output_plans=10,
                precision_first=SimpleNamespace(
                    enabled=True,
                    allowed_strategy_types=["single_leg", "vertical_spread"],
                )
            )
        ),
    )
    monkeypatch.setattr(
        "services.analysis_service.app.llm.agents.synthesizer_agent.get_settings",
        lambda: settings,
    )

    trade_symbols = [f"SYM{i:02d}" for i in range(12)]
    prompt = SynthesizerAgent()._build_prompt(
        agent_outputs={},
        signals_summary=[{"symbol": symbol, "close_price": 100.0, "volume": 1_000_000, "volatility_regime": "normal"} for symbol in trade_symbols],
        critic_feedback=None,
        signal_date=None,
        trade_symbols=trade_symbols,
    )

    assert '"max_total_positions":10' in prompt
    assert '"max_daily_loss"' not in prompt
    assert '"max_margin_usage"' not in prompt
    assert '"portfolio_delta_limit"' not in prompt
    assert '"portfolio_gamma_limit"' not in prompt
    assert "Generate at most 10 symbol_plans total" in prompt


def test_synthesizer_prompt_does_not_apply_global_cap_when_disabled(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                max_output_plans=10,
                precision_first=SimpleNamespace(
                    enabled=True,
                    allowed_strategy_types=["single_leg", "vertical_spread"],
                )
            )
        ),
    )
    monkeypatch.setattr(
        "services.analysis_service.app.llm.agents.synthesizer_agent.get_settings",
        lambda: settings,
    )

    trade_symbols = [f"SYM{i:02d}" for i in range(12)]
    prompt = SynthesizerAgent()._build_prompt(
        agent_outputs={},
        signals_summary=[{"symbol": symbol, "close_price": 100.0, "volume": 1_000_000, "volatility_regime": "normal"} for symbol in trade_symbols],
        critic_feedback=None,
        signal_date=None,
        trade_symbols=trade_symbols,
        apply_output_cap=False,
    )

    assert '"max_total_positions":10' in prompt
    assert "Use max_total_positions=10 as the portfolio cap" in prompt

    assert "Every leg MUST include `price_tolerance` as a decimal fraction" in _SYNTHESIZER_SYSTEM_PROMPT
    assert "Only use generic 0.005-0.015 for liquid ETFs/blue chips when Chain.liquidity_tier is unknown" in _SYNTHESIZER_SYSTEM_PROMPT
    assert "Buying (`side=buy`): prefer the tighter end" in _SYNTHESIZER_SYSTEM_PROMPT


def test_normalize_blueprint_payload_removes_legacy_risk_limit_fields():
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

    assert "max_daily_loss" not in normalized
    assert "max_margin_usage" not in normalized
    assert "portfolio_delta_limit" not in normalized
    assert "portfolio_gamma_limit" not in normalized
    assert stats["legacy_top_level_fields_removed"] == 4


def test_normalize_blueprint_payload_repairs_mislabeled_four_leg_vertical_spread():
    payload = {
        "trading_date": "2026-04-29",
        "generated_at": "2026-04-28T03:39:57",
        "model_provider": "closeai",
        "model_version": "claude-sonnet-4-20250514",
        "market_regime": "neutral",
        "symbol_plans": [
            {
                "underlying": "TSLA",
                "strategy_type": "vertical_spread",
                "direction": "neutral",
                "legs": [
                    {"expiry": "2026-06-05", "strike": 240, "option_type": "put", "side": "buy", "quantity": 1},
                    {"expiry": "2026-06-05", "strike": 245, "option_type": "put", "side": "sell", "quantity": 1},
                    {"expiry": "2026-06-05", "strike": 255, "option_type": "call", "side": "sell", "quantity": 1},
                    {"expiry": "2026-06-05", "strike": 260, "option_type": "call", "side": "buy", "quantity": 1},
                ],
                "max_loss_per_trade": 500,
                "confidence": 0.5,
            },
        ],
    }

    normalized, stats = _normalize_blueprint_payload(payload, signal_date=None)

    assert normalized["symbol_plans"][0]["strategy_type"] == "iron_condor"
    assert stats["strategy_type_repaired"] == 1
    assert stats["strategy_type_repair_samples"] == [
        {"underlying": "TSLA", "from": "vertical_spread", "to": "iron_condor", "legs": 4},
    ]

    blueprint = LLMTradingBlueprint.model_validate(normalized)

    assert blueprint.symbol_plans[0].strategy_type.value == "iron_condor"