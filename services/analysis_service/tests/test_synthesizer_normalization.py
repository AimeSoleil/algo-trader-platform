from __future__ import annotations

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