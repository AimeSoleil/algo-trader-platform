from __future__ import annotations

from shared.models.blueprint import OptionLeg, SymbolPlan
from services.analysis_service.app.llm.agents.portfolio_selector import PlanCandidate, PortfolioSelector


def _make_single_leg_plan(symbol: str, *, confidence: float) -> SymbolPlan:
    return SymbolPlan(
        underlying=symbol,
        strategy_type="single_leg",
        direction="bullish",
        legs=[OptionLeg(expiry="2026-05-15", strike=100, option_type="call", side="buy")],
        max_loss_per_trade=500,
        confidence=confidence,
        max_position_size=1.0,
        max_contracts=1,
    )


def _make_vertical_spread_plan(symbol: str, *, confidence: float) -> SymbolPlan:
    return SymbolPlan(
        underlying=symbol,
        strategy_type="vertical_spread",
        direction="bullish",
        legs=[
            OptionLeg(expiry="2026-05-15", strike=100, option_type="call", side="buy"),
            OptionLeg(expiry="2026-05-15", strike=105, option_type="call", side="sell"),
        ],
        max_loss_per_trade=500,
        confidence=confidence,
        max_position_size=1.0,
        max_contracts=1,
    )


def _make_iron_condor_plan(symbol: str, *, confidence: float) -> SymbolPlan:
    return SymbolPlan(
        underlying=symbol,
        strategy_type="iron_condor",
        direction="neutral",
        legs=[
            OptionLeg(expiry="2026-05-15", strike=95, option_type="put", side="buy"),
            OptionLeg(expiry="2026-05-15", strike=100, option_type="put", side="sell"),
            OptionLeg(expiry="2026-05-15", strike=110, option_type="call", side="sell"),
            OptionLeg(expiry="2026-05-15", strike=115, option_type="call", side="buy"),
        ],
        max_loss_per_trade=500,
        confidence=confidence,
        max_position_size=1.0,
        max_contracts=1,
    )


def test_selector_prefers_valid_duplicate_candidate_over_confidence_cap_violation():
    selector = PortfolioSelector()

    blocked_candidate = PlanCandidate(
        plan=_make_single_leg_plan("AAPL", confidence=0.72),
        chunk_index=0,
        original_order=0,
        quality_score=1.0,
        chunk_id="chunk-0",
        agent_outputs={
            "trend": {
                "symbols": [
                    {
                        "symbol": "AAPL",
                        "confidence_cap": 0.70,
                    }
                ]
            }
        },
    )
    fallback_candidate = PlanCandidate(
        plan=_make_vertical_spread_plan("AAPL", confidence=0.68),
        chunk_index=1,
        original_order=1,
        quality_score=1.0,
        chunk_id="chunk-1",
        agent_outputs={},
    )

    selected_plans, metadata = selector.select(
        candidates=[blocked_candidate, fallback_candidate],
        trade_symbols={"AAPL"},
        chunk_limits=[],
        precision_first_enabled=True,
        allowed_strategy_types=["single_leg", "vertical_spread"],
    )

    assert [plan.strategy_type.value for plan in selected_plans] == ["vertical_spread"]
    duplicate_info = metadata["duplicate_symbols"]["AAPL"]
    assert duplicate_info["selected_chunk_id"] == "chunk-1"
    assert duplicate_info["dropped_chunk_ids"] == ["chunk-0"]
    assert metadata["output_targets"]["max_total_positions"]["value"] == 1
    assert metadata["output_targets"]["max_total_positions"]["configured_cap"] == 10


def test_selector_simple_structures_only_respects_allowed_strategy_types():
    selector = PortfolioSelector()
    candidate = PlanCandidate(
        plan=_make_iron_condor_plan("AAPL", confidence=0.7),
        chunk_index=0,
        original_order=0,
        quality_score=1.0,
        chunk_id="chunk-0",
        agent_outputs={
            "trend": {
                "symbols": [
                    {
                        "symbol": "AAPL",
                        "simple_structures_only": True,
                    }
                ]
            }
        },
    )

    candidate_summaries, _ = selector.build_review_inputs(
        candidates=[candidate],
        trade_symbols={"AAPL"},
        precision_first_enabled=True,
        allowed_strategy_types=["single_leg", "vertical_spread", "iron_condor", "calendar_spread"],
    )

    assert candidate_summaries[0]["machine_readable_gate_ok"] is True
    assert candidate_summaries[0]["precision_first_breakdown"]["simple_structure_conflict_agents"] == []


def test_selector_caps_output_plans_to_configured_max():
    selector = PortfolioSelector()
    candidates = [
        PlanCandidate(
            plan=_make_single_leg_plan(f"SYM{i:02d}", confidence=0.95 - i * 0.01),
            chunk_index=i,
            original_order=i,
            quality_score=1.0,
            chunk_id=f"chunk-{i}",
            agent_outputs={},
        )
        for i in range(12)
    ]

    selected_plans, metadata = selector.select(
        candidates=candidates,
        trade_symbols={candidate.plan.underlying.upper() for candidate in candidates},
        chunk_limits=[],
        max_output_plans=10,
    )

    assert len(selected_plans) == 10
    assert metadata["output_plan_count"] == 10
    assert metadata["max_output_plans"] == 10
    assert metadata["filtered_symbols"] == ["SYM10", "SYM11"]
    assert metadata["output_targets"]["max_total_positions"] == {
        "value": 10,
        "source": "configured_max_output_plans",
        "configured_cap": 10,
        "chunk_proposals": [],
    }