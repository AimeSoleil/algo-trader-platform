from __future__ import annotations

from types import SimpleNamespace

from pydantic import ValidationError
import pytest

from shared.models.blueprint import LLMTradingBlueprint, OptionLeg, SymbolPlan
from shared.models.signal import (
    CrossAssetIndicators,
    DataQuality,
    OptionIndicators,
    SignalFeatures,
    StockIndicators,
)
from services.analysis_service.app.llm.agents.models import PostMergeConflictExplanation, PostMergeReview
from services.analysis_service.app.llm.agents.orchestrator import AgentOrchestrator


def _make_sf(
    symbol: str,
    *,
    option_indicators: OptionIndicators | None = None,
    cross_asset_indicators: CrossAssetIndicators | None = None,
    data_quality: DataQuality | None = None,
) -> SignalFeatures:
    return SignalFeatures(
        symbol=symbol,
        date="2026-04-29",
        computed_at="2026-04-28T20:00:00",
        close_price=100.0,
        daily_return=0.01,
        volume=1_000_000,
        volatility_regime="normal",
        stock_indicators=StockIndicators(),
        option_indicators=option_indicators or OptionIndicators(),
        cross_asset_indicators=cross_asset_indicators or CrossAssetIndicators(),
        data_quality=data_quality or DataQuality(score=1.0, stock_bar_count=260, option_row_count=200),
    )


def _make_plan(
    symbol: str,
    *,
    confidence: float = 0.5,
    strategy_type: str = "single_leg",
    direction: str = "bullish",
    max_position_size: float = 1.0,
    max_contracts: int = 1,
) -> SymbolPlan:
    return SymbolPlan(
        underlying=symbol,
        strategy_type=strategy_type,
        direction=direction,
        legs=[OptionLeg(expiry="2026-05-15", strike=100, option_type="call", side="buy")],
        max_loss_per_trade=500,
        confidence=confidence,
        max_position_size=max_position_size,
        max_contracts=max_contracts,
    )


def _make_blueprint(
    symbols: list[tuple],
    *,
    max_total_positions: int,
    max_daily_loss: float = 2_500.0,
    max_margin_usage: float = 0.8,
    portfolio_delta_limit: float = 0.9,
    portfolio_gamma_limit: float = 0.2,
    analysis_chunk_id: str | None = None,
    agent_outputs: dict | None = None,
) -> LLMTradingBlueprint:
    plans: list[SymbolPlan] = []
    for item in symbols:
        if len(item) == 2:
            symbol, confidence = item
            plans.append(_make_plan(symbol, confidence=confidence))
            continue
        if len(item) == 6:
            symbol, confidence, strategy_type, direction, max_position_size, max_contracts = item
            plans.append(_make_plan(
                symbol,
                confidence=confidence,
                strategy_type=strategy_type,
                direction=direction,
                max_position_size=max_position_size,
                max_contracts=max_contracts,
            ))
            continue
        raise AssertionError(f"unsupported symbol fixture: {item}")

    reasoning_context = {}
    if analysis_chunk_id:
        reasoning_context["analysis_chunk_id"] = analysis_chunk_id
    if agent_outputs is not None:
        reasoning_context["agent_outputs"] = agent_outputs

    return LLMTradingBlueprint(
        trading_date="2026-04-29",
        generated_at="2026-04-28T20:00:00",
        market_regime="neutral",
        symbol_plans=plans,
        max_total_positions=max_total_positions,
        max_daily_loss=max_daily_loss,
        max_margin_usage=max_margin_usage,
        portfolio_delta_limit=portfolio_delta_limit,
        portfolio_gamma_limit=portfolio_gamma_limit,
        reasoning_context=reasoning_context or None,
    )


class _Provider:
    name = "test"


class _LLMProvider:
    name = "test"

    async def generate(self, *args, **kwargs):
        raise AssertionError("generate should not be called when review is monkeypatched")


@pytest.mark.asyncio
async def test_generate_single_pass_disables_synthesizer_output_cap_for_chunk_batches(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                agent_models_override=SimpleNamespace(synthesizer=None, critic=None),
                max_critic_revisions=0,
            ),
        ),
    )

    monkeypatch.setattr(
        "services.analysis_service.app.llm.agents.orchestrator.get_settings",
        lambda: settings,
    )

    orchestrator = AgentOrchestrator(provider=_Provider())
    captured: dict[str, object] = {}

    async def _fake_run_specialists(self, *args, **kwargs):
        return {}

    def _fake_compact_for_synthesis(self, agent_outputs, trade_sym_set):
        return {}

    def _fake_compute_consensus(self, agent_outputs, trade_sym_set):
        return {}

    def _fake_classify_market_condition(self, agent_outputs):
        return "neutral"

    async def _fake_synthesize(**kwargs):
        captured["apply_output_cap"] = kwargs["apply_output_cap"]
        return _make_blueprint(
            [("AAPL", 0.75)],
            max_total_positions=1,
            analysis_chunk_id="chunk-0",
        )

    monkeypatch.setattr(AgentOrchestrator, "_run_specialists", _fake_run_specialists)
    monkeypatch.setattr(AgentOrchestrator, "_compact_for_synthesis", _fake_compact_for_synthesis)
    monkeypatch.setattr(AgentOrchestrator, "_compute_consensus", _fake_compute_consensus)
    monkeypatch.setattr(AgentOrchestrator, "_classify_market_condition", _fake_classify_market_condition)
    monkeypatch.setattr(orchestrator._synthesizer, "synthesize", _fake_synthesize)

    blueprint = await orchestrator._generate_single_pass(
        signal_features=[_make_sf("AAPL")],
        current_positions=None,
        previous_execution=None,
        provider=_Provider(),
        signal_date=None,
        is_chunk=True,
        analysis_chunk_id="chunk-0",
        usage_tracker=None,
        trade_symbols=["AAPL"],
        market_snapshot=None,
    )

    assert captured["apply_output_cap"] is False
    assert [plan.underlying for plan in blueprint.symbol_plans] == ["AAPL"]


@pytest.mark.asyncio
async def test_chunked_merge_keeps_all_ranked_plans(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                orchestrator_chunk_size=2,
                orchestrator_max_parallel=2,
                precision_first=SimpleNamespace(
                    enabled=True,
                    allowed_strategy_types=["single_leg", "vertical_spread"],
                ),
            ),
            risk=SimpleNamespace(
                blueprint_limits=SimpleNamespace(
                    max_daily_loss=9_999.0,
                    max_margin_usage=0.9,
                    portfolio_delta_limit=0.95,
                    portfolio_gamma_limit=0.4,
                )
            ),
        ),
        common=SimpleNamespace(
            watchlist=SimpleNamespace(
                for_data_signal=["AAPL", "MSFT", "NVDA", "TSLA"],
                for_trade_benchmark=[],
            )
        ),
    )

    monkeypatch.setattr(
        "services.analysis_service.app.llm.agents.orchestrator.get_settings",
        lambda: settings,
    )

    orchestrator = AgentOrchestrator(provider=_Provider())

    async def _fake_generate_single_pass(self, **kwargs):
        trade_symbols = kwargs["trade_symbols"]
        if trade_symbols == ["AAPL", "MSFT"]:
            return _make_blueprint(
                [("AAPL", 0.55), ("MSFT", 0.91)],
                max_total_positions=2,
                max_daily_loss=2_400.0,
                max_margin_usage=0.75,
                portfolio_delta_limit=0.7,
                portfolio_gamma_limit=0.15,
                analysis_chunk_id="chunk-0",
            )
        if trade_symbols == ["NVDA", "TSLA"]:
            return _make_blueprint(
                [("NVDA", 0.87), ("TSLA", 0.42)],
                max_total_positions=2,
                max_daily_loss=2_200.0,
                max_margin_usage=0.7,
                portfolio_delta_limit=0.65,
                portfolio_gamma_limit=0.12,
                analysis_chunk_id="chunk-1",
            )
        raise AssertionError(f"unexpected trade_symbols: {trade_symbols}")

    monkeypatch.setattr(
        AgentOrchestrator,
        "_generate_single_pass",
        _fake_generate_single_pass,
    )

    blueprint = await orchestrator.generate([
        _make_sf("AAPL"),
        _make_sf("MSFT"),
        _make_sf("NVDA"),
        _make_sf("TSLA"),
    ])

    assert blueprint.max_total_positions == 4
    assert len(blueprint.symbol_plans) == 4
    assert [plan.underlying for plan in blueprint.symbol_plans] == ["MSFT", "NVDA", "AAPL", "TSLA"]
    assert [plan.confidence for plan in blueprint.symbol_plans] == [0.91, 0.87, 0.55, 0.42]
    assert blueprint.reasoning_context is not None
    assert blueprint.reasoning_context["post_merge_phase"]["selected_symbols"] == ["MSFT", "NVDA", "AAPL", "TSLA"]
    assert blueprint.reasoning_context["post_merge_phase"]["selection_mode"] == "dedupe_and_rank_all"
    assert blueprint.reasoning_context["post_merge_phase"]["chunk_limit_proposals"] == [
        {"chunk_index": 0, "chunk_id": "chunk-0", "max_total_positions": 2},
        {"chunk_index": 1, "chunk_id": "chunk-1", "max_total_positions": 2},
    ]
    assert blueprint.reasoning_context["post_merge_phase"]["output_targets"]["max_total_positions"] == {
        "value": 4,
        "source": "selected_plan_count",
        "configured_cap": 10,
        "chunk_proposals": [2, 2],
    }
    assert "final_limit_sources" not in blueprint.reasoning_context["post_merge_phase"]


@pytest.mark.asyncio
async def test_chunked_merge_caps_output_plans_to_configured_max(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                orchestrator_chunk_size=2,
                orchestrator_max_parallel=2,
                max_output_plans=2,
                precision_first=SimpleNamespace(
                    enabled=True,
                    allowed_strategy_types=["single_leg", "vertical_spread"],
                ),
            ),
        ),
        common=SimpleNamespace(
            watchlist=SimpleNamespace(
                for_data_signal=["AAPL", "MSFT", "NVDA", "TSLA"],
                for_trade_benchmark=[],
            )
        ),
    )

    monkeypatch.setattr(
        "services.analysis_service.app.llm.agents.orchestrator.get_settings",
        lambda: settings,
    )

    orchestrator = AgentOrchestrator(provider=_Provider())

    async def _fake_generate_single_pass(self, **kwargs):
        trade_symbols = kwargs["trade_symbols"]
        if trade_symbols == ["AAPL", "MSFT"]:
            return _make_blueprint(
                [("AAPL", 0.91), ("MSFT", 0.88)],
                max_total_positions=2,
                analysis_chunk_id="chunk-0",
            )
        if trade_symbols == ["NVDA", "TSLA"]:
            return _make_blueprint(
                [("NVDA", 0.85), ("TSLA", 0.82)],
                max_total_positions=2,
                analysis_chunk_id="chunk-1",
            )
        raise AssertionError(f"unexpected trade_symbols: {trade_symbols}")

    monkeypatch.setattr(AgentOrchestrator, "_generate_single_pass", _fake_generate_single_pass)

    blueprint = await orchestrator.generate([
        _make_sf("AAPL"),
        _make_sf("MSFT"),
        _make_sf("NVDA"),
        _make_sf("TSLA"),
    ])

    assert blueprint.max_total_positions == 2
    assert [plan.underlying for plan in blueprint.symbol_plans] == ["AAPL", "MSFT"]
    assert blueprint.reasoning_context["post_merge_phase"]["filtered_symbols"] == ["NVDA", "TSLA"]
    assert blueprint.reasoning_context["post_merge_phase"]["output_targets"]["max_total_positions"] == {
        "value": 2,
        "source": "configured_max_output_plans",
        "configured_cap": 2,
        "chunk_proposals": [2, 2],
    }


@pytest.mark.asyncio
async def test_chunked_merge_prefers_higher_scoring_duplicate_symbol(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                orchestrator_chunk_size=2,
                orchestrator_max_parallel=2,
            ),
            risk=SimpleNamespace(
                blueprint_limits=SimpleNamespace(
                    max_daily_loss=2_000.0,
                    max_margin_usage=0.5,
                    portfolio_delta_limit=0.5,
                    portfolio_gamma_limit=0.1,
                )
            ),
        ),
        common=SimpleNamespace(
            watchlist=SimpleNamespace(
                for_data_signal=["AAPL", "MSFT", "NVDA"],
                for_trade_benchmark=[],
            )
        ),
    )

    monkeypatch.setattr(
        "services.analysis_service.app.llm.agents.orchestrator.get_settings",
        lambda: settings,
    )

    orchestrator = AgentOrchestrator(provider=_Provider())

    async def _fake_generate_single_pass(self, **kwargs):
        trade_symbols = kwargs["trade_symbols"]
        if trade_symbols == ["AAPL", "MSFT"]:
            return _make_blueprint(
                [("AAPL", 0.52), ("MSFT", 0.61)],
                max_total_positions=2,
                analysis_chunk_id="chunk-0",
            )
        if trade_symbols == ["NVDA"]:
            return _make_blueprint(
                [("AAPL", 0.88), ("NVDA", 0.59)],
                max_total_positions=2,
                analysis_chunk_id="chunk-1",
            )
        raise AssertionError(f"unexpected trade_symbols: {trade_symbols}")

    monkeypatch.setattr(
        AgentOrchestrator,
        "_generate_single_pass",
        _fake_generate_single_pass,
    )

    blueprint = await orchestrator.generate([
        _make_sf("AAPL"),
        _make_sf("MSFT"),
        _make_sf("NVDA"),
    ])

    assert [plan.underlying for plan in blueprint.symbol_plans] == ["AAPL", "MSFT", "NVDA"]
    duplicate_info = blueprint.reasoning_context["post_merge_phase"]["duplicate_symbols"]["AAPL"]
    assert duplicate_info["selected_chunk_id"] == "chunk-1"
    assert duplicate_info["dropped_chunk_ids"] == ["chunk-0"]


@pytest.mark.asyncio
async def test_chunked_merge_uses_portfolio_impact_heuristic(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                orchestrator_chunk_size=2,
                orchestrator_max_parallel=2,
                precision_first=SimpleNamespace(
                    enabled=True,
                    allowed_strategy_types=["single_leg", "vertical_spread"],
                ),
            ),
            risk=SimpleNamespace(
                blueprint_limits=SimpleNamespace(
                    max_daily_loss=2_000.0,
                    max_margin_usage=0.5,
                    portfolio_delta_limit=0.5,
                    portfolio_gamma_limit=0.1,
                )
            ),
        ),
        common=SimpleNamespace(
            watchlist=SimpleNamespace(
                for_data_signal=["AAPL", "MSFT", "NVDA"],
                for_trade_benchmark=[],
            )
        ),
    )

    monkeypatch.setattr(
        "services.analysis_service.app.llm.agents.orchestrator.get_settings",
        lambda: settings,
    )

    orchestrator = AgentOrchestrator(provider=_Provider())

    async def _fake_generate_single_pass(self, **kwargs):
        trade_symbols = kwargs["trade_symbols"]
        if trade_symbols == ["AAPL", "MSFT"]:
            return _make_blueprint(
                [
                    ("AAPL", 0.89, "single_leg", "bullish", 1.5, 3),
                    ("MSFT", 0.83, "single_leg", "bullish", 0.4, 1),
                ],
                max_total_positions=2,
                analysis_chunk_id="chunk-0",
            )
        if trade_symbols == ["NVDA"]:
            return _make_blueprint(
                [("NVDA", 0.6)],
                max_total_positions=2,
                analysis_chunk_id="chunk-1",
            )
        raise AssertionError(f"unexpected trade_symbols: {trade_symbols}")

    monkeypatch.setattr(
        AgentOrchestrator,
        "_generate_single_pass",
        _fake_generate_single_pass,
    )

    blueprint = await orchestrator.generate([
        _make_sf("AAPL"),
        _make_sf("MSFT"),
        _make_sf("NVDA"),
    ])

    assert [plan.underlying for plan in blueprint.symbol_plans] == ["MSFT", "AAPL", "NVDA"]
    decisions = {item["symbol"]: item for item in blueprint.reasoning_context["post_merge_phase"]["decisions"]}
    assert decisions["MSFT"]["portfolio_impact_score"] > decisions["AAPL"]["portfolio_impact_score"]
    assert "size_penalty" in decisions["MSFT"]["portfolio_impact_breakdown"]
    assert "strategy_penalty" in decisions["MSFT"]["portfolio_impact_breakdown"]
    assert blueprint.reasoning_context["post_merge_phase"]["ranking_method"] == "precision_first_confidence_quality_portfolio_impact_weighted"


@pytest.mark.asyncio
async def test_chunked_merge_penalizes_same_direction_existing_holding_and_concentration(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                orchestrator_chunk_size=1,
                orchestrator_max_parallel=2,
            ),
            risk=SimpleNamespace(
                blueprint_limits=SimpleNamespace(
                    max_daily_loss=2_000.0,
                    max_margin_usage=0.5,
                    portfolio_delta_limit=0.5,
                    portfolio_gamma_limit=0.1,
                )
            ),
        ),
        common=SimpleNamespace(
            watchlist=SimpleNamespace(
                for_data_signal=["AAPL", "MSFT"],
                for_trade_benchmark=[],
            )
        ),
    )

    monkeypatch.setattr(
        "services.analysis_service.app.llm.agents.orchestrator.get_settings",
        lambda: settings,
    )

    orchestrator = AgentOrchestrator(provider=_Provider())

    async def _fake_generate_single_pass(self, **kwargs):
        trade_symbols = kwargs["trade_symbols"]
        if trade_symbols == ["AAPL"]:
            return _make_blueprint(
                [
                    ("AAPL", 0.86, "single_leg", "bullish", 0.8, 1),
                ],
                max_total_positions=2,
                analysis_chunk_id="chunk-0",
            )
        if trade_symbols == ["MSFT"]:
            return _make_blueprint(
                [
                    ("MSFT", 0.82, "single_leg", "bullish", 0.8, 1),
                ],
                max_total_positions=2,
                analysis_chunk_id="chunk-1",
            )
        raise AssertionError(f"unexpected trade_symbols: {trade_symbols}")

    monkeypatch.setattr(
        AgentOrchestrator,
        "_generate_single_pass",
        _fake_generate_single_pass,
    )

    blueprint = await orchestrator.generate(
        [
            _make_sf("AAPL"),
            _make_sf("MSFT"),
        ],
        current_positions={
            "positions": [
                {"underlying": "AAPL", "direction": "bullish", "quantity": 2},
                {"underlying": "QQQ", "direction": "bullish", "quantity": 1},
                {"underlying": "NVDA", "direction": "bullish", "quantity": 1},
            ]
        },
    )

    assert [plan.underlying for plan in blueprint.symbol_plans] == ["MSFT", "AAPL"]
    decisions = {item["symbol"]: item for item in blueprint.reasoning_context["post_merge_phase"]["decisions"]}
    aapl_breakdown = decisions["AAPL"]["portfolio_impact_breakdown"]
    msft_breakdown = decisions["MSFT"]["portfolio_impact_breakdown"]
    assert aapl_breakdown["existing_underlying_penalty"] > 0.0
    assert aapl_breakdown["same_direction_penalty"] > 0.0
    assert aapl_breakdown["concentration_penalty"] > 0.0
    assert aapl_breakdown["total_penalty"] > msft_breakdown["total_penalty"]
    assert blueprint.reasoning_context["post_merge_phase"]["current_position_context"]["direction_counts"]["bullish"] == 3


@pytest.mark.asyncio
async def test_chunked_merge_applies_llm_post_merge_ranking(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                orchestrator_chunk_size=2,
                orchestrator_max_parallel=2,
                agent_models_override=SimpleNamespace(post_merge=""),
                precision_first=SimpleNamespace(
                    enabled=True,
                    allowed_strategy_types=["single_leg", "vertical_spread"],
                ),
            ),
            risk=SimpleNamespace(
                blueprint_limits=SimpleNamespace(
                    max_daily_loss=2_000.0,
                    max_margin_usage=0.5,
                    portfolio_delta_limit=0.5,
                    portfolio_gamma_limit=0.1,
                )
            ),
        ),
        common=SimpleNamespace(
            watchlist=SimpleNamespace(
                for_data_signal=["AAPL", "MSFT", "NVDA"],
                for_trade_benchmark=[],
            )
        ),
    )

    monkeypatch.setattr(
        "services.analysis_service.app.llm.agents.orchestrator.get_settings",
        lambda: settings,
    )

    orchestrator = AgentOrchestrator(provider=_LLMProvider())
    captured_review_inputs = {}

    async def _fake_generate_single_pass(self, **kwargs):
        trade_symbols = kwargs["trade_symbols"]
        if trade_symbols == ["AAPL", "MSFT"]:
            return _make_blueprint(
                [
                    ("AAPL", 0.72, "single_leg", "bullish", 1.0, 1),
                    ("MSFT", 0.88, "single_leg", "bullish", 0.6, 1),
                ],
                max_total_positions=2,
                analysis_chunk_id="chunk-0",
                agent_outputs={
                    "trend": {
                        "symbols": [
                            {
                                "symbol": "MSFT",
                                "trade_allowed": True,
                                "confidence_cap": 0.86,
                                "simple_structures_only": False,
                                "blocked_reasons": ["counter_trend_setup"],
                            }
                        ]
                    }
                },
            )
        if trade_symbols == ["NVDA"]:
            return _make_blueprint(
                [("NVDA", 0.7, "covered_call", "bullish", 1.0, 1)],
                max_total_positions=2,
                analysis_chunk_id="chunk-1",
            )
        raise AssertionError(f"unexpected trade_symbols: {trade_symbols}")

    async def _fake_review(**kwargs):
        captured_review_inputs.update(kwargs)
        return PostMergeReview(
            selected_symbols=["AAPL", "MSFT"],
            ranking=["AAPL", "MSFT", "NVDA"],
            portfolio_summary="Prefer AAPL first for portfolio balance.",
            risk_notes=["Avoid over-concentrating into one tech winner narrative."],
            conflict_explanations=[
                PostMergeConflictExplanation(
                    symbol="AAPL",
                    decision="keep",
                    rationale="AAPL is promoted by better precision-first and portfolio impact inputs despite lower raw confidence.",
                )
            ],
        )

    monkeypatch.setattr(AgentOrchestrator, "_generate_single_pass", _fake_generate_single_pass)
    monkeypatch.setattr(orchestrator._post_merge_portfolio_agent, "review", _fake_review)

    blueprint = await orchestrator.generate([
        _make_sf("AAPL"),
        _make_sf("MSFT"),
        _make_sf("NVDA"),
    ])

    assert [plan.underlying for plan in blueprint.symbol_plans] == ["AAPL", "NVDA", "MSFT"]
    llm_review = blueprint.reasoning_context["post_merge_phase"]["llm_review"]
    assert llm_review["status"] == "applied"
    assert llm_review["ranking"] == ["AAPL", "MSFT", "NVDA"]
    assert llm_review["portfolio_summary"] == "Prefer AAPL first for portfolio balance."
    assert captured_review_inputs["candidate_count"] == 3
    assert captured_review_inputs["selector_metadata"]["ranking_method"] == "precision_first_confidence_quality_portfolio_impact_weighted"
    assert "max_total_positions" not in captured_review_inputs["selector_metadata"]
    assert captured_review_inputs["selector_metadata"]["deterministic_sort_priority"][0] == "machine_readable_gate_ok"
    review_candidates = {item["symbol"]: item for item in captured_review_inputs["candidate_summaries"]}
    assert review_candidates["AAPL"]["precision_first_score"] > review_candidates["MSFT"]["precision_first_score"]
    assert review_candidates["MSFT"]["machine_readable_gate_ok"] is False
    assert review_candidates["MSFT"]["precision_first_breakdown"]["trade_blocked_agents"] == []
    assert review_candidates["MSFT"]["precision_first_breakdown"]["confidence_caps"]["trend"] == 0.86
    assert review_candidates["MSFT"]["precision_first_breakdown"]["blocked_reasons"] == ["counter_trend_setup"]
    assert "portfolio_impact_breakdown" in review_candidates["AAPL"]
    assert "selector_base_score" in review_candidates["AAPL"]


@pytest.mark.asyncio
async def test_chunked_merge_precision_first_prefers_simple_allowed_strategy(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                orchestrator_chunk_size=1,
                orchestrator_max_parallel=2,
                precision_first=SimpleNamespace(
                    enabled=True,
                    allowed_strategy_types=["single_leg", "vertical_spread"],
                ),
            ),
            risk=SimpleNamespace(
                blueprint_limits=SimpleNamespace(
                    max_daily_loss=2_000.0,
                    max_margin_usage=0.5,
                    portfolio_delta_limit=0.5,
                    portfolio_gamma_limit=0.1,
                )
            ),
        ),
        common=SimpleNamespace(
            watchlist=SimpleNamespace(
                for_data_signal=["AAPL", "MSFT"],
                for_trade_benchmark=[],
            )
        ),
    )

    monkeypatch.setattr(
        "services.analysis_service.app.llm.agents.orchestrator.get_settings",
        lambda: settings,
    )

    orchestrator = AgentOrchestrator(provider=_Provider())

    async def _fake_generate_single_pass(self, **kwargs):
        trade_symbols = kwargs["trade_symbols"]
        if trade_symbols == ["AAPL"]:
            return _make_blueprint(
                [("AAPL", 0.71, "single_leg", "bullish", 0.8, 1)],
                max_total_positions=1,
                analysis_chunk_id="chunk-0",
            )
        if trade_symbols == ["MSFT"]:
            return _make_blueprint(
                [("MSFT", 0.93, "covered_call", "neutral", 0.8, 1)],
                max_total_positions=1,
                analysis_chunk_id="chunk-1",
            )
        raise AssertionError(f"unexpected trade_symbols: {trade_symbols}")

    monkeypatch.setattr(AgentOrchestrator, "_generate_single_pass", _fake_generate_single_pass)

    blueprint = await orchestrator.generate([
        _make_sf("AAPL"),
        _make_sf("MSFT"),
    ])

    assert [plan.underlying for plan in blueprint.symbol_plans] == ["AAPL", "MSFT"]
    decisions = {item["symbol"]: item for item in blueprint.reasoning_context["post_merge_phase"]["decisions"]}
    assert decisions["AAPL"]["precision_first_score"] > decisions["MSFT"]["precision_first_score"]
    assert decisions["MSFT"]["precision_first_breakdown"]["strategy_scope_penalty"] > 0.0
    assert blueprint.reasoning_context["post_merge_phase"]["ranking_method"] == "precision_first_confidence_quality_portfolio_impact_weighted"


@pytest.mark.asyncio
async def test_chunked_merge_precision_first_prefers_fewer_gate_conflicts(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                orchestrator_chunk_size=1,
                orchestrator_max_parallel=2,
                precision_first=SimpleNamespace(
                    enabled=True,
                    allowed_strategy_types=["single_leg", "vertical_spread"],
                ),
            ),
            risk=SimpleNamespace(
                blueprint_limits=SimpleNamespace(
                    max_daily_loss=2_000.0,
                    max_margin_usage=0.5,
                    portfolio_delta_limit=0.5,
                    portfolio_gamma_limit=0.1,
                )
            ),
        ),
        common=SimpleNamespace(
            watchlist=SimpleNamespace(
                for_data_signal=["AAPL", "MSFT"],
                for_trade_benchmark=[],
            )
        ),
    )

    monkeypatch.setattr(
        "services.analysis_service.app.llm.agents.orchestrator.get_settings",
        lambda: settings,
    )

    orchestrator = AgentOrchestrator(provider=_Provider())

    async def _fake_generate_single_pass(self, **kwargs):
        trade_symbols = kwargs["trade_symbols"]
        if trade_symbols == ["AAPL"]:
            return _make_blueprint(
                [("AAPL", 0.74, "single_leg", "bullish", 0.8, 1)],
                max_total_positions=1,
                analysis_chunk_id="chunk-0",
            )
        if trade_symbols == ["MSFT"]:
            return _make_blueprint(
                [("MSFT", 0.92, "single_leg", "bullish", 0.8, 1)],
                max_total_positions=1,
                analysis_chunk_id="chunk-1",
                agent_outputs={
                    "trend": {
                        "symbols": [
                            {
                                "symbol": "MSFT",
                                "trade_allowed": False,
                                "confidence_cap": 0.6,
                                "simple_structures_only": False,
                                "blocked_reasons": ["counter_trend_setup"],
                            }
                        ]
                    }
                },
            )
        raise AssertionError(f"unexpected trade_symbols: {trade_symbols}")

    monkeypatch.setattr(AgentOrchestrator, "_generate_single_pass", _fake_generate_single_pass)

    blueprint = await orchestrator.generate([
        _make_sf("AAPL"),
        _make_sf("MSFT"),
    ])

    assert [plan.underlying for plan in blueprint.symbol_plans] == ["AAPL", "MSFT"]
    decisions = {item["symbol"]: item for item in blueprint.reasoning_context["post_merge_phase"]["decisions"]}
    assert decisions["AAPL"]["precision_first_score"] > decisions["MSFT"]["precision_first_score"]
    assert decisions["MSFT"]["precision_first_breakdown"]["trade_blocked_agents"] == ["trend"]
    assert decisions["MSFT"]["precision_first_breakdown"]["confidence_caps"]["trend"] == 0.6


@pytest.mark.asyncio
async def test_chunked_generate_uses_market_snapshot_instead_of_benchmark_chunk_injection(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                orchestrator_chunk_size=2,
                orchestrator_max_parallel=2,
            ),
            risk=SimpleNamespace(
                blueprint_limits=SimpleNamespace(
                    max_daily_loss=2_000.0,
                    max_margin_usage=0.5,
                    portfolio_delta_limit=0.5,
                    portfolio_gamma_limit=0.1,
                )
            ),
        ),
        common=SimpleNamespace(
            watchlist=SimpleNamespace(
                for_data_signal=["AAPL", "MSFT", "NVDA"],
                for_trade_benchmark=["SPY", "QQQ"],
            )
        ),
    )

    monkeypatch.setattr(
        "services.analysis_service.app.llm.agents.orchestrator.get_settings",
        lambda: settings,
    )

    orchestrator = AgentOrchestrator(provider=_Provider())
    captured_calls: list[dict[str, object]] = []

    async def _fake_generate_single_pass(self, **kwargs):
        captured_calls.append(
            {
                "signal_symbols": [sf.symbol for sf in kwargs["signal_features"]],
                "trade_symbols": kwargs["trade_symbols"],
                "market_snapshot": kwargs["market_snapshot"],
            }
        )
        return _make_blueprint(
            [(symbol, 0.6) for symbol in kwargs["trade_symbols"]],
            max_total_positions=3,
            analysis_chunk_id=f"chunk-{len(captured_calls) - 1}",
        )

    monkeypatch.setattr(AgentOrchestrator, "_generate_single_pass", _fake_generate_single_pass)

    blueprint = await orchestrator.generate([
        _make_sf("AAPL"),
        _make_sf("MSFT"),
        _make_sf("NVDA"),
        _make_sf("SPY"),
        _make_sf("QQQ"),
    ])

    assert len(captured_calls) == 2
    assert captured_calls[0]["signal_symbols"] == ["AAPL", "MSFT"]
    assert captured_calls[1]["signal_symbols"] == ["NVDA"]
    market_snapshot = captured_calls[0]["market_snapshot"]
    assert market_snapshot is not None
    assert market_snapshot["symbols"] == ["SPY", "QQQ"]
    assert captured_calls[1]["market_snapshot"] == market_snapshot
    assert [plan.underlying for plan in blueprint.symbol_plans] == ["AAPL", "MSFT", "NVDA"]


@pytest.mark.asyncio
async def test_chunked_generate_skips_non_validation_batch_failures(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                orchestrator_chunk_size=1,
                orchestrator_max_parallel=2,
            ),
            risk=SimpleNamespace(
                blueprint_limits=SimpleNamespace(
                    max_daily_loss=2_000.0,
                    max_margin_usage=0.5,
                    portfolio_delta_limit=0.5,
                    portfolio_gamma_limit=0.1,
                )
            ),
        ),
        common=SimpleNamespace(
            watchlist=SimpleNamespace(
                for_data_signal=["AAPL", "MSFT"],
                for_trade_benchmark=[],
            )
        ),
    )

    monkeypatch.setattr(
        "services.analysis_service.app.llm.agents.orchestrator.get_settings",
        lambda: settings,
    )

    orchestrator = AgentOrchestrator(provider=_Provider())

    async def _fake_generate_single_pass(self, **kwargs):
        trade_symbols = kwargs["trade_symbols"]
        if trade_symbols == ["AAPL"]:
            raise RuntimeError("upstream timeout")
        if trade_symbols == ["MSFT"]:
            return _make_blueprint(
                [("MSFT", 0.83)],
                max_total_positions=1,
                analysis_chunk_id=kwargs["analysis_chunk_id"],
            )
        raise AssertionError(f"unexpected trade_symbols: {trade_symbols}")

    monkeypatch.setattr(AgentOrchestrator, "_generate_single_pass", _fake_generate_single_pass)

    blueprint = await orchestrator.generate([
        _make_sf("AAPL"),
        _make_sf("MSFT"),
    ])

    assert [plan.underlying for plan in blueprint.symbol_plans] == ["MSFT"]
    assert blueprint.reasoning_context is not None
    skipped_contexts = [
        ctx for ctx in blueprint.reasoning_context["chunk_contexts"]
        if ctx.get("pipeline") == "agentic_chunk_skipped"
    ]
    assert len(skipped_contexts) == 1
    assert skipped_contexts[0]["trade_symbols"] == ["AAPL"]
    assert skipped_contexts[0]["error_type"] == "RuntimeError"
    assert skipped_contexts[0]["skip_reason"] == "batch_exception"


@pytest.mark.asyncio
async def test_chunked_generate_validation_error_skips_chunk_and_continues_later_batches(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                orchestrator_chunk_size=1,
                orchestrator_max_parallel=1,
            ),
            risk=SimpleNamespace(
                blueprint_limits=SimpleNamespace(
                    max_daily_loss=2_000.0,
                    max_margin_usage=0.5,
                    portfolio_delta_limit=0.5,
                    portfolio_gamma_limit=0.1,
                )
            ),
        ),
        common=SimpleNamespace(
            watchlist=SimpleNamespace(
                for_data_signal=["AAPL", "MSFT"],
                for_trade_benchmark=[],
            )
        ),
    )

    monkeypatch.setattr(
        "services.analysis_service.app.llm.agents.orchestrator.get_settings",
        lambda: settings,
    )

    orchestrator = AgentOrchestrator(provider=_Provider())
    calls: list[tuple[str, ...]] = []

    async def _fake_generate_single_pass(self, **kwargs):
        trade_symbols = tuple(kwargs["trade_symbols"])
        calls.append(trade_symbols)
        if trade_symbols == ("AAPL",):
            LLMTradingBlueprint.model_validate({"symbol_plans": [{"underlying": "AAPL"}]})
        return _make_blueprint(
            [("MSFT", 0.83)],
            max_total_positions=1,
            analysis_chunk_id=kwargs["analysis_chunk_id"],
        )

    monkeypatch.setattr(AgentOrchestrator, "_generate_single_pass", _fake_generate_single_pass)

    blueprint = await orchestrator.generate([
        _make_sf("AAPL"),
        _make_sf("MSFT"),
    ])

    assert calls == [("AAPL",), ("MSFT",)]
    assert [plan.underlying for plan in blueprint.symbol_plans] == ["MSFT"]
    skipped_contexts = [
        ctx for ctx in blueprint.reasoning_context["chunk_contexts"]
        if ctx.get("pipeline") == "agentic_chunk_skipped"
    ]
    assert len(skipped_contexts) == 1
    assert skipped_contexts[0]["error_type"] == "ValidationError"
    assert skipped_contexts[0]["trade_symbols"] == ["AAPL"]


@pytest.mark.asyncio
async def test_chunked_merge_validation_error_from_post_merge_review_is_not_swallowed(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                orchestrator_chunk_size=1,
                orchestrator_max_parallel=2,
                agent_models_override=SimpleNamespace(post_merge=""),
                precision_first=SimpleNamespace(
                    enabled=True,
                    allowed_strategy_types=["single_leg", "vertical_spread"],
                ),
            ),
            risk=SimpleNamespace(
                blueprint_limits=SimpleNamespace(
                    max_daily_loss=2_000.0,
                    max_margin_usage=0.5,
                    portfolio_delta_limit=0.5,
                    portfolio_gamma_limit=0.1,
                )
            ),
        ),
        common=SimpleNamespace(
            watchlist=SimpleNamespace(
                for_data_signal=["AAPL", "MSFT"],
                for_trade_benchmark=[],
            )
        ),
    )

    monkeypatch.setattr(
        "services.analysis_service.app.llm.agents.orchestrator.get_settings",
        lambda: settings,
    )

    orchestrator = AgentOrchestrator(provider=_LLMProvider())

    async def _fake_generate_single_pass(self, **kwargs):
        symbol = kwargs["trade_symbols"][0]
        return _make_blueprint(
            [(symbol, 0.75)],
            max_total_positions=1,
            analysis_chunk_id=kwargs["analysis_chunk_id"],
        )

    async def _fake_review(**kwargs):
        LLMTradingBlueprint.model_validate({"symbol_plans": [{"underlying": "AAPL"}]})

    monkeypatch.setattr(AgentOrchestrator, "_generate_single_pass", _fake_generate_single_pass)
    monkeypatch.setattr(orchestrator._post_merge_portfolio_agent, "review", _fake_review)

    with pytest.raises(ValidationError):
        await orchestrator.generate([
            _make_sf("AAPL"),
            _make_sf("MSFT"),
        ])


@pytest.mark.asyncio
async def test_pre_synthesis_filter_keeps_wide_spread_symbols_for_ranking(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                orchestrator_chunk_size=5,
                orchestrator_max_parallel=2,
            ),
            risk=SimpleNamespace(
                blueprint_limits=SimpleNamespace(
                    max_daily_loss=2_000.0,
                    max_margin_usage=0.5,
                    portfolio_delta_limit=0.5,
                    portfolio_gamma_limit=0.1,
                )
            ),
        ),
        common=SimpleNamespace(
            watchlist=SimpleNamespace(
                for_data_signal=["AAPL", "MSFT", "NVDA"],
                for_trade_benchmark=[],
            )
        ),
    )

    monkeypatch.setattr(
        "services.analysis_service.app.llm.agents.orchestrator.get_settings",
        lambda: settings,
    )

    orchestrator = AgentOrchestrator(provider=_Provider())
    captured_call: dict[str, object] = {}

    async def _fake_generate_single_pass(self, **kwargs):
        captured_call["trade_symbols"] = kwargs["trade_symbols"]
        return _make_blueprint(
            [(symbol, 0.6) for symbol in kwargs["trade_symbols"]],
            max_total_positions=3,
            analysis_chunk_id="single-0",
        )

    monkeypatch.setattr(AgentOrchestrator, "_generate_single_pass", _fake_generate_single_pass)

    blueprint = await orchestrator.generate([
        _make_sf("AAPL"),
        _make_sf("MSFT", data_quality=DataQuality(score=0.2, stock_bar_count=260, option_row_count=10)),
        _make_sf(
            "NVDA",
            option_indicators=OptionIndicators(bid_ask_spread_ratio=0.46),
            data_quality=DataQuality(score=1.0, stock_bar_count=260, option_row_count=200),
        ),
    ])

    assert captured_call["trade_symbols"] == ["AAPL", "NVDA"]
    pre_synthesis_filter = blueprint.reasoning_context["pre_synthesis_filter"]
    assert pre_synthesis_filter["kept_symbol_count"] == 2
    dropped = {
        item["symbol"]: {reason["rule"] for reason in item["reasons"]}
        for item in pre_synthesis_filter["dropped_symbols"]
    }
    assert dropped["MSFT"] == {"data_quality_skip_threshold"}
    assert "NVDA" not in dropped


@pytest.mark.asyncio
async def test_pre_synthesis_filter_drops_symbol_with_no_precision_first_strategy_left(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                orchestrator_chunk_size=5,
                orchestrator_max_parallel=2,
                precision_first=SimpleNamespace(
                    enabled=True,
                    allowed_strategy_types=["single_leg", "vertical_spread", "iron_condor", "calendar_spread"],
                ),
            ),
            risk=SimpleNamespace(
                blueprint_limits=SimpleNamespace(
                    max_daily_loss=2_000.0,
                    max_margin_usage=0.5,
                    portfolio_delta_limit=0.5,
                    portfolio_gamma_limit=0.1,
                )
            ),
        ),
        common=SimpleNamespace(
            watchlist=SimpleNamespace(
                for_data_signal=["AAPL", "MSFT"],
                for_trade_benchmark=[],
            )
        ),
    )

    monkeypatch.setattr(
        "services.analysis_service.app.llm.agents.orchestrator.get_settings",
        lambda: settings,
    )

    orchestrator = AgentOrchestrator(provider=_Provider())
    captured_call: dict[str, object] = {}

    async def _fake_generate_single_pass(self, **kwargs):
        captured_call["trade_symbols"] = kwargs["trade_symbols"]
        return _make_blueprint(
            [(symbol, 0.6) for symbol in kwargs["trade_symbols"]],
            max_total_positions=2,
            analysis_chunk_id="single-0",
        )

    monkeypatch.setattr(AgentOrchestrator, "_generate_single_pass", _fake_generate_single_pass)

    blueprint = await orchestrator.generate([
        _make_sf(
            "AAPL",
            cross_asset_indicators=CrossAssetIndicators(earnings_proximity_days=1),
        ),
        _make_sf("MSFT"),
    ])

    assert captured_call["trade_symbols"] == ["MSFT"]
    pre_synthesis_filter = blueprint.reasoning_context["pre_synthesis_filter"]
    dropped = {item["symbol"]: item for item in pre_synthesis_filter["dropped_symbols"]}
    assert dropped["AAPL"]["eligible_strategy_types"] == []
    assert {reason["rule"] for reason in dropped["AAPL"]["reasons"]} == {"precision_first_no_eligible_strategy"}


@pytest.mark.asyncio
async def test_pre_synthesis_coarse_ranking_orders_all_symbols_for_analysis(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                orchestrator_chunk_size=10,
                orchestrator_max_parallel=2,
            ),
            risk=SimpleNamespace(
                blueprint_limits=SimpleNamespace(
                    max_daily_loss=2_000.0,
                    max_margin_usage=0.5,
                    portfolio_delta_limit=0.5,
                    portfolio_gamma_limit=0.1,
                )
            ),
        ),
        common=SimpleNamespace(
            watchlist=SimpleNamespace(
                for_data_signal=["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META"],
                for_trade_benchmark=[],
            )
        ),
    )

    monkeypatch.setattr(
        "services.analysis_service.app.llm.agents.orchestrator.get_settings",
        lambda: settings,
    )

    orchestrator = AgentOrchestrator(provider=_Provider())
    captured_call: dict[str, object] = {}

    async def _fake_generate_single_pass(self, **kwargs):
        captured_call["trade_symbols"] = kwargs["trade_symbols"]
        return _make_blueprint(
            [(symbol, 0.6) for symbol in kwargs["trade_symbols"]],
            max_total_positions=5,
            analysis_chunk_id="single-0",
        )

    monkeypatch.setattr(AgentOrchestrator, "_generate_single_pass", _fake_generate_single_pass)

    blueprint = await orchestrator.generate([
        _make_sf("AAPL", data_quality=DataQuality(score=1.0, stock_bar_count=260, option_row_count=200)),
        _make_sf("MSFT", data_quality=DataQuality(score=0.95, stock_bar_count=260, option_row_count=180)),
        _make_sf("NVDA", data_quality=DataQuality(score=0.90, stock_bar_count=260, option_row_count=160)),
        _make_sf("TSLA", data_quality=DataQuality(score=0.85, stock_bar_count=260, option_row_count=140)),
        _make_sf("AMZN", data_quality=DataQuality(score=0.80, stock_bar_count=260, option_row_count=120)),
        _make_sf(
            "META",
            option_indicators=OptionIndicators(bid_ask_spread_ratio=0.18),
            data_quality=DataQuality(score=0.45, stock_bar_count=260, option_row_count=40),
        ),
    ])

    assert captured_call["trade_symbols"] == ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META"]
    triage = blueprint.reasoning_context["pre_synthesis_triage"]
    assert triage["analysis_symbol_count"] == 6
    assert triage["ranked_symbol_count"] == 6
    assert triage["analysis_order"] == ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META"]
    assert triage["ranked_symbols"][0]["action"] == "analyze"
    assert triage["ranked_symbols"][-1]["symbol"] == "META"
    assert triage["ranked_symbols"][-1]["action"] == "analyze"
    assert "priority rank 6" in triage["ranked_symbols"][-1]["decision_reason"]


@pytest.mark.asyncio
async def test_pre_synthesis_coarse_ranking_uses_configured_weights_for_ordering(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                orchestrator_chunk_size=10,
                orchestrator_max_parallel=2,
                coarse_ranking=SimpleNamespace(
                    weights=SimpleNamespace(
                        data_quality=0.1,
                        option_coverage=0.1,
                        liquidity=0.6,
                        strategy_eligibility=0.1,
                        earnings_buffer=0.1,
                    ),
                ),
            ),
            risk=SimpleNamespace(
                blueprint_limits=SimpleNamespace(
                    max_daily_loss=2_000.0,
                    max_margin_usage=0.5,
                    portfolio_delta_limit=0.5,
                    portfolio_gamma_limit=0.1,
                )
            ),
        ),
        common=SimpleNamespace(
            watchlist=SimpleNamespace(
                for_data_signal=["AAPL", "MSFT"],
                for_trade_benchmark=[],
            )
        ),
    )

    monkeypatch.setattr(
        "services.analysis_service.app.llm.agents.orchestrator.get_settings",
        lambda: settings,
    )

    orchestrator = AgentOrchestrator(provider=_Provider())
    captured_call: dict[str, object] = {}

    async def _fake_generate_single_pass(self, **kwargs):
        captured_call["trade_symbols"] = kwargs["trade_symbols"]
        return _make_blueprint(
            [(symbol, 0.6) for symbol in kwargs["trade_symbols"]],
            max_total_positions=1,
            analysis_chunk_id="single-0",
        )

    monkeypatch.setattr(AgentOrchestrator, "_generate_single_pass", _fake_generate_single_pass)

    blueprint = await orchestrator.generate([
        _make_sf(
            "AAPL",
            option_indicators=OptionIndicators(bid_ask_spread_ratio=0.15),
            data_quality=DataQuality(score=1.0, stock_bar_count=260, option_row_count=200),
        ),
        _make_sf(
            "MSFT",
            option_indicators=OptionIndicators(bid_ask_spread_ratio=0.0),
            data_quality=DataQuality(score=0.5, stock_bar_count=260, option_row_count=200),
        ),
    ])

    assert captured_call["trade_symbols"] == ["MSFT", "AAPL"]
    triage = blueprint.reasoning_context["pre_synthesis_triage"]
    assert triage["weights"] == {
        "data_quality": 0.1,
        "option_coverage": 0.1,
        "liquidity": 0.6,
        "strategy_eligibility": 0.1,
        "earnings_buffer": 0.1,
    }
    assert triage["analysis_order"] == ["MSFT", "AAPL"]


@pytest.mark.asyncio
async def test_pre_synthesis_filter_returns_valid_empty_blueprint_when_everything_is_dropped(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                orchestrator_chunk_size=5,
                orchestrator_max_parallel=2,
            ),
            risk=SimpleNamespace(
                blueprint_limits=SimpleNamespace(
                    max_daily_loss=2_000.0,
                    max_margin_usage=0.5,
                    portfolio_delta_limit=0.5,
                    portfolio_gamma_limit=0.1,
                )
            ),
        ),
        common=SimpleNamespace(
            watchlist=SimpleNamespace(
                for_data_signal=["AAPL"],
                for_trade_benchmark=[],
            )
        ),
    )

    monkeypatch.setattr(
        "services.analysis_service.app.llm.agents.orchestrator.get_settings",
        lambda: settings,
    )

    orchestrator = AgentOrchestrator(provider=_Provider())

    async def _unexpected_generate_single_pass(self, **kwargs):
        raise AssertionError("_generate_single_pass should not be called when all symbols are filtered")

    monkeypatch.setattr(AgentOrchestrator, "_generate_single_pass", _unexpected_generate_single_pass)

    blueprint = await orchestrator.generate([
        _make_sf("AAPL", data_quality=DataQuality(score=0.2, stock_bar_count=260, option_row_count=10)),
    ])

    assert blueprint.symbol_plans == []
    assert str(blueprint.trading_date) == "2026-04-29"
    assert blueprint.model_provider == "test"
    assert blueprint.model_version == "unknown"
    assert blueprint.reasoning_context["pipeline"] == "agentic_empty"
    assert blueprint.reasoning_context["pre_synthesis_filter"]["dropped_symbol_count"] == 1
    assert blueprint.reasoning_context["pre_synthesis_triage"]["analysis_symbol_count"] == 0