from __future__ import annotations

from types import SimpleNamespace

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


def _make_sf(symbol: str) -> SignalFeatures:
    return SignalFeatures(
        symbol=symbol,
        date="2026-04-29",
        computed_at="2026-04-28T20:00:00",
        close_price=100.0,
        daily_return=0.01,
        volume=1_000_000,
        volatility_regime="normal",
        stock_indicators=StockIndicators(),
        option_indicators=OptionIndicators(),
        cross_asset_indicators=CrossAssetIndicators(),
        data_quality=DataQuality(),
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
        reasoning_context={"analysis_chunk_id": analysis_chunk_id} if analysis_chunk_id else None,
    )


class _Provider:
    name = "test"


class _LLMProvider:
    name = "test"

    async def generate(self, *args, **kwargs):
        raise AssertionError("generate should not be called when review is monkeypatched")


@pytest.mark.asyncio
async def test_chunked_merge_trims_plans_to_max_total_positions(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                orchestrator_chunk_size=2,
                orchestrator_max_parallel=2,
            )
        ),
        trade_service=SimpleNamespace(
            risk=SimpleNamespace(
                blueprint_limits=SimpleNamespace(
                    max_daily_loss=2_000.0,
                    max_margin_usage=0.5,
                    portfolio_delta_limit=0.5,
                    portfolio_gamma_limit=0.1,
                )
            )
        ),
        common=SimpleNamespace(
            watchlist=SimpleNamespace(
                for_trade=["AAPL", "MSFT", "NVDA", "TSLA"],
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

    assert blueprint.max_total_positions == 2
    assert blueprint.max_daily_loss == 2_000.0
    assert blueprint.max_margin_usage == 0.5
    assert blueprint.portfolio_delta_limit == 0.5
    assert blueprint.portfolio_gamma_limit == 0.1
    assert len(blueprint.symbol_plans) == 2
    assert [plan.underlying for plan in blueprint.symbol_plans] == ["MSFT", "NVDA"]
    assert [plan.confidence for plan in blueprint.symbol_plans] == [0.91, 0.87]
    assert blueprint.reasoning_context is not None
    assert blueprint.reasoning_context["post_merge_phase"]["selected_symbols"] == ["MSFT", "NVDA"]
    assert blueprint.reasoning_context["post_merge_phase"]["final_limit_sources"]["max_daily_loss"]["source"] == "risk_policy"


@pytest.mark.asyncio
async def test_chunked_merge_prefers_higher_scoring_duplicate_symbol(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                orchestrator_chunk_size=2,
                orchestrator_max_parallel=2,
            )
        ),
        trade_service=SimpleNamespace(
            risk=SimpleNamespace(
                blueprint_limits=SimpleNamespace(
                    max_daily_loss=2_000.0,
                    max_margin_usage=0.5,
                    portfolio_delta_limit=0.5,
                    portfolio_gamma_limit=0.1,
                )
            )
        ),
        common=SimpleNamespace(
            watchlist=SimpleNamespace(
                for_trade=["AAPL", "MSFT", "NVDA"],
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

    assert [plan.underlying for plan in blueprint.symbol_plans] == ["AAPL", "MSFT"]
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
            )
        ),
        trade_service=SimpleNamespace(
            risk=SimpleNamespace(
                blueprint_limits=SimpleNamespace(
                    max_daily_loss=2_000.0,
                    max_margin_usage=0.5,
                    portfolio_delta_limit=0.5,
                    portfolio_gamma_limit=0.1,
                )
            )
        ),
        common=SimpleNamespace(
            watchlist=SimpleNamespace(
                for_trade=["AAPL", "MSFT", "NVDA"],
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

    assert [plan.underlying for plan in blueprint.symbol_plans] == ["MSFT", "AAPL"]
    decisions = {item["symbol"]: item for item in blueprint.reasoning_context["post_merge_phase"]["decisions"]}
    assert decisions["MSFT"]["portfolio_impact_score"] > decisions["AAPL"]["portfolio_impact_score"]
    assert "size_penalty" in decisions["MSFT"]["portfolio_impact_breakdown"]
    assert "strategy_penalty" in decisions["MSFT"]["portfolio_impact_breakdown"]
    assert blueprint.reasoning_context["post_merge_phase"]["ranking_method"] == "confidence_quality_portfolio_impact_weighted"


@pytest.mark.asyncio
async def test_chunked_merge_penalizes_same_direction_existing_holding_and_concentration(monkeypatch):
    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                orchestrator_chunk_size=1,
                orchestrator_max_parallel=2,
            )
        ),
        trade_service=SimpleNamespace(
            risk=SimpleNamespace(
                blueprint_limits=SimpleNamespace(
                    max_daily_loss=2_000.0,
                    max_margin_usage=0.5,
                    portfolio_delta_limit=0.5,
                    portfolio_gamma_limit=0.1,
                )
            )
        ),
        common=SimpleNamespace(
            watchlist=SimpleNamespace(
                for_trade=["AAPL", "MSFT"],
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
            )
        ),
        trade_service=SimpleNamespace(
            risk=SimpleNamespace(
                blueprint_limits=SimpleNamespace(
                    max_daily_loss=2_000.0,
                    max_margin_usage=0.5,
                    portfolio_delta_limit=0.5,
                    portfolio_gamma_limit=0.1,
                )
            )
        ),
        common=SimpleNamespace(
            watchlist=SimpleNamespace(
                for_trade=["AAPL", "MSFT", "NVDA"],
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
        trade_symbols = kwargs["trade_symbols"]
        if trade_symbols == ["AAPL", "MSFT"]:
            return _make_blueprint(
                [
                    ("AAPL", 0.72, "single_leg", "bullish", 1.0, 1),
                    ("MSFT", 0.88, "single_leg", "bullish", 0.6, 1),
                ],
                max_total_positions=2,
                analysis_chunk_id="chunk-0",
            )
        if trade_symbols == ["NVDA"]:
            return _make_blueprint(
                [("NVDA", 0.7)],
                max_total_positions=2,
                analysis_chunk_id="chunk-1",
            )
        raise AssertionError(f"unexpected trade_symbols: {trade_symbols}")

    async def _fake_review(**kwargs):
        return PostMergeReview(
            selected_symbols=["AAPL", "MSFT"],
            ranking=["AAPL", "MSFT", "NVDA"],
            portfolio_summary="Prefer AAPL first for portfolio balance.",
            risk_notes=["Avoid over-concentrating into one tech winner narrative."],
            conflict_explanations=[
                PostMergeConflictExplanation(
                    symbol="AAPL",
                    decision="keep",
                    rationale="AAPL is promoted by global portfolio ranking despite lower raw heuristic score.",
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

    assert [plan.underlying for plan in blueprint.symbol_plans] == ["AAPL", "MSFT"]
    llm_review = blueprint.reasoning_context["post_merge_phase"]["llm_review"]
    assert llm_review["status"] == "applied"
    assert llm_review["ranking"] == ["AAPL", "MSFT", "NVDA"]
    assert llm_review["portfolio_summary"] == "Prefer AAPL first for portfolio balance."