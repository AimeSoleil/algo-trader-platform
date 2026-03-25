"""Test chunker split and merge logic.

Covers edge cases: 0 symbols, single chunk, multi-chunk,
benchmark injection, and merge strategies.
"""
from __future__ import annotations

from datetime import date

import pytest

from shared.models.blueprint import (
    LLMTradingBlueprint,
    OptionLeg,
    SymbolPlan,
)
from services.analysis_service.app.llm.chunker import (
    merge_blueprints,
    split_signal_features,
)
from shared.models.signal import (
    CrossAssetIndicators,
    DataQuality,
    OptionIndicators,
    SignalFeatures,
    StockIndicators,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_sf(symbol: str) -> SignalFeatures:
    return SignalFeatures(
        symbol=symbol,
        date="2026-03-24",
        computed_at="2026-03-23T20:00:00",
        close_price=100.0,
        daily_return=0.01,
        volume=1_000_000,
        volatility_regime="normal",
        stock_indicators=StockIndicators(),
        option_indicators=OptionIndicators(),
        cross_asset_indicators=CrossAssetIndicators(),
        data_quality=DataQuality(),
    )


def _make_bp(
    plans: list[dict] | None = None,
    market_regime: str = "neutral",
    max_daily_loss: float = 2000.0,
    portfolio_delta_limit: float = 0.5,
) -> LLMTradingBlueprint:
    if plans is None:
        plans = []
    symbol_plans = []
    for p in plans:
        symbol_plans.append(SymbolPlan(
            underlying=p.get("underlying", "AAPL"),
            strategy_type="single_leg",
            direction="bullish",
            legs=[OptionLeg(expiry="2026-04-17", strike=150, option_type="call", side="buy")],
            confidence=p.get("confidence", 0.5),
            max_loss_per_trade=500,
        ))
    return LLMTradingBlueprint(
        trading_date="2026-03-24",
        generated_at="2026-03-23T20:00:00",
        market_regime=market_regime,
        market_analysis="test",
        symbol_plans=symbol_plans,
        max_daily_loss=max_daily_loss,
        portfolio_delta_limit=portfolio_delta_limit,
    )


# ---------------------------------------------------------------------------
# split_signal_features
# ---------------------------------------------------------------------------


class TestSplitSignalFeatures:
    def test_empty_list(self):
        chunks = split_signal_features([], chunk_size=5, benchmark_symbols=["SPY"])
        assert chunks == [[]]

    def test_single_symbol_no_split(self):
        features = [_make_sf("AAPL")]
        chunks = split_signal_features(features, chunk_size=5, benchmark_symbols=["SPY"])
        assert len(chunks) == 1
        assert len(chunks[0]) == 1

    def test_benchmarks_in_every_chunk(self):
        features = [_make_sf(s) for s in ["SPY", "QQQ", "AAPL", "MSFT", "TSLA",
                                           "NVDA", "GOOG", "AMZN", "META", "NFLX"]]
        chunks = split_signal_features(features, chunk_size=3, benchmark_symbols=["SPY", "QQQ"])
        for chunk in chunks:
            symbols = {sf.symbol for sf in chunk}
            assert "SPY" in symbols, "Benchmark SPY missing from chunk"
            assert "QQQ" in symbols, "Benchmark QQQ missing from chunk"

    def test_no_split_when_under_chunk_size(self):
        features = [_make_sf(s) for s in ["SPY", "AAPL", "MSFT"]]
        chunks = split_signal_features(features, chunk_size=5, benchmark_symbols=["SPY"])
        assert len(chunks) == 1

    def test_correct_number_of_chunks(self):
        # 2 benchmarks + 7 non-benchmark, chunk_size=3 → ceil(7/3)=3 chunks
        symbols = ["SPY", "QQQ", "A", "B", "C", "D", "E", "F", "G"]
        features = [_make_sf(s) for s in symbols]
        chunks = split_signal_features(features, chunk_size=3, benchmark_symbols=["SPY", "QQQ"])
        assert len(chunks) == 3


# ---------------------------------------------------------------------------
# merge_blueprints
# ---------------------------------------------------------------------------


class TestMergeBlueprints:
    def test_merge_single_blueprint(self):
        bp = _make_bp(plans=[{"underlying": "AAPL"}])
        merged = merge_blueprints([bp])
        assert len(merged.symbol_plans) == 1
        assert merged.symbol_plans[0].underlying == "AAPL"

    def test_merge_dedup_keeps_higher_confidence(self):
        bp1 = _make_bp(plans=[{"underlying": "AAPL", "confidence": 0.6}])
        bp2 = _make_bp(plans=[{"underlying": "AAPL", "confidence": 0.8}])
        merged = merge_blueprints([bp1, bp2])
        assert len(merged.symbol_plans) == 1
        assert merged.symbol_plans[0].confidence == 0.8

    def test_merge_takes_min_risk_params(self):
        bp1 = _make_bp(max_daily_loss=2000.0, portfolio_delta_limit=0.5)
        bp2 = _make_bp(max_daily_loss=1500.0, portfolio_delta_limit=0.3)
        merged = merge_blueprints([bp1, bp2])
        assert merged.max_daily_loss == 1500.0
        assert merged.portfolio_delta_limit == 0.3

    def test_merge_market_regime_from_first(self):
        bp1 = _make_bp(market_regime="high_vol_bear")
        bp2 = _make_bp(market_regime="neutral")
        merged = merge_blueprints([bp1, bp2])
        assert merged.market_regime == "high_vol_bear"

    def test_merge_distinct_symbols(self):
        bp1 = _make_bp(plans=[{"underlying": "AAPL"}])
        bp2 = _make_bp(plans=[{"underlying": "MSFT"}])
        merged = merge_blueprints([bp1, bp2])
        underlyings = {p.underlying for p in merged.symbol_plans}
        assert underlyings == {"AAPL", "MSFT"}
