"""Test the LLM prompt serialization pipeline.

Covers cross_asset field completeness, sparse filtering,
and signal data structure.
"""
from __future__ import annotations

import json

import pytest

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


def _make_signal_features(**overrides) -> SignalFeatures:
    """Build a minimal valid SignalFeatures using defaults."""
    defaults = dict(
        symbol="AAPL",
        date="2026-03-24",
        computed_at="2026-03-23T20:00:00",
        close_price=185.50,
        daily_return=0.012,
        volume=50_000_000,
        volatility_regime="normal",
        stock_indicators=StockIndicators(),
        option_indicators=OptionIndicators(),
        cross_asset_indicators=CrossAssetIndicators(),
        data_quality=DataQuality(),
    )
    defaults.update(overrides)
    return SignalFeatures(**defaults)


# ---------------------------------------------------------------------------
# Cross-Asset fields in serialization
# ---------------------------------------------------------------------------


class TestCrossAssetSerialization:
    """Verify _serialize_one_signal includes all multi-benchmark + VIX fields."""

    def test_all_cross_asset_fields_present(self):
        from services.analysis_service.app.llm.prompts import _serialize_one_signal

        sf = _make_signal_features(
            cross_asset_indicators=CrossAssetIndicators(
                spy_beta=1.2,
                index_correlation_20d=0.85,
                qqq_beta=1.1,
                qqq_correlation_20d=0.78,
                iwm_beta=0.9,
                iwm_correlation_20d=0.65,
                tlt_correlation_20d=-0.3,
                vix_level=18.5,
                vix_percentile_52w=0.45,
                vix_correlation_20d=-0.6,
            ),
        )

        text = _serialize_one_signal(sf)
        # Parse the JSON portion (after header line)
        lines = text.split("\n", 1)
        assert len(lines) == 2, "Expected header + JSON"
        data = json.loads(lines[1])

        ca = data.get("cross_asset", {})
        # New multi-benchmark fields
        assert "qqq_beta" in ca
        assert "qqq_correlation_20d" in ca
        assert "iwm_beta" in ca
        assert "iwm_correlation_20d" in ca
        assert "tlt_correlation_20d" in ca
        # VIX fields
        assert "vix_level" in ca
        assert "vix_percentile_52w" in ca
        assert "vix_correlation_20d" in ca

    def test_sparse_filtering_removes_zero_values(self):
        from services.analysis_service.app.llm.prompts import _serialize_one_signal

        sf = _make_signal_features()  # all defaults (mostly 0.0)

        text = _serialize_one_signal(sf)
        lines = text.split("\n", 1)
        assert len(lines) == 2
        data = json.loads(lines[1])

        # stock_trend should not contain fields that are 0.0
        trend = data.get("stock_trend", {})
        # Fields like adx_14=0.0 should be pruned
        for key, val in trend.items():
            if isinstance(val, (int, float)):
                assert val != 0 or key in (
                    # Some fields are semantically meaningful at 0
                    "rsi_divergence", "macd_hist_divergence",
                ), f"Zero-value field {key}={val} should have been pruned"

    def test_header_contains_symbol(self):
        from services.analysis_service.app.llm.prompts import _serialize_one_signal

        sf = _make_signal_features(symbol="TSLA")
        text = _serialize_one_signal(sf)
        assert text.startswith("### TSLA")


# ---------------------------------------------------------------------------
# Signal structure completeness
# ---------------------------------------------------------------------------


class TestSignalStructure:
    def test_all_top_level_sections_present(self):
        from services.analysis_service.app.llm.prompts import _serialize_one_signal

        # Use non-zero values so sparse filtering doesn't prune sections
        si = StockIndicators(adx_14=25.0, rsi_14=55.0, vwap=185.0, cmf_20=0.1, hv_20d=0.25)
        oi = OptionIndicators(iv_rank=0.5, iv_percentile=0.5, current_iv=0.3, pcr_volume=0.8, pcr_oi=0.9)
        sf = _make_signal_features(
            cross_asset_indicators=CrossAssetIndicators(spy_beta=1.1),
            stock_indicators=si,
            option_indicators=oi,
        )
        text = _serialize_one_signal(sf)
        lines = text.split("\n", 1)
        data = json.loads(lines[1])

        # With non-zero values, core sections should be present
        # (option_greeks may be pruned if all greeks are 0.0)
        expected_sections = {
            "price", "stock_trend", "stock_vol", "stock_flow",
            "option_vol_surface", "option_chain",
            "cross_asset",
        }
        assert expected_sections.issubset(set(data.keys())), (
            f"Missing sections: {expected_sections - set(data.keys())}"
        )


# ---------------------------------------------------------------------------
# Degraded-indicator section exclusion
# ---------------------------------------------------------------------------


class TestDegradedSectionExclusion:
    """Verify _serialize_one_signal excludes sections for fully-degraded categories."""

    def _parse_json(self, sf: SignalFeatures) -> dict:
        from services.analysis_service.app.llm.prompts import _serialize_one_signal

        text = _serialize_one_signal(sf)
        return json.loads(text.split("\n", 1)[1])

    def test_stock_all_degraded_excludes_stock_sections(self):
        si = StockIndicators(adx_14=25.0, rsi_14=55.0, vwap=185.0, cmf_20=0.1, hv_20d=0.25)
        oi = OptionIndicators(iv_rank=0.5, current_iv=0.3, pcr_volume=0.8)
        sf = _make_signal_features(
            stock_indicators=si,
            option_indicators=oi,
            cross_asset_indicators=CrossAssetIndicators(spy_beta=1.1),
            data_quality=DataQuality(
                complete=False, score=0.3,
                degraded_indicators=["stock:all"],
            ),
        )
        data = self._parse_json(sf)

        # Stock sections should be absent
        assert "stock_trend" not in data
        assert "stock_vol" not in data
        assert "stock_flow" not in data
        # Option + cross-asset sections should remain
        assert "option_vol_surface" in data
        assert "cross_asset" in data
        # data_quality should note excluded categories
        assert data["data_quality"]["excluded_categories"] == ["stock"]

    def test_option_all_degraded_excludes_option_sections(self):
        si = StockIndicators(adx_14=25.0, rsi_14=55.0, vwap=185.0, cmf_20=0.1, hv_20d=0.25)
        oi = OptionIndicators(iv_rank=0.5, current_iv=0.3, pcr_volume=0.8)
        sf = _make_signal_features(
            stock_indicators=si,
            option_indicators=oi,
            cross_asset_indicators=CrossAssetIndicators(spy_beta=1.1),
            data_quality=DataQuality(
                complete=False, score=0.4,
                degraded_indicators=["option:all"],
            ),
        )
        data = self._parse_json(sf)

        # Option sections should be absent
        assert "option_vol_surface" not in data
        assert "option_greeks" not in data
        assert "option_chain" not in data
        assert "option_spreads" not in data
        # Stock sections should remain
        assert "stock_trend" in data
        assert data["data_quality"]["excluded_categories"] == ["option"]

    def test_no_degradation_keeps_all_sections(self):
        si = StockIndicators(adx_14=25.0, rsi_14=55.0, vwap=185.0, cmf_20=0.1, hv_20d=0.25)
        oi = OptionIndicators(iv_rank=0.5, current_iv=0.3, pcr_volume=0.8)
        sf = _make_signal_features(
            stock_indicators=si,
            option_indicators=oi,
            cross_asset_indicators=CrossAssetIndicators(spy_beta=1.1),
        )
        data = self._parse_json(sf)

        assert "stock_trend" in data
        assert "option_vol_surface" in data
        assert "data_quality" not in data  # complete=True, no section added

    def test_partial_degradation_keeps_all_sections(self):
        si = StockIndicators(adx_14=25.0, rsi_14=55.0)
        sf = _make_signal_features(
            stock_indicators=si,
            data_quality=DataQuality(
                complete=False, score=0.7,
                degraded_indicators=["stock:ema_50", "option:iv_rank"],
            ),
        )
        data = self._parse_json(sf)

        # Partial degradation should NOT exclude sections
        assert "stock_trend" in data
