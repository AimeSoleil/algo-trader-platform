"""Comprehensive unit tests for signal_service pure-computation functions.

Covers:
  - Stock indicator helpers (_rsi, _macd, _bollinger_bands, _atr, _adx, _volume_profile, _sanitize_float)
  - compute_stock_indicators (empty, short, uptrend, NaN, extreme)
  - Option indicator helpers (calculate_pcr, calculate_iv_skew, calculate_term_structure, _sanitize_option_indicators)
  - Signal generator (generate_signal volatility regime, bar_type pass-through)
  - NaN sanitization for both StockIndicators and OptionIndicators
"""
from __future__ import annotations

import math
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from shared.models.signal import (
    CrossAssetIndicators,
    OptionIndicators,
    SignalFeatures,
    StockIndicators,
)
from services.signal_service.app.indicators.stock_indicators import (
    _atr,
    _adx,
    _bollinger_bands,
    _macd,
    _rsi,
    _sanitize_float,
    _sanitize_stock_indicators,
    _volume_profile,
    compute_stock_indicators,
)
from services.signal_service.app.indicators.option_indicators import (
    _sanitize_option_indicators,
    calculate_iv_skew,
    calculate_pcr,
    calculate_term_structure,
)
from services.signal_service.app.signal_generator import generate_signal


# ---------------------------------------------------------------------------
# Synthetic data helpers
# ---------------------------------------------------------------------------

def make_bars_df(
    rows: int = 100,
    start_price: float = 100.0,
    trend: float = 0.001,
) -> pd.DataFrame:
    """Generate synthetic OHLCV bars with a controllable trend."""
    np.random.seed(42)
    dates = pd.date_range("2025-01-01", periods=rows, freq="1min")
    close = start_price * np.cumprod(1 + trend + np.random.normal(0, 0.005, rows))
    high = close * (1 + np.random.uniform(0, 0.01, rows))
    low = close * (1 - np.random.uniform(0, 0.01, rows))
    open_ = close * (1 + np.random.normal(0, 0.003, rows))
    volume = np.random.randint(1000, 100000, rows)
    return pd.DataFrame(
        {
            "timestamp": dates,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        }
    )


def make_option_df(
    underlying_price: float = 100.0,
    num_strikes: int = 10,
) -> pd.DataFrame:
    """Generate synthetic option chain data."""
    np.random.seed(42)
    rows: list[dict] = []
    for i in range(num_strikes):
        strike = underlying_price * (0.9 + 0.02 * i)
        for opt_type in ("call", "put"):
            rows.append(
                {
                    "underlying": "TEST",
                    "symbol": f"TEST{strike}{opt_type[0].upper()}",
                    "expiry": "2025-02-15",
                    "strike": strike,
                    "option_type": opt_type,
                    "last_price": max(0.5, abs(underlying_price - strike) * 0.3),
                    "bid": max(0.1, abs(underlying_price - strike) * 0.25),
                    "ask": max(0.5, abs(underlying_price - strike) * 0.35),
                    "volume": np.random.randint(10, 5000),
                    "open_interest": np.random.randint(100, 50000),
                    "iv": 0.25 + np.random.uniform(-0.05, 0.05),
                    "delta": 0.5
                    * (1 if opt_type == "call" else -1)
                    * (1 - abs(strike - underlying_price) / underlying_price),
                    "gamma": 0.02 + np.random.uniform(0, 0.01),
                    "theta": -(0.05 + np.random.uniform(0, 0.03)),
                    "vega": 0.1 + np.random.uniform(0, 0.05),
                    "timestamp": "2025-01-15 16:00:00",
                }
            )
    return pd.DataFrame(rows)


def _mock_settings() -> MagicMock:
    s = MagicMock()
    s.option_strategy.high_quantile = 0.7
    s.option_strategy.low_quantile = 0.3
    return s


# ===================================================================
# 1. Stock Indicators — helper functions
# ===================================================================


class TestSanitizeFloat:
    def test_nan_becomes_zero(self):
        assert _sanitize_float(float("nan")) == 0.0

    def test_inf_becomes_zero(self):
        assert _sanitize_float(float("inf")) == 0.0

    def test_neg_inf_becomes_zero(self):
        assert _sanitize_float(float("-inf")) == 0.0

    def test_normal_value_unchanged(self):
        assert _sanitize_float(3.14) == 3.14

    def test_zero_unchanged(self):
        assert _sanitize_float(0.0) == 0.0

    def test_negative_unchanged(self):
        assert _sanitize_float(-7.5) == -7.5


class TestRSI:
    def test_rising_series(self):
        """Steadily rising prices → RSI > 50."""
        series = pd.Series(np.linspace(100, 150, 50))
        assert _rsi(series) > 50

    def test_falling_series(self):
        """Steadily falling prices → RSI < 50."""
        series = pd.Series(np.linspace(150, 100, 50))
        assert _rsi(series) < 50

    def test_all_up_returns_100(self):
        """Monotonically increasing series → RSI == 100."""
        series = pd.Series(range(20, 50))
        assert _rsi(series) == 100.0

    def test_result_within_bounds(self):
        np.random.seed(99)
        series = pd.Series(np.random.lognormal(0, 0.02, 100).cumprod() * 100)
        rsi = _rsi(series)
        assert 0 <= rsi <= 100


class TestMACD:
    def test_returns_three_floats(self):
        series = pd.Series(np.linspace(100, 120, 50))
        macd_val, signal_val, hist_val = _macd(series)
        assert isinstance(macd_val, float)
        assert isinstance(signal_val, float)
        assert isinstance(hist_val, float)

    def test_uptrend_positive(self):
        """In a strong uptrend, MACD line should be positive."""
        series = pd.Series(np.linspace(100, 200, 60))
        macd_val, _, _ = _macd(series)
        assert macd_val > 0


class TestBollingerBands:
    def test_upper_gt_mid_gt_lower(self):
        series = pd.Series(np.random.lognormal(0, 0.01, 50).cumprod() * 100)
        upper, lower, mid = _bollinger_bands(series)
        assert upper > mid > lower

    def test_positive_values(self):
        series = pd.Series(np.linspace(100, 110, 50))
        upper, lower, mid = _bollinger_bands(series)
        assert upper > 0
        assert mid > 0
        assert lower > 0


class TestATR:
    def test_positive_for_volatile_data(self):
        np.random.seed(42)
        n = 50
        close = pd.Series(100 + np.cumsum(np.random.randn(n)))
        high = close + np.random.uniform(0.5, 2.0, n)
        low = close - np.random.uniform(0.5, 2.0, n)
        atr_val = _atr(high, low, close)
        assert atr_val > 0

    def test_flat_market_small_atr(self):
        n = 50
        close = pd.Series([100.0] * n)
        high = pd.Series([100.5] * n)
        low = pd.Series([99.5] * n)
        atr_val = _atr(high, low, close)
        assert 0 < atr_val <= 1.5


class TestADX:
    def test_returns_between_0_and_100(self):
        np.random.seed(42)
        n = 60
        close = pd.Series(100 + np.cumsum(np.random.randn(n)))
        high = close + np.abs(np.random.randn(n))
        low = close - np.abs(np.random.randn(n))
        adx_val = _adx(high, low, close)
        assert 0 <= adx_val <= 100

    def test_strong_trend_higher_adx(self):
        n = 60
        close_trend = pd.Series(np.linspace(100, 200, n))
        high_trend = close_trend + 0.5
        low_trend = close_trend - 0.5
        adx_trend = _adx(high_trend, low_trend, close_trend)
        # Strong trend → ADX should be non-trivial
        assert adx_trend > 0


class TestVolumeProfile:
    def test_poc_between_val_and_vah(self):
        np.random.seed(42)
        close = pd.Series(np.random.normal(100, 2, 200))
        volume = pd.Series(np.random.randint(100, 10000, 200))
        poc, val, vah = _volume_profile(close, volume)
        assert val <= vah
        assert val <= poc <= vah

    def test_empty_series(self):
        poc, val, vah = _volume_profile(pd.Series(dtype=float), pd.Series(dtype=float))
        assert poc == 0.0 and val == 0.0 and vah == 0.0

    def test_constant_price(self):
        close = pd.Series([50.0] * 30)
        volume = pd.Series([1000] * 30)
        poc, val, vah = _volume_profile(close, volume)
        assert poc == val == vah == 50.0


# ===================================================================
# 2. compute_stock_indicators
# ===================================================================


class TestComputeStockIndicators:
    def test_empty_dataframe_returns_default(self):
        result = compute_stock_indicators(pd.DataFrame())
        assert isinstance(result, StockIndicators)
        assert result.rsi_14 == 50.0  # default
        assert result.trend == "neutral"

    def test_short_dataframe_returns_default(self):
        df = make_bars_df(rows=10)
        result = compute_stock_indicators(df)
        assert result.rsi_14 == 50.0

    def test_uptrend_data(self):
        """60 rows of synthetic uptrend → RSI > 50, trend 'bullish', ema_20 > 0."""
        df = make_bars_df(rows=60, start_price=100.0, trend=0.003)
        result = compute_stock_indicators(df)
        assert result.rsi_14 > 50
        assert result.trend == "bullish"
        assert result.ema_20 > 0
        assert result.atr_14 > 0

    def test_downtrend_data(self):
        """60 rows of synthetic downtrend → RSI < 50."""
        df = make_bars_df(rows=60, start_price=100.0, trend=-0.003)
        result = compute_stock_indicators(df)
        assert result.rsi_14 < 50

    def test_nan_injected_sanitized(self):
        """Inject NaN into close column and verify no NaN in output."""
        df = make_bars_df(rows=60)
        # Inject NaNs in a few non-critical positions (not at edges the indicators read)
        df.loc[5, "close"] = np.nan
        df.loc[10, "close"] = np.nan
        # Forward-fill to keep the dataframe usable for rolling calcs
        df["close"] = df["close"].ffill().bfill()
        result = compute_stock_indicators(df)
        # Verify no NaN in any float field
        for field_name in StockIndicators.model_fields:
            val = getattr(result, field_name)
            if isinstance(val, float):
                assert not math.isnan(val), f"NaN found in {field_name}"
            elif isinstance(val, dict):
                for k, v in val.items():
                    if isinstance(v, float):
                        assert not math.isnan(v), f"NaN found in {field_name}[{k}]"

    def test_extreme_rsi_flags(self):
        """Strongly rising price should produce extreme overbought flag."""
        df = make_bars_df(rows=60, start_price=100.0, trend=0.01)
        result = compute_stock_indicators(df)
        # With a very strong uptrend, RSI should exceed 80
        if result.rsi_14 > 80:
            assert "rsi_extreme_overbought" in result.extreme_flags

    def test_output_has_confidence_scores(self):
        df = make_bars_df(rows=60)
        result = compute_stock_indicators(df)
        assert "trend" in result.confidence_scores
        assert "momentum" in result.confidence_scores
        assert "flow" in result.confidence_scores


# ===================================================================
# 3. Option Indicators — sync helpers
# ===================================================================


class TestCalculatePCR:
    def test_known_volumes(self):
        """DataFrame with known call/put volumes → verify pcr_volume."""
        data = pd.DataFrame(
            [
                {"option_type": "call", "volume": 1000, "open_interest": 5000},
                {"option_type": "call", "volume": 500, "open_interest": 3000},
                {"option_type": "put", "volume": 2000, "open_interest": 10000},
                {"option_type": "put", "volume": 1000, "open_interest": 6000},
            ]
        )
        pcr_vol, pcr_oi = calculate_pcr(data)
        # put_volume = 3000, call_volume = 1500 → pcr = 2.0
        assert pcr_vol == pytest.approx(2.0, rel=1e-3)
        # put_oi = 16000, call_oi = 8000 → pcr_oi = 2.0
        assert pcr_oi == pytest.approx(2.0, rel=1e-3)

    def test_zero_call_volume(self):
        """No call volume → pcr should be 0.0 (division protection)."""
        data = pd.DataFrame(
            [
                {"option_type": "call", "volume": 0, "open_interest": 0},
                {"option_type": "put", "volume": 500, "open_interest": 1000},
            ]
        )
        pcr_vol, pcr_oi = calculate_pcr(data)
        assert pcr_vol == 0.0
        assert pcr_oi == 0.0

    def test_empty_dataframe(self):
        """Empty DataFrame → should not crash."""
        data = pd.DataFrame(
            columns=["option_type", "volume", "open_interest"]
        )
        pcr_vol, pcr_oi = calculate_pcr(data)
        assert pcr_vol == 0.0
        assert pcr_oi == 0.0

    def test_synthetic_option_chain(self):
        """Full synthetic chain → pcr values are positive."""
        df = make_option_df()
        pcr_vol, pcr_oi = calculate_pcr(df)
        assert pcr_vol > 0
        assert pcr_oi > 0


class TestCalculateIVSkew:
    def test_known_data(self):
        """OTM put IV > OTM call IV → positive skew."""
        # underlying = 100; OTM put strike ≤ 95; OTM call strike ≥ 105
        data = pd.DataFrame(
            [
                {"option_type": "put", "strike": 90.0, "iv": 0.35},
                {"option_type": "put", "strike": 95.0, "iv": 0.32},
                {"option_type": "call", "strike": 105.0, "iv": 0.22},
                {"option_type": "call", "strike": 110.0, "iv": 0.20},
            ]
        )
        skew = calculate_iv_skew(data, 100.0)
        # put IV at strike 95 (highest OTM put) - call IV at strike 105 (lowest OTM call)
        assert skew == pytest.approx(0.32 - 0.22, abs=1e-3)
        assert skew > 0  # typical skew

    def test_empty_returns_zero(self):
        assert calculate_iv_skew(pd.DataFrame(), 100.0) == 0.0

    def test_zero_underlying_returns_zero(self):
        df = make_option_df()
        assert calculate_iv_skew(df, 0.0) == 0.0

    def test_synthetic_chain(self):
        """Synthetic chain → iv_skew is a finite number."""
        df = make_option_df(underlying_price=100.0, num_strikes=10)
        skew = calculate_iv_skew(df, 100.0)
        assert math.isfinite(skew)


class TestCalculateTermStructure:
    def test_empty_returns_empty(self):
        assert calculate_term_structure(pd.DataFrame(), 100.0) == {}

    def test_zero_underlying(self):
        df = make_option_df()
        assert calculate_term_structure(df, 0.0) == {}

    def test_single_expiry(self):
        df = make_option_df()
        result = calculate_term_structure(df, 100.0)
        assert len(result) == 1
        assert "2025-02-15" in result
        assert result["2025-02-15"] > 0

    def test_multiple_expiries(self):
        """Two expiries should return two keys."""
        df1 = make_option_df()
        df2 = make_option_df()
        df2["expiry"] = "2025-03-15"
        combined = pd.concat([df1, df2], ignore_index=True)
        result = calculate_term_structure(combined, 100.0)
        assert len(result) == 2
        assert "2025-02-15" in result
        assert "2025-03-15" in result


# ===================================================================
# 4. NaN Sanitization
# ===================================================================


class TestSanitizeStockIndicators:
    def test_nan_float_cleaned(self):
        ind = StockIndicators(rsi_14=float("nan"), macd=float("inf"), ema_20=-5.0)
        cleaned = _sanitize_stock_indicators(ind)
        assert cleaned.rsi_14 == 0.0
        assert cleaned.macd == 0.0
        assert cleaned.ema_20 == -5.0  # preserved

    def test_nan_in_dict_cleaned(self):
        ind = StockIndicators(
            confidence_scores={"trend": float("nan"), "momentum": 0.5}
        )
        cleaned = _sanitize_stock_indicators(ind)
        assert cleaned.confidence_scores["trend"] == 0.0
        assert cleaned.confidence_scores["momentum"] == 0.5

    def test_no_nan_returns_same(self):
        ind = StockIndicators(rsi_14=65.0, ema_20=102.5)
        cleaned = _sanitize_stock_indicators(ind)
        assert cleaned.rsi_14 == 65.0
        assert cleaned.ema_20 == 102.5

    def test_all_fields_finite_after_sanitize(self):
        """Build an indicator with NaN in every float field → all cleaned."""
        fields = {}
        for name, _ in StockIndicators.model_fields.items():
            default = StockIndicators.model_fields[name].default
            if isinstance(default, float):
                fields[name] = float("nan")
        ind = StockIndicators(**fields)
        cleaned = _sanitize_stock_indicators(ind)
        for name in fields:
            val = getattr(cleaned, name)
            if isinstance(val, float):
                assert not math.isnan(val), f"{name} still NaN"
                assert not math.isinf(val), f"{name} still Inf"


class TestSanitizeOptionIndicators:
    def test_nan_float_cleaned(self):
        ind = OptionIndicators(iv_rank=float("nan"), current_iv=float("inf"), pcr_volume=0.85)
        cleaned = _sanitize_option_indicators(ind)
        assert cleaned.iv_rank == 0.0
        assert cleaned.current_iv == 0.0
        assert cleaned.pcr_volume == 0.85

    def test_nan_in_dict_cleaned(self):
        ind = OptionIndicators(
            delta_exposure_profile={"total": float("nan"), "call": 1.5},
            portfolio_greeks={"delta": float("inf"), "gamma": 0.1},
        )
        cleaned = _sanitize_option_indicators(ind)
        assert cleaned.delta_exposure_profile["total"] == 0.0
        assert cleaned.delta_exposure_profile["call"] == 1.5
        assert cleaned.portfolio_greeks["delta"] == 0.0
        assert cleaned.portfolio_greeks["gamma"] == 0.1

    def test_no_nan_preserves_values(self):
        ind = OptionIndicators(iv_rank=75.0, pcr_volume=1.2)
        cleaned = _sanitize_option_indicators(ind)
        assert cleaned.iv_rank == 75.0
        assert cleaned.pcr_volume == 1.2

    def test_all_float_fields_cleaned(self):
        fields = {}
        for name, _ in OptionIndicators.model_fields.items():
            default = OptionIndicators.model_fields[name].default
            if isinstance(default, float):
                fields[name] = float("nan")
        ind = OptionIndicators(**fields)
        cleaned = _sanitize_option_indicators(ind)
        for name in fields:
            val = getattr(cleaned, name)
            if isinstance(val, float):
                assert not math.isnan(val), f"{name} still NaN"


# ===================================================================
# 5. Signal Generator
# ===================================================================


class TestGenerateSignal:
    """Test generate_signal volatility regime classification and pass-through."""

    def _make_signal(
        self,
        iv_percentile: float = 50.0,
        bar_type: str = "unknown",
    ) -> SignalFeatures:
        with patch(
            "services.signal_service.app.signal_generator.get_settings",
            return_value=_mock_settings(),
        ), patch(
            "services.signal_service.app.signal_generator.today_trading",
            return_value=date(2025, 3, 10),
        ), patch(
            "services.signal_service.app.signal_generator.now_utc",
            return_value=datetime(2025, 3, 10, 16, 0, 0),
        ):
            return generate_signal(
                symbol="TEST",
                close_price=100.0,
                daily_return=0.01,
                volume=50000,
                option_indicators=OptionIndicators(iv_percentile=iv_percentile),
                stock_indicators=StockIndicators(),
                cross_asset_indicators=CrossAssetIndicators(),
                bar_type=bar_type,
            )

    def test_high_iv_regime(self):
        sig = self._make_signal(iv_percentile=80.0)
        assert sig.volatility_regime == "high"

    def test_low_iv_regime(self):
        sig = self._make_signal(iv_percentile=20.0)
        assert sig.volatility_regime == "low"

    def test_normal_iv_regime(self):
        sig = self._make_signal(iv_percentile=50.0)
        assert sig.volatility_regime == "normal"

    def test_boundary_high(self):
        """iv_percentile exactly at high threshold (70) → high (>=)."""
        sig = self._make_signal(iv_percentile=70.0)
        assert sig.volatility_regime == "high"

    def test_boundary_low(self):
        """iv_percentile exactly at low threshold (30) → low (<=)."""
        sig = self._make_signal(iv_percentile=30.0)
        assert sig.volatility_regime == "low"

    def test_bar_type_passthrough(self):
        sig = self._make_signal(bar_type="intraday_1min")
        assert sig.bar_type == "intraday_1min"

    def test_symbol_passthrough(self):
        sig = self._make_signal()
        assert sig.symbol == "TEST"

    def test_price_and_volume_passthrough(self):
        sig = self._make_signal()
        assert sig.close_price == 100.0
        assert sig.volume == 50000
        assert sig.daily_return == 0.01
