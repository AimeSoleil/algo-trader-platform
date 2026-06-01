from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from services.signal_service.app.cross_asset import build_cross_asset_indicators


def _close_path(start: float, returns: list[float]) -> list[float]:
    close_values = [start]
    for daily_return in returns:
        close_values.append(close_values[-1] * (1 + daily_return))
    return close_values


def test_option_vs_stock_volume_ratio_uses_share_equivalent_contract_volume():
    bars_df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=30, freq="D"),
            "close": [100.0 + idx for idx in range(30)],
        }
    )

    result = build_cross_asset_indicators(
        symbol="AAPL",
        bars_df=bars_df,
        bar_returns=bars_df["close"].pct_change().dropna(),
        iv_history=pd.Series(dtype=float),
        benchmark_returns={},
        vix_bars=pd.DataFrame(),
        total_volume=50_000_000,
        total_option_volume=500_000,
        hedge_ratio=0.0,
        trading_date=date(2026, 1, 30),
    )

    assert result.option_vs_stock_volume_ratio == 1.0


def test_stock_iv_correlation_uses_strict_20day_overlapping_window():
    dates = pd.date_range("2026-01-01", periods=25, freq="D")
    returns = [
        0.010, -0.004, 0.012, -0.006, 0.008,
        -0.003, 0.011, -0.005, 0.009, -0.007,
        0.013, -0.002, 0.010, -0.004, 0.012,
        -0.006, 0.008, -0.003, 0.011, -0.005,
        0.009, -0.007, 0.013, -0.002,
    ]
    close_values = _close_path(100.0, returns)
    iv_values = _close_path(0.25, returns)
    bars_df = pd.DataFrame({"timestamp": dates, "close": close_values})
    iv_history = pd.Series(iv_values, index=dates, dtype=float)

    result = build_cross_asset_indicators(
        symbol="AAPL",
        bars_df=bars_df,
        bar_returns=bars_df["close"].pct_change().dropna(),
        iv_history=iv_history,
        benchmark_returns={},
        vix_bars=pd.DataFrame(),
        total_volume=50_000_000,
        total_option_volume=500_000,
        hedge_ratio=0.0,
        trading_date=date(2026, 1, 25),
    )

    assert result.stock_iv_correlation == pytest.approx(1.0, abs=1e-6)


def test_stock_iv_correlation_requires_20_overlapping_daily_observations():
    dates = pd.date_range("2026-01-01", periods=15, freq="D")
    returns = [0.01, -0.01, 0.008, -0.004, 0.009, -0.003, 0.007, -0.002, 0.006, -0.001, 0.005, -0.002, 0.004, -0.001]
    bars_df = pd.DataFrame({"timestamp": dates, "close": _close_path(100.0, returns)})
    iv_history = pd.Series(_close_path(0.25, returns), index=dates, dtype=float)

    result = build_cross_asset_indicators(
        symbol="AAPL",
        bars_df=bars_df,
        bar_returns=bars_df["close"].pct_change().dropna(),
        iv_history=iv_history,
        benchmark_returns={},
        vix_bars=pd.DataFrame(),
        total_volume=50_000_000,
        total_option_volume=500_000,
        hedge_ratio=0.0,
        trading_date=date(2026, 1, 15),
    )

    assert result.stock_iv_correlation == 0.0