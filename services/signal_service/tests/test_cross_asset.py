from __future__ import annotations

from datetime import date

import pandas as pd

from services.signal_service.app.cross_asset import build_cross_asset_indicators


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
        option_df=pd.DataFrame(),
        benchmark_returns={},
        vix_bars=pd.DataFrame(),
        total_volume=50_000_000,
        total_option_volume=500_000,
        hedge_ratio=0.0,
        trading_date=date(2026, 1, 30),
    )

    assert result.option_vs_stock_volume_ratio == 1.0