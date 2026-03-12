"""信号组装器 — 将计算好的指标组装为 SignalFeatures

职责边界：Signal Service 是量化测量仪表，只输出纯数值特征。
交易方向、策略选择、仓位管理由 Analysis Service（LLM）决定。
"""
from __future__ import annotations

from shared.models.signal import CrossAssetIndicators, SignalFeatures, OptionIndicators, StockIndicators
from shared.config import get_settings
from shared.utils import get_logger, now_utc, today_trading

logger = get_logger("signal_generator")


def generate_signal(
    symbol: str,
    close_price: float,
    daily_return: float,
    volume: int,
    option_indicators: OptionIndicators,
    stock_indicators: StockIndicators,
    cross_asset_indicators: CrossAssetIndicators,
    bar_type: str = "unknown",
) -> SignalFeatures:
    """组装 SignalFeatures，仅做 volatility_regime 分类。"""
    settings = get_settings()

    iv = option_indicators.iv_rank
    if iv > settings.option_strategy.iv_threshold_high:
        vol_regime = "high"
    elif iv < settings.option_strategy.iv_threshold_low:
        vol_regime = "low"
    else:
        vol_regime = "normal"

    return SignalFeatures(
        symbol=symbol,
        date=today_trading(),
        computed_at=now_utc(),
        close_price=close_price,
        daily_return=daily_return,
        volume=volume,
        bar_type=bar_type,
        option_indicators=option_indicators,
        stock_indicators=stock_indicators,
        cross_asset_indicators=cross_asset_indicators,
        volatility_regime=vol_regime,
    )
