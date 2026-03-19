"""信号组装器 — 将计算好的指标组装为 SignalFeatures

职责边界：Signal Service 是量化测量仪表，只输出纯数值特征。
交易方向、策略选择、仓位管理由 Analysis Service（LLM）决定。
"""
from __future__ import annotations

from datetime import date

from shared.models.signal import CrossAssetIndicators, DataQuality, SignalFeatures, OptionIndicators, StockIndicators
from shared.config import get_settings
from shared.data_quality import (
    DataQualityConfig,
    build_quality_warnings,
    compute_quality_score,
)
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
    trading_date: date | None = None,
    stock_bar_count: int = 0,
    option_row_count: int = 0,
) -> SignalFeatures:
    """组装 SignalFeatures，做 volatility_regime 分类和数据质量标注。"""
    settings = get_settings()

    iv_pct = option_indicators.iv_percentile
    high_thr = settings.option_strategy.high_quantile * 100  # e.g. 70.0
    low_thr  = settings.option_strategy.low_quantile  * 100  # e.g. 30.0
    if iv_pct >= high_thr:
        vol_regime = "high"
    elif iv_pct <= low_thr:
        vol_regime = "low"
    else:
        vol_regime = "normal"

    # ── Build data quality annotation ──
    # 合并股票 + 期权降级指标列表
    all_degraded = (
        stock_indicators.degraded_indicators
        + option_indicators.degraded_indicators
    )
    # 使用集中化的质量评估模块（权重可通过 config.yaml 调节）
    dq_cfg = DataQualityConfig.from_settings(settings)
    warnings = build_quality_warnings(stock_bar_count, option_row_count)
    is_complete = not all_degraded and not warnings
    quality_score = compute_quality_score(
        stock_bar_count, option_row_count, all_degraded, cfg=dq_cfg,
    )

    data_quality = DataQuality(
        complete=is_complete,
        score=quality_score,
        warnings=warnings,
        stock_bar_count=stock_bar_count,
        option_row_count=option_row_count,
        degraded_indicators=all_degraded,
    )

    return SignalFeatures(
        symbol=symbol,
        date=trading_date or today_trading(),
        computed_at=now_utc(),
        close_price=close_price,
        daily_return=daily_return,
        volume=volume,
        bar_type=bar_type,
        option_indicators=option_indicators,
        stock_indicators=stock_indicators,
        cross_asset_indicators=cross_asset_indicators,
        volatility_regime=vol_regime,
        data_quality=data_quality,
    )
