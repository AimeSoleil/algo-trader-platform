"""信号生成器 — 综合期权+股票指标生成交易信号"""
from __future__ import annotations

from datetime import date, datetime

from shared.models.signal import CrossAssetIndicators, SignalFeatures, OptionIndicators, StockIndicators
from shared.config import get_settings
from shared.utils import get_logger

logger = get_logger("signal_generator")


def generate_signal(
    symbol: str,
    close_price: float,
    daily_return: float,
    volume: int,
    option_indicators: OptionIndicators,
    stock_indicators: StockIndicators,
    cross_asset_indicators: CrossAssetIndicators,
) -> SignalFeatures:
    """综合期权+股票指标生成交易信号"""
    settings = get_settings()

    # ── 综合评分 ──
    score = 0.0
    strategies: list[str] = []

    # 1) IV Rank 信号
    iv = option_indicators.iv_rank
    iv_high = settings.option_strategy.iv_threshold_high
    iv_low = settings.option_strategy.iv_threshold_low

    if iv > iv_high:
        # 高 IV → 卖权策略
        score -= 0.3
        strategies.extend(["iron_condor", "vertical_spread_credit", "strangle_sell"])
        vol_regime = "high"
    elif iv < iv_low:
        # 低 IV → 买权策略
        score += 0.2
        strategies.extend(["straddle_buy", "calendar_spread"])
        vol_regime = "low"
    else:
        # 中等 IV → 价差策略
        strategies.append("vertical_spread")
        vol_regime = "normal"

    # 2) PCR 信号
    pcr = option_indicators.pcr_volume
    if pcr > 1.5:
        score -= 0.2  # 看空
    elif pcr < 0.5:
        score += 0.2  # 看多

    # 3) 趋势信号
    if stock_indicators.trend == "bullish":
        score += 0.3
        if "covered_call" not in strategies:
            strategies.append("covered_call")
    elif stock_indicators.trend == "bearish":
        score -= 0.3
        if "protective_put" not in strategies:
            strategies.append("protective_put")

    # 4) RSI 信号
    rsi = stock_indicators.rsi_14
    if rsi > 70:
        score -= 0.1
    elif rsi < 30:
        score += 0.1

    # 5) Cross-asset 风险/对冲信息
    if cross_asset_indicators.stock_iv_correlation < -0.4:
        score += 0.05
    if abs(cross_asset_indicators.delta_adjusted_hedge_ratio) > 100:
        score -= 0.05

    # 6) 专业策略组合映射
    if stock_indicators.adx_14 > 25 and stock_indicators.cmf_20 > 0:
        strategies.append("stock_trend_following")
    if stock_indicators.bollinger_band_width < 0.05 and stock_indicators.garch_vol_forecast > stock_indicators.hv_20d:
        strategies.append("stock_volatility_breakout")
    if iv > iv_high and option_indicators.theta_decay_rate > 0:
        strategies.append("option_volatility_selling")
    if option_indicators.iv_percentile < iv_low and option_indicators.term_structure_slope > 0:
        strategies.append("option_volatility_buying")
    if option_indicators.vertical_spread_risk_reward > 1.0:
        strategies.append("option_spread_trading")
    if abs(option_indicators.portfolio_greeks.get("delta", 0.0)) > 100:
        strategies.append("delta_neutral_hedging")
    if stock_indicators.adx_14 > 20 and stock_indicators.hv_iv_spread < 0:
        strategies.append("mixed_stock_option_hedge")

    # 去重
    strategies = list(dict.fromkeys(strategies))

    # 归一化 score 到 [-1, 1]
    score = max(-1.0, min(1.0, score))

    # 信号类型
    if score > 0.5:
        signal_type = "strong_buy"
    elif score > 0.2:
        signal_type = "buy"
    elif score < -0.5:
        signal_type = "strong_sell"
    elif score < -0.2:
        signal_type = "sell"
    else:
        signal_type = "neutral"

    return SignalFeatures(
        symbol=symbol,
        date=date.today(),
        computed_at=datetime.now(),
        close_price=close_price,
        daily_return=daily_return,
        volume=volume,
        option_indicators=option_indicators,
        stock_indicators=stock_indicators,
        cross_asset_indicators=cross_asset_indicators,
        signal_score=round(score, 4),
        signal_type=signal_type,
        volatility_regime=vol_regime,
        suggested_strategies=strategies,
    )
