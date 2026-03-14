"""股票技术指标计算"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from shared.models.signal import StockIndicators
from shared.utils import get_logger

from .cal_utils import (
    adx as _adx,
    atr as _atr,
    bollinger_bands as _bollinger_bands,
    cmf as _cmf,
    ema as _ema,
    garch_like_forecast as _garch_like_forecast,
    ichimoku as _ichimoku,
    linear_reg_slope as _linear_reg_slope,
    macd as _macd,
    macd_hist_divergence as _macd_hist_divergence,
    rsi as _rsi,
    rsi_divergence as _rsi_divergence,
    sanitize_float as _sanitize_float,
    sma as _sma,
    stoch_rsi as _stoch_rsi,
    tick_volume_delta as _tick_volume_delta,
    volume_profile as _volume_profile,
)

logger = get_logger("stock_indicators")


def _sanitize_stock_indicators(ind: StockIndicators) -> StockIndicators:
    """Sanitize all float fields — replace NaN/Inf with 0.0."""
    updates: dict = {}
    for name, field_info in StockIndicators.model_fields.items():
        val = getattr(ind, name)
        if isinstance(val, float):
            clean = _sanitize_float(val)
            if clean != val or (isinstance(val, float) and math.isnan(val)):
                updates[name] = clean
        elif isinstance(val, dict):
            cleaned = {k: (_sanitize_float(v) if isinstance(v, float) else v) for k, v in val.items()}
            if cleaned != val:
                updates[name] = cleaned
    return ind.model_copy(update=updates) if updates else ind


def compute_stock_indicators(bars_df: pd.DataFrame) -> StockIndicators:
    """从日线 DataFrame 计算完整股票技术指标集。

    输入要求:
        bars_df — 按时间升序的 daily OHLCV，最多 260 行（约 1 年）。
        最少 30 行才能计算，否则返回默认值。

    指标说明:
    ┌─────────────────────────┬──────────────────────────────────────────────────┬──────────┐
    │ 指标                    │ 用途                                             │ 最少天数  │
    ├─────────────────────────┼──────────────────────────────────────────────────┼──────────┤
    │ RSI(14)                 │ 超买/超卖震荡器 (0-100)                          │ 15       │
    │ MACD(12,26,9)           │ 趋势/动量信号 (金叉/死叉)                        │ 35       │
    │ Bollinger Bands(20,2σ)  │ 波动率通道，突破/回归交易                        │ 20       │
    │ ATR(14)                 │ 平均真实波幅，用于止损/仓位计算                   │ 15       │
    │ ADX(14)                 │ 趋势强度 (0-100)，>25 有趋势，>40 强趋势          │ 28       │
    │ EMA(20) / EMA(50)       │ 短/中期均线，交叉判断趋势方向                     │ 20 / 50  │
    │ SMA(200)                │ 长期趋势基准，牛/熊分界线                        │ 200      │
    │ Keltner Channel         │ EMA(20) ± 1.5×ATR，与 BB 配合检测 squeeze       │ 20       │
    │ Ichimoku Cloud          │ 多维支撑/阻力系统 (转换线/基准线/先行带)          │ 52       │
    │ Linear Reg Slope(20)    │ 20 日收盘价线性回归斜率，归一化为每日变化率        │ 20       │
    │ BB Width                │ (上轨-下轨)/中轨，低值 = 波动率压缩               │ 20       │
    │ HV(20d)                 │ 20 日历史波动率 (年化 √252)                      │ 20       │
    │ GARCH forecast          │ GARCH(1,1) 或 EWMA 波动率预测 (年化)             │ 30       │
    │ VWAP                    │ 成交量加权平均价，机构公允价格参考                │ 1        │
    │ Volume Profile          │ POC/VAL/VAH — 成交量密集区上下沿                 │ 1        │
    │ CMF(20)                 │ Chaikin 资金流 (-1~+1)，正=买压，负=卖压          │ 20       │
    │ Tick Volume Delta       │ 涨跌 bar 成交量净比，衡量盘中买卖力道             │ 1        │
    │ Stochastic RSI(14)      │ RSI 的随机指标化 (0-1)，更灵敏的超买超卖          │ 28       │
    │ RSI Divergence          │ 价格方向 vs RSI 方向不一致评分 (-1/0/+1)          │ 19       │
    │ MACD Hist Divergence    │ 价格动量 vs MACD Histogram 方向一致性              │ 11       │
    │ Trend / Trend Strength  │ EMA20 vs EMA50 交叉 + 价格位置判断趋势方向/强度   │ 50       │
    └─────────────────────────┴──────────────────────────────────────────────────┴──────────┘
    """
    if bars_df.empty or len(bars_df) < 30:
        logger.warning("stock_indicators.insufficient_data", rows=len(bars_df))
        return StockIndicators()

    close = bars_df["close"]
    high = bars_df["high"]
    low = bars_df["low"]

    rsi = _rsi(close, 14)
    macd_val, macd_sig, macd_hist = _macd(close)
    bb_upper, bb_lower, bb_mid = _bollinger_bands(close)
    atr = _atr(high, low, close)
    adx = _adx(high, low, close)
    tenkan, kijun, span_a, span_b = _ichimoku(high, low)
    lin_slope = _linear_reg_slope(close)

    ema_20 = round(float(_ema(close, 20).iloc[-1]), 4)
    ema_50 = round(float(_ema(close, 50).iloc[-1]), 4) if len(close) >= 50 else 0.0
    sma_200 = round(float(_sma(close, 200).iloc[-1]), 4) if len(close) >= 200 else 0.0

    keltner_mid = ema_20
    keltner_upper = round(ema_20 + 1.5 * atr, 4)
    keltner_lower = round(ema_20 - 1.5 * atr, 4)

    bollinger_width = round((bb_upper - bb_lower) / bb_mid, 6) if abs(bb_mid) > 1e-9 else 0.0

    ret = close.pct_change().dropna()
    # Always daily bars now (intraday is handled separately in tasks.py)
    hv_20d = round(float(ret.tail(20).std() * np.sqrt(252)), 6) if len(ret) >= 2 else 0.0
    garch_forecast = _garch_like_forecast(close)

    typical_price = (high + low + close) / 3
    cum_vol = bars_df["volume"].cumsum().replace(0, np.nan)
    vwap = round(float((typical_price * bars_df["volume"]).cumsum().iloc[-1] / cum_vol.iloc[-1]), 4)

    poc, val, vah = _volume_profile(close, bars_df["volume"])
    cmf_20 = _cmf(high, low, close, bars_df["volume"])
    tvd = _tick_volume_delta(bars_df["open"], close, bars_df["volume"])
    stoch_rsi = _stoch_rsi(close)
    rsi_div = _rsi_divergence(close, rsi)
    macd_div = _macd_hist_divergence(close, macd_hist)

    # 趋势判断 — ema_50 不可用时退回 neutral 避免误判
    current_price = float(close.iloc[-1])
    if ema_50 == 0.0:
        trend = "neutral"
        trend_strength = 0.0
    elif ema_20 > ema_50 and current_price > ema_20:
        trend = "bullish"
        trend_strength = min((current_price - ema_50) / ema_50 * 10, 1.0)
    elif ema_20 < ema_50 and current_price < ema_20:
        trend = "bearish"
        trend_strength = min((ema_50 - current_price) / ema_50 * 10, 1.0)
    else:
        trend = "neutral"
        trend_strength = 0.3

    confidence_scores = {
        "trend": round(min(1.0, adx / 50.0), 4),
        "momentum": round(min(1.0, abs(macd_hist) * 5), 4),
        "flow": round(min(1.0, abs(cmf_20)), 4),
    }

    extreme_flags: list[str] = []
    if rsi > 80:
        extreme_flags.append("rsi_extreme_overbought")
    if rsi < 20:
        extreme_flags.append("rsi_extreme_oversold")
    if adx > 40:
        extreme_flags.append("strong_trend")
    if bollinger_width < 0.03:
        extreme_flags.append("volatility_squeeze")

    result = StockIndicators(
        rsi_14=rsi,
        macd=macd_val,
        macd_signal=macd_sig,
        macd_histogram=macd_hist,
        ema_20=ema_20,
        ema_50=ema_50,
        sma_200=sma_200,
        bollinger_upper=bb_upper,
        bollinger_lower=bb_lower,
        bollinger_mid=bb_mid,
        atr_14=atr,
        adx_14=adx,
        keltner_mid=keltner_mid,
        keltner_upper=keltner_upper,
        keltner_lower=keltner_lower,
        ichimoku_tenkan=tenkan,
        ichimoku_kijun=kijun,
        ichimoku_span_a=span_a,
        ichimoku_span_b=span_b,
        linear_reg_slope=lin_slope,
        bollinger_band_width=bollinger_width,
        hv_20d=hv_20d,
        garch_vol_forecast=garch_forecast,
        vwap=vwap,
        volume_profile_poc=poc,
        volume_profile_val=val,
        volume_profile_vah=vah,
        cmf_20=cmf_20,
        tick_volume_delta=tvd,
        rsi_divergence=rsi_div,
        stoch_rsi=stoch_rsi,
        macd_hist_divergence=macd_div,
        trend=trend,
        trend_strength=round(trend_strength, 4),
        confidence_scores=confidence_scores,
        extreme_flags=extreme_flags,
    )
    return _sanitize_stock_indicators(result)
