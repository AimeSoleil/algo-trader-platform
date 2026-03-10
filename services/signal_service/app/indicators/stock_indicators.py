"""股票技术指标计算"""
from __future__ import annotations

import numpy as np
import pandas as pd

from shared.models.signal import StockIndicators
from shared.utils import get_logger

logger = get_logger("stock_indicators")


def _ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def _sma(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(window=period).mean()


def _rsi(series: pd.Series, period: int = 14) -> float:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()

    if avg_loss.iloc[-1] == 0:
        return 100.0
    rs = avg_gain.iloc[-1] / avg_loss.iloc[-1]
    return round(100 - (100 / (1 + rs)), 2)


def _macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> tuple[float, float, float]:
    ema_fast = _ema(series, fast)
    ema_slow = _ema(series, slow)
    macd_line = ema_fast - ema_slow
    signal_line = _ema(macd_line, signal)
    histogram = macd_line - signal_line
    return (
        round(float(macd_line.iloc[-1]), 4),
        round(float(signal_line.iloc[-1]), 4),
        round(float(histogram.iloc[-1]), 4),
    )


def _bollinger_bands(series: pd.Series, period: int = 20, std_dev: float = 2.0) -> tuple[float, float, float]:
    mid = _sma(series, period)
    std = series.rolling(window=period).std()
    upper = mid + std_dev * std
    lower = mid - std_dev * std
    return (
        round(float(upper.iloc[-1]), 4),
        round(float(lower.iloc[-1]), 4),
        round(float(mid.iloc[-1]), 4),
    )


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> float:
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return round(float(tr.rolling(window=period).mean().iloc[-1]), 4)


def _adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> float:
    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()

    plus_di = 100 * (plus_dm.rolling(window=period).mean() / atr.replace(0, np.nan))
    minus_di = 100 * (minus_dm.rolling(window=period).mean() / atr.replace(0, np.nan))

    dx = (100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)).fillna(0)
    adx_series = dx.rolling(window=period).mean()
    return round(float(adx_series.iloc[-1]), 4) if not adx_series.empty else 0.0


def _ichimoku(high: pd.Series, low: pd.Series) -> tuple[float, float, float, float]:
    tenkan = ((high.rolling(9).max() + low.rolling(9).min()) / 2).iloc[-1]
    kijun = ((high.rolling(26).max() + low.rolling(26).min()) / 2).iloc[-1]
    span_a = (tenkan + kijun) / 2
    span_b = ((high.rolling(52).max() + low.rolling(52).min()) / 2).iloc[-1]
    return tuple(round(float(v), 4) if pd.notna(v) else 0.0 for v in (tenkan, kijun, span_a, span_b))


def _linear_reg_slope(series: pd.Series, period: int = 20) -> float:
    if len(series) < period:
        return 0.0
    y = series.tail(period).to_numpy(dtype=float)
    x = np.arange(period, dtype=float)
    slope = np.polyfit(x, y, 1)[0]
    base = abs(y[-1]) if abs(y[-1]) > 1e-9 else 1.0
    return round(float(slope / base), 6)


def _cmf(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series, period: int = 20) -> float:
    denom = (high - low).replace(0, np.nan)
    mfm = ((close - low) - (high - close)) / denom
    mfv = mfm.fillna(0.0) * volume
    cmf_series = mfv.rolling(period).sum() / volume.rolling(period).sum().replace(0, np.nan)
    return round(float(cmf_series.iloc[-1]), 4) if not cmf_series.empty else 0.0


def _stoch_rsi(series: pd.Series, period: int = 14) -> float:
    rsi_series = series.diff().pipe(lambda d: d.where(d > 0, 0.0)).rolling(period).mean() / (
        -series.diff().pipe(lambda d: d.where(d < 0, 0.0)).rolling(period).mean().replace(0, np.nan)
    )
    rsi_series = 100 - 100 / (1 + rsi_series.replace([np.inf, -np.inf], np.nan)).fillna(50)
    rsi_min = rsi_series.rolling(period).min()
    rsi_max = rsi_series.rolling(period).max()
    stoch = (rsi_series - rsi_min) / (rsi_max - rsi_min).replace(0, np.nan)
    return round(float(stoch.iloc[-1]), 4) if not stoch.empty else 0.0


def _tick_volume_delta(open_: pd.Series, close: pd.Series, volume: pd.Series) -> float:
    signed = np.where(close >= open_, volume, -volume)
    denom = float(volume.sum()) if float(volume.sum()) > 0 else 1.0
    return round(float(np.sum(signed) / denom), 4)


def _volume_profile(close: pd.Series, volume: pd.Series, bins: int = 20) -> tuple[float, float, float]:
    if close.empty:
        return 0.0, 0.0, 0.0

    min_p, max_p = float(close.min()), float(close.max())
    if max_p - min_p < 1e-9:
        return round(min_p, 4), round(min_p, 4), round(max_p, 4)

    edges = np.linspace(min_p, max_p, bins + 1)
    bucket_idx = np.clip(np.digitize(close.to_numpy(), edges) - 1, 0, bins - 1)
    vol_by_bucket = np.zeros(bins)
    for idx, vol in zip(bucket_idx, volume.to_numpy()):
        vol_by_bucket[idx] += float(vol)

    poc_idx = int(np.argmax(vol_by_bucket))
    poc = (edges[poc_idx] + edges[poc_idx + 1]) / 2

    sorted_idx = np.argsort(vol_by_bucket)[::-1]
    total = vol_by_bucket.sum()
    target = total * 0.7
    cum = 0.0
    used = []
    for idx in sorted_idx:
        used.append(idx)
        cum += vol_by_bucket[idx]
        if cum >= target:
            break

    val = edges[min(used)]
    vah = edges[max(used) + 1]
    return round(float(poc), 4), round(float(val), 4), round(float(vah), 4)


def _garch_like_forecast(close: pd.Series, period: int = 30) -> float:
    ret = close.pct_change().dropna().tail(period)
    if ret.empty:
        return 0.0

    try:
        from arch import arch_model  # type: ignore

        model = arch_model(ret * 100, vol="GARCH", p=1, q=1, mean="Zero", dist="normal")
        fit = model.fit(disp="off")
        forecast = fit.forecast(horizon=1)
        var = float(forecast.variance.iloc[-1, 0]) / 10000
        return round(float(np.sqrt(max(var, 0.0)) * np.sqrt(252)), 6)
    except Exception:
        lam = 0.94
        ewma_var = 0.0
        for r in ret:
            ewma_var = lam * ewma_var + (1 - lam) * float(r) ** 2
        return round(float(np.sqrt(max(ewma_var, 0.0)) * np.sqrt(252)), 6)


def _rsi_divergence(close: pd.Series, rsi_value: float, period: int = 14) -> float:
    if len(close) < period + 5:
        return 0.0
    px_change = float(close.iloc[-1] - close.iloc[-period])
    rsi_centered = rsi_value - 50.0
    score = np.sign(px_change) * -np.sign(rsi_centered)
    return round(float(score), 4)


def _macd_hist_divergence(close: pd.Series, macd_hist: float, period: int = 10) -> float:
    if len(close) < period + 1:
        return 0.0
    px_momentum = float(close.iloc[-1] - close.iloc[-period])
    score = np.sign(px_momentum) * np.sign(macd_hist)
    return round(float(score), 4)


def compute_stock_indicators(bars_df: pd.DataFrame) -> StockIndicators:
    """从K线 DataFrame 计算完整股票指标集"""
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

    # 趋势判断
    current_price = float(close.iloc[-1])
    if ema_20 > ema_50 and current_price > ema_20:
        trend = "bullish"
        trend_strength = min((current_price - ema_50) / ema_50 * 10, 1.0) if ema_50 > 0 else 0.5
    elif ema_20 < ema_50 and current_price < ema_20:
        trend = "bearish"
        trend_strength = min((ema_50 - current_price) / ema_50 * 10, 1.0) if ema_50 > 0 else 0.5
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

    return StockIndicators(
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
