"""技术指标计算工具函数

本模块提供 stock_indicators 和 option_indicators 共用的底层计算函数。
所有函数均为纯计算（无 IO），接收 pandas Series / DataFrame，返回标量或 Series。
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# 基础均线 (Moving Averages)
# ---------------------------------------------------------------------------

def ema(series: pd.Series, period: int) -> pd.Series:
    """指数移动平均 (Exponential Moving Average)。

    使用 span=period 的 EWMA，adjust=False 表示递归形式：
        EMA_t = α × price_t + (1-α) × EMA_{t-1},  α = 2/(period+1)

    Args:
        series: 价格序列（通常为收盘价）。
        period: 窗口期数。

    Returns:
        与输入等长的 EMA 序列。
    """
    return series.ewm(span=period, adjust=False).mean()


def sma(series: pd.Series, period: int) -> pd.Series:
    """简单移动平均 (Simple Moving Average)。

    SMA_t = (price_{t} + price_{t-1} + ... + price_{t-period+1}) / period

    Args:
        series: 价格序列。
        period: 窗口期数。

    Returns:
        与输入等长的 SMA 序列，前 period-1 个值为 NaN。
    """
    return series.rolling(window=period).mean()


# ---------------------------------------------------------------------------
# 动量 / 震荡指标 (Momentum / Oscillators)
# ---------------------------------------------------------------------------

def rsi(series: pd.Series, period: int = 14) -> float:
    """相对强弱指标 (Relative Strength Index, Cutler 变体)。

    使用 SMA 而非 Wilder 平滑（Cutler 变体），计算最近 period 日的
    平均涨幅 / 平均跌幅，映射到 0-100：
        RSI = 100 - 100 / (1 + RS),  RS = avg_gain / avg_loss

    Args:
        series: 价格序列。
        period: 回看期数，默认 14。

    Returns:
        最新 RSI 值 (0-100)，保留 2 位小数。
    """
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()

    if avg_loss.iloc[-1] == 0:
        return 100.0
    rs = avg_gain.iloc[-1] / avg_loss.iloc[-1]
    return round(100 - (100 / (1 + rs)), 2)


def macd(
    series: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> tuple[float, float, float]:
    """MACD (Moving Average Convergence Divergence)。

    MACD Line  = EMA(fast) - EMA(slow)
    Signal Line = EMA(MACD Line, signal)
    Histogram   = MACD Line - Signal Line

    金叉 (bullish crossover): MACD Line 上穿 Signal Line
    死叉 (bearish crossover): MACD Line 下穿 Signal Line

    Args:
        series: 价格序列。
        fast: 快线 EMA 周期，默认 12。
        slow: 慢线 EMA 周期，默认 26。
        signal: 信号线 EMA 周期，默认 9。

    Returns:
        (macd_line, signal_line, histogram)，各保留 4 位小数。
    """
    ema_fast = ema(series, fast)
    ema_slow = ema(series, slow)
    macd_line = ema_fast - ema_slow
    signal_line = ema(macd_line, signal)
    histogram = macd_line - signal_line
    return (
        round(float(macd_line.iloc[-1]), 4),
        round(float(signal_line.iloc[-1]), 4),
        round(float(histogram.iloc[-1]), 4),
    )


# ---------------------------------------------------------------------------
# 波动率 / 通道 (Volatility / Bands)
# ---------------------------------------------------------------------------

def bollinger_bands(
    series: pd.Series,
    period: int = 20,
    std_dev: float = 2.0,
) -> tuple[float, float, float]:
    """布林带 (Bollinger Bands)。

    Middle = SMA(period)
    Upper  = Middle + std_dev × σ(period)
    Lower  = Middle - std_dev × σ(period)

    价格触及上轨可能超买，触及下轨可能超卖；带宽收缩预示突破。

    Args:
        series: 价格序列。
        period: SMA 和标准差窗口，默认 20。
        std_dev: 标准差倍数，默认 2.0。

    Returns:
        (upper, lower, middle)，各保留 4 位小数。
    """
    mid = sma(series, period)
    std = series.rolling(window=period).std()
    upper = mid + std_dev * std
    lower = mid - std_dev * std
    return (
        round(float(upper.iloc[-1]), 4),
        round(float(lower.iloc[-1]), 4),
        round(float(mid.iloc[-1]), 4),
    )


def atr(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    period: int = 14,
) -> float:
    """平均真实波幅 (Average True Range)。

    True Range = max(H-L, |H-prevC|, |L-prevC|)
    ATR = SMA(True Range, period)

    用于度量市场波动幅度，常用于止损距离和仓位计算。

    Args:
        high: 最高价序列。
        low: 最低价序列。
        close: 收盘价序列。
        period: 回看期数，默认 14。

    Returns:
        最新 ATR 值，保留 4 位小数。
    """
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return round(float(tr.rolling(window=period).mean().iloc[-1]), 4)


def adx(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    period: int = 14,
) -> float:
    """平均趋向指标 (Average Directional Index)。

    衡量趋势强度 (0-100)，不区分方向:
        +DI = 100 × SMA(+DM) / ATR
        -DI = 100 × SMA(-DM) / ATR
        DX  = |+DI - -DI| / (+DI + -DI) × 100
        ADX = SMA(DX, period)

    ADX > 25 表示存在趋势，> 40 为强趋势。

    Args:
        high: 最高价序列。
        low: 最低价序列。
        close: 收盘价序列。
        period: 回看期数，默认 14。

    Returns:
        最新 ADX 值，保留 4 位小数。
    """
    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr_series = tr.rolling(window=period).mean()

    plus_di = 100 * (plus_dm.rolling(window=period).mean() / atr_series.replace(0, np.nan))
    minus_di = 100 * (minus_dm.rolling(window=period).mean() / atr_series.replace(0, np.nan))

    dx = (100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)).fillna(0)
    adx_series = dx.rolling(window=period).mean()
    return round(float(adx_series.iloc[-1]), 4) if not adx_series.empty else 0.0


def ichimoku(
    high: pd.Series,
    low: pd.Series,
) -> tuple[float, float, float, float]:
    """一目均衡图 (Ichimoku Cloud)。

    转换线 (Tenkan-sen)  = (9日最高 + 9日最低) / 2
    基准线 (Kijun-sen)   = (26日最高 + 26日最低) / 2
    先行带A (Span A)     = (转换线 + 基准线) / 2
    先行带B (Span B)     = (52日最高 + 52日最低) / 2

    价格在云上方为多头，云下方为空头；云的厚度反映支撑/阻力强度。

    Args:
        high: 最高价序列。
        low: 最低价序列。

    Returns:
        (tenkan, kijun, span_a, span_b)，各保留 4 位小数，NaN 替换为 0.0。
    """
    tenkan = ((high.rolling(9).max() + low.rolling(9).min()) / 2).iloc[-1]
    kijun = ((high.rolling(26).max() + low.rolling(26).min()) / 2).iloc[-1]
    span_a = (tenkan + kijun) / 2
    span_b = ((high.rolling(52).max() + low.rolling(52).min()) / 2).iloc[-1]
    return tuple(round(float(v), 4) if pd.notna(v) else 0.0 for v in (tenkan, kijun, span_a, span_b))


# ---------------------------------------------------------------------------
# 回归 / 斜率
# ---------------------------------------------------------------------------

def linear_reg_slope(series: pd.Series, period: int = 20) -> float:
    """线性回归斜率 (归一化)。

    对最近 period 个收盘价做一元线性回归 y = ax + b，
    返回斜率 a 除以最新价格的归一化值，表示每日平均变化率。

    Args:
        series: 价格序列。
        period: 回归窗口，默认 20。

    Returns:
        归一化斜率，保留 6 位小数。数据不足时返回 0.0。
    """
    if len(series) < period:
        return 0.0
    y = series.tail(period).to_numpy(dtype=float)
    x = np.arange(period, dtype=float)
    slope = np.polyfit(x, y, 1)[0]
    base = abs(y[-1]) if abs(y[-1]) > 1e-9 else 1.0
    return round(float(slope / base), 6)


# ---------------------------------------------------------------------------
# 资金流 / 成交量 (Money Flow / Volume)
# ---------------------------------------------------------------------------

def cmf(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    volume: pd.Series,
    period: int = 20,
) -> float:
    """Chaikin 资金流指标 (Chaikin Money Flow)。

    MFM = [(Close - Low) - (High - Close)] / (High - Low)
    MFV = MFM × Volume
    CMF = Σ(MFV, period) / Σ(Volume, period)

    范围 [-1, +1]，正值表示买压，负值表示卖压。

    Args:
        high: 最高价序列。
        low: 最低价序列。
        close: 收盘价序列。
        volume: 成交量序列。
        period: 回看期数，默认 20。

    Returns:
        最新 CMF 值，保留 4 位小数。
    """
    denom = (high - low).replace(0, np.nan)
    mfm = ((close - low) - (high - close)) / denom
    mfv = mfm.fillna(0.0) * volume
    cmf_series = mfv.rolling(period).sum() / volume.rolling(period).sum().replace(0, np.nan)
    return round(float(cmf_series.iloc[-1]), 4) if not cmf_series.empty else 0.0


def stoch_rsi(series: pd.Series, period: int = 14) -> float:
    """随机 RSI (Stochastic RSI)。

    先计算 RSI 序列，再对 RSI 做随机指标化:
        StochRSI = (RSI - RSI_min) / (RSI_max - RSI_min)

    范围 [0, 1]，比普通 RSI 更灵敏，更易触及极值区域。

    Args:
        series: 价格序列。
        period: RSI 和随机化窗口，默认 14。

    Returns:
        最新 StochRSI 值 (0-1)，保留 4 位小数。
    """
    rsi_series = series.diff().pipe(lambda d: d.where(d > 0, 0.0)).rolling(period).mean() / (
        -series.diff().pipe(lambda d: d.where(d < 0, 0.0)).rolling(period).mean().replace(0, np.nan)
    )
    rsi_series = 100 - 100 / (1 + rsi_series.replace([np.inf, -np.inf], np.nan)).fillna(50)
    rsi_min = rsi_series.rolling(period).min()
    rsi_max = rsi_series.rolling(period).max()
    stoch = (rsi_series - rsi_min) / (rsi_max - rsi_min).replace(0, np.nan)
    return round(float(stoch.iloc[-1]), 4) if not stoch.empty else 0.0


def tick_volume_delta(
    open_: pd.Series,
    close: pd.Series,
    volume: pd.Series,
) -> float:
    """逐笔成交量净值 (Tick Volume Delta)。

    将每根 bar 的成交量按涨跌分配正负号:
        close >= open → +volume (买压)
        close <  open → -volume (卖压)
    最终除以总成交量得到 [-1, +1] 归一化值。

    Args:
        open_: 开盘价序列。
        close: 收盘价序列。
        volume: 成交量序列。

    Returns:
        归一化净值，保留 4 位小数。
    """
    # Use last 20 bars to match CMF window — recent directional flow
    open_t = open_.tail(20)
    close_t = close.tail(20)
    volume_t = volume.tail(20)
    signed = np.where(close_t >= open_t, volume_t, -volume_t)
    denom = float(volume_t.sum()) if float(volume_t.sum()) > 0 else 1.0
    return round(float(np.sum(signed) / denom), 4)


def volume_profile(
    close: pd.Series,
    volume: pd.Series,
    bins: int = 20,
) -> tuple[float, float, float]:
    """成交量分布 (Volume Profile)。

    将价格区间分为 bins 个等宽桶，统计每桶的累计成交量:
        POC (Point of Control) — 成交量最大桶的中点价格
        VAL (Value Area Low)   — 70% 成交量集中区的下沿
        VAH (Value Area High)  — 70% 成交量集中区的上沿

    Args:
        close: 收盘价序列。
        volume: 成交量序列。
        bins: 价格桶数，默认 20。

    Returns:
        (poc, val, vah)，各保留 4 位小数。空数据返回 (0, 0, 0)。
    """
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


# ---------------------------------------------------------------------------
# 波动率预测
# ---------------------------------------------------------------------------

def garch_like_forecast(close: pd.Series, period: int = 30) -> float:
    """GARCH(1,1) 波动率预测，回退到 EWMA。

    优先使用 arch 库拟合 GARCH(1,1) 模型，预测下一期方差并年化:
        σ_annual = √(variance_forecast) × √252
    若 arch 不可用或拟合失败，使用 EWMA 方差 (λ=0.94) 替代:
        σ²_t = λ × σ²_{t-1} + (1-λ) × r²_t

    Args:
        close: 收盘价序列。
        period: 用于拟合的收益率期数，默认 30。

    Returns:
        年化波动率预测值，保留 6 位小数。
    """
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


# ---------------------------------------------------------------------------
# 背离检测 (Divergence)
# ---------------------------------------------------------------------------

def rsi_divergence(close: pd.Series, rsi_value: float, period: int = 14) -> float:
    """RSI 背离检测 (简化版)。

    比较近 period 日的价格变化方向与当前 RSI 相对 50 的偏移方向:
        score = sign(price_change) × -sign(RSI - 50)
    +1 = 看跌背离 (价格涨但 RSI 偏弱)
    -1 = 看涨背离 (价格跌但 RSI 偏强)
     0 = 无明显背离

    Args:
        close: 价格序列。
        rsi_value: 当前 RSI 值。
        period: 价格变化回看期数，默认 14。

    Returns:
        背离评分 (-1, 0, +1)，保留 4 位小数。
    """
    if len(close) < period + 5:
        return 0.0
    px_change = float(close.iloc[-1] - close.iloc[-period])
    rsi_centered = rsi_value - 50.0
    score = np.sign(px_change) * -np.sign(rsi_centered)
    return round(float(score), 4)


def macd_hist_divergence(close: pd.Series, macd_hist: float, period: int = 10) -> float:
    """MACD Histogram 背离检测 (简化版)。

    比较近 period 日的价格动量方向与 MACD Histogram 方向:
        score = sign(price_momentum) × sign(histogram)
    +1 = 方向一致 (趋势确认)
    -1 = 方向不一致 (潜在背离)

    Args:
        close: 价格序列。
        macd_hist: 当前 MACD Histogram 值。
        period: 价格动量回看期数，默认 10。

    Returns:
        一致性评分 (-1 或 +1)，保留 4 位小数。
    """
    if len(close) < period + 1:
        return 0.0
    px_momentum = float(close.iloc[-1] - close.iloc[-period])
    score = np.sign(px_momentum) * np.sign(macd_hist)
    return round(float(score), 4)


# ---------------------------------------------------------------------------
# 数据清洗 (Sanitization)
# ---------------------------------------------------------------------------

def sanitize_float(v: float) -> float:
    """将 NaN / Inf 替换为 0.0。

    用于清理计算结果中可能出现的无效浮点数，防止下游序列化或存储出错。

    Args:
        v: 待检查的浮点数。

    Returns:
        原值或 0.0（当输入为 NaN/±Inf 时）。
    """
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return 0.0
    return v
