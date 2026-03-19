"""信号与指标数据模型"""
from __future__ import annotations
from datetime import date, datetime
from pydantic import BaseModel, Field


class OptionIndicators(BaseModel):
    """期权指标集"""
    iv_rank: float = 0.0  # IV Rank: min-max归一化 (current - min) / (max - min) * 100
    iv_percentile: float = 0.0  # IV Percentile: percentileofscore — 历史值中低于当前值的百分比
    current_iv: float = 0.0  # 当前平均IV
    historical_iv_30d: float = 0.0  # 30日历史IV
    pcr_volume: float = 0.0  # Put/Call 成交量比
    pcr_oi: float = 0.0  # Put/Call 持仓量比
    iv_skew: float = 0.0  # IV偏斜（25delta put IV - 25delta call IV）
    term_structure_slope: float = 0.0  # 期限结构斜率
    atm_iv: dict[str, float] = Field(default_factory=dict)  # {expiry_str: atm_iv}

    # Professional volatility surface features
    vol_surface_fit_error: float = 0.0

    # Greek / risk features
    delta_exposure_profile: dict[str, float] = Field(default_factory=dict)
    gamma_peak_strike: float = 0.0
    theta_decay_rate: float = 0.0
    vanna: float = 0.0
    charm: float = 0.0
    portfolio_greeks: dict[str, float] = Field(default_factory=dict)

    # Chain structure features
    oi_concentration_top5: float = 0.0
    bid_ask_spread_ratio: float = 0.0
    option_volume_imbalance: float = 0.0

    # Spread / arbitrage features
    vertical_spread_risk_reward: float = 0.0
    calendar_spread_theta_capture: float = 0.0
    butterfly_pricing_error: float = 0.0
    box_spread_arbitrage: float = 0.0

    confidence_scores: dict[str, float] = Field(default_factory=dict)
    extreme_flags: list[str] = Field(default_factory=list)
    degraded_indicators: list[str] = Field(
        default_factory=list,
        description="因数据不足而降级/使用默认值的指标",
    )


class StockIndicators(BaseModel):
    """股票技术指标集"""
    rsi_14: float = 50.0
    macd: float = 0.0
    macd_signal: float = 0.0
    macd_histogram: float = 0.0
    ema_20: float = 0.0
    ema_50: float = 0.0
    sma_200: float = 0.0
    bollinger_upper: float = 0.0
    bollinger_lower: float = 0.0
    bollinger_mid: float = 0.0
    atr_14: float = 0.0  # Average True Range

    # Professional trend features
    adx_14: float = 0.0
    keltner_mid: float = 0.0
    keltner_upper: float = 0.0
    keltner_lower: float = 0.0
    ichimoku_tenkan: float = 0.0
    ichimoku_kijun: float = 0.0
    ichimoku_span_a: float = 0.0
    ichimoku_span_b: float = 0.0
    linear_reg_slope: float = 0.0

    # Professional volatility features
    bollinger_band_width: float = 0.0
    hv_20d: float = 0.0
    hv_iv_spread: float = 0.0
    garch_vol_forecast: float = 0.0

    # Professional flow features
    vwap: float = 0.0
    volume_profile_poc: float = 0.0
    volume_profile_val: float = 0.0
    volume_profile_vah: float = 0.0
    cmf_20: float = 0.0
    tick_volume_delta: float = 0.0

    # Professional momentum features
    rsi_divergence: float = 0.0
    stoch_rsi: float = 0.0
    macd_hist_divergence: float = 0.0

    trend: str = "neutral"  # "bullish" / "bearish" / "neutral"
    trend_strength: float = 0.0  # 0-1

    confidence_scores: dict[str, float] = Field(default_factory=dict)
    extreme_flags: list[str] = Field(default_factory=list)
    degraded_indicators: list[str] = Field(
        default_factory=list,
        description="因数据不足而降级/使用默认值的指标",
    )


class CrossAssetIndicators(BaseModel):
    stock_iv_correlation: float = 0.0
    option_vs_stock_volume_ratio: float = 0.0
    delta_adjusted_hedge_ratio: float = 0.0
    spy_beta: float = 0.0  # Beta relative to SPY (equity market sensitivity)
    sector_relative_strength: float = 0.0  # Relative strength vs sector ETF
    earnings_proximity_days: int = -1  # Days until next earnings (-1 = unknown)
    index_correlation_20d: float = 0.0  # 20-day rolling correlation to SPY
    confidence_scores: dict[str, float] = Field(default_factory=dict)


class DataQuality(BaseModel):
    """数据质量标注 — 标记信号计算所依赖的数据是否充足。

    complete=True 表示所有指标均基于完整数据计算；
    complete=False 表示部分指标可能降级或使用默认值。
    """
    complete: bool = True
    score: float = Field(1.0, ge=0.0, le=1.0, description="综合质量评分 0-1")
    warnings: list[str] = Field(default_factory=list, description="质量问题描述")
    stock_bar_count: int = Field(0, description="股票 OHLCV 行数")
    option_row_count: int = Field(0, description="期权链行数")
    degraded_indicators: list[str] = Field(
        default_factory=list,
        description="因数据不足而降级/使用默认值的指标名",
    )


class SignalFeatures(BaseModel):
    """某标的某日的完整特征集（Signal Service 输出）"""
    symbol: str
    date: date
    computed_at: datetime
    
    # 价格信息
    close_price: float = 0.0
    daily_return: float = 0.0
    volume: int = 0
    bar_type: str = "unknown"  # "intraday_1min" / "daily" / "unknown"
    
    # 指标
    option_indicators: OptionIndicators = Field(default_factory=OptionIndicators)
    stock_indicators: StockIndicators = Field(default_factory=StockIndicators)
    cross_asset_indicators: CrossAssetIndicators = Field(default_factory=CrossAssetIndicators)
    
    # 市场状态分类（基于 config 中的 IV 阈值）
    volatility_regime: str = "normal"  # "high" / "normal" / "low"

    # 数据质量标注
    data_quality: DataQuality = Field(default_factory=DataQuality)
