"""Output models for specialist analysis agents.

Each agent produces a structured analysis that the Synthesizer agent
consumes to build the final LLMTradingBlueprint.
"""
from __future__ import annotations

from enum import Enum
import re
from typing import Any

from pydantic import BaseModel, Field, field_validator

_NUMBER_RE = re.compile(r"[+-]?\d+(?:\.\d+)?")


def _coerce_int_like(value: Any) -> Any:
    if value is None or isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(round(value))
    if not isinstance(value, str):
        return value

    matches = _NUMBER_RE.findall(value)
    if not matches:
        return value

    numbers = [float(match) for match in matches]
    if len(numbers) == 1:
        return int(round(numbers[0]))
    return int(round(sum(numbers) / len(numbers)))


# ---------------------------------------------------------------------------
# Shared types
# ---------------------------------------------------------------------------


class RegimeType(str, Enum):
    TRENDING_UP = "trending_up"
    TRENDING_DOWN = "trending_down"
    RANGE_BOUND = "range_bound"
    SQUEEZE = "squeeze"
    REVERSAL_WARNING = "reversal_warning"
    NEUTRAL = "neutral"


class VolRegime(str, Enum):
    HIGH_VOL = "high_vol"
    LOW_VOL = "low_vol"
    NORMAL = "normal"
    NORMAL_VOL = "normal_vol"
    SQUEEZE = "squeeze"
    BACKWARDATION = "backwardation"
    EVENT_RISK = "event_risk"


class FlowSignal(str, Enum):
    STRONG_BUY = "strong_buy"
    MODERATE_BUY = "moderate_buy"
    NEUTRAL = "neutral"
    MODERATE_SELL = "moderate_sell"
    STRONG_SELL = "strong_sell"
    CONFLICTING = "conflicting"


class StrategyCandidate(BaseModel):
    """A candidate strategy recommended by an analysis agent."""
    strategy_type: str  # e.g. "vertical_spread", "iron_condor"
    direction: str  # "bullish", "bearish", "neutral"
    reasoning: str
    confidence: float = Field(0.5, ge=0.0, le=1.0)
    constraints: list[str] = Field(default_factory=list, description="Conditions or caveats")
    entry_conditions: str = Field("", description="Specific conditions required to enter the position")
    exit_conditions: str = Field("", description="Exit triggers: stop-loss, take-profit, time-based")


class SymbolAnalysis(BaseModel):
    """Base per-symbol analysis from any agent."""
    symbol: str
    reasoning: str = ""
    confidence: float = Field(0.5, ge=0.0, le=1.0)
    data_quality_penalty: float = Field(0.0, ge=0.0, le=1.0, description="Confidence reduction from degraded data quality (0=no penalty, 1=fully degraded)")
    agreement_count: int = Field(0, ge=0, le=6, description="Number of specialist agents agreeing on direction (0-6). Used for consensus scoring.")


# ---------------------------------------------------------------------------
# Agent-specific outputs
# ---------------------------------------------------------------------------


class TrendSymbolAnalysis(SymbolAnalysis):
    """Per-symbol output from TrendAgent."""
    regime: RegimeType = RegimeType.NEUTRAL
    trend_direction: str = "neutral"  # "bullish", "bearish", "neutral"
    trend_strength: float = 0.0
    adx_zone: str = "transition"  # "trending", "range_bound", "transition", "extreme"
    adx_z_score: float = 0.0
    iv_rank: float = Field(0.0, ge=0.0, le=100.0, description="IV Rank 0-100")
    divergence_detected: bool = False
    divergence_type: str | None = None  # "rsi_macd_bullish", "rsi_macd_bearish"
    false_positive_risk: str = "medium"  # "low", "medium", "high"
    strategies: list[StrategyCandidate] = Field(default_factory=list)


class TrendAnalysis(BaseModel):
    """Full output from TrendAgent."""
    symbols: list[TrendSymbolAnalysis] = Field(default_factory=list)
    market_trend_summary: str = ""


class VolatilitySymbolAnalysis(SymbolAnalysis):
    """Per-symbol output from VolatilityAgent."""
    vol_regime: VolRegime = VolRegime.NORMAL
    iv_rank_zone: str = "neutral"  # "high", "low", "neutral"
    iv_percentile_divergence: bool = False
    hv_iv_assessment: str = "neutral"  # "implied_rich", "realized_exceeds", "neutral"
    garch_divergence: bool = False
    garch_divergence_direction: str | None = Field(None, description="vol_rise, vol_fall, or null")
    surface_mispricing: bool = False
    event_risk_present: bool = False
    liquidity_status: str = Field("high", description="high or low")
    strategies: list[StrategyCandidate] = Field(default_factory=list)


class VolatilityAnalysis(BaseModel):
    """Full output from VolatilityAgent."""
    symbols: list[VolatilitySymbolAnalysis] = Field(default_factory=list)
    market_vol_summary: str = ""


class FlowSymbolAnalysis(SymbolAnalysis):
    """Per-symbol output from FlowAgent."""
    flow_signal: FlowSignal = FlowSignal.NEUTRAL
    volume_anomaly: bool = False
    vwap_bias: str = "neutral"  # "bullish", "bearish", "neutral"
    position_size_modifier: float = Field(1.0, ge=0.0, le=1.5, description="1.0 = full, 0.5 = half, etc.")
    false_breakout_risk: str = Field("low", description="low, medium, or high")
    event_risk_present: bool = False
    liquidity_status: str = Field("high", description="high or low")
    confirming_indicators_count: int = Field(0, ge=0, description="Number of confirming flow indicators")


class FlowAnalysis(BaseModel):
    """Full output from FlowAgent."""
    symbols: list[FlowSymbolAnalysis] = Field(default_factory=list)


class ChainSymbolAnalysis(SymbolAnalysis):
    """Per-symbol output from ChainAgent."""
    front_expiry_dte: int = Field(0, ge=0, description="Estimated DTE of front-month expiry analyzed")
    liquidity_ok: bool = True
    hard_block: bool = False  # bid-ask ratio > 0.20
    liquidity_tier: str = Field("L1", description="L1 (excellent) through L5 (hard block)")
    event_risk_present: bool = False
    pcr_signal: str = "neutral"  # "contrarian_bullish", "contrarian_bearish", "directional_bullish", "directional_bearish", "neutral"
    gamma_pin_active: bool = False
    gamma_pin_strike: float | None = None
    pin_strength: float = Field(0.0, ge=0.0, le=1.0, description="OI_concentration × DTE_decay")
    institutional_flow: str = "neutral"  # "call_buying", "put_buying", "neutral"
    net_delta_exposure: str = "neutral"  # "bullish", "bearish", "neutral"
    confirming_indicators_count: int = Field(0, ge=0, description="Number of confirming chain indicators")
    suggested_strikes: dict[str, Any] = Field(default_factory=dict, description="Optimal strike recommendations")


class ChainAnalysis(BaseModel):
    """Full output from ChainAgent."""
    symbols: list[ChainSymbolAnalysis] = Field(default_factory=list)


class SpreadSymbolAnalysis(SymbolAnalysis):
    """Per-symbol output from SpreadAgent."""
    best_spread_type: str | None = None  # "vertical", "calendar", "butterfly", "box_arb"
    risk_reward_ratio: float = 0.0
    effective_rr: float | None = Field(None, description="Cost-adjusted R:R after transaction costs")
    theta_capture: float = 0.0
    mispricing_detected: bool = False
    arb_opportunity: bool = False
    optimal_dte: int | None = None
    liquidity_status: str = Field("adequate", description="adequate, wide, illiquid")
    event_risk_present: bool = False
    constraints: list[str] = Field(default_factory=list)

    @field_validator("optimal_dte", mode="before")
    @classmethod
    def _coerce_optimal_dte(cls, v: Any) -> Any:
        return _coerce_int_like(v)


class SpreadAnalysis(BaseModel):
    """Full output from SpreadAgent."""
    symbols: list[SpreadSymbolAnalysis] = Field(default_factory=list)


class CrossAssetSymbolAnalysis(SymbolAnalysis):
    """Per-symbol output from CrossAssetAgent."""
    correlation_regime: str = "normal"  # "fear", "decoupled", "bullish_vol", "normal", "event_driven"
    dominant_benchmark: str = "SPY"  # "SPY", "QQQ", "IWM", "idiosyncratic"
    rate_sensitive: bool = False
    risk_off_signal: bool = False
    regime_transition: bool = False  # SPY / QQQ / IWM correlations diverging
    regime_days: int = Field(0, ge=0, description="Consecutive days the current correlation regime has persisted. <5 = transitioning, >=5 = confirmed.")
    vix_environment: str = "normal"  # "panic", "elevated", "normal", "complacent"
    gex_regime: str = Field("neutral", description="positive (vol suppressed), negative (vol amplified), neutral")
    position_size_modifier: float = Field(1.0, ge=0.0, le=1.5)
    hedging_needed: bool = False
    effective_size_modifier: float = Field(1.0, ge=0.0, le=1.5, description="Combined position size modifier after all adjustments. If <0.3, recommend skip.")
    master_override: bool = Field(True, description="True = this modifier overrides all other strategy module sizing")


class CrossAssetAnalysis(BaseModel):
    """Full output from CrossAssetAgent."""
    symbols: list[CrossAssetSymbolAnalysis] = Field(default_factory=list)
    market_regime: str = "neutral"
    vix_summary: str = ""
    cross_asset_summary: str = ""


# ---------------------------------------------------------------------------
# Critic output
# ---------------------------------------------------------------------------


class CriticIssue(BaseModel):
    """A single issue found by the Critic agent."""
    severity: str = "warning"  # "error", "warning", "info"
    symbol: str | None = None  # None = portfolio-level
    category: str = ""  # "rule_violation", "risk_breach", "logic_error", "missing_data"
    description: str = ""
    suggested_fix: str = ""


class CriticVerdict(BaseModel):
    """Output from the Critic agent."""
    verdict: str = "pass"  # "pass", "revise"
    issues: list[CriticIssue] = Field(default_factory=list)
    summary: str = ""
