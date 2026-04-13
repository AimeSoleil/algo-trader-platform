"""Output models for specialist analysis agents.

Each agent produces a structured analysis that the Synthesizer agent
consumes to build the final LLMTradingBlueprint.
"""
from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


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
    SQUEEZE = "squeeze"
    BACKWARDATION = "backwardation"


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
    divergence_detected: bool = False
    divergence_type: str | None = None  # "bullish", "bearish"
    strategies: list[StrategyCandidate] = Field(default_factory=list)


class TrendAnalysis(BaseModel):
    """Full output from TrendAgent."""
    symbols: list[TrendSymbolAnalysis] = Field(default_factory=list)
    market_trend_summary: str = ""


class VolatilitySymbolAnalysis(SymbolAnalysis):
    """Per-symbol output from VolatilityAgent."""
    vol_regime: VolRegime = VolRegime.NORMAL
    iv_rank_zone: str = "neutral"  # "high", "low", "neutral"
    hv_iv_assessment: str = "neutral"  # "implied_rich", "realized_exceeds", "neutral"
    garch_divergence: bool = False
    surface_mispricing: bool = False
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
    false_breakout_risk: bool = False


class FlowAnalysis(BaseModel):
    """Full output from FlowAgent."""
    symbols: list[FlowSymbolAnalysis] = Field(default_factory=list)


class ChainSymbolAnalysis(SymbolAnalysis):
    """Per-symbol output from ChainAgent."""
    liquidity_ok: bool = True
    hard_block: bool = False  # bid-ask > 0.20
    pcr_signal: str = "neutral"  # "contrarian_bullish", "contrarian_bearish", "neutral"
    gamma_pin_active: bool = False
    gamma_pin_strike: float | None = None
    institutional_flow: str = "neutral"  # "call_buying", "put_buying", "neutral"
    suggested_strikes: dict[str, Any] = Field(default_factory=dict, description="Optimal strike recommendations")


class ChainAnalysis(BaseModel):
    """Full output from ChainAgent."""
    symbols: list[ChainSymbolAnalysis] = Field(default_factory=list)


class SpreadSymbolAnalysis(SymbolAnalysis):
    """Per-symbol output from SpreadAgent."""
    best_spread_type: str | None = None  # "vertical", "calendar", "butterfly", "box_arb"
    risk_reward_ratio: float = 0.0
    theta_capture: float = 0.0
    mispricing_detected: bool = False
    arb_opportunity: bool = False
    optimal_dte: int | None = None
    constraints: list[str] = Field(default_factory=list)


class SpreadAnalysis(BaseModel):
    """Full output from SpreadAgent."""
    symbols: list[SpreadSymbolAnalysis] = Field(default_factory=list)


class CrossAssetSymbolAnalysis(SymbolAnalysis):
    """Per-symbol output from CrossAssetAgent."""
    correlation_regime: str = "normal"  # "fear", "decoupled", "bullish_vol", "normal"
    dominant_benchmark: str = "SPY"  # which benchmark explains most variance
    rate_sensitive: bool = False
    safe_haven_correlated: bool = False   # GLD corr > 0.3
    credit_stress_exposure: bool = False  # HYG corr > 0.3
    energy_exposure: bool = False         # XLE corr > 0.3
    crypto_correlated: bool = False       # IBIT corr > 0.3
    risk_off_signal: bool = False
    regime_transition: bool = False  # SPY / QQQ / IWM correlations diverging
    regime_days: int = Field(0, ge=0, description="Consecutive days the current correlation regime has persisted. <5 = transitioning, >=5 = confirmed.")
    vix_environment: str = "normal"  # "panic", "elevated", "normal", "complacent"
    position_size_modifier: float = Field(1.0, ge=0.0, le=1.5)
    hedging_needed: bool = False
    hedge_direction: str | None = None  # "buy_shares", "sell_shares"
    effective_size_modifier: float = Field(1.0, ge=0.0, le=1.5, description="Combined position size modifier after all adjustments. If <0.3, recommend skip.")


class CrossAssetAnalysis(BaseModel):
    """Full output from CrossAssetAgent."""
    symbols: list[CrossAssetSymbolAnalysis] = Field(default_factory=list)
    market_regime: str = "neutral"
    vix_summary: str = ""


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
