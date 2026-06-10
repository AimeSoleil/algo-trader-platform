"""Output models for specialist analysis agents.

Each agent produces a structured analysis that the Synthesizer agent
consumes to build the final LLMTradingBlueprint.
"""
from __future__ import annotations

from enum import Enum
import re
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

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


def _normalize_enum_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


# ---------------------------------------------------------------------------
# Shared types
# ---------------------------------------------------------------------------


class RegimeType(str, Enum):
    TRENDING_UP = "trending_up"
    TRENDING_DOWN = "trending_down"
    RANGE_BOUND = "range_bound"
    SQUEEZE = "squeeze"
    REVERSAL_WARNING = "reversal_warning"
    REVERSAL_CONFIRMED = "reversal_confirmed"
    NEUTRAL = "neutral"


class VolRegime(str, Enum):
    # ── Single-factor regimes ──────────────────────────────────────────────
    HIGH_VOL = "high_vol"           # IV Rank > 70; sell-premium bias
    LOW_VOL = "low_vol"             # IV Rank < 30; buy-premium bias
    NORMAL = "normal"               # IV Rank 30-70; no strong vol edge
    NORMAL_VOL = "normal_vol"       # alias kept for backward compat
    SQUEEZE = "squeeze"             # BB squeeze + IV Rank < 30; breakout imminent
    CONTANGO = "contango"           # Back > front IV; carry-friendly term structure
    BACKWARDATION = "backwardation" # Front > back IV; elevated near-term fear
    EVENT_RISK = "event_risk"       # Earnings/catalyst ≤ 5d; confidence caps apply
    # ── Compound regimes (two simultaneous conditions) ─────────────────────
    HIGH_VOL_CONTANGO = "high_vol_contango"           # High IV but orderly term structure; classic short-vol regime
    LOW_VOL_CONTANGO = "low_vol_contango"             # Compressed IV + contango; calm surface, long-vol only if other signals confirm
    HIGH_VOL_BACKWARDATION = "high_vol_backwardation"   # Panic spike + inverted term; Iron Butterfly only DTE>14
    LOW_VOL_BACKWARDATION = "low_vol_backwardation"     # Unusual low-IV inversion; defined-risk long vol only
    HIGH_VOL_EVENT_RISK = "high_vol_event_risk"         # Elevated IV + imminent catalyst; sell confidence ≤ 0.2
    LOW_VOL_SQUEEZE = "low_vol_squeeze"                 # IV compressed + BB squeeze; buy straddle/calendar before breakout
    BACKWARDATION_EVENT_RISK = "backwardation_event_risk"  # Inverted term + event; no short vol, defined-risk only


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
    reasoning: str = ""
    confidence: float = Field(0.5, ge=0.0, le=1.0)
    constraints: list[str] = Field(default_factory=list, description="Human-readable risk hints or caveats only; avoid pseudo-execution commands or encoded machine tokens.")
    mandatory_constraints: list[str] = Field(default_factory=list, description="Human-readable guardrails mirrored into constraints when needed; avoid pseudo-execution commands or encoded sizing/structure tokens.")
    entry_conditions: str = Field("", description="Specific conditions required to enter the position")
    exit_conditions: str = Field("", description="Exit triggers: stop-loss, take-profit, time-based")

    @model_validator(mode="after")
    def _synchronize_constraints(self) -> StrategyCandidate:
        if self.mandatory_constraints and not self.constraints:
            self.constraints = list(self.mandatory_constraints)
        if self.constraints and not self.mandatory_constraints:
            self.mandatory_constraints = list(self.constraints)
        return self


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
    vix_level: float = 0.0
    iv_rank: float | None = Field(None, ge=0.0, le=100.0, description="IV Rank 0-100 when available; null means unknown")
    divergence_detected: bool = False
    divergence_type: str | None = None  # "rsi_macd_bullish", "rsi_macd_bearish"
    false_positive_risk: str = "medium"  # "low", "medium", "high"
    signal_type: str = "multi_indicator"  # "single_indicator", "multi_indicator"
    trade_allowed: bool = True
    confidence_cap: float | None = Field(None, ge=0.0, le=1.0)
    simple_structures_only: bool = False
    blocked_reasons: list[str] = Field(default_factory=list)
    strategies: list[StrategyCandidate] = Field(default_factory=list)

    @field_validator("iv_rank", mode="before")
    @classmethod
    def _coerce_iv_rank(cls, v: Any) -> Any:
        if v is None or v == "":
            return None
        return v


class TrendAnalysis(BaseModel):
    """Full output from TrendAgent."""
    symbols: list[TrendSymbolAnalysis] = Field(default_factory=list)
    market_trend_summary: str = ""


class VolatilitySymbolAnalysis(SymbolAnalysis):
    """Per-symbol output from VolatilityAgent."""
    vol_regime: VolRegime = VolRegime.NORMAL
    iv_rank_zone: str = "neutral"  # "high", "low", "neutral"

    @field_validator("vol_regime", mode="before")
    @classmethod
    def _coerce_vol_regime(cls, v: Any) -> Any:
        if isinstance(v, str) and "," in v:
            v = v.split(",")[0].strip()
        if not isinstance(v, str):
            return v

        normalized = _normalize_enum_token(v)
        if normalized in VolRegime._value2member_map_:
            return normalized

        parts = {part for part in normalized.split("_") if part}

        alias_map = {
            ("high",): VolRegime.HIGH_VOL.value,
            ("low",): VolRegime.LOW_VOL.value,
            ("high", "vol"): VolRegime.HIGH_VOL.value,
            ("low", "vol"): VolRegime.LOW_VOL.value,
            ("neutral",): VolRegime.NORMAL.value,
            ("normal", "vol"): VolRegime.NORMAL_VOL.value,
            ("contango",): VolRegime.CONTANGO.value,
            ("event", "risk"): VolRegime.EVENT_RISK.value,
            ("contango", "high", "vol"): VolRegime.HIGH_VOL_CONTANGO.value,
            ("contango", "low", "vol"): VolRegime.LOW_VOL_CONTANGO.value,
            ("backwardation", "high", "vol"): VolRegime.HIGH_VOL_BACKWARDATION.value,
            ("backwardation", "low", "vol"): VolRegime.LOW_VOL_BACKWARDATION.value,
            ("event", "high", "risk", "vol"): VolRegime.HIGH_VOL_EVENT_RISK.value,
            ("low", "squeeze", "vol"): VolRegime.LOW_VOL_SQUEEZE.value,
            ("backwardation", "event", "risk"): VolRegime.BACKWARDATION_EVENT_RISK.value,
        }
        aliased = alias_map.get(tuple(sorted(parts)))
        if aliased is not None:
            return aliased

        # When the model emits an unsupported superset compound, collapse it to the
        # closest supported regime with the most conservative risk semantics.
        superset_alias_rules = (
            ({"backwardation", "event", "risk"}, VolRegime.BACKWARDATION_EVENT_RISK.value),
            ({"high", "vol", "event", "risk"}, VolRegime.HIGH_VOL_EVENT_RISK.value),
            ({"high", "vol", "backwardation"}, VolRegime.HIGH_VOL_BACKWARDATION.value),
            ({"low", "vol", "backwardation"}, VolRegime.LOW_VOL_BACKWARDATION.value),
            ({"low", "vol", "squeeze"}, VolRegime.LOW_VOL_SQUEEZE.value),
            ({"high", "vol", "contango"}, VolRegime.HIGH_VOL_CONTANGO.value),
            ({"low", "vol", "contango"}, VolRegime.LOW_VOL_CONTANGO.value),
        )
        for required_parts, canonical in superset_alias_rules:
            if required_parts.issubset(parts):
                return canonical

        return normalized

    iv_percentile_divergence: bool = False
    hv_iv_assessment: str = "neutral"  # "implied_rich", "realized_exceeds", "neutral"
    garch_divergence: bool = False
    garch_divergence_direction: str | None = Field(None, description="vol_rise, vol_fall, or null")
    surface_mispricing: bool = False
    mispricing_magnitude: float = 0.0
    event_risk_present: bool = False
    vix_level: float = 0.0
    earnings_proximity_days: int | None = Field(None, ge=0)
    liquidity_status: str = Field("high", description="high or low")
    signal_type: str = "multi_indicator"  # "single_indicator", "multi_indicator"
    trade_allowed: bool = True
    confidence_cap: float | None = Field(None, ge=0.0, le=1.0)
    simple_structures_only: bool = False
    blocked_reasons: list[str] = Field(default_factory=list)
    strategies: list[StrategyCandidate] = Field(default_factory=list)

    @field_validator("earnings_proximity_days", mode="before")
    @classmethod
    def _coerce_earnings_proximity_days(cls, v: Any) -> Any:
        return _coerce_int_like(v)


class VolatilityAnalysis(BaseModel):
    """Full output from VolatilityAgent."""
    symbols: list[VolatilitySymbolAnalysis] = Field(default_factory=list)
    market_vol_summary: str = ""


class FlowSymbolAnalysis(SymbolAnalysis):
    """Per-symbol output from FlowAgent."""
    flow_signal: FlowSignal = FlowSignal.NEUTRAL
    signal_strength: str = "single_indicator"  # "single_indicator", "dual_indicator", "triple_indicator"
    volume_anomaly: bool = False
    vwap_bias: str = "neutral"  # "bullish", "bearish", "neutral"
    position_size_modifier: float = Field(1.0, ge=0.0, le=1.5, description="Advisory conviction/risk-framing scalar for manual-trader mode; not an automatic sizing instruction.")
    false_breakout_risk: str = Field("low", description="low, medium, or high")
    event_risk_present: bool = False
    liquidity_status: str = Field("high", description="high or low")
    trade_allowed: bool = True
    confidence_cap: float | None = Field(None, ge=0.0, le=1.0)
    simple_structures_only: bool = False
    blocked_reasons: list[str] = Field(default_factory=list)
    confirming_indicators_count: int = Field(0, ge=0, description="Number of confirming flow indicators")


class FlowAnalysis(BaseModel):
    """Full output from FlowAgent."""
    symbols: list[FlowSymbolAnalysis] = Field(default_factory=list)


class ChainSymbolAnalysis(SymbolAnalysis):
    """Per-symbol output from ChainAgent."""
    front_expiry_dte: int | None = Field(0, ge=0, description="Estimated DTE of front-month expiry analyzed")
    iv_rank: float | None = Field(None, ge=0.0, le=100.0, description="IV Rank 0-100 when available; null means unknown")
    earnings_proximity_days: int | None = Field(None, ge=0)
    liquidity_ok: bool = True
    hard_block: bool = False  # bid-ask ratio > 0.30
    liquidity_tier: str = Field("L1", description="L1 (excellent) through L5 (hard block)")
    event_risk_present: bool = False
    trade_allowed: bool = True
    confidence_cap: float | None = Field(None, ge=0.0, le=1.0)
    simple_structures_only: bool = False
    blocked_reasons: list[str] = Field(default_factory=list)
    pcr_signal: str = "neutral"  # "contrarian_bullish", "contrarian_bearish", "directional_bullish", "directional_bearish", "neutral"
    gamma_pin_active: bool = False
    gamma_pin_strike: float | None = None
    pin_strength: float = Field(0.0, ge=0.0, le=1.0, description="OI_concentration × DTE_decay")
    institutional_flow: str = "neutral"  # "call_buying", "put_buying", "neutral"
    net_delta_exposure: str = "neutral"  # "bullish", "bearish", "neutral"
    confirming_indicators_count: int = Field(0, ge=0, description="Number of confirming chain indicators")
    suggested_strategies: list[str] = Field(default_factory=list, description="Optional strategy suggestions emitted by the chain prompt")
    suggested_strikes: dict[str, Any] = Field(default_factory=dict, description="Optimal strike recommendations")

    @field_validator("front_expiry_dte", mode="before")
    @classmethod
    def _coerce_front_expiry_dte(cls, v: Any) -> Any:
        return _coerce_int_like(v)

    @field_validator("earnings_proximity_days", mode="before")
    @classmethod
    def _coerce_chain_earnings_proximity_days(cls, v: Any) -> Any:
        return _coerce_int_like(v)

    @field_validator("iv_rank", mode="before")
    @classmethod
    def _coerce_chain_iv_rank(cls, v: Any) -> Any:
        if v is None or v == "":
            return None
        return v


class ChainAnalysis(BaseModel):
    """Full output from ChainAgent."""
    symbols: list[ChainSymbolAnalysis] = Field(default_factory=list)


class SpreadSymbolAnalysis(SymbolAnalysis):
    """Per-symbol output from SpreadAgent."""
    best_spread_type: str | None = None  # "vertical", "calendar", "reverse_calendar", "butterfly", "iron_condor", "box_arb"
    risk_reward_ratio: float = 0.0
    effective_rr: float | None = Field(None, description="Optional cost-aware R:R when explicitly defensible from provided inputs")
    theta_capture: float = 0.0
    mispricing_detected: bool = False
    arb_opportunity: bool = False
    arb_priority: int = Field(0, ge=0, le=10)
    optimal_dte: int | None = None
    iv_rank: float | None = Field(None, ge=0.0, le=100.0, description="IV Rank 0-100 when available; null means unknown")
    vix_level: float = 0.0
    earnings_proximity_days: int | None = Field(None, ge=0)
    liquidity_status: str = Field("adequate", description="adequate, wide, illiquid")
    event_risk_present: bool = False
    trade_allowed: bool = True
    confidence_cap: float | None = Field(None, ge=0.0, le=1.0)
    simple_structures_only: bool = False
    blocked_reasons: list[str] = Field(default_factory=list)
    confirming_indicators_count: int = Field(0, ge=0, le=4, description="Number of explicit spread confirmations supporting the selected structure")
    position_size_modifier: float = Field(1.0, ge=0.0, le=1.2, description="Advisory conviction/risk-framing scalar for manual-trader mode; use for reasoning only, not automatic sizing.")
    constraints: list[str] = Field(default_factory=list, description="Human-readable caveats only; not automatic execution instructions or pseudo-code tokens.")

    @field_validator("optimal_dte", mode="before")
    @classmethod
    def _coerce_optimal_dte(cls, v: Any) -> Any:
        return _coerce_int_like(v)

    @field_validator("earnings_proximity_days", mode="before")
    @classmethod
    def _coerce_spread_earnings_proximity_days(cls, v: Any) -> Any:
        return _coerce_int_like(v)


class SpreadAnalysis(BaseModel):
    """Full output from SpreadAgent."""
    symbols: list[SpreadSymbolAnalysis] = Field(default_factory=list)


class CrossAssetSymbolAnalysis(SymbolAnalysis):
    """Per-symbol output from CrossAssetAgent."""
    correlation_regime: str = "normal"  # "fear", "decoupled", "bullish_vol", "normal", "event_driven"
    dominant_benchmark: str = "idiosyncratic"  # "SPY", "QQQ", "IWM", "idiosyncratic"
    rate_sensitive: bool = False
    risk_off_signal: bool = False
    regime_transition: bool = False  # SPY / QQQ / IWM correlations diverging
    regime_days: int | None = Field(None, ge=0, description="Consecutive days the current correlation regime has persisted when explicit persistence data is available; null = unknown/unconfirmed.")
    vix_environment: str = "normal"  # "panic", "elevated", "normal", "complacent"
    vix_percentile_60d: float = Field(0.0, ge=0.0, le=1.0)
    gex_regime: str = Field("neutral", description="positive (vol suppressed), negative (vol amplified), neutral")
    earnings_proximity_days: int | None = Field(None, ge=0)
    event_risk_present: bool = False
    correlation_significance: float = Field(0.0, ge=0.0)
    signal_type: str = "multi_indicator"  # "single_indicator", "multi_indicator"
    hedging_needed: bool = False
    effective_size_modifier: float = Field(1.0, ge=0.0, le=2.0, description="Advisory cross-asset risk-framing scalar for manual-trader mode; use for reasoning and ranking, not automatic skip/sizing by itself.")
    master_override: bool = Field(True, description="True = this is the dominant cross-asset risk-framing field for downstream reasoning, not an automatic sizing override.")
    blocked_reasons: list[str] = Field(default_factory=list)

    @field_validator("regime_days", "earnings_proximity_days", mode="before")
    @classmethod
    def _coerce_cross_asset_ints(cls, v: Any) -> Any:
        return _coerce_int_like(v)


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


class PostMergeConflictExplanation(BaseModel):
    """LLM explanation for a symbol-level portfolio ranking choice."""

    symbol: str
    type: str = ""  # candidate_conflict | priority_override | diversification_adjustment | risk_demotion
    decision: str = "keep"  # keep | drop | deprioritize
    rationale: str = ""


class PostMergeReview(BaseModel):
    """Structured output from the post-merge portfolio review agent."""

    selected_symbols: list[str] = Field(default_factory=list)
    ranking: list[str] = Field(default_factory=list)
    portfolio_summary: str = ""
    risk_notes: list[str] = Field(default_factory=list)
    conflict_explanations: list[PostMergeConflictExplanation] = Field(default_factory=list)
