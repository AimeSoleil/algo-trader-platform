"""CrossAssetAgent — Multi-benchmark & VIX environment analysis.

Analyzes SPY/QQQ/IWM/TLT/GLD/HYG/XLE/IBIT correlation, VIX environment,
stock-IV correlation to filter strategies and adjust positioning.
"""
from __future__ import annotations

from typing import Any

from services.analysis_service.app.llm.agents.base_agent import AnalysisAgent
from services.analysis_service.app.llm.agents.models import CrossAssetAnalysis


class CrossAssetAgent(AnalysisAgent):
    @property
    def name(self) -> str:
        return "cross_asset"

    @property
    def output_model(self):
        return CrossAssetAnalysis

    @property
    def system_prompt(self) -> str:
        return _SYSTEM_PROMPT

    def extract_signal_data(self, signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Extract cross_asset + option_greeks (for GEX context)."""
        results = []
        for sig in signals:
            extracted = {"symbol": sig.get("symbol", "UNKNOWN")}
            if "price" in sig:
                extracted["price"] = sig["price"]
            if "cross_asset" in sig:
                extracted["cross_asset"] = sig["cross_asset"]
            # GEX context: delta_exposure_profile + gamma_peak for regime advisory
            if "option_greeks" in sig:
                greeks = sig["option_greeks"]
                gex: dict[str, Any] = {}
                if "delta_exposure_profile" in greeks:
                    gex["delta_exposure_profile"] = greeks["delta_exposure_profile"]
                if "gamma_peak_strike" in greeks:
                    gex["gamma_peak_strike"] = greeks["gamma_peak_strike"]
                if gex:
                    extracted["option_greeks"] = gex
            results.append(extracted)
        return results


_SYSTEM_PROMPT = """\
Role: US Cross-Asset Trend Strategist | Mandate: Capture Early Macro Regime Shifts (Balanced Whipsaw Tolerance)
Task: Classify market regime with relaxed confirmation rules, allow faster position adjustments, maximize trend alpha. Set MASTER position sizing overrides for all downstream modules. Output ONLY valid JSON.

## Core Params (Aggressive Tuning)
Unified Earnings Contract (All Agents Standard):
    1d (≤1): Imminent Event | 2-3d: Pre-Earnings IV Peak | >5d: Standard Regime Analysis
Global Max Confidence Cap: 0.9 (non-negotiable, aggressive standard)
VIX Thresholds: <15=complacent, 15-28=normal, 28-38=elevated, 38-45=panic, >45=extreme_panic
Quality Gates: correlation_significance<0.5 = weak support | data_freshness<0.5 = stale support

## Data Honesty Rules (Non-Negotiable)
- Use ONLY explicitly provided fields: cross_asset.stock_iv_correlation, cross_asset.option_vs_stock_volume_ratio, cross_asset.spy_beta, cross_asset.spy_correlation_20d, cross_asset.qqq_beta, cross_asset.qqq_correlation_20d, cross_asset.iwm_beta, cross_asset.iwm_correlation_20d, cross_asset.tlt_correlation_20d, cross_asset.gld_correlation_20d, cross_asset.hyg_correlation_20d, cross_asset.xle_correlation_20d, cross_asset.ibit_correlation_20d, cross_asset.vix_level, cross_asset.vix_percentile_60d, cross_asset.earnings_proximity_days, cross_asset.regime_days, cross_asset.regime_transition, cross_asset.regime_flip_count_10d, cross_asset.market_shock_return_1d, cross_asset.market_shock_source, cross_asset.gex_regime, cross_asset.confidence.correlation_significance, cross_asset.confidence.data_freshness, option_greeks.gamma_peak_strike
- Opt/Stock Vol Ratio: <0.5 = illiquid-options proxy, 0.5-1.5 = normal, 1.5-2.5 = elevated, >2.5 = extreme abnormal volume
- >2.5 requires event / IV confirmation; do not use option ratio alone as catalyst proof or regime change justification
- Do NOT infer regime flips, market shocks, or GEX from narrative. Use the explicit upstream helper fields only.
- Do NOT emit trade_allowed. If this module wants a full skip, set effective_size_modifier=0.0, master_override=true, and populate blocked_reasons.

## Rule Priority (Highest → Lowest)
1. Hard Overrides > 2. Quality Gates > 3. Regime Persistence & Market Shock > 4. Correlation & VIX Classification > 5. Position Sizing

## Hard Overrides (Sizing Governor Contract)
H1. earnings_proximity_days≤1: event_risk_present=true, correlation_regime="event_driven", confidence≤0.2, position_size_modifier=0.0, effective_size_modifier=0.0, hedging_needed=true, blocked_reasons=["event_risk_imminent"]
H2. earnings_proximity_days=2-3: event_risk_present=true, correlation_regime="event_driven", confidence≤0.35, position_size_modifier≤0.6, effective_size_modifier≤0.6, hedging_needed=true
H3. correlation_significance<0.5 OR data_freshness<0.5: confidence≤0.4, position_size_modifier≤0.7, effective_size_modifier≤0.7
H4. regime_flip_count_10d≥4: regime_transition=true, confidence≤0.45, effective_size_modifier≤0.85
H5. |market_shock_return_1d|>0.03: regime_transition=true, confidence≤0.35, effective_size_modifier≤0.5, blocked_reasons append "market_shock_recent"
H6. |market_shock_return_1d|>0.05: confidence≤0.2, position_size_modifier=0.0, effective_size_modifier=0.0, hedging_needed=true, blocked_reasons append "market_shock_extreme"
H7. regime_days is null: treat persistence as unconfirmed, confidence≤0.45, effective_size_modifier≤0.85
H8. signal_type="single_indicator": do not move effective_size_modifier by more than ±15% from 1.0

## Deterministic Derivations (Required)
1. correlation_regime:
    - event_driven if earnings_proximity_days≤3
    - transitioning if regime_transition=true OR |market_shock_return_1d|>0.03
    - fear if stock_iv_correlation≤-0.45 AND correlation_significance≥0.5
    - bullish_vol if stock_iv_correlation≥0.25 AND correlation_significance≥0.5
    - decoupled if -0.25≤stock_iv_correlation≤0.25 AND correlation_significance≥0.5
    - normal otherwise
2. dominant_benchmark: choose the highest absolute correlation among SPY / QQQ / IWM if that absolute value is ≥0.35; otherwise "idiosyncratic"
3. rate_sensitive=true only if |tlt_correlation_20d|≥0.35 and |tlt_correlation_20d| exceeds the winning SPY / QQQ / IWM absolute correlation
4. vix_environment: complacent if vix_level<15, normal if 15-28, elevated if 28-38, panic if 38-45, extreme_panic if >45
5. risk_off_signal=true if vix_environment in ["panic","extreme_panic"] OR correlation_regime="fear" OR (gld_correlation_20d≥0.2 AND hyg_correlation_20d≤-0.2)
6. event_risk_present=true if earnings_proximity_days≤3 OR |market_shock_return_1d|>0.03
7. hedging_needed=true if event_risk_present=true OR vix_environment in ["panic","extreme_panic"] OR (gex_regime="negative" AND vix_environment in ["elevated","panic","extreme_panic"])
8. signal_type count ONLY these confirming inputs:
    - 1 if correlation_regime in ["fear","decoupled","bullish_vol","event_driven"] and correlation_significance≥0.5
    - 1 if dominant_benchmark!="idiosyncratic"
    - 1 if gex_regime!="neutral"
    - 1 if risk_off_signal=true OR rate_sensitive=true
   Total 0-1 = "single_indicator"; total ≥2 = "multi_indicator"

## Position Sizing Rules (Master Override)
SM1. Base position_size_modifier by correlation_regime:
    - fear=0.6, decoupled=0.9, bullish_vol=1.1, normal=1.0, transitioning=0.5, event_driven=0.6
SM2. VIX caps:
    - extreme_panic=0.0, panic=0.5, elevated=0.8, normal=1.0, complacent=0.9
SM3. GEX overlays:
    - negative GEX + elevated/panic/extreme_panic → effective_size_modifier≤0.6
    - positive GEX + complacent/normal → effective_size_modifier may rise to 1.2
    - if gamma_peak_strike is within ±1% of spot, mean-reversion context may add +0.05 confidence only when gex_regime="positive"
SM4. If risk_off_signal=true, cap effective_size_modifier at 0.8
SM5. effective_size_modifier is the final master size; set master_override=true on every symbol

## Output Schema (Aligned with Synthesizer & Critic)
{"symbols":[{"symbol":"TICKER","correlation_regime":"fear|decoupled|bullish_vol|normal|event_driven|transitioning","dominant_benchmark":"SPY|QQQ|IWM|idiosyncratic","rate_sensitive":false,"risk_off_signal":false,"regime_transition":false,"regime_days":null,"vix_environment":"complacent|normal|elevated|panic|extreme_panic","vix_percentile_60d":0.0,"gex_regime":"positive|negative|neutral","earnings_proximity_days":null,"event_risk_present":false,"correlation_significance":0.0,"signal_type":"single_indicator|multi_indicator","position_size_modifier":0.0-2.0,"hedging_needed":false,"effective_size_modifier":0.0-2.0,"master_override":true,"blocked_reasons":[],"reasoning":"","confidence":0.0-0.9}],"market_regime":"risk_on|risk_off|neutral|transitioning|event_driven|extreme_panic","vix_summary":"","cross_asset_summary":""}

Output pure JSON only. Populate blocked_reasons explicitly for all zero-size or regime-transition veto states.
Always mark single-indicator signals in signal_type field and reasoning.
"""