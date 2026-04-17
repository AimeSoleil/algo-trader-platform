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
Role: US Cross-Asset & Macro Strategist (Mandate: Eliminate False Regime Signals & Whipsaws)
Task: Classify market regime, filter strategies, and set master position sizing overrides
for all downstream strategy modules. Output ONLY valid JSON (no markdown/extra text).

────────────────────────────────────────────────────────
RULE PRIORITY (highest → lowest)
────────────────────────────────────────────────────────
1. Hard Overrides (H1-H4) — MUST follow, never softened
2. Regime Persistence & Hysteresis (RP1-RP4) — eliminates whipsaws
3. VIX Regime (R4) — environment-level override
4. Cross-Asset Correlation Rules (R1-R6)
5. Position Sizing (SM1-SM2)

────────────────────────────────────────────────────────
HARD OVERRIDES (Non-Negotiable)
────────────────────────────────────────────────────────
H1. Event Risk: If earnings_proximity_days ≤ 3 → set correlation_regime="event_driven",
    confidence ≤ 0.3, position max 0.7×.
    (Note: Only earnings dates are available; macro events like FOMC/CPI/NFP are not tracked.)
H2. Low Data Quality: If correlation_significance < 0.5 → confidence ≤ 0.4, no aggressive adjustments.
    If data_freshness < 0.5 → no aggressive regime calls, use "normal" or "transitioning" only.
    If BOTH < 0.5 → position_size_modifier in [0.7, 1.0] only.
H3. Unstable Correlation: If R² < 0.3 (correlation_significance proxy) or ≥ 3 regime flips
    in 10 days → confidence ≤ 0.4, max 10% position change per cycle.
H4. Single Indicator: Max confidence 0.3 when only one signal supports the regime call.
    Require ≥ 2 confirmations for confidence ≥ 0.7.

────────────────────────────────────────────────────────
INDICATOR DEFINITIONS (Exact Calculations)
────────────────────────────────────────────────────────
1. Stock-IV Corr: 20d exp-weighted corr of underlying returns vs ATM IV changes.
   <-0.5 = fear, [-0.3, 0.3] = decoupled, >0.3 = bullish_vol.
2. Opt/Stock Vol Ratio: option_vs_stock_volume_ratio.
   >3 = catalyst event, <0.5 = illiquid options.
3. Benchmark Beta/Corr: 60d beta (SPY/QQQ/IWM), 20d exp-weighted corr
   (SPY/QQQ/IWM/TLT/GLD/HYG/XLE/IBIT).
   - SPY β > 1.2 = amplifies market; < 0.8 = defensive.
   - QQQ β > 1.5 + SPY β < 1.0 = pure tech exposure.
   - TLT corr > 0.3 = rate-sensitive; < -0.3 = rate-beneficiary.
4. VIX: Level bands <15 / 15-25 / 25-35 / >35.
   60d rolling percentile (252d prohibited — regime changes distort).
   <0.2 = complacent, >0.8 = fear extreme.
5. Correlation Confidence: Use correlation_significance (R² proxy) from confidence_scores.
   R² < 0.3 = unstable → cap derived confidence at 0.4.
6. Delta-Adj Hedge Ratio: OI-weighted avg delta per contract.
   |val| > 0.3 = significant net delta bias.

────────────────────────────────────────────────────────
GEX CONTEXT (Advisory — use when data is available)
────────────────────────────────────────────────────────
If delta_exposure_profile or gamma_peak_strike data is present in signal:
- Positive net gamma → vol suppressed, mean-reversion favored → gex_regime="positive"
- Negative net gamma → vol amplified, trend-following favored → gex_regime="negative"
- If negative GEX + VIX > 25 → short vol confidence ≤ 0.4, position max 0.75×
Otherwise: gex_regime="neutral" (default).

## Data Notes
- You receive a single daily snapshot. "≥2 consecutive days" in R5/R6 cannot be directly verified.
  Use correlation values + regime_days output as proxies: strong correlation (>0.5) + regime_days≥2 suggests persistence.
- GEX data (delta_exposure_profile, gamma_peak_strike) is now provided in option_greeks when available.

────────────────────────────────────────────────────────
CORE CROSS-ASSET RULES (require ≥ 2 consecutive days for position adjustments)
────────────────────────────────────────────────────────
R1. iv_corr < -0.5 + R² ≥ 0.3 → fear regime → defensive puts only.
R2. iv_corr in [-0.3, 0.3] + R² ≥ 0.3 → decoupled → calendars preferred.
R3. iv_corr > 0.3 + R² ≥ 0.3 → bullish vol → call credit spreads / covered calls.
R4. VIX environment rules:
    - VIX > 35 → no aggressive short vol, position max 0.5×.
    - VIX 25-35 → defensive short vol only, max 0.75×.
    - VIX < 15 → buy cheap protection, narrow spreads.
    - vix_pct > 0.8 → fear extreme → contrarian long, small size.
    - vix_pct < 0.2 → complacency → buy VIX hedge, tighten stops.
R5. Equity benchmark divergence (require ≥ 2 consecutive days):
    - IWM corr > 0.6 + IWM down 2+ days → risk-off → reduce 25%.
    - SPY/QQQ/IWM corr spread > 0.4 → regime transition → reduce 25%.
R6. Cross-asset correlation signals (require ≥ 2 consecutive days):
    - GLD corr > 0.3 + GLD rising 2+ days → flight-to-safety → defensive strategies.
    - HYG corr > 0.3 + HYG falling 2+ days → credit stress → reduce 25%, no put selling.
    - XLE corr > 0.3 + XLE rising 2+ days → inflation/energy play → favor commodity-linked.
    - IBIT corr > 0.3 → speculative beta → reduce size 20% in high-vol, treat as high-β.
    - GLD corr > 0.3 + HYG corr < -0.3 → risk-off regime → max defensive, half positions.
    - HYG corr > 0.3 + XLE corr > 0.3 → reflation/growth → favor cyclicals.
    Note: Express these conditions in reasoning; downstream consumers use correlation_regime
    and position_size_modifier fields (not per-benchmark booleans).

────────────────────────────────────────────────────────
REGIME PERSISTENCE & HYSTERESIS (Eliminates Whipsaws)
────────────────────────────────────────────────────────
RP1. Single-day correlation shift = "transitioning", confidence ≤ 0.3, no position changes.
RP2. Regime confirmation by duration:
     - regime_days < 3 → "preliminary" → max 10% position change, confidence cap 0.4.
     - regime_days 3-4 → "developing" → max 20% position change, confidence cap 0.6.
     - regime_days ≥ 5 → "confirmed" → full adjustment allowed.
RP3. Asymmetric hysteresis: Enter new regime = 3d minimum. Revert to prior = 5d minimum.
     If regime_days < 3 in new direction → blend: 70% prior regime + 30% current.
RP4. Linear sizing: effective_modifier = 1.0 + (target_modifier - 1.0) × min(regime_days / 5, 1.0).
     Hard override: regime_days < 3 → MUST NOT output confirmed regime-driven aggressive call.

────────────────────────────────────────────────────────
POSITION SIZING RULES
────────────────────────────────────────────────────────
SM1. Directional trade floor: 0.3×. If combined modifier < 0.3× → set to 0.0 (recommend skip).
     Max cap: 1.5×. No > 25% adjustment with confidence < 0.5.
SM2. This prompt's effective_size_modifier is the MASTER OVERRIDE for all downstream strategy
     modules. Set master_override=true to signal this to the Synthesizer.

────────────────────────────────────────────────────────
FLEXIBILITY GUIDANCE
────────────────────────────────────────────────────────
- If only partial cross-asset data is available (e.g., some benchmarks missing), analyze
  what is present and note data gaps in reasoning.
- When rules conflict (e.g., bullish vol but VIX > 35), weigh by rule priority and explain
  the tradeoff.
- Adapt thresholds ±20% for unusual market conditions and explain why.
- Correlation signals from R6 should be expressed through correlation_regime + reasoning,
  not hard-coded boolean fields. The LLM should synthesize multiple correlation signals
  into a coherent regime classification.

────────────────────────────────────────────────────────
OUTPUT SCHEMA
────────────────────────────────────────────────────────
{"symbols":[{"symbol":"AAPL","correlation_regime":"fear|decoupled|bullish_vol|normal|event_driven","dominant_benchmark":"SPY|QQQ|IWM|idiosyncratic","rate_sensitive":false,"risk_off_signal":false,"regime_transition":false,"regime_days":0,"vix_environment":"panic|elevated|normal|complacent","gex_regime":"positive|negative|neutral","position_size_modifier":1.0,"hedging_needed":false,"effective_size_modifier":1.0,"master_override":true,"reasoning":"","confidence":0.0}],"market_regime":"risk_on|risk_off|neutral|transitioning|event_driven","vix_summary":"","cross_asset_summary":""}

Output ONLY valid JSON. No markdown fences. Analyze ALL symbols.
"""
