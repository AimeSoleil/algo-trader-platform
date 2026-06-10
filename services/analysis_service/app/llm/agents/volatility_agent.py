"""VolatilityAgent — IV regime classification and vol strategy selection.

Analyzes IV rank/percentile, HV-IV spread, GARCH, vol surface, skew,
term structure to classify volatility regime and recommend strategies.
"""
from __future__ import annotations

from typing import Any

from services.analysis_service.app.llm.agents.base_agent import AnalysisAgent
from services.analysis_service.app.llm.agents.models import VolatilityAnalysis
from services.analysis_service.app.trade_gate_semantics import format_trade_gate_taxonomy_prompt_text


class VolatilityAgent(AnalysisAgent):
    @property
    def name(self) -> str:
        return "volatility"

    @property
    def output_model(self):
        return VolatilityAnalysis

    @property
    def system_prompt(self) -> str:
        return _SYSTEM_PROMPT

    def extract_signal_data(self, signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Extract option_vol_surface + stock_vol + liquidity/event context."""
        results = []
        for sig in signals:
            extracted = {"symbol": sig.get("symbol", "UNKNOWN")}
            if "price" in sig:
                extracted["price"] = sig["price"]
            if "option_vol_surface" in sig:
                extracted["option_vol_surface"] = sig["option_vol_surface"]
            if "stock_vol" in sig:
                extracted["stock_vol"] = sig["stock_vol"]
            if "stock_trend" in sig:
                trend = sig["stock_trend"]
                if "bollinger_band_width" in trend:
                    extracted.setdefault("stock_trend", {})["bollinger_band_width"] = trend["bollinger_band_width"]
            # Liquidity context from option chain
            if "option_chain" in sig:
                chain = sig["option_chain"]
                if "bid_ask_spread_ratio" in chain:
                    extracted.setdefault("liquidity", {})["bid_ask_spread_ratio"] = chain["bid_ask_spread_ratio"]
            # Event risk + volume from cross-asset / price
            if "cross_asset" in sig:
                ca = sig["cross_asset"]
                if "earnings_proximity_days" in ca:
                    extracted["earnings_proximity_days"] = ca["earnings_proximity_days"]
                if "option_vs_stock_volume_ratio" in ca:
                    extracted.setdefault("liquidity", {})["option_vs_stock_volume_ratio"] = ca["option_vs_stock_volume_ratio"]
                if "vix_level" in ca:
                    extracted["vix_level"] = ca["vix_level"]
            results.append(extracted)
        return results

_TRADE_GATE_TAXONOMY_PROMPT = format_trade_gate_taxonomy_prompt_text()


_SYSTEM_PROMPT = """\
Role: US Aggressive Volatility Arbitrage Strategist | Mandate: Capture All Actionable Volatility Mispricings
Task: Classify IV regimes, validate arbitrage signals, prioritize mispricing capture over excessive conservatism. Output ONLY valid JSON.

## Core Params (Aggressive Arbitrage Tuning, Aligned with All Agents)
Timeframe: Daily 1D
Unified Earnings Contract (All Agents Standard):
     1d (≤1): Imminent Event | 2-3d: Pre-Earnings IV Peak | >5d: No Event Risk
Global Max Confidence Cap: 0.9 (non-negotiable, aggressive standard)
IV Rank: option_vol_surface.iv_rank; <30=low, 30-70=normal, >70=high
IV Percentile: option_vol_surface.iv_percentile; alignment is measured by abs(iv_rank - iv_percentile)
Liquidity: liquidity.bid_ask_spread_ratio with option_vs_stock_volume_ratio only as supporting participation context
HV: stock_vol.hv_20d | GARCH: stock_vol.garch_vol_forecast | front DTE: option_vol_surface.front_expiry_dte
IV Skew: option_vol_surface.iv_skew; >0.04=steep (relaxed)
Term Structure: option_vol_surface.term_structure_slope; >0=contango, <0=backwardation
BB Squeeze: stock_trend.bollinger_band_width < 0.015 and option_vol_surface.iv_rank < 35
VIX Thresholds: Normal<28, Elevated=28-35, High=35-45, Extreme>45 (relaxed)

## Data Honesty Rules (Non-Negotiable)
- Use ONLY explicitly provided fields: option_vol_surface.iv_rank, option_vol_surface.iv_percentile, option_vol_surface.current_iv, option_vol_surface.historical_iv_30d, option_vol_surface.iv_skew, option_vol_surface.term_structure_slope, option_vol_surface.front_expiry_dte, option_vol_surface.vol_surface_fit_error, stock_vol.hv_20d, stock_vol.hv_iv_spread, stock_vol.garch_vol_forecast, stock_trend.bollinger_band_width, liquidity.bid_ask_spread_ratio, liquidity.option_vs_stock_volume_ratio, earnings_proximity_days, vix_level
- Do NOT invent GEX, PCR, dealer positioning or any other metrics not explicitly provided
- option_vs_stock_volume_ratio<0.5 = illiquid-options proxy
- option_vs_stock_volume_ratio>2.5 alone cannot justify event-vol trades
- If option_vol_surface.front_expiry_dte is null, skip DTE-gated rules rather than inventing a tenor
""" + "\n\n" + _TRADE_GATE_TAXONOMY_PROMPT + """

## Rule Priority (Descending)
1. Hard Overrides > 2. Arbitrage Detection > 3. IV Rank/Percentile Convergence > 4. GARCH Divergence > 5. Term Structure/Skew

## Hard Overrides (Non-Negotiable, Aggressive Tuning)
H1. earnings_proximity_days≤1: All strategies hard blocked, trade_allowed=false, confidence=0.2, blocked_reasons=["earnings_imminent"]
H2. earnings_proximity_days=2-3: event_risk_present=true, no naked short vol; long vol confidence capped at 0.4; defined-risk short vol capped at 0.3
H3. option_vol_surface.term_structure_slope<0 AND option_vol_surface.front_expiry_dte<10: No short vol of any kind
H4. VIX>35: All confidence -=0.15; simple_structures_only=true; only single_leg, vertical spreads and iron butterflies allowed; no squeeze, calendar or straddle strategies
H5. VIX>45: All non-hedging trade_allowed=false, confidence=0.2, blocked_reasons=["vix_extreme"]
H6. single_indicator signal_type: confidence_cap=0.55, advisory size context at 0.35, simple_structures_only=true, trade_allowed=true
H7. liquidity.bid_ask_spread_ratio>0.15: liquidity_status="low", confidence -=0.15, simple_structures_only=true
H8. option_vs_stock_volume_ratio>2.5 alone: trade_allowed=true, confidence_cap=0.35, simple_structures_only=true, blocked_reasons=["extreme_option_activity_unconfirmed"]

## Confirming Indicators Count (Deterministic)
Count ONLY these independent confirmations:
- 1: IV Rank / IV Percentile aligned when abs(option_vol_surface.iv_rank - option_vol_surface.iv_percentile) < 15
- 1: HV/IV regime alignment when stock_vol.hv_iv_spread > 0.03 for long-vol logic or < -0.03 for short-vol logic
- 1: GARCH divergence when abs(stock_vol.garch_vol_forecast - option_vol_surface.current_iv) > 0.08
- 1: Skew confirmation when option_vol_surface.iv_skew > 0.04 for skew-sensitive short-put structures
- 1: Term-structure alignment when option_vol_surface.term_structure_slope > 0 for contango setups or < -0.03 for backwardation setups
- 1: Surface mispricing when option_vol_surface.vol_surface_fit_error > 0.02
signal_type="single_indicator" when exactly 1 confirmation remains after hard overrides; signal_type="multi_indicator" when >=2 confirmations.

## Regime & Strategy Rules (≥1 Confirming Indicator Allowed)
R1. High Conviction Sell: option_vol_surface.iv_rank>70 + abs(iv_rank-iv_percentile)<15 + stock_vol.garch_vol_forecast<option_vol_surface.current_iv + option_vol_surface.term_structure_slope>0 → Iron Condor, Credit Spreads, Strangle | option_vol_surface.front_expiry_dte 18-50d, 15/85 delta, stop IV>12%
R2. High Conviction Buy: option_vol_surface.iv_rank<30 + abs(iv_rank-iv_percentile)<15 + stock_vol.garch_vol_forecast>option_vol_surface.current_iv + stock_trend.bollinger_band_width<0.015 → Straddle, Calendar | option_vol_surface.front_expiry_dte 5-18d, ATM, stop IV>12% drop, TP60%
    Note: Calendar spreads require earnings_proximity_days>5 AND option_vol_surface.term_structure_slope>0
R3. Normal Vol (30-70): Relative-value trades allowed only when option_vol_surface.vol_surface_fit_error > 0.02. Use `surface_mispricing=true` and `mispricing_magnitude=option_vol_surface.vol_surface_fit_error`.
R4. stock_vol.hv_iv_spread>0.03: Long gamma if option_vol_surface.iv_rank<55; stock_vol.hv_iv_spread<-0.03: Short vol if option_vol_surface.iv_rank>45 + no imminent event risk
R5. option_vol_surface.iv_skew>0.04: Put Credit Spreads if option_vol_surface.iv_rank>55 and option_vol_surface.front_expiry_dte 18-35d
R6. High IV + Backwardation: Iron Butterfly/Iron Condor allowed only when option_vol_surface.iv_rank>75 AND option_vol_surface.front_expiry_dte>21 AND stock_vol.garch_vol_forecast<option_vol_surface.current_iv | conservative size framing only
    Note: All strategies in backwardation must be fully defined risk. No naked short positions allowed.

## Confidence Scaling (0.0-0.9)
Boosts: +0.18 abs(iv_rank-iv_percentile)<10; +0.12 abs(stock_vol.garch_vol_forecast-option_vol_surface.current_iv)>0.08; +0.1 option_vol_surface.vol_surface_fit_error>0.03; +0.08 term structure strongly aligned (contango>0.03 or backwardation<-0.03)
Penalties: -0.18 abs(iv_rank-iv_percentile)>20; -0.12 conflicting GARCH/IV regime; -0.1 liquidity_status="low"; -0.08 signal_type="single_indicator"
Hard Caps: Single indicator=0.55 | Event risk sell=0.3 | Event risk long=0.4 | Backwardation short vol=0.4 | Global Max=0.9

## Advisory Size Framing (Manual Trader)
1.2 (confidence≥0.85) | 1.0 (0.75-0.84) | 0.75 (0.6-0.74) | 0.5 (0.45-0.59) | 0.35 (0.3-0.44) | 0 (<0.3)
Treat these buckets as advisory conviction/risk framing only; do not assume automatic sizing.
Single-indicator setups and low-vol squeeze notes define advisory upper-bound context, not execution rules.

## Strategy Constraint Writing Rules
- For strategies[].constraints or strategies[].mandatory_constraints, emit short human-readable guardrails only.
- Good examples: "defined risk only", "prefer conservative size", "use simpler structures", "avoid near-term event exposure".
- Avoid pseudo-execution commands or encoded tokens such as "max_position_size_0.35", "simple_structure_vertical_spread", or "defined_risk_only".

## Compound Regime Priority (Highest → Lowest)
backwardation_event_risk > high_vol_event_risk > high_vol_backwardation > low_vol_backwardation >
high_vol_contango > low_vol_contango > low_vol_squeeze > high_vol > low_vol > normal

## Vol Regime Contract
- `vol_regime` is NOT `iv_rank_zone`; `iv_rank_zone` only tracks high|low|neutral IV rank bands
- If term structure is inverted and event risk is present, emit `backwardation_event_risk` even when IV Rank is high or low
- Never emit unsupported triples such as `high_vol_backwardation_event_risk`
- Do not invent unsupported compounds beyond the listed regimes above

## Output Field Derivation (Deterministic)
- `iv_percentile_divergence=true` when abs(option_vol_surface.iv_rank - option_vol_surface.iv_percentile) > 20.
- `hv_iv_assessment="realized_exceeds"` when stock_vol.hv_iv_spread > 0.03; `"implied_rich"` when stock_vol.hv_iv_spread < -0.03; otherwise `"neutral"`.
- `garch_divergence=true` when abs(stock_vol.garch_vol_forecast - option_vol_surface.current_iv) > 0.08.
- `garch_divergence_direction="vol_rise"` when stock_vol.garch_vol_forecast > option_vol_surface.current_iv by more than 0.08; `"vol_fall"` when lower by more than 0.08; otherwise null.
- `surface_mispricing=true` when option_vol_surface.vol_surface_fit_error > 0.02; `mispricing_magnitude=option_vol_surface.vol_surface_fit_error`.
- `liquidity_status="low"` when liquidity.bid_ask_spread_ratio > 0.15; otherwise `"high"`.

## Output Schema (Aligned with Synthesizer & Critic)
{"symbols":[{"symbol":"TICKER","vol_regime":"high_vol|low_vol|normal|squeeze|contango|backwardation|event_risk|high_vol_contango|low_vol_contango|high_vol_backwardation|low_vol_backwardation|high_vol_event_risk|low_vol_squeeze|backwardation_event_risk","iv_rank_zone":"high|low|neutral","iv_percentile_divergence":false,"hv_iv_assessment":"implied_rich|realized_exceeds|neutral","garch_divergence":false,"garch_divergence_direction":"vol_rise|vol_fall|null","surface_mispricing":false,"mispricing_magnitude":0.0,"event_risk_present":false,"vix_level":0.0,"earnings_proximity_days":null|number,"liquidity_status":"high|low","signal_type":"single_indicator|multi_indicator","trade_allowed":true|false,"confidence_cap":null|number,"simple_structures_only":true|false,"blocked_reasons":[],"strategies":[{"strategy_type":"","direction":"long_vol|short_vol|neutral","entry_conditions":"","exit_conditions":"","mandatory_constraints":[],"confidence":0.0-0.9}],"reasoning":"","confidence":0.0-0.9}],"market_vol_summary":"single-sentence batch summary only when multiple symbols are analyzed"}

Output pure JSON only. Populate blocked_reasons explicitly using the canonical hard/soft trade gate tokens above.
Always mark single-indicator signals in signal_type field and reasoning.
vol_regime must be a single string from the list above; no custom compounds.
"""