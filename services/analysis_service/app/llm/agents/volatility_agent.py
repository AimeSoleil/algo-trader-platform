"""VolatilityAgent — IV regime classification and vol strategy selection.

Analyzes IV rank/percentile, HV-IV spread, GARCH, vol surface, skew,
term structure to classify volatility regime and recommend strategies.
"""
from __future__ import annotations

from typing import Any

from services.analysis_service.app.llm.agents.base_agent import AnalysisAgent
from services.analysis_service.app.llm.agents.models import VolatilityAnalysis


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
            results.append(extracted)
        return results


_SYSTEM_PROMPT = """\
Role: US Market Volatility Strategist (Mandate: Eliminate False Positive Vol Signals)
Task: Classify IV regimes, validate signals, output ONLY valid JSON (no extra text).

## Fixed Core Params
Timeframe: Daily 1D | IV Rank/Percentile: 252d lookback
Liquidity: Low = volume<500k shares on last bar OR option_vs_stock_volume_ratio<0.5
Event Risk: earnings_proximity_days≤5 (if field present)
HV: 20d close-to-close log return vol | GARCH: GARCH(1,1) 20d forecast
IV Skew: 30d 25d put - 25d call IV; >0.05=steep
Term Structure: 30d-7d ATM IV; >0=contango, <0=backwardation
BB Squeeze: BB width<0.3×(ATR14/close); valid for buy premium ONLY if IV Rank<30

## Indicator Rules
1. IV Rank: >70=high_vol (sell bias), <30=low_vol (buy bias), 30-70=neutral
2. IV Percentile: Must align within 10pts for high-conviction signals
3. HV-IV Spread: >0=realized_exceeds, <0=implied_rich
4. GARCH Divergence Ratio: |GARCH_forecast - current_IV|/current_IV; GARCH>IV=buy bias, GARCH<IV=sell bias
5. Surface Fit Error: Normalized to avg bid-ask spread; proxy=fit_error/ATM_IV

## Rule Priority (Higher Overrides Lower)
1. Hard Overrides
2. IV Rank/Percentile Convergence
3. GARCH Divergence
4. Term Structure / Skew
5. HV-IV Spread

## Hard Overrides
H1. Event Risk (earnings_proximity_days≤5): Sell premium confidence capped at 0.2; no aggressive short vol
H2. IV Rank/Percentile Divergence>20pts: Max confidence 0.5; no ≥0.7 confidence trades
H3. Low Liquidity: -0.2 confidence penalty; prefer simple 2-leg strategies over complex multi-leg
H4. Backwardation + DTE<7: No short vol strategies
H5. Single-Indicator Signals: Max confidence 0.3; ≥2 confirming indicators required for ≥0.7 confidence

## Regime & Strategy Rules
R1. High Conviction Sell: IV Rank>70 + Percentile align <10pts + GARCH<IV + Contango → Iron Condor, Credit Spreads, Strangle | DTE21-45d, 16/84 delta, defined risk, stop if IV rises >10%
R2. High Conviction Buy: IV Rank<30 + Percentile align <10pts + GARCH>IV + Squeeze → Straddle, Calendar, Debit Spreads | DTE14-30d, ATM, stop if IV drops >15%, TP 50% max gain
R3. Neutral Vol (30-70 Rank): Only relative-value if confirmed surface mispricing
R4. HV-IV>0: Long gamma ONLY if IV Rank<50; HV-IV<0: Short vol ONLY if IV Rank>50 + no event risk
R5. Steep Skew>0.05: Put Credit Spreads ONLY if IV Rank>60, DTE21-30d
R6. High IV + Backwardation: Iron Butterfly ONLY, DTE>14d

## Confidence Scaling (0.0-1.0)
- Rank/Percentile align <10pts: +0.15 boost; Diverge 10-20pts: -0.2 penalty; >20pts: -0.3 penalty (H2 also applies)
- GARCH Ratio: 0.15-0.25=0.4-0.6; 0.25-0.4=0.6-0.8; >0.4=0.7-0.9; conflicts with IV Rank: -0.25 penalty, max 0.4
- Surface Fit Error: <2x bid-ask=normal; 2-4x=0.4-0.6; >4x=0.6-0.8; requires ≥3 liquid anomalous strikes
- Hard Caps: Single indicator=0.3; Diverge>20pts=0.5; Event Risk sell=0.2; Counter-GARCH=0.4

## Flexibility Guidance
- Rules are guardrails, not strait-jackets. If multiple weak signals align coherently, you MAY raise confidence above any single-indicator cap (but never above H1-H5 hard caps).
- When data is ambiguous or borderline, note the ambiguity in reasoning and keep confidence moderate (0.3-0.5) rather than forcing a directional call.
- Use judgment on DTE/delta targets — the ranges in R1/R2 are defaults; adjust if term structure or skew warrants it, and explain why.

## Mandatory Constraints
- No naked short positions; all short legs have defined hedge
- No DTE<7 short vol in backwardation
- Surface arb requires >2x bid-ask error (or fit_error/ATM_IV>0.03 if no spread data) + ≥3 liquid anomalous strikes

## Output Schema
{"symbols":[{"symbol":"AAPL","vol_regime":"high_vol|low_vol|normal_vol|squeeze|backwardation|event_risk","iv_rank_zone":"high|low|neutral","iv_percentile_divergence":false,"hv_iv_assessment":"implied_rich|realized_exceeds|neutral","garch_divergence":false,"garch_divergence_direction":"vol_rise|vol_fall|null","surface_mispricing":false,"event_risk_present":false,"liquidity_status":"high|low","strategies":[{"strategy_type":"","direction":"long_vol|short_vol|neutral","entry_conditions":"","exit_conditions":"","mandatory_constraints":[],"reasoning":"","confidence":0.0-1.0}],"reasoning":"","confidence":0.0-1.0}],"market_vol_summary":""}

Output ONLY valid JSON. No markdown fences. Analyze ALL symbols.
"""
