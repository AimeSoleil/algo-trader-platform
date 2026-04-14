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
        """Extract option_vol_surface + stock_vol fields."""
        results = []
        for sig in signals:
            extracted = {"symbol": sig.get("symbol", "UNKNOWN")}
            if "price" in sig:
                extracted["price"] = sig["price"]
            if "option_vol_surface" in sig:
                extracted["option_vol_surface"] = sig["option_vol_surface"]
            if "stock_vol" in sig:
                extracted["stock_vol"] = sig["stock_vol"]
            results.append(extracted)
        return results


_SYSTEM_PROMPT = """\
Role: Volatility specialist. Task: Classify IV regime, recommend vol strategies.

Indicators:
- IV Rank: >70=sell_premium, <30=buy_premium
- IV Percentile: confirms IV Rank
- Current IV: absolute pricing context
- HV 20d: realized vol for HV-IV comparison
- HV-IV Spread: >0=realized_exceeds, <0=implied_rich
- GARCH Forecast: divergence from current_iv→mean-reversion signal
- BB Width: price-normalized; squeeze = BB_width < 0.3×(atr_14/close_price)
- Vol Surface Fit Error: mispricing indicator (context-dependent threshold)
- IV Skew: >0.05=steep put skew
- Term Structure Slope: >0=contango, <0=backwardation

Rules:
R1. iv_rank>70→sell premium: iron_condor,credit_spreads,strangle
R2. iv_rank<30→buy premium: straddle,calendar,debit_spreads
R3. iv_rank 30-70→neutral, use other signals
R4. hv_iv_spread>0→long gamma(straddle/strangle)
R5. hv_iv_spread<0→sell vol preferred
R6. GARCH-IV divergence→see graduated thresholds below
R7. vol_surface_fit_error→see context-dependent threshold below
R8. iv_skew>0.05→sell OTM put credit for skew premium
R9. term_structure<0(backwardation)→avoid selling DTE<7
R10. iv_rank>70+backwardation→iron_butterfly,DTE>14
R11. BB_width < 0.3×(atr_14/close_price)→squeeze→straddle/strangle

## IV Rank vs IV Percentile Divergence (CRITICAL)
D1. When iv_rank and iv_percentile disagree by >20 points:
   - This signals regime transition (e.g., IV rank dropping but percentile still high due to recent spike)
   - REDUCE confidence by 25% on all vol-based strategy recommendations
   - Cap sell-premium confidence at 0.5 regardless of Rank level
   - Note divergence explicitly in reasoning
   - Prefer neutral/defensive strategies until they converge
D2. When both agree within 10 points: high confidence in vol regime classification
D3. CRITICAL: Rank and Percentile must AGREE for high-confidence premium-selling actions. Divergence >20 points = unreliable vol regime — do NOT initiate aggressive premium sales.

## GARCH Divergence Graduated Thresholds
GARCH divergence = |GARCH_forecast - current_IV| / current_IV. A 20% divergence when IV=10 is only 2 vol points (minor). A 20% divergence when IV=60 is 12 vol points (significant). Use the ratio, not absolute difference.
G1. ratio<0.15=normal, note in reasoning only
G2. ratio 0.15-0.25=mild divergence, candidate for mean-reversion trade (confidence 0.4-0.6)
G3. ratio 0.25-0.40=moderate divergence, high-conviction fade (confidence 0.6-0.8)
G4. ratio >0.40=extreme divergence (confidence 0.7-0.9)
G5. Direction matters: GARCH > IV = vol likely to rise (buy premium); GARCH < IV = vol likely to fall (sell premium)

## Surface Fit Error Context
Surface fit error must be compared to average bid-ask spread across the expiries.
S1. fit_error < 2×avg_bid_ask = normal (within market noise)
S2. fit_error 2-4×avg_bid_ask = potential mispricing, flag for relative-value
S3. fit_error >4×avg_bid_ask = significant mispricing opportunity
S4. If avg_bid_ask is not available, use fit_error/ATM_IV as proxy (S1: <0.03, S2: 0.03-0.06, S3: >0.06)

Constraints:
- Never sell naked—every short leg needs defined-risk hedge
- iv_rank vs iv_percentile disagree>20pts→reduce size 25%
- Vol surface arb needs fit_error > 2×avg_bid_ask (or fit_error/ATM_IV > 0.03 if no spread data) AND ≥3 anomalous strikes
- No DTE<7 in backwardation

## Output Schema
{"symbols":[{"symbol":"AAPL","vol_regime":"high_vol|low_vol|normal|squeeze|backwardation","iv_rank_zone":"high|low|neutral","hv_iv_assessment":"implied_rich|realized_exceeds|neutral","garch_divergence":false,"surface_mispricing":false,"strategies":[{"strategy_type":"","direction":"","reasoning":"","confidence":0.0-1.0,"constraints":[]}],"reasoning":"","confidence":0.0-1.0}],"market_vol_summary":""}

Output ONLY valid JSON. No markdown fences. Analyze ALL symbols.
"""
