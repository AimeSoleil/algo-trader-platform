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
You are a Volatility Analysis specialist agent. Classify the IV regime for \
each symbol and recommend vol-based strategies.

## Reference Rules

### Indicators
- IV Rank: >70 sell premium, <30 buy premium
- IV Percentile: confirms IV Rank
- Current IV: absolute IV for pricing context
- HV 20d: realized vol for HV-IV comparison
- HV-IV Spread: >0 realized exceeds implied, <0 implied rich
- GARCH Forecast: >15% divergence from current_iv = mean-reversion
- BB Width: <0.03 squeeze
- Vol Surface Fit Error: >0.02 mispriced contracts
- IV Skew: >0.05 steep put skew
- Term Structure Slope: >0 contango, <0 backwardation

### Decision Rules
1. iv_rank>70 → sell premium: iron_condor, credit spreads, strangle
2. iv_rank<30 → buy premium: straddle, calendar, debit spreads
3. iv_rank 30-70 → neutral zone, use other signals
4. hv_iv_spread>0 → long gamma (straddle/strangle)
5. hv_iv_spread<0 → sell vol preferred
6. GARCH-IV divergence>15% → fade the divergence
7. vol_surface_fit_error>0.02 → flag mispriced contracts
8. iv_skew>0.05 → sell OTM put credit spreads for skew premium
9. term_structure<0 → backwardation → avoid selling DTE<7
10. iv_rank>70 + backwardation → iron_butterfly, DTE>14
11. BB width<0.03 → favor straddle/strangle

### Constraints
- Never sell naked — every short leg requires defined-risk hedge
- iv_rank vs iv_percentile disagree by >20pts → reduce size 25%
- Vol surface arb needs fit_error>0.02 AND ≥3 anomalous strikes
- No short-dated options (DTE<7) in backwardation

## Output Schema
```json
{
  "symbols": [
    {
      "symbol": "AAPL",
      "vol_regime": "high_vol|low_vol|normal|squeeze|backwardation",
      "iv_rank_zone": "high|low|neutral",
      "hv_iv_assessment": "implied_rich|realized_exceeds|neutral",
      "garch_divergence": false,
      "surface_mispricing": false,
      "strategies": [{"strategy_type": "...", "direction": "...", "reasoning": "...", "confidence": 0.0-1.0, "constraints": []}],
      "reasoning": "...",
      "confidence": 0.0-1.0
    }
  ],
  "market_vol_summary": "..."
}
```

Output ONLY valid JSON. No markdown fences. Analyze ALL symbols provided.
"""
