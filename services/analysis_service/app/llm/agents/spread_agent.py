"""SpreadAgent — Spread & Arbitrage evaluation.

Analyzes vertical R:R, calendar theta, butterfly pricing, box arb,
IV skew, and term structure for optimal multi-leg construction.
"""
from __future__ import annotations

from typing import Any

from services.analysis_service.app.llm.agents.base_agent import AnalysisAgent
from services.analysis_service.app.llm.agents.models import SpreadAnalysis


class SpreadAgent(AnalysisAgent):
    @property
    def name(self) -> str:
        return "spread"

    @property
    def output_model(self):
        return SpreadAnalysis

    @property
    def system_prompt(self) -> str:
        return _SYSTEM_PROMPT

    def extract_signal_data(self, signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Extract option_spreads + option_vol_surface fields."""
        results = []
        for sig in signals:
            extracted = {"symbol": sig.get("symbol", "UNKNOWN")}
            if "price" in sig:
                extracted["price"] = sig["price"]
            if "option_spreads" in sig:
                extracted["option_spreads"] = sig["option_spreads"]
            if "option_vol_surface" in sig:
                extracted["option_vol_surface"] = sig["option_vol_surface"]
            results.append(extracted)
        return results


_SYSTEM_PROMPT = """\
Role: Spread & Arbitrage specialist. Task: Evaluate multi-leg structures for R:R, theta, pricing anomalies.

Indicators:
- Vertical R:R: >2.0=favorable, <0.5=poor
- Calendar Theta: >0.05/day=attractive
- Butterfly Pricing Error: >0.10=mispriced wings
- Box Spread Arb: >0.01=risk-free profit
- IV Skew: >0.05→sell OTM puts
- Term Structure: >0=contango(calendar favorable)
- IV Rank: 30-60=ideal for calendars
- Bid-Ask: >0.10/leg→reject

Rules:
R1. vertical_rr>2.0→favorable→prefer this vertical
R2. vertical_rr<0.5→poor→avoid or reverse
R3. calendar_theta>0.05+contango+iv_rank 30-60→enter calendar
R4. butterfly_error>0.10→mispriced→cheap wings or arb
R5. box_arb>0.01→risk-free→execute if all legs liquid
R6. iv_skew>0.05→sell OTM put credit for skew premium
R7. DTE 30-45 optimal for verticals
R8. Short-leg DTE 14-21 optimal for calendars
R9. bid_ask>0.10/leg→reject spread
R10. breakeven prob<40%→reject

Constraints:
- Max $5 spread width for standard accounts
- All legs simultaneously—never leg in
- No legging when VIX>30 or move>2%
- Net credit/debit>5× commissions
- Fully defined risk only
- Arb signals decay fast(re-verify if >60s old)

## Output Schema
{"symbols":[{"symbol":"AAPL","best_spread_type":"vertical|calendar|butterfly|box_arb","risk_reward_ratio":0.0,"theta_capture":0.0,"mispricing_detected":false,"arb_opportunity":false,"optimal_dte":null,"constraints":[],"reasoning":"","confidence":0.0-1.0}]}

Output ONLY valid JSON. No markdown fences. Analyze ALL symbols.
"""
