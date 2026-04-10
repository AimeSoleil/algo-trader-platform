"""ChainAgent — Option Chain Structure analysis.

Analyzes PCR, OI concentration, bid-ask spreads, volume imbalance,
gamma pinning, and theta decay for strike selection and liquidity filtering.
"""
from __future__ import annotations

from typing import Any

from services.analysis_service.app.llm.agents.base_agent import AnalysisAgent
from services.analysis_service.app.llm.agents.models import ChainAnalysis


class ChainAgent(AnalysisAgent):
    @property
    def name(self) -> str:
        return "chain"

    @property
    def output_model(self):
        return ChainAnalysis

    @property
    def system_prompt(self) -> str:
        return _SYSTEM_PROMPT

    def extract_signal_data(self, signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Extract price + option_chain + option_greeks fields."""
        results = []
        for sig in signals:
            extracted = {"symbol": sig.get("symbol", "UNKNOWN")}
            if "price" in sig:
                extracted["price"] = sig["price"]
            if "option_chain" in sig:
                extracted["option_chain"] = sig["option_chain"]
            if "option_greeks" in sig:
                extracted["option_greeks"] = sig["option_greeks"]
            results.append(extracted)
        return results


_SYSTEM_PROMPT = """\
Role: Option Chain Structure specialist. Task: Strike selection, liquidity filter, sentiment.

Indicators:
- PCR Volume: >1.5=extreme bearish, <0.5=extreme bullish
- PCR OI: longer-term positioning
- OI Concentration Top5: >0.80=pinned
- Bid-Ask Spread Ratio: >0.15=illiquid, <0.05=excellent
- Volume Imbalance: >0.4=heavy call, <-0.4=heavy put
- Delta Exposure: call vs put delta positioning
- Gamma Peak Strike: price gravitates here near expiry
- Theta Decay Rate: premium-selling context

Rules:
R1. PCR>1.5→extreme bearish→contrarian bullish(needs trend confirm)
R2. PCR<0.5→extreme bullish→contrarian bearish(needs confirm)
R3. OI_conc>0.80+DTE≤5→gamma pin→butterfly at gamma_peak
R4. bid_ask>0.15→illiquid→wider limits
R5. bid_ask>0.20→HARD BLOCK: do not trade
R6. vol_imbalance>0.4→institutional call buying→bullish
R7. vol_imbalance<-0.4→institutional put buying→bearish/hedge
R8. theta high+iv_rank>50→theta-selling edge→credit strategies
R9. theta high+iv_rank<30→calendar preferred
R10. gamma_peak within 1% of close→pinning→short premium here

Constraints:
- Every leg: daily volume≥100
- Exit strikes: OI≥500
- Hard reject: bid_ask>20% of mid
- PCR contrarian needs confirmation
- Gamma pin valid only DTE≤5

## Output Schema
{"symbols":[{"symbol":"AAPL","liquidity_ok":true,"hard_block":false,"pcr_signal":"contrarian_bullish|contrarian_bearish|neutral","gamma_pin_active":false,"gamma_pin_strike":null,"institutional_flow":"call_buying|put_buying|neutral","suggested_strikes":{},"reasoning":"","confidence":0.0-1.0}]}

Output ONLY valid JSON. No markdown fences. Analyze ALL symbols.
"""
