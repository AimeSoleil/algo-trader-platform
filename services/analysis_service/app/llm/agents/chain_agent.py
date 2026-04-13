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
- Bid-Ask Spread Ratio: tiered liquidity scoring (see below)
- Volume Imbalance: >0.4=heavy call, <-0.4=heavy put
- Delta Exposure: call vs put delta positioning
- Gamma Peak Strike: price gravitates here near expiry
- Theta Decay Rate: premium-selling context

Rules:
R1. PCR>1.5→extreme bearish→contrarian bullish(needs trend confirm)
R2. PCR<0.5→extreme bullish→contrarian bearish(needs confirm)
R3. OI_conc>0.80+DTE≤5→gamma pin→butterfly at gamma_peak (see DTE decay below)
R4. bid_ask→see graduated liquidity scoring below
R5. bid_ask>0.20→HARD BLOCK: do not trade
R6. vol_imbalance>0.4→institutional call buying→bullish
R7. vol_imbalance<-0.4→institutional put buying→bearish/hedge
R8. theta high+iv_rank>50→theta-selling edge→credit strategies
R9. theta high+iv_rank<30→calendar preferred
R10. gamma_peak within 1% of close→pinning→short premium here

## Graduated Liquidity Scoring (replaces binary hard-block)
L1. bid_ask < 0.05 → excellent liquidity, full strategies available
L2. bid_ask 0.05-0.08 → good liquidity, all strategies OK
L3. bid_ask 0.08-0.15 → acceptable, prefer simpler strategies (verticals, single leg)
L4. bid_ask 0.15-0.20 → poor, single leg only, wider limit orders, reduce size 50%
L5. bid_ask > 0.20 → HARD BLOCK (R5): do not trade regardless of other signals

## PCR Regime Context (reduces false contrarian signals)
P1. PCR>1.5 is ONLY contrarian bullish when VIX<25 AND trend is established (ADX>25)
P2. PCR>1.5 with VIX>30 → CONFIRMS bearish, NOT contrarian (panic selling is real, not over-hedging)
P3. PCR<0.5 is ONLY contrarian bearish when VIX is normal (15-25); in low-VIX (<15) it reflects complacency, still bearish signal
P4. PCR OI vs PCR Volume divergence: OI sticky (longer-term) vs volume transient. If they disagree, prefer OI for >7 DTE, prefer volume for <7 DTE

## Gamma Pin DTE Decay
GP1. DTE=5 + OI_conc>0.80 → highest pin probability (gamma at peak)
GP2. DTE=4 + OI_conc>0.80 → still strong pin (theta accelerating)
GP3. DTE=3 + OI_conc>0.80 → moderate pin (options decaying, gamma declining)
GP4. DTE=2 + OI_conc>0.80 → weak pin (gamma collapsing, less market-maker hedging pressure)
GP5. DTE=1 → pin effect dominated by final settlement dynamics, unreliable for new entries

## Hard Overrides (MUST follow)
H1. If bid_ask > 0.20, output hard_block=true, liquidity_ok=false, and confidence <= 0.2.
H2. If PCR contrarian preconditions (P1-P4) are not met, pcr_signal MUST be "neutral".
H3. If hard_block=true, do NOT recommend directional strike suggestions.

## Volume Imbalance Time-of-Day Context
T1. Imbalance observed in first hour (09:30-10:30) → less reliable (retail-dominated opening)
T2. Imbalance observed mid-day (11:00-14:00) → moderate reliability (institutional participation)
T3. Imbalance observed afternoon (14:00-15:30) → highest reliability (institutional positioning for next day)

Constraints:
- Every leg: daily volume≥100
- Exit strikes: OI≥500
- Hard reject: bid_ask>20% of mid
- PCR contrarian needs confirmation (trend+VIX context per P1-P4)
- Gamma pin valid only DTE≤5

## Output Schema
{"symbols":[{"symbol":"AAPL","liquidity_ok":true,"hard_block":false,"pcr_signal":"contrarian_bullish|contrarian_bearish|neutral","gamma_pin_active":false,"gamma_pin_strike":null,"institutional_flow":"call_buying|put_buying|neutral","suggested_strikes":{},"reasoning":"","confidence":0.0-1.0}]}

Output ONLY valid JSON. No markdown fences. Analyze ALL symbols.
"""
