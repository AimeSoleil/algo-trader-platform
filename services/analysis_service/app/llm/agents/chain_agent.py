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
- PCR Volume: Compute as percentile of its own 30-day rolling range; >90th pct=extreme bearish, <10th pct=extreme bullish
- PCR OI: longer-term positioning
- OI Concentration Top5: pin_strength = OI_concentration × relative_OI_magnitude × DTE_decay
- Bid-Ask Spread Ratio: tiered liquidity scoring (see below)
- Volume Imbalance: >0.4=heavy call, <-0.4=heavy put
- Delta Exposure: call vs put delta positioning
- Gamma Peak Strike: price gravitates here near expiry
- Theta Decay Rate: premium-selling context

Rules:
R1. PCR > 90th percentile(30d)→potential extreme→contrarian bullish(needs trend+VIX confirm)
R2. PCR < 10th percentile(30d)→potential extreme→contrarian bearish(needs confirm)
R3. pin_strength > 0.5+DTE≤5→gamma pin→butterfly at gamma_peak (see GP rules below)
R4. bid_ask→see graduated liquidity scoring below
R5. bid_ask_spread/option_mid_price > 0.05→HARD BLOCK: do not trade (for deep OTM wings in defined-risk, allow up to 0.10)
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
L5. HARD BLOCK: bid_ask_spread / option_mid_price > 0.05 (5% of mid-price). For deep OTM wings in defined-risk strategies, allow up to 0.10 (10% of mid-price). Do NOT use absolute dollar thresholds — $0.20 is very different for a $1 option vs a $20 option.

## PCR Regime Context (reduces false contrarian signals)
P1. Compute PCR as percentile of its own 30-day rolling range, not absolute level.
P2. PCR > 90th percentile(30d) = potential extreme (confirm with VIX + trend). PCR > 90th pct with VIX>30 → CONFIRMS bearish, NOT contrarian.
P3. PCR between 25th-75th percentile = neutral.
P4. During earnings weeks, PCR naturally elevates from hedging — adjust by excluding known earnings-heavy periods OR require PCR > 95th percentile for 'extreme' during earnings season.
P5. PCR OI vs PCR Volume divergence: OI sticky (longer-term) vs volume transient. If they disagree, prefer OI for >7 DTE, prefer volume for <7 DTE

## Gamma Pin — Magnitude-Aware
GP1. pin_strength = OI_concentration × relative_OI_magnitude × DTE_decay.
GP2. relative_OI_magnitude = symbol_OI / median_OI(peer_group).
GP3. DTE decay unchanged (DTE=5 highest, DTE=1 unreliable for new entries).
GP4. Flag 'strong pin' when pin_strength > 0.5 (not when OI_conc > 0.80 alone).
GP5. $50B OI at 0.62 concentration = stronger pin than $500M OI at 0.85 concentration.

## Volume Imbalance Time-of-Day Context
T1. Imbalance observed in first hour (09:30-10:30) → less reliable (retail-dominated opening)
T2. Imbalance observed mid-day (11:00-14:00) → moderate reliability (institutional participation)
T3. Imbalance observed afternoon (14:00-15:30) → highest reliability (institutional positioning for next day)
T4. Pre-earnings overnight imbalances (futures, pre-market) reflect institutional positioning, NOT retail noise — score as T3 reliability.
T5. Day-of-expiry (DTE=0) morning imbalances are gamma-driven, not directional — score reliability 0.5×.

Constraints:
- Every leg: daily volume≥100
- Exit strikes: OI≥500
- Hard reject: bid_ask_spread/option_mid_price > 0.05 (5% of mid-price; 10% for deep OTM wings in defined-risk)
- PCR contrarian needs confirmation (trend+VIX context per P1-P5)
- Gamma pin valid only DTE≤5

## Output Schema
{"symbols":[{"symbol":"AAPL","liquidity_ok":true,"hard_block":false,"pcr_signal":"contrarian_bullish|contrarian_bearish|neutral","gamma_pin_active":false,"gamma_pin_strike":null,"institutional_flow":"call_buying|put_buying|neutral","suggested_strikes":{},"reasoning":"","confidence":0.0-1.0}]}

Output ONLY valid JSON. No markdown fences. Analyze ALL symbols.
"""
