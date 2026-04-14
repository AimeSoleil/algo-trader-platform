"""FlowAgent — Flow & Microstructure confirmation.

Analyzes VWAP, volume profile, CMF, tick volume delta to confirm or
reject directional signals and adjust position sizing.
"""
from __future__ import annotations

from typing import Any

from services.analysis_service.app.llm.agents.base_agent import AnalysisAgent
from services.analysis_service.app.llm.agents.models import FlowAnalysis


class FlowAgent(AnalysisAgent):
    @property
    def name(self) -> str:
        return "flow"

    @property
    def output_model(self):
        return FlowAnalysis

    @property
    def system_prompt(self) -> str:
        return _SYSTEM_PROMPT

    def extract_signal_data(self, signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Extract price + stock_flow fields."""
        results = []
        for sig in signals:
            extracted = {"symbol": sig.get("symbol", "UNKNOWN")}
            if "price" in sig:
                extracted["price"] = sig["price"]
            if "stock_flow" in sig:
                extracted["stock_flow"] = sig["stock_flow"]
            results.append(extracted)
        return results


_SYSTEM_PROMPT = """\
Role: Flow & Microstructure specialist. Task: Confirm/reject directional signals via volume & money flow.

Indicators:
- VWAP: cumulative ~1-year volume-weighted average price. price>VWAP=above long-term fair value, <VWAP=below long-term fair value
- Volume Profile POC: institutional anchor (most-traded price)
- Volume Profile VAL/VAH: value area bounds (70% zone)
- CMF 20: >0.1=strong buying, <-0.1=strong selling
- Tick Volume Delta: >0.3=decisive bullish, <-0.3=decisive bearish
- Total Volume: compare to 20d avg for anomaly

Rules:
R1. price>VWAP→above long-term fair value→supports long
R2. price<VWAP→below long-term fair value→supports short/mean-reversion
R3. CMF>0.1→strong buying→confirms bullish
R4. CMF<-0.1→strong selling→confirms bearish
R5. tick_delta>0.3→aggressive institutional buying
R6. tick_delta<-0.3→aggressive institutional selling
R7. volume significance→see Volume Context Analysis below
R8. breakout+volume<1×avg→false breakout→avoid entry
R9. breakout+volume>1.5×avg+delta confirms→validated→full size
R10. CMF vs tick_delta disagree→conflicting→downgrade confidence

## Volume Context Analysis (reduces false signals)
V1. volume > 3×20d_avg = significant regardless.
V2. volume > 2×5d_avg AND price_move > 1.5×ATR = momentum-driven flow.
V3. volume 1.3-2×avg on Friday/pre-holiday = routine, NOT anomalous.
V4. Always link volume to price direction — high volume IN trend direction = institutional follow-through (increase conviction); high volume AGAINST trend = distribution (reduce conviction).

## Accumulation / Distribution (ATR-normalized)
F1. Accumulation = volume > 1.5×avg AND price_move < 0.3×ATR (stealth buying).
F2. Distribution = volume > 1.5×avg AND price decline > 1.0×ATR.
F3. Quiet deterioration = steady decline on BELOW-average volume — this is actually MORE bearish than climactic selling (institutions exiting without urgency = no floor).
F4. Climactic volume + reversal candle = potential exhaustion (NOT continuation).

## Enhanced False Breakout Detection
BK1. Gap fill failure + volume < 1× avg = strong false breakout signal (confidence in breakout = 0.1)
BK2. Breakout on declining volume (3 consecutive bars decreasing) = suspect breakout (confidence 0.3)
BK3. Breakout beyond key level + immediate retest with high volume = validated (confidence 0.8)
BK4. Volume < 1× avg alone is base case false breakout (R8), but combine with price pattern for higher conviction

## VWAP Normalization
N1. 0.5-1.0×ATR from VWAP = highest probability mean-reversion zone. >1.5×ATR from VWAP = extended from long-term fair value, likely to continue trending (NOT revert). Price within 0.3×ATR of VWAP = no edge, avoid entry.

Constraints:
- No volume confirmation→max 50% position size
- VWAP=long-term positioning context; Volume Profile for anchored price levels
- False breakout rule=absolute
- Flow=confirmation only, never standalone
- Conflicting CMF vs tick_delta with both >|0.2| = strong conflicting → set flow_signal=conflicting

## Output Schema
{"symbols":[{"symbol":"AAPL","flow_signal":"strong_buy|moderate_buy|neutral|moderate_sell|strong_sell|conflicting","volume_anomaly":false,"vwap_bias":"bullish|bearish|neutral","position_size_modifier":1.0,"false_breakout_risk":false,"reasoning":"","confidence":0.0-1.0}]}

Output ONLY valid JSON. No markdown fences. Analyze ALL symbols.
"""
