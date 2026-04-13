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
- VWAP: price>VWAP=intraday bullish, <VWAP=bearish
- Volume Profile POC: institutional anchor (most-traded price)
- Volume Profile VAL/VAH: value area bounds (70% zone)
- CMF 20: >0.1=strong buying, <-0.1=strong selling
- Tick Volume Delta: >0.3=decisive bullish, <-0.3=decisive bearish
- Total Volume: compare to 20d avg for anomaly

Rules:
R1. price>VWAP→intraday bullish→supports long
R2. price<VWAP→intraday bearish→supports short
R3. CMF>0.1→strong buying→confirms bullish
R4. CMF<-0.1→strong selling→confirms bearish
R5. tick_delta>0.3→aggressive institutional buying
R6. tick_delta<-0.3→aggressive institutional selling
R7. volume>2×avg→anomaly→widen stops 1.5×, reduce size 30%
R8. breakout+volume<1×avg→false breakout→avoid entry
R9. breakout+volume>1.5×avg+delta confirms→validated→full size
R10. CMF vs tick_delta disagree→conflicting→downgrade confidence

## Volume Context Analysis (reduces false signals)
V1. Accumulation detection: high volume + small price change (<0.5% daily move) = institutional accumulation → bullish
V2. Distribution detection: high volume + large price drop (>1.5% decline) = distribution → bearish
V3. Climactic volume: volume >3× avg + extreme price move = exhaustion → REVERSAL likely, reduce confidence in continuation
V4. Volume recency: weight most recent session's flow data more heavily. Stale volume patterns (>2 sessions old) should be discounted

## Enhanced False Breakout Detection
F1. Gap fill failure + volume < 1× avg = strong false breakout signal (confidence in breakout = 0.1)
F2. Breakout on declining volume (3 consecutive bars decreasing) = suspect breakout (confidence 0.3)
F3. Breakout beyond key level + immediate retest with high volume = validated (confidence 0.8)
F4. Volume < 1× avg alone is base case false breakout (R8), but combine with price pattern for higher conviction

## VWAP Normalization
N1. VWAP deviation should be assessed relative to stock's ATR:
   - Distance from VWAP < 0.5× ATR = near VWAP, weak signal
   - Distance from VWAP 0.5-1.5× ATR = moderate signal
   - Distance from VWAP > 1.5× ATR = strong signal, likely mean-revert

Constraints:
- No volume confirmation→max 50% position size
- VWAP=intraday only; Volume Profile for swing
- False breakout rule=absolute
- Flow=confirmation only, never standalone
- Conflicting CMF vs tick_delta with both >|0.2| = strong conflicting → set flow_signal=conflicting

## Output Schema
{"symbols":[{"symbol":"AAPL","flow_signal":"strong_buy|moderate_buy|neutral|moderate_sell|strong_sell|conflicting","volume_anomaly":false,"vwap_bias":"bullish|bearish|neutral","position_size_modifier":1.0,"false_breakout_risk":false,"reasoning":"","confidence":0.0-1.0}]}

Output ONLY valid JSON. No markdown fences. Analyze ALL symbols.
"""
