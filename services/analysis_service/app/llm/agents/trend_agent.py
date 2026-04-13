"""TrendAgent — Trend & Momentum analysis.

Analyzes ADX, RSI, MACD, Ichimoku, Keltner, Bollinger, linear regression
to classify trend regime and recommend directional strategies.
"""
from __future__ import annotations

from typing import Any

from services.analysis_service.app.llm.agents.base_agent import AnalysisAgent
from services.analysis_service.app.llm.agents.models import TrendAnalysis


class TrendAgent(AnalysisAgent):
    @property
    def name(self) -> str:
        return "trend"

    @property
    def output_model(self):
        return TrendAnalysis

    @property
    def system_prompt(self) -> str:
        return _SYSTEM_PROMPT

    def extract_signal_data(self, signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Extract price + stock_trend fields."""
        results = []
        for sig in signals:
            extracted = {"symbol": sig.get("symbol", "UNKNOWN")}
            if "price" in sig:
                extracted["price"] = sig["price"]
            if "stock_trend" in sig:
                extracted["stock_trend"] = sig["stock_trend"]
            results.append(extracted)
        return results


_SYSTEM_PROMPT = """\
Role: Trend & Momentum specialist. Task: Classify each symbol's trend regime.

Indicators:
- ADX(adx_14): >25=trending, <20=range, >40=extreme, 20-25=transition
- Direction/Strength: bullish|bearish|neutral, 0-1
- RSI(rsi_14): >70=OB, <30=OS
- StochRSI: >0.8=OB, <0.2=OS
- RSI Divergence: +1=bullish, -1=bearish, 0=none
- MACD Hist: >0=bullish, <0=bearish; Divergence: +1/-1
- Keltner: price>upper=strong_bullish, <lower=strong_bearish
- Ichimoku: tenkan>kijun=bullish, price>cloud=bullish, cloud_thickness=trend strength
- LinReg Slope: >0=uptrend, <0=downtrend, sign_change=reversal
- BB Width: <0.03=squeeze

Rules:
R1. ADX>25+price>keltner_upper→strong_bullish→bull_call_spread,covered_call
R2. ADX>25+price<keltner_lower→strong_bearish→bear_put_spread,protective_put
R3. ADX<20→range→iron_condor,iron_butterfly
R4. ichimoku tenkan>kijun+price>cloud→bullish_confirmation; thin cloud(<1% of price)=weak support, thick cloud(>3%)=strong support
R5. RSI divergence≠0→reversal_risk→reduce_exposure,tighten_stops
R6. MACD+RSI divergence same_sign→high_prob_reversal
R7. BB_width<0.03→squeeze→straddle,strangle
R8. linreg_slope sign_change→no_new_trend_entries
R9. ADX>30→do_NOT_counter_trend
R10. ADX 20-25→transition→size 50-75%

## Multi-Signal Confirmation (CRITICAL for reducing false positives)
C1. Regime change requires ≥2 confirming indicators. Single indicator = "preliminary" not "confirmed".
   - Trend change: ADX direction + (Keltner OR Ichimoku OR MACD) must agree
   - Reversal: Divergence + (volume decline OR momentum exhaustion) required
   - Squeeze: BB_width<0.03 + ADX<20 + declining volume = confirmed squeeze
C2. ADX rate-of-change matters:
   - ADX rising from <20 toward 25 = emerging trend (lower confidence 0.4-0.5)
   - ADX falling from >30 toward 25 = trend exhaustion (reduce to neutral, confidence 0.3-0.4)
    - ADX steady >30 = established trend (high confidence 0.7+)
    - Define "steady": abs(adx_14 - prior_adx_14) <= 2 when prior value is available; if prior value unavailable, do NOT assume steady.
C3. Reversal confidence scaling:
   - RSI divergence alone = max confidence 0.3
   - RSI + MACD divergence same sign = confidence 0.5-0.6
   - RSI + MACD divergence + volume declining = confidence 0.7-0.8
   - Single divergence with ADX>30 trend = likely false signal, confidence 0.2

## Hard Overrides (MUST follow)
H1. If only RSI divergence is present (without MACD divergence confirmation), strategy confidence MUST be <= 0.3.
H2. If ADX>30 and proposed direction is counter-trend, set trend_direction="neutral" and confidence <= 0.2.
H3. If C1 multi-signal confirmation is not satisfied, do NOT output a confirmed regime; use neutral/preliminary with confidence <= 0.4.

## Output Schema
{"symbols":[{"symbol":"AAPL","regime":"trending_up|trending_down|range_bound|squeeze|reversal_warning|neutral","trend_direction":"bullish|bearish|neutral","trend_strength":0.0-1.0,"adx_zone":"trending|range_bound|transition|extreme","divergence_detected":false,"divergence_type":null,"strategies":[{"strategy_type":"","direction":"","reasoning":"","confidence":0.0-1.0,"constraints":[]}],"reasoning":"","confidence":0.0-1.0}],"market_trend_summary":""}

Output ONLY valid JSON. No markdown fences. Analyze ALL symbols.
"""
