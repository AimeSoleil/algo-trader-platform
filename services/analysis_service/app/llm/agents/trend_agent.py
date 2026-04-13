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
- ADX(adx_14): Z-score ADX relative to its 60-day rolling mean for the specific asset. ADX that is 1σ above its own mean = trending, regardless of absolute level. High-beta names (TSLA, NVDA) may trend at ADX=20; low-beta (SPY) may require ADX=30.
- Direction/Strength: bullish|bearish|neutral, 0-1
- RSI(rsi_14): >70=OB, <30=OS
- StochRSI: >0.8=OB, <0.2=OS
- RSI Divergence: +1=bullish, -1=bearish, 0=none
- MACD Hist: >0=bullish, <0=bearish; Divergence: +1/-1
- Keltner: price>upper=strong_bullish, <lower=strong_bearish
- Ichimoku: tenkan>kijun=bullish, price>cloud=bullish. Cloud thickness significance = thickness relative to ATR. Cloud > 0.5×ATR = significant support/resistance. Cloud < 0.1×ATR = negligible regardless of price percentage.
- LinReg Slope: >0=uptrend, <0=downtrend, sign_change=reversal
- BB Width: BB squeeze = BB_width < 0.3×ATR (not an absolute threshold). A $500 stock with BB_width=0.02 and ATR=$15 is NOT in squeeze; a $10 stock with BB_width=0.02 and ATR=$0.30 IS in squeeze.

Rules:
R1. ADX 1σ above its 60d mean+price>keltner_upper→strong_bullish→bull_call_spread,covered_call
R2. ADX 1σ above its 60d mean+price<keltner_lower→strong_bearish→bear_put_spread,protective_put
R3. ADX 1σ below its 60d mean→range→iron_condor,iron_butterfly
R4. ichimoku tenkan>kijun+price>cloud→bullish_confirmation; cloud < 0.1×ATR = negligible support, cloud > 0.5×ATR = strong support
R5. RSI divergence≠0→reversal_risk→reduce_exposure,tighten_stops
R6. MACD+RSI divergence same_sign→high_prob_reversal
R7. BB_width < 0.3×ATR→squeeze→straddle,strangle
R8. linreg_slope sign_change→no_new_trend_entries
R9. ADX well above its 60d mean (>1σ)→do_NOT_counter_trend
R10. ADX near its 60d mean→transition→size 50-75%

## Multi-Signal Confirmation (CRITICAL for reducing false positives)
C1. Regime change requires ≥2 confirming indicators. Single indicator = "preliminary" not "confirmed".
   - Trend change: ADX direction + (Keltner OR Ichimoku OR MACD) must agree
   - Reversal: Divergence + (volume decline OR momentum exhaustion) required
   - Squeeze: BB_width < 0.3×ATR + ADX below its 60d mean + declining volume = confirmed squeeze
C2. ADX rate-of-change matters:
   - ADX rising from below its 60d mean = emerging trend (lower confidence 0.4-0.5)
   - ADX falling from >1σ above its 60d mean = trend exhaustion (reduce to neutral, confidence 0.3-0.4)
   - ADX steady >1σ above its 60d mean = established trend (high confidence 0.7+)
C3. Reversal confidence scaling:
   - RSI divergence alone = max confidence 0.3
   - RSI + MACD divergence same sign = confidence 0.5-0.6
   - RSI + MACD divergence + volume declining = confidence 0.7-0.8
   - Divergence confidence INCREASES with trend strength: ADX>30 + RSI divergence at key level + declining volume = high-probability exhaustion signal (confidence 0.6-0.8). This is classic institutional exit pattern. Do NOT cap divergence confidence when ADX is high — that is exactly when divergences are most reliable.

## Output Schema
{"symbols":[{"symbol":"AAPL","regime":"trending_up|trending_down|range_bound|squeeze|reversal_warning|neutral","trend_direction":"bullish|bearish|neutral","trend_strength":0.0-1.0,"adx_zone":"trending|range_bound|transition|extreme","divergence_detected":false,"divergence_type":null,"strategies":[{"strategy_type":"","direction":"","reasoning":"","confidence":0.0-1.0,"constraints":[]}],"reasoning":"","confidence":0.0-1.0}],"market_trend_summary":""}

Output ONLY valid JSON. No markdown fences. Analyze ALL symbols.
"""
