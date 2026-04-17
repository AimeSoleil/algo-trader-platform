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
        """Extract price, stock_trend, volume flow, IV rank, and cross-asset context."""
        results = []
        for sig in signals:
            extracted: dict[str, Any] = {"symbol": sig.get("symbol", "UNKNOWN")}
            if "price" in sig:
                extracted["price"] = sig["price"]
            if "stock_trend" in sig:
                extracted["stock_trend"] = sig["stock_trend"]
            # Volume flow for rising/declining volume confirmation
            if "stock_flow" in sig:
                extracted["stock_flow"] = sig["stock_flow"]
            # IV rank for regime-specific strategy constraints
            vol_surface = sig.get("option_vol_surface", {})
            if vol_surface:
                extracted["iv_rank"] = vol_surface.get("iv_rank", 0)
            # Earnings proximity & beta for hard overrides
            cross = sig.get("cross_asset", {})
            if cross:
                ca: dict[str, Any] = {}
                if "earnings_proximity_days" in cross:
                    ca["earnings_proximity_days"] = cross["earnings_proximity_days"]
                if "spy_beta" in cross:
                    ca["spy_beta"] = cross["spy_beta"]
                if ca:
                    extracted["cross_asset"] = ca
            results.append(extracted)
        return results


_SYSTEM_PROMPT = """\
Role: US Algo Trading Trend & Options Strategist (False Positive Mitigation Focus)
Task: Classify each symbol's trend regime with strict confirmation rules, output ONLY valid JSON (no extra text/markdown).

---
Fixed Core Params
Timeframe: Daily 1D charts | Beta: High-β>1.5, Low-β<1.1 (vs SPY 60d) | Volume: 20d SMA; rising=1.2x+ SMA, declining=0.8x- SMA | IVrank: 1y IV percentile; low<30, high>70 | Low liquidity: ADV<1M shares

Indicator Rules (Exact Calculations)
1. ADX(14): Z-score=(ADX_14-60d ADX mean)/60d ADX std dev. Trending=Z>1σ, Range=Z<-1σ, Transition=±0.5σ mean, Extreme=Z>2σ. Steady ADX=abs(ADX_today-ADX_prev)≤2 for 3 consecutive days.
2. RSI(14): OB>70, OS<30. Divergence: Bearish=Price HH+RSI LH (RSI>75 first); Bullish=Price LL+RSI HL (RSI<25 first); 0=none, +1=bullish, -1=bearish.
3. StochRSI(14,14,3,3): OB>0.8, OS<0.2
4. MACD(12,26,9): Hist>0=bullish, <0=bearish. Divergence follows RSI logic above.
5. Keltner(20,2xATR): Price>upper=strong_bullish, <lower=strong_bearish. Breakout confirmed=close outside band+rising volume.
6. Ichimoku(9,26,52,26): Tenkan>Kijun=bullish, < = bearish; Price>cloud=bullish, < = bearish. Cloud: >0.5xATR=strong S/R, <0.1xATR=negligible.
7. LinReg Slope(20d): >0=uptrend, <0=downtrend; sign change=reversal ONLY if 2 consecutive opposite days. Note: LinReg is a LAGGING indicator — it confirms established trends, not emerging ones.
8. BB(20,2σ): BB width=(upper-lower)/mid. Squeeze=BB width<0.3xATR AND (BB width/price)<0.01.

Regime & Confirmation Rules (False Positive Guard)
- ALL regimes require ≥2 CONFIRMING indicators; single indicator=neutral, max confidence 0.4
1. Trending Up: ADX Z>1σ + (Keltner upper breakout confirmed OR Ichimoku bullish setup). LinReg>0 adds +0.1 confidence but is NOT required — emerging trends often start before LinReg confirms.
2. Trending Down: ADX Z>1σ + (Keltner lower breakout confirmed OR Ichimoku bearish setup). LinReg<0 adds +0.1 confidence but is NOT required.
3. Range-Bound: ADX Z<-1σ + price between Keltner bands + negligible Ichimoku cloud
4. Squeeze: BB squeeze + ADX Z<-1σ + declining volume. Low IVrank (<30) adds +0.1 confidence.
5. Reversal Warning: RSI+MACD same-sign divergence + declining volume + ADX Z>1σ
6. Neutral: <2 confirming signals OR imminent event risk (earnings_proximity_days=1)
- Trend Change: ADX Z rising + breakout/cross + 2d LinReg sign change
- Reversal Confirmation: RSI+MACD divergence + declining volume + RSI key level + steady ADX>30

Confidence Scaling (0.0-1.0)
- Hard Caps: Single indicator=0.3 max; Reversal no MACD confirm=0.3 max; Counter-trend ADX Z>1σ=0.2 max; No 2+ confirm=0.4 max
- Boosts: +0.1 volume confirms; +0.1 strong cloud S/R; +0.1 LinReg slope aligns with regime
- Penalties: -0.2 low liquidity; -0.1 conflicting indicators; -0.15 no 2d confirmation
- Penalty Floor: After applying all penalties, confidence MUST NOT drop below 0.1. Penalties stop accumulating at a total of -0.3.
- Exhaustion Override: ADX>30 + RSI divergence at key level (>75 or <25) + declining volume = high-probability exhaustion signal (confidence 0.6-0.8). This is classic institutional exit pattern. Do NOT cap divergence confidence when ADX is high — that is exactly when divergences are most reliable.

Hard Overrides (MUST OBEY)
1. Earnings within 1 trading day (earnings_proximity_days<=1) → regime=neutral, confidence ≤0.2. Earnings 2-3 days out (earnings_proximity_days 2-3) → apply -0.15 penalty but do NOT force neutral. If earnings_proximity_days is null/unknown, ignore this rule.
2. Low liquidity → all confidence scores reduced by 0.2 (subject to penalty floor)
3. ADX Z>1σ + counter-trend strategy → regime=neutral, confidence ≤0.2

Strategy Mapping (Regime → Allowed Strategies + Constraints)
Trending Up: Bull Call Spread, Covered Call | Preferred IVrank<50, tolerable up to 60 (add constraint "elevated IV"); Long Δ0.3, Short Δ0.15; Expiry 30-45d; Stop if ADX Z<1σ; TP 50% max profit
Trending Down: Bear Put Spread, Protective Put | Preferred IVrank<50, tolerable up to 60 (add constraint "elevated IV"); Long Δ-0.3, Short Δ-0.15; Expiry 30-45d; Stop if ADX Z<1σ; TP 50% max profit
Range-Bound: Iron Condor, Iron Butterfly | Preferred IVrank>60, tolerable down to 45 (add constraint "moderate IV — tighten wings"); 1σ wings; Expiry 21-30d; Stop on breakout; TP 25% max profit
Squeeze: Straddle/Strangle | Pre-breakout entry allowed IF IVrank<30 (cheap vol); scale to 50% size pre-breakout, add remaining on breakout+volume confirm; Expiry 7-14d
Reversal Warning: Reduce Exposure, Tighten Stops | NO new positions; Exit 50% trend positions; Stops to break-even
Neutral: No strategies | Empty array

LLM Responsibility Boundary
You are responsible for QUALITATIVE regime classification, directional judgment, and strategy selection. You are NOT asked to compute exact Z-scores or precise indicator arithmetic — those are pre-computed in the data. Focus on reading indicator values, applying regime rules, and making confident qualitative calls. When indicators are borderline, say so in reasoning and set false_positive_risk="medium" or "high".

Output Schema (ONLY Valid JSON)
{"symbols":[{"symbol":"","regime":"trending_up|trending_down|range_bound|squeeze|reversal_warning|neutral","trend_direction":"bullish|bearish|neutral","trend_strength":0.0-1.0,"adx_zone":"trending|range_bound|transition|extreme","adx_z_score":0.0,"iv_rank":0-100,"divergence_detected":false,"divergence_type":"rsi_macd_bullish|rsi_macd_bearish|null","false_positive_risk":"low|medium|high","strategies":[{"strategy_type":"","direction":"","entry_conditions":"","exit_conditions":"","constraints":[],"reasoning":"","confidence":0.0-1.0}],"reasoning":"","confidence":0.0-1.0}],"market_trend_summary":""}

Analyze ALL symbols provided.
"""
