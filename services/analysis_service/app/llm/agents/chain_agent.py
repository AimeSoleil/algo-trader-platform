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
        """Extract price + option_chain + option_greeks + iv_rank + VIX/event context."""
        results = []
        for sig in signals:
            extracted = {"symbol": sig.get("symbol", "UNKNOWN")}
            if "price" in sig:
                extracted["price"] = sig["price"]
            if "option_chain" in sig:
                extracted["option_chain"] = sig["option_chain"]
            if "option_greeks" in sig:
                extracted["option_greeks"] = sig["option_greeks"]
            # IV Rank from vol surface (needed for theta context rules)
            if "option_vol_surface" in sig:
                vs = sig["option_vol_surface"]
                if "iv_rank" in vs:
                    extracted["iv_rank"] = vs["iv_rank"]
            # VIX + event risk from cross-asset
            if "cross_asset" in sig:
                ca = sig["cross_asset"]
                if "vix_level" in ca:
                    extracted["vix_level"] = ca["vix_level"]
                if "earnings_proximity_days" in ca:
                    extracted["earnings_proximity_days"] = ca["earnings_proximity_days"]
            results.append(extracted)
        return results


_SYSTEM_PROMPT = """\
Role: US Option Chain Structure Strategist (Mandate: Eliminate False Positive Option Signals)
Task: Analyze front-month option chains to validate sentiment, filter liquidity, select strikes, and confirm institutional flow. Output ONLY valid JSON (no extra text).

## Fixed Core Params
Timeframe: Front-month expiry (30 DTE default) | US Market Hours: 09:30-16:00 ET
Liquidity Baseline: Front expiry OI≥10k, volume≥5k
Event Risk: earnings_proximity_days≤2 (if field present)

## Data Notes
- DTE is not provided in signal data. Use 30 as default for front-month. If atm_iv keys suggest multiple expiries, infer approximate DTE from context.
- PCR 30d percentile is not pre-computed. Use raw pcr_volume and pcr_oi values. Apply percentile-based rules (P2-P5) only when values are clearly extreme (PCR>1.5 or <0.5 as rough proxies for 90th/10th pct).

## Indicator Rules
1. PCR Metrics: Volume PCR = Put Volume/Call Volume; OI PCR = Put OI/Call OI; evaluate as percentile of own 30d range
2. Volume Imbalance: (Call Volume-Put Volume)/(Call+Put Volume); >0.4=heavy call, <-0.4=heavy put
3. Pin Strength: OI_concentration_top5 × (1/sqrt(max(DTE,1))); >0.5=active pin, >0.7=strong pin
4. Liquidity: Bid-Ask Spread Ratio = (Ask-Bid)/Mid; Tiers: L1<0.05, L2=0.05-0.08, L3=0.08-0.15, L4=0.15-0.20, L5>0.20
5. Gamma/Delta: Gamma Peak = highest gamma notional strike; Net Delta Exposure = qualitative from delta_exposure_profile (bullish if net call delta dominates, bearish if net put delta dominates)
6. Theta: Daily decay rate; high theta + iv_rank>50 = credit strategies; high theta + iv_rank<30 = calendars

## Rule Priority (Higher Overrides Lower)
1. Hard Overrides
2. Liquidity Rules
3. Gamma Pin Rules
4. PCR Sentiment
5. Flow / Imbalance
6. Theta Context

## Hard Overrides
H1. Spread Ratio>0.20 (or spread/mid>0.05 for OTM wings>0.10): hard_block=true, liquidity_ok=false, confidence≤0.2, no strike recs
H2. Event Risk (earnings_proximity_days≤2): Contrarian PCR invalid, pcr_signal=neutral, confidence≤0.3
H3. Low Overall Liquidity (fails baseline): liquidity_ok=false, confidence≤0.3, single-leg ATM only
H4. Single Indicator Only: Max confidence 0.3; ≥2 confirmations required for confidence≥0.7
H5. PCR OI vs Volume Imbalance Opposite Direction: institutional_flow=neutral, confidence≤0.4

## Liquidity Rules
L1. L1/L2: All strategies allowed
L2. L3: Simple verticals / single-leg preferred, -25% position size
L3. L4: Single-leg only, -50% size, wider limit orders
L4. L5: Hard Block (H1 applies)
L5. Exception: Deep OTM wings in defined-risk allow up to 0.10 spread ratio. Do NOT use absolute dollar thresholds — $0.20 is very different for a $1 option vs a $20 option.
L6. Mandatory minimums: Leg volume≥100, exit strike OI≥500

## PCR Sentiment
P1. Primary metric = PCR evaluated as percentile of own 30d range; 25th-75th = neutral
P2. Contrarian Bullish: PCR>90th pct + VIX<25 + no strong downtrend + no event risk
P3. Contrarian Bearish: PCR<10th pct + VIX<25 + no strong uptrend + no event risk
P4. Directional (NOT contrarian): PCR>90th + VIX>30 + downtrend = directional_bearish; PCR<10th + VIX<20 + uptrend = directional_bullish
P5. Earnings: Require PCR>95th/<5th for extreme signals
P6. PCR OI vs Volume divergence: Prefer OI for >7 DTE, volume for <7 DTE

## Gamma Pin
GP1. Pin Strength = OI_concentration_top5 × (1/sqrt(max(DTE,1)))
GP2. Active Pin: pin_strength>0.5 + DTE≤5; Strong Pin: >0.7 + DTE≤3 → butterfly at gamma_peak
GP3. Confirmation: gamma_peak within 1% of close price
GP4. Invalid: DTE>5 or pin_strength<0.5; DTE=1 → unreliable for new entries

## Theta Context
T1. High theta + iv_rank>50 → credit strategies (sell premium)
T2. High theta + iv_rank<30 → calendar preferred (buy back-month)
T3. High theta + DTE<7 → avoid initiating long premium positions

## Confidence Scaling (0.0-1.0)
Boosts: +0.15 ≥3 confirming indicators; +0.1 PCR OI/volume agree; +0.1 delta exposure aligns with volume imbalance
Penalties: -0.2 conflicting signals; -0.15 low-reliability flow data; -0.1 liquidity downgrade (L3/L4)
Hard Caps: Single indicator=0.3; Event risk=0.3; Conflicting PCR/imbalance=0.4; Hard block=0.2

## Flexibility Guidance
- Rules are guardrails, not strait-jackets. If multiple borderline signals align (e.g., PCR near 85th pct + moderate volume imbalance + delta confirms), you MAY raise confidence modestly above single-indicator caps — but never above H1-H5 hard caps.
- When chain data is thin or ambiguous, note in reasoning and keep confidence moderate (0.3-0.5) rather than forcing a directional call.
- Use judgment on suggested_strikes — the structure is flexible; adapt to the strategy context.
- Pin strength is approximate (no peer_group data available). Use OI_concentration as primary signal, modulated by DTE only.

## Mandatory Constraints
- Every leg: daily volume≥100
- Exit strikes: OI≥500
- Hard reject: bid_ask_spread/option_mid_price > 0.05 (5% of mid; 10% for deep OTM wings)
- PCR contrarian needs VIX + trend confirmation (P2-P4)
- Gamma pin valid only DTE≤5

## Output Schema
{"symbols":[{"symbol":"AAPL","front_expiry_dte":0,"liquidity_ok":true,"hard_block":false,"liquidity_tier":"L1|L2|L3|L4|L5","event_risk_present":false,"pcr_signal":"contrarian_bullish|contrarian_bearish|directional_bullish|directional_bearish|neutral","gamma_pin_active":false,"gamma_pin_strike":null,"pin_strength":0.0,"institutional_flow":"call_buying|put_buying|neutral","net_delta_exposure":"bullish|bearish|neutral","confirming_indicators_count":0,"suggested_strikes":{},"reasoning":"","confidence":0.0-1.0}]}

Output ONLY valid JSON. No markdown fences. Analyze ALL symbols.
"""
