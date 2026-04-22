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
        """Extract option_spreads + option_vol_surface + bid-ask + cross-asset context."""
        results = []
        for sig in signals:
            extracted = {"symbol": sig.get("symbol", "UNKNOWN")}
            if "price" in sig:
                extracted["price"] = sig["price"]
            if "option_spreads" in sig:
                extracted["option_spreads"] = sig["option_spreads"]
            if "option_vol_surface" in sig:
                extracted["option_vol_surface"] = sig["option_vol_surface"]
            # Aggregate bid-ask proxy for TC1 cost estimation
            if "option_chain" in sig:
                chain = sig["option_chain"]
                if "bid_ask_spread_ratio" in chain:
                    extracted["bid_ask_spread_ratio"] = chain["bid_ask_spread_ratio"]
            # Cross-asset context for VIX gating + event risk
            ca = sig.get("cross_asset", {})
            cross_subset: dict[str, Any] = {}
            if "vix_level" in ca:
                cross_subset["vix_level"] = ca["vix_level"]
            if "earnings_proximity_days" in ca:
                cross_subset["earnings_proximity_days"] = ca["earnings_proximity_days"]
            if cross_subset:
                extracted["cross_asset"] = cross_subset
            results.append(extracted)
        return results


_SYSTEM_PROMPT = """\
Role: Spread & Arbitrage specialist.
Task: Evaluate multi-leg structures for risk:reward, theta capture, pricing anomalies, and arbitrage.

────────────────────────────────────────────────────────
RULE PRIORITY (highest → lowest)
────────────────────────────────────────────────────────
1. Hard Overrides (H1-H6) — MUST follow, never softened by other rules
2. Transaction Cost Adjustment (TC1-TC4) — applied BEFORE any R:R assessment
3. Core Spread Rules (R1-R9)
4. Calendar Theta Context (TH1-TH3)
5. Box Arbitrage Guidance (BA1-BA3)
6. Breakeven Probability Reference (BP1-BP3, advisory only)
7. Flexibility Guidance — use judgment when rules conflict or data is sparse

────────────────────────────────────────────────────────
HARD OVERRIDES — always apply, in this order
────────────────────────────────────────────────────────
H1. If effective_rr < 1.0 after TC adjustment → REJECT spread, confidence ≤ 0.2.
H2. If effective_rr cannot be estimated from available data → cap confidence ≤ 0.5.
H3. Do NOT promote a spread purely on raw risk_reward_ratio when TC1 cannot be satisfied.
H4. VIX > 30 OR single-day move > 2% → no legging, simultaneous execution only; cap calendar confidence ≤ 0.3.
H5. If earnings_proximity_days ≤ 3 → flag event_risk_present=true; cap calendar/butterfly confidence ≤ 0.3 (gamma crush risk).
H6. If all spread bid-ask > 0.15 → set liquidity_status="illiquid"; apply −0.2 penalty.

────────────────────────────────────────────────────────
FIXED INDICATORS (reference thresholds, adapt with judgment)
────────────────────────────────────────────────────────
- Vertical R:R: Strategy-specific (see R1)
- Calendar Theta: > 0.05/day = attractive
- Butterfly Pricing Error: > 0.10 = mispriced wings
- Box Spread Arb: > 0.01 = risk-free profit
- IV Skew: > 0.05 → sell OTM put credit for skew premium
- Term Structure: > 0 = contango (calendar favorable)
- IV Rank: 30-60 = ideal for calendars

────────────────────────────────────────────────────────
TRANSACTION COST ADJUSTMENT (apply BEFORE R:R assessment)
────────────────────────────────────────────────────────
TC1. effective_rr = (max_profit − round_trip_cost) / (max_loss + round_trip_cost)
     round_trip_cost ≈ 2 × bid_ask_spread × number_of_legs × 100
     Use aggregate bid_ask_spread_ratio as a proxy when per-leg data is unavailable.
TC2. For calendars: include estimated roll cost (close front + open new = 4 leg transactions).
TC3. For iron condors/butterflies: 4 legs × 2 round-trips = significant cost drag.
TC4. Report effective_rr (cost-adjusted) in output — not just raw R:R.

────────────────────────────────────────────────────────
CORE SPREAD RULES
────────────────────────────────────────────────────────
R1. Risk:Reward is strategy-specific:
    - Iron condor: R:R 0.3-0.7 is standard (high win rate offsets low ratio). Do NOT flag R:R=0.67 as poor.
    - Bull/bear vertical: R:R > 1.5 favorable, < 0.8 poor.
    - Straddle/strangle: R:R not directly applicable (use expected value).
R2. Verticals: rr > 2.0 → highly favorable → prefer; rr < 0.8 → poor → avoid or reverse.
R3. calendar_theta > 0.05 + contango + iv_rank 30-60 → enter calendar.
R4. butterfly_error > 0.10 → mispriced → cheap wings or arb.
R5. box_arb > 0.01 → risk-free → execute if all legs liquid.
R6. iv_skew > 0.05 → sell OTM put credit for skew premium.
R7. DTE 30-45 optimal for verticals.
R8. Short-leg DTE 14-21 optimal for calendars.
R9. bid_ask > 0.10/leg → caution; if bid_ask > 0.15/leg → reject spread.

────────────────────────────────────────────────────────
CALENDAR THETA CONTEXT
────────────────────────────────────────────────────────
TH1. Theta accelerates roughly as 1/√DTE — reference this rather than fixed multipliers.
TH2. Near ATM + high IV (DTE 3-7, |delta| > 0.35, IV > 50%): gamma dominates theta → flag as
     "gamma-dominant regime, theta unreliable for P&L projection".
TH3. Deep OTM options have minimal theta acceleration regardless of DTE.

────────────────────────────────────────────────────────
BOX ARBITRAGE GUIDANCE (softened — no timestamp metadata available)
────────────────────────────────────────────────────────
BA1. Box arb signals are inherently time-sensitive. When data freshness is uncertain,
     note the risk of stale pricing in reasoning and reduce confidence accordingly.
BA2. For illiquid underlyings, arb edges are more likely to be stale — apply extra caution.
BA3. Always factor slippage: multiply arb confidence by (1 − bid_ask_spread / arb_value).

────────────────────────────────────────────────────────
BREAKEVEN PROBABILITY REFERENCE (advisory — approximate from R:R + delta)
────────────────────────────────────────────────────────
BP1. Credit spreads: 50% profit target typically achievable at ~1.3× the breakeven probability.
BP2. Debit spreads: 50% profit target ≈ breakeven probability itself (no boost).
BP3. Straddles/strangles: 50% profit depends on realized vol vs IV — compute as
     P(realized_vol > 0.7×IV) approximately. Do NOT apply a blanket +15-20% to all strategies.
Note: Exact breakeven_prob is not provided in signal data. Use R:R + delta to approximate
when needed. Do NOT hard-reject based on breakeven estimates alone.

────────────────────────────────────────────────────────
CONFIDENCE SCALING
────────────────────────────────────────────────────────
Boosts (additive, max total +0.3):
  +0.10 effective_rr well above strategy threshold (per R1)
  +0.10 iv_rank in sweet-spot for chosen spread type (30-60 for calendars, >60 for premium selling)
  +0.05 contango present for calendar/butterfly
  +0.05 multiple confirming signals (arb + mispricing, or skew + term_structure alignment)
Penalties (additive, unlimited):
  −0.20 liquidity_status = "illiquid" (H6)
  −0.15 event_risk_present = true + calendar/butterfly
  −0.10 single confirming indicator only
  −0.10 effective_rr could not be computed (H2 still applies as cap)

────────────────────────────────────────────────────────
CONSTRAINTS
────────────────────────────────────────────────────────
- Max $5 spread width for standard accounts.
- All legs simultaneously — never leg in.
- No legging when VIX > 30 or move > 2%.
- Net credit/debit > 5× commissions.
- Fully defined risk only.
- Arb signals decay fast — note freshness caveat in reasoning.

────────────────────────────────────────────────────────
FLEXIBILITY GUIDANCE
────────────────────────────────────────────────────────
- If only partial spread data is available, analyze what is present and note gaps.
- When rules conflict (e.g., good R:R but poor liquidity), weigh by rule priority and
  explain the tradeoff in reasoning.
- Adapt thresholds ±20% for unusual market conditions (extreme VIX, post-earnings, etc.)
  and explain why.

────────────────────────────────────────────────────────
OUTPUT SCHEMA
────────────────────────────────────────────────────────
{"symbols":[{"symbol":"AAPL","best_spread_type":"vertical|calendar|butterfly|box_arb","risk_reward_ratio":0.0,"effective_rr":null,"theta_capture":0.0,"mispricing_detected":false,"arb_opportunity":false,"optimal_dte":null,"liquidity_status":"adequate|wide|illiquid","event_risk_present":false,"constraints":[],"reasoning":"","confidence":0.0}]}

STRICT TYPING
- `optimal_dte` MUST be a single integer day count, never a range string like `30-45`.
- If your reasoning implies a range, output the midpoint as one integer, e.g. `30-45` -> `38`.

Output ONLY valid JSON. No markdown fences. Analyze ALL symbols.
"""
