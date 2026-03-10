# Spread & Arbitrage

> Identify optimal multi-leg structures by evaluating risk-reward, theta differential, and pricing anomalies.

## Indicators

| Indicator | Field | Key Levels |
|---|---|---|
| Vertical Risk-Reward | `option_indicators.vertical_spread_risk_reward` | >2.0: favorable · <0.5: poor |
| Calendar Theta Capture | `option_indicators.calendar_spread_theta_capture` | >0.05/day: attractive |
| Butterfly Pricing Error | `option_indicators.butterfly_pricing_error` | >0.10: mispriced wings |
| Box Spread Arbitrage | `option_indicators.box_spread_arbitrage` | >0.01 (1%): risk-free profit |
| IV Skew | `option_indicators.iv_skew` | >0.05: sell expensive OTM puts |
| Term Structure | `option_indicators.term_structure_slope` | >0: contango → calendar favorable |
| IV Rank | `option_indicators.iv_rank` | 30–60: ideal for calendars |
| Bid-Ask Spread | `option_indicators.bid_ask_spread_ratio` | >0.10 per leg → reject spread |

## Rules

1. IF `vertical_spread_risk_reward > 2.0` → favorable → prefer this vertical
2. IF `vertical_spread_risk_reward < 0.5` → poor → avoid or reverse direction
3. IF `calendar_spread_theta_capture > 0.05` AND `term_structure_slope > 0` AND `iv_rank` 30–60 → enter calendar
4. IF `butterfly_pricing_error > 0.10` → mispriced → cheap wings entry or arb
5. IF `box_spread_arbitrage > 0.01` → risk-free → execute if all 4 legs liquid
6. IF `iv_skew > 0.05` → sell OTM put credit spread for skew premium
7. DTE 30–45 → optimal for verticals
8. Short-leg DTE 14–21 → optimal for calendars
9. IF `bid_ask_spread_ratio > 0.10` per leg → reject spread
10. IF breakeven probability < 40% → reject regardless of risk-reward

## Constraints

- Max spread width: $5 for standard accounts
- All legs simultaneously — never leg in
- Never leg into spreads when VIX > 30 or underlying move > 2%
- Net credit/debit must exceed commissions by ≥5×
- No naked exposure — fully defined-risk
- Arb signals decay fast — re-verify if age > 60s

## Example

Given: `vertical_spread_risk_reward=2.3`, `iv_skew=0.07`, `iv_rank=55`, `term_structure_slope=0.03`, `bid_ask_spread_ratio=0.06`
Analysis: Good R:R (2.3:1). Steep put skew. Moderate IV and contango.
Action: `vertical_spread` (bull put credit). 35 DTE. Entry: `iv_rank >= 50`. Stop: `pnl_percent <= -100`. Target: `pnl_percent >= 50`.
