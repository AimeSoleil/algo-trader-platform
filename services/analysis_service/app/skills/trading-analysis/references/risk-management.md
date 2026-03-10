# Risk Management

> Enforce portfolio-level risk limits on every blueprint. No strategy passes without satisfying these constraints.

## Indicators

| Indicator | Field | Key Levels |
|---|---|---|
| Portfolio Greeks | `option_indicators.portfolio_greeks` | Dict with delta, gamma, theta, vega keys |
| Vanna | `option_indicators.vanna` | dDelta/dIV — if >0 and IV dropping, delta rises |
| Charm | `option_indicators.charm` | dDelta/dTime — overnight delta drift |
| Trend Strength | `stock_indicators.trend_strength` | 0.0–1.0; governs directional tilt allowance |
| Delta-Adjusted Hedge | `cross_asset_indicators.delta_adjusted_hedge_ratio` | Shares to delta-neutralize; >0 = buy, <0 = sell |

## Rules

### Portfolio Greeks
1. IF `|portfolio_delta| > 0.5` AND `trend_strength < 0.3` → hedge immediately
2. IF `|portfolio_delta| > 0.5` AND `trend_strength > 0.7` → allow tilt to 0.8, tighten stop
3. IF `|portfolio_delta|` in [0.3, 0.5] AND `trend_strength` in [0.3, 0.7] → monitor, prepare hedge
4. IF `|portfolio_gamma| > 0.1` → mandatory position reduction near expiry
5. IF `portfolio_vega > 500/pt` → reduce long vega or add short vol
6. IF `portfolio_vega < -500/pt` → buy protective options or close short vol

### Vanna / Charm
7. IF `vanna > 0` AND IV declining → delta drifts higher → pre-sell shares
8. IF `charm > 0.05` → delta increases overnight → sell shares before close
9. IF `charm < -0.05` → delta decreases overnight → buy shares before close

### Position Sizing
10. Risk per trade ≤ 2% of account equity
11. Risk per sector ≤ 10% of account equity
12. Max margin usage < 50% of buying power
13. Correlated positions (correlation > 0.7, same strategy) → reduce combined size by 30%

### Loss Limits
14. Soft warning at daily P&L < -$1,000 → reduce sizes by 50%, tighten stops
15. Hard stop at daily P&L < -$2,000 → close all intraday positions, no new entries

## Constraints

- portfolio_delta_limit: |delta| ≤ 0.5 (relaxable to 0.8 with trend_strength > 0.7)
- portfolio_gamma_limit: |gamma| ≤ 0.1
- max_daily_loss: $2,000
- max_margin_usage: 50%
- Every blueprint MUST set: max_daily_loss, max_margin_usage, portfolio_delta_limit, portfolio_gamma_limit
- Every SymbolPlan MUST include stop_loss_amount and max_loss_per_trade

## Example

Given: `portfolio_greeks={delta: 0.6, gamma: 0.08, theta: 15.0, vega: 320.0}`, `trend_strength=0.75`, `charm=0.03`
Analysis: Delta 0.6 > 0.5, but trend_strength 0.75 allows 0.8 tilt. Gamma safe. Vega safe. Charm < 0.05.
Action: Allow position. Blueprint: `portfolio_delta_limit=0.8`, `portfolio_gamma_limit=0.1`, `max_daily_loss=2000`, `max_margin_usage=0.5`.
