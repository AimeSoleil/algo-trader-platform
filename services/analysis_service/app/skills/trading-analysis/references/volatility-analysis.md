# Volatility Analysis

> Classify the current IV regime and determine whether to sell premium, buy premium, or exploit vol surface mispricings.

## Indicators

| Indicator | Field | Key Levels |
|---|---|---|
| IV Rank | `option_indicators.iv_rank` | >70: sell premium · <30: buy premium |
| IV Percentile | `option_indicators.iv_percentile` | Confirms IV Rank; weight more heavily for skewed distributions |
| Current IV | `option_indicators.current_iv` | Absolute IV level — context for ATM pricing |
| HV 20d | `stock_indicators.hv_20d` | 20-day realized vol for HV-IV comparison |
| HV-IV Spread | `stock_indicators.hv_iv_spread` | >0: realized exceeds implied · <0: implied is rich |
| GARCH Forecast | `stock_indicators.garch_vol_forecast` | Compare to current_iv; >15% divergence = mean-reversion signal |
| BB Width | `stock_indicators.bollinger_band_width` | <0.03: squeeze (pending expansion) · >0.10: expanded |
| Vol Surface Fit Error | `option_indicators.vol_surface_fit_error` | >0.02: mispriced contracts likely exist |
| IV Skew | `option_indicators.iv_skew` | >0.05: steep put skew · <-0.02: unusual call skew |
| Term Structure | `option_indicators.term_structure_slope` | >0: contango (normal) · <0: backwardation (near-term event risk) |

## Rules

1. IF `iv_rank > 70` → sell premium: iron_condor, vertical_spread (credit), strangle (defined-risk only)
2. IF `iv_rank < 30` → buy premium: straddle, calendar_spread, vertical_spread (debit)
3. IF `iv_rank` 30–70 → neutral zone; use other references to decide direction
4. IF `hv_iv_spread > 0` → realized > implied → consider long gamma (straddle, strangle)
5. IF `hv_iv_spread < 0` → implied is rich → sell vol strategies preferred
6. IF `abs(garch_vol_forecast - current_iv) / current_iv > 0.15` → fade the divergence
7. IF `vol_surface_fit_error > 0.02` → flag mispriced contracts; look for relative-value trades
8. IF `iv_skew > 0.05` → steep put skew → sell OTM put credit spreads to harvest skew premium
9. IF `term_structure_slope < 0` → backwardation → avoid selling options with DTE < 7
10. IF `iv_rank > 70` AND `term_structure_slope < 0` → iron_butterfly centered at ATM, expiry > 14 DTE
11. IF `bollinger_band_width < 0.03` → squeeze → favor straddle/strangle entries

## Constraints

- Never sell naked options — every short leg requires a defined-risk hedge
- Always define `max_loss_per_trade` and `stop_loss_amount` before entry
- Respect portfolio vega limits from risk-management reference
- When `iv_rank` and `iv_percentile` disagree by >20 pts → reduce position size by 25%
- Vol surface arbitrage requires `vol_surface_fit_error > 0.02` AND ≥3 anomalous strikes
- Do not sell short-dated options (DTE < 7) when `term_structure_slope < 0`

## Example

Given: `iv_rank=78`, `hv_iv_spread=-0.03`, `term_structure_slope=0.02`, `iv_skew=0.06`
Analysis: IV elevated (rank 78) → sell premium. Implied rich vs realized (spread -0.03) confirms. Normal contango. Steep put skew (0.06) → harvest via put credit spread.
Action: `vertical_spread` (bull put credit spread), 30-45 DTE, short put at 25-delta, long put 5 pts lower. Entry: `iv_rank >= 70`. Stop: `pnl_percent <= -100`.
