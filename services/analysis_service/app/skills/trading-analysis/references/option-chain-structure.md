# Option Chain Structure

> Analyze options microstructure for strike selection, strategy construction, and execution quality.

## Indicators

| Indicator | Field | Key Levels |
|---|---|---|
| PCR Volume | `option_indicators.pcr_volume` | >1.5: extreme bearish sentiment · <0.5: extreme bullish |
| PCR OI | `option_indicators.pcr_oi` | Longer-term positioning; confirms/contradicts volume PCR |
| OI Concentration Top 5 | `option_indicators.oi_concentration_top5` | >0.80: market pinned to key strikes |
| Bid-Ask Spread Ratio | `option_indicators.bid_ask_spread_ratio` | >0.15: illiquid · <0.05: excellent |
| Volume Imbalance | `option_indicators.option_volume_imbalance` | >0.4: heavy call flow · <-0.4: heavy put flow |
| Delta Exposure Profile | `option_indicators.delta_exposure_profile` | call_delta >> put_delta: bullish positioning |
| Gamma Peak Strike | `option_indicators.gamma_peak_strike` | Price gravitates here near expiry |
| Theta Decay Rate | `option_indicators.theta_decay_rate` | Higher near expiry; premium-selling context |

## Rules

1. IF `pcr_volume > 1.5` → extreme bearish → contrarian bullish (confirm with trend first)
2. IF `pcr_volume < 0.5` → extreme bullish → contrarian bearish (confirm first)
3. IF `oi_concentration_top5 > 0.80` AND DTE ≤ 5 → gamma pin → butterfly at gamma_peak_strike
4. IF `bid_ask_spread_ratio > 0.15` → illiquid → avoid or use wider limits
5. IF `bid_ask_spread_ratio > 0.20` → HARD BLOCK: do not trade this chain
6. IF `option_volume_imbalance > 0.4` → institutional call buying → supports bullish
7. IF `option_volume_imbalance < -0.4` → institutional put buying → bearish or hedging
8. IF `theta_decay_rate` high AND `iv_rank > 50` → theta-selling edge → credit strategies
9. IF `theta_decay_rate` high AND `iv_rank < 30` → calendar_spread preferred
10. IF `gamma_peak_strike` within 1% of `close_price` → pinning → short premium centered here

## Constraints

- Every leg: daily volume ≥ 100 contracts
- Exit strikes: OI ≥ 500
- Hard reject: bid-ask spread > 20% of mid-price
- PCR contrarian signals require confirmation — never act alone
- Gamma pin logic only valid DTE ≤ 5
- Volume imbalance < -0.4 near earnings may be hedging — flag context

## Example

Given: `pcr_volume=1.8`, `option_volume_imbalance=-0.45`, `bid_ask_spread_ratio=0.07`, `oi_concentration_top5=0.85`, `gamma_peak_strike=180.00`, `close_price=180.50`, DTE=3
Analysis: Extreme put-heavy (PCR 1.8). Chain liquid (0.07). Heavy pinning (OI 85%, gamma peak 180, DTE 3).
Action: `iron_butterfly` at 180, 3 DTE. Entry: `underlying_price` between [179, 181]. Stop: `pnl_percent <= -50`.
