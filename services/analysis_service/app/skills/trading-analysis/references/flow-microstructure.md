# Flow & Microstructure

> Confirm or reject directional/breakout signals using volume and money flow data.

## Indicators

| Indicator | Field | Key Levels |
|---|---|---|
| VWAP | `stock_indicators.vwap` | Price > VWAP: intraday bullish · Price < VWAP: bearish |
| Volume Profile POC | `stock_indicators.volume_profile_poc` | Most-traded price = institutional anchor |
| Volume Profile VAL | `stock_indicators.volume_profile_val` | Value area low — 70% volume zone lower bound |
| Volume Profile VAH | `stock_indicators.volume_profile_vah` | Value area high — 70% volume zone upper bound |
| CMF | `stock_indicators.cmf_20` | >0.1: strong buying · <-0.1: strong selling |
| Tick Volume Delta | `stock_indicators.tick_volume_delta` | >0.3: decisively bullish · <-0.3: decisively bearish |
| Total Volume | `volume` | Compare to 20d avg for anomaly detection |

## Rules

1. IF `close_price > vwap` → intraday bullish bias → supports long entries
2. IF `close_price < vwap` → intraday bearish bias → supports short entries
3. IF `cmf_20 > 0.1` → strong buying pressure → confirms bullish thesis
4. IF `cmf_20 < -0.1` → strong selling pressure → confirms bearish thesis
5. IF `tick_volume_delta > 0.3` → aggressive institutional buying → high-confidence bull
6. IF `tick_volume_delta < -0.3` → aggressive institutional selling → high-confidence bear
7. IF `volume > 2× avg` → volume anomaly → widen stops 1.5×, reduce size by 30%
8. IF breakout flagged AND `volume < 1× avg` → likely false breakout → avoid entry
9. IF breakout flagged AND `volume > 1.5× avg` AND delta confirms → validated breakout → full size
10. IF `cmf_20` and `tick_volume_delta` disagree → conflicting flow → downgrade confidence

## Constraints

- Without volume confirmation, max 50% position size for directional trades
- VWAP is intraday only — use Volume Profile for swing decisions
- False breakout rule is absolute — no entry on breakout with volume < 1× avg
- Flow signals are confirmation only — never standalone entry triggers

## Example

Given: `close_price=185.50`, `vwap=184.80`, `cmf_20=0.15`, `tick_volume_delta=0.35`, volume 1.3× avg
Analysis: Price above VWAP. Strong buying pressure (CMF 0.15). Aggressive buying (delta 0.35). Healthy volume.
Action: Confirms bullish thesis. Full position sizing approved.
