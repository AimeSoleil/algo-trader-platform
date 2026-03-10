# Trend & Momentum

> Determine directional bias and momentum health to select trend-following, mean-reversion, or vol-expansion strategies.

## Indicators

| Indicator | Field | Key Levels |
|---|---|---|
| ADX | `stock_indicators.adx_14` | >25: trending · <20: range-bound · >40: extreme trend |
| Trend Direction | `stock_indicators.trend` | "bullish" / "bearish" / "neutral" |
| Trend Strength | `stock_indicators.trend_strength` | 0.0–1.0 continuous |
| RSI | `stock_indicators.rsi_14` | >70: overbought · <30: oversold |
| Stochastic RSI | `stock_indicators.stoch_rsi` | >0.8: overbought · <0.2: oversold |
| RSI Divergence | `stock_indicators.rsi_divergence` | +1: bullish div · -1: bearish div · 0: none |
| MACD Histogram | `stock_indicators.macd_histogram` | >0: bullish momentum · <0: bearish |
| MACD Hist Divergence | `stock_indicators.macd_hist_divergence` | +1: bullish div · -1: bearish div |
| Keltner Upper | `stock_indicators.keltner_upper` | Price above = strong bullish |
| Keltner Lower | `stock_indicators.keltner_lower` | Price below = strong bearish |
| Ichimoku Tenkan | `stock_indicators.ichimoku_tenkan` | Tenkan > Kijun = bullish |
| Ichimoku Kijun | `stock_indicators.ichimoku_kijun` | Kijun > Tenkan = bearish |
| Ichimoku Span A/B | `stock_indicators.ichimoku_span_a/b` | Price above cloud = bullish · below = bearish |
| Linear Reg Slope | `stock_indicators.linear_reg_slope` | >0: uptrend · <0: downtrend · sign change = reversal warning |
| BB Width | `stock_indicators.bollinger_band_width` | <0.03: squeeze (imminent expansion) |

## Rules

1. IF `adx_14 > 25` AND `close_price > keltner_upper` → strong bullish → bull call spread, covered_call
2. IF `adx_14 > 25` AND `close_price < keltner_lower` → strong bearish → bear put spread, protective_put
3. IF `adx_14 < 20` → range-bound → iron_condor, iron_butterfly
4. IF `ichimoku_tenkan > ichimoku_kijun` AND `close_price > ichimoku_span_a` AND `close_price > ichimoku_span_b` → bullish confirmation
5. IF `rsi_divergence != 0` → potential reversal → reduce directional exposure, tighten stops
6. IF `macd_hist_divergence != 0` → momentum exhaustion → corroborates RSI divergence
7. IF `rsi_divergence != 0` AND `macd_hist_divergence != 0` (same sign) → high-probability reversal
8. IF `bollinger_band_width < 0.03` → squeeze → straddle/strangle entry
9. IF `linear_reg_slope` sign just changed → trend reversal warning → no new trend-following entries
10. IF `adx_14 > 30` AND trend confirmed → do NOT enter counter-trend positions

## Constraints

- Never fight a strong trend (ADX > 30 with confirmed direction)
- Divergence is a warning, not a trigger — requires ≥1 confirming signal
- BB squeeze direction unknown — default non-directional unless 2+ indicators agree
- ADX 20–25 (transition) → reduce position size to 50–75%
- `|linear_reg_slope| < 0.001` → directionless; no directional bias

## Example

Given: `adx_14=32`, `trend=bullish`, `close_price=185.50`, `keltner_upper=184.00`, `ichimoku_tenkan=183.20`, `ichimoku_kijun=181.50`, `rsi_14=62`, `rsi_divergence=0`
Analysis: Strong bullish (ADX 32, price above Keltner upper). Ichimoku confirms. RSI healthy (62, no divergence).
Action: `vertical_spread` (bull call spread), 30-45 DTE. Entry: `underlying_price >= 185`. Stop: `underlying_price < 181.50`.
