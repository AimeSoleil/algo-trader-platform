# Cross-Asset Signals

> Confirm or filter strategy recommendations using stock-IV correlation and options activity ratios.

## Indicators

| Indicator | Field | Key Levels |
|---|---|---|
| Stock-IV Correlation | `cross_asset_indicators.stock_iv_correlation` | <-0.5: fear regime · near 0: decoupled · >0.3: bullish vol |
| Options/Stock Vol Ratio | `cross_asset_indicators.option_vs_stock_volume_ratio` | >3× normal: catalyst · <0.5×: illiquid options |
| Delta-Adj Hedge Ratio | `cross_asset_indicators.delta_adjusted_hedge_ratio` | >0: buy shares · <0: sell · |value| > 200: significant |
| Correlation Confidence | `cross_asset_indicators.confidence_scores` | Need ≥ 0.5 for actionable signals |

## Rules

1. IF `stock_iv_correlation < -0.5` AND confidence ≥ 0.5 → fear regime → sell put spreads cautiously, buy protective puts
2. IF `stock_iv_correlation` in [-0.3, 0.3] AND confidence ≥ 0.5 → decoupled → calendar_spread works well
3. IF `stock_iv_correlation > 0.3` AND confidence ≥ 0.5 → bullish vol → sell calls/call spreads
4. IF confidence < 0.5 → discard correlation signal
5. IF `option_vs_stock_volume_ratio > 3.0` → catalyst imminent → straddle/strangle, widen stops
6. IF `option_vs_stock_volume_ratio < 0.5` → illiquid options → avoid multi-leg, max width 2 strikes
7. IF `|delta_adjusted_hedge_ratio| > 200` → significant hedging needed → factor into strategy
8. IF `|delta_adjusted_hedge_ratio| < 50` → delta-neutral → no hedging action
9. IF stock trend bullish AND IV rising → divergence → reduce bullish size by 30%
10. IF stock trend bearish AND IV falling → unusual complacency → monitor for contrarian long

## Constraints

- Never make >25% position changes based solely on cross-asset signals with confidence < 0.5
- Cross-asset is confirmation only — never standalone entry trigger
- Correlation regime change requires 5 consecutive days before adoption
- When `option_vs_stock_volume_ratio < 0.5` → max spread width 2 strikes
- When `|hedge_ratio| > 200` → split into tranches (max 100 shares each)

## Example

Given: `stock_iv_correlation=-0.62`, `option_vs_stock_volume_ratio=1.2`, `delta_adjusted_hedge_ratio=85`, confidence=0.72, trend=bullish, IV falling
Analysis: Fear regime (high confidence). Normal activity. Moderate hedge. Bullish + IV falling → convergent.
Action: Confirms bullish thesis. Sell put spreads with elevated premium. No cross-asset warnings.
