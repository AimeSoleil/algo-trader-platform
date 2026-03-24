# Cross-Asset Signals

> Confirm or filter strategy recommendations using multi-benchmark exposure, volatility environment, stock-IV correlation, and options activity ratios.

## Indicators

### Core Indicators

| Indicator | Field | Key Levels |
|---|---|---|
| Stock-IV Correlation | `cross_asset_indicators.stock_iv_correlation` | <-0.5: fear regime · near 0: decoupled · >0.3: bullish vol |
| Options/Stock Vol Ratio | `cross_asset_indicators.option_vs_stock_volume_ratio` | >3× normal: catalyst · <0.5×: illiquid options |
| Delta-Adj Hedge Ratio | `cross_asset_indicators.delta_adjusted_hedge_ratio` | >0: buy shares · <0: sell · |value| > 200: significant |
| Correlation Confidence | `cross_asset_indicators.confidence_scores` | Need ≥ 0.5 for actionable signals |

### Multi-Benchmark Beta & Correlation

| Indicator | Field | Interpretation |
|---|---|---|
| SPY Beta (60d) | `cross_asset_indicators.spy_beta` | Broad market sensitivity. β>1: amplifies market moves; β<1: defensive |
| SPY Correlation (20d) | `cross_asset_indicators.index_correlation_20d` | >0.7: market-driven · <0.3: idiosyncratic |
| QQQ Beta (60d) | `cross_asset_indicators.qqq_beta` | Tech/growth sensitivity. High QQQ β + low SPY β = pure tech play |
| QQQ Correlation (20d) | `cross_asset_indicators.qqq_correlation_20d` | >0.7: tech-driven · divergence from SPY = sector rotation signal |
| IWM Beta (60d) | `cross_asset_indicators.iwm_beta` | Small-cap risk exposure. IWM often leads market turns before SPY |
| IWM Correlation (20d) | `cross_asset_indicators.iwm_correlation_20d` | High IWM corr + falling IWM = risk-off signal for stock |
| TLT Correlation (20d) | `cross_asset_indicators.tlt_correlation_20d` | Rate sensitivity. >0.3: growth stock (rates↓ = good); <-0.3: value stock (rates↑ = good) |

### VIX Environment

| Indicator | Field | Key Levels |
|---|---|---|
| VIX Level | `cross_asset_indicators.vix_level` | <15: low vol · 15-25: normal · 25-35: elevated · >35: panic |
| VIX 52w Percentile | `cross_asset_indicators.vix_percentile_52w` | <0.2: complacent · >0.8: fear extreme |
| VIX Correlation (20d) | `cross_asset_indicators.vix_correlation_20d` | Most stocks negative (-0.3 to -0.7); positive = unusual/contrarian |

## Rules

### Core Cross-Asset Rules (Original)

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

### Multi-Benchmark Rules

11. IF `spy_beta > 1.2` AND `qqq_beta < 0.5` → value / cyclical stock, not tech-driven → avoid growth-based strategies
12. IF `qqq_beta > 1.5` AND `spy_beta < 1.0` → pure tech momentum play → use tech sector context for timing
13. IF `iwm_correlation_20d > 0.6` AND IWM trending down → risk-off regime → reduce position size by 30%, tighten stops
14. IF `iwm_beta > 1.3` → high small-cap risk exposure → position size accordingly, wider stops
15. IF `tlt_correlation_20d > 0.3` → growth/duration-sensitive stock → monitor rate decisions, reduce ahead of FOMC
16. IF `tlt_correlation_20d < -0.3` → value/rate-beneficiary stock → can increase size in rising rate environment
17. IF SPY, QQQ, IWM correlations all diverging (max spread > 0.4) → regime transition → reduce all positions by 25%

### VIX Environment Rules

18. IF `vix_level > 30` → elevated fear → sell premium aggressively (put spreads), wider strikes for defined risk
19. IF `vix_level < 15` → complacent market → buy cheap protection, narrow spreads, avoid naked short vol
20. IF `vix_percentile_52w > 0.8` → fear extreme → contrarian long opportunity, but size small
21. IF `vix_percentile_52w < 0.2` → complacency extreme → buy VIX calls as hedge, tighten all stops
22. IF `vix_correlation_20d > 0` (positive) → stock rises with fear → unusual, likely short squeeze or safe haven
23. IF `vix_level > 25` AND `vix_correlation_20d < -0.5` → stock highly fear-sensitive → cut to half position, add protective puts

## Constraints

- Never make >25% position changes based solely on cross-asset signals with confidence < 0.5
- Cross-asset is confirmation only — never standalone entry trigger
- Correlation regime change requires 5 consecutive days before adoption
- When `option_vs_stock_volume_ratio < 0.5` → max spread width 2 strikes
- When `|hedge_ratio| > 200` → split into tranches (max 100 shares each)
- Multi-benchmark signals need `multi_benchmark_quality ≥ 0.5` to be actionable
- VIX signals need `vix_quality = 1.0` (VIX data available) to be actionable

## Example

Given: `spy_beta=1.15`, `qqq_beta=1.82`, `iwm_beta=0.45`, `tlt_correlation_20d=0.35`, `vix_level=22.5`, `vix_percentile_52w=0.65`, `vix_correlation_20d=-0.42`, `stock_iv_correlation=-0.62`, confidence=0.72, trend=bullish, IV stable
Analysis: High QQQ beta + moderate SPY beta → tech-driven stock. Positive TLT correlation → rate-sensitive growth. VIX normal range, moderate fear sensitivity. Fear regime correlation. Bullish trend.
Action: Confirms tech-momentum thesis. Sell put spreads with elevated premium. Monitor FOMC for rate sensitivity. No IWM risk-off signal. Moderate VIX = normal positioning.
