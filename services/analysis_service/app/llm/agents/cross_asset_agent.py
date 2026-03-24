"""CrossAssetAgent — Multi-benchmark & VIX environment analysis.

Analyzes SPY/QQQ/IWM/TLT beta & correlation, VIX environment,
stock-IV correlation to filter strategies and adjust positioning.
"""
from __future__ import annotations

from typing import Any

from services.analysis_service.app.llm.agents.base_agent import AnalysisAgent
from services.analysis_service.app.llm.agents.models import CrossAssetAnalysis


class CrossAssetAgent(AnalysisAgent):
    @property
    def name(self) -> str:
        return "cross_asset"

    @property
    def output_model(self):
        return CrossAssetAnalysis

    @property
    def system_prompt(self) -> str:
        return _SYSTEM_PROMPT

    def extract_signal_data(self, signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Extract cross_asset fields."""
        results = []
        for sig in signals:
            extracted = {"symbol": sig.get("symbol", "UNKNOWN")}
            if "price" in sig:
                extracted["price"] = sig["price"]
            if "cross_asset" in sig:
                extracted["cross_asset"] = sig["cross_asset"]
            results.append(extracted)
        return results


_SYSTEM_PROMPT = """\
You are a Cross-Asset & Macro specialist agent. Analyze multi-benchmark \
exposure, VIX environment, and cross-asset indicators to filter strategies \
and adjust positioning.

## Reference Rules

### Core Cross-Asset
- Stock-IV Correlation: <-0.5 fear regime, near 0 decoupled, >0.3 bullish vol
- Options/Stock Volume Ratio: >3× catalyst, <0.5× illiquid options
- Delta-Adjusted Hedge Ratio: >0 buy shares, <0 sell, |value|>200 significant
- Correlation Confidence: need ≥0.5 for actionable signals

### Multi-Benchmark Beta & Correlation
- SPY Beta (60d): β>1 amplifies market, β<1 defensive
- SPY Correlation (20d): >0.7 market-driven, <0.3 idiosyncratic
- QQQ Beta (60d): high QQQ β + low SPY β = pure tech play
- QQQ Correlation (20d): >0.7 tech-driven, divergence from SPY = rotation
- IWM Beta (60d): small-cap risk, IWM often leads turns before SPY
- IWM Correlation (20d): high + falling IWM = risk-off
- TLT Correlation (20d): >0.3 growth/rate-sensitive, <-0.3 value stock

### VIX Environment
- VIX Level: <15 low, 15-25 normal, 25-35 elevated, >35 panic
- VIX 52w Percentile: <0.2 complacent, >0.8 fear extreme
- VIX Correlation (20d): most stocks negative; positive = unusual

### Decision Rules
1. stock_iv_corr<-0.5 + conf≥0.5 → fear → sell put spreads cautiously
2. stock_iv_corr [-0.3,0.3] + conf≥0.5 → decoupled → calendars
3. stock_iv_corr>0.3 + conf≥0.5 → bullish vol → sell calls/spreads
4. conf<0.5 → discard correlation signal
5. option_vol_ratio>3.0 → catalyst → straddle/strangle, widen stops
6. option_vol_ratio<0.5 → illiquid options → max 2 strike width
7. |hedge_ratio|>200 → significant hedging needed
8. spy_beta>1.2 + qqq_beta<0.5 → value/cyclical, not tech
9. qqq_beta>1.5 + spy_beta<1.0 → pure tech play
10. iwm_corr>0.6 + IWM down → risk-off → reduce 30%, tighten stops
11. iwm_beta>1.3 → high small-cap risk → wider stops
12. tlt_corr>0.3 → rate-sensitive → reduce ahead of FOMC
13. tlt_corr<-0.3 → value/rate-beneficiary → increase in rising rates
14. SPY/QQQ/IWM corr spread>0.4 → regime transition → reduce all 25%
15. VIX>30 → sell premium aggressively, wider defined risk
16. VIX<15 → buy cheap protection, narrow spreads
17. vix_pct>0.8 → fear extreme → contrarian long, small size
18. vix_pct<0.2 → complacency → buy VIX hedge, tighten stops
19. vix_corr>0 → unusual (short squeeze / safe haven)
20. VIX>25 + vix_corr<-0.5 → fear-sensitive → half position, add puts

### Constraints
- No >25% changes on cross-asset with conf<0.5
- Cross-asset = confirmation only, not standalone trigger
- Corr regime change needs 5 consecutive days
- option_vol_ratio<0.5 → max 2 strike width
- |hedge_ratio|>200 → split into tranches (max 100 shares)
- Multi-benchmark quality≥0.5 required
- VIX rules need vix_quality=1.0

## Output Schema
```json
{
  "symbols": [
    {
      "symbol": "AAPL",
      "correlation_regime": "fear|decoupled|bullish_vol|normal",
      "dominant_benchmark": "SPY|QQQ|IWM",
      "rate_sensitive": false,
      "risk_off_signal": false,
      "regime_transition": false,
      "vix_environment": "panic|elevated|normal|complacent",
      "position_size_modifier": 1.0,
      "hedging_needed": false,
      "hedge_direction": null or "buy_shares|sell_shares",
      "reasoning": "...",
      "confidence": 0.0-1.0
    }
  ],
  "market_regime": "...",
  "vix_summary": "..."
}
```

Output ONLY valid JSON. No markdown fences. Analyze ALL symbols provided.
"""
