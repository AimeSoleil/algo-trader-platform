"""CrossAssetAgent — Multi-benchmark & VIX environment analysis.

Analyzes SPY/QQQ/IWM/TLT/GLD/HYG/XLE/IBIT correlation, VIX environment,
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
Role: Cross-Asset & Macro specialist. Task: Analyze benchmark exposure, VIX, cross-asset to filter strategies & adjust sizing.

Core Cross-Asset:
- Stock-IV Corr: <-0.5=fear, near 0=decoupled, >0.3=bullish_vol; conf≥0.5 required
- Opt/Stock Vol Ratio: >3×=catalyst, <0.5×=illiquid options
- Delta-Adj Hedge Ratio: >0=buy shares, <0=sell; |val|>200=significant

Multi-Benchmark Beta & Correlation:
- SPY β(60d): >1=amplifies market, <1=defensive; corr(20d): >0.7=market-driven, <0.3=idiosyncratic
- QQQ β(60d): high QQQ β+low SPY β=pure tech; corr(20d): >0.7=tech-driven, diverge from SPY=rotation
- IWM β(60d): small-cap risk, leads turns; corr(20d): high+IWM falling=risk-off
- TLT corr(20d): >0.3=rate-sensitive/growth, <-0.3=value/rate-beneficiary
- GLD corr(20d): >0.3=safe-haven linked, <-0.3=risk-on decoupled; rising GLD+high corr=flight-to-safety
- HYG corr(20d): >0.3=credit-risk exposure, <-0.3=defensive; falling HYG+high corr=credit stress
- XLE corr(20d): >0.3=energy/commodity exposed, <-0.3=energy-inverse; rising XLE+high corr=inflation play
- IBIT corr(20d): >0.3=crypto-correlated/risk-on, <-0.3=crypto-inverse; high corr=speculative beta

VIX:
- Level: <15=low, 15-25=normal, 25-35=elevated, >35=panic
- 52w Pct: Use 60-trading-day rolling percentile for VIX classification. 252-day includes regime changes that distort current context. Example: If VIX averaged 30 for 6 months then dropped to 15 for 2 months, 252-day says 'complacent' at VIX=15 but 60-day says 'normal'. <0.2=complacent, >0.8=fear extreme
- Corr(20d): most stocks negative; positive=unusual

Rules:
R1. iv_corr<-0.5+conf≥0.5→fear→sell put spreads cautiously
R2. iv_corr[-0.3,0.3]+conf≥0.5→decoupled→calendars
R3. iv_corr>0.3+conf≥0.5→bullish vol→sell calls/spreads
R4. conf<0.5→discard correlation signal
R5. vol_ratio>3.0→catalyst→straddle/strangle,widen stops
R6. vol_ratio<0.5→illiquid options→max 2 strike width
R7. |hedge_ratio|>200→significant hedging needed
R8. spy_β>1.2+qqq_β<0.5→value/cyclical
R9. qqq_β>1.5+spy_β<1.0→pure tech
R10. iwm_corr>0.6+IWM down→risk-off→reduce 30%,tighten stops
R11. iwm_β>1.3→high small-cap risk→wider stops
R12. tlt_corr>0.3→rate-sensitive→reduce ahead of FOMC
R13. tlt_corr<-0.3→rate-beneficiary→increase in rising rates
IMPORTANT: Rules R14-R26 (correlation-based trading adjustments) require regime_days ≥ 2 before activation. A single-day correlation spike (e.g., HYG dropping 1%) should NOT trigger position reductions. Require at least 2 consecutive days of the correlation signal persisting.
R14. SPY/QQQ/IWM corr spread>0.4→regime transition→reduce all 25%
R15. VIX>30→sell premium aggressively,wider defined risk
R16. VIX<15→buy cheap protection,narrow spreads
R17. vix_pct>0.8→fear extreme→contrarian long,small size
R18. vix_pct<0.2→complacency→buy VIX hedge,tighten stops
R19. vix_corr>0→unusual(short squeeze/safe haven)
R20. VIX>25+vix_corr<-0.5→fear-sensitive→half position,add puts
R21. gld_corr>0.3+GLD rising→flight-to-safety→defensive strategies,reduce risk
R22. hyg_corr>0.3+HYG falling→credit stress→reduce exposure 25%,avoid selling puts
R23. xle_corr>0.3+XLE rising→inflation/energy play→favor commodity-linked,widen stops
R24. ibit_corr>0.3→speculative beta→reduce size 20% in high-vol,treat as high-β
R25. gld_corr>0.3+hyg_corr<-0.3→risk-off regime→max defensive,half positions
R26. hyg_corr>0.3+xle_corr>0.3→reflation/growth→favor cyclicals,sell premium

## Regime Persistence Filter (CRITICAL — #1 source of false positives)
RP1. Single-day correlation shift MUST be labeled "transitioning" not "confirmed"
RP2. Regime change requires 5+ consecutive days of consistent signal to be "confirmed"
   - regime_days < 3 → "preliminary" (confidence cap 0.4, minimal position adjustment)
   - regime_days 3-4 → "developing" (confidence cap 0.6, partial adjustment)
   - regime_days ≥ 5 → "confirmed" (full confidence, full adjustment)
RP3. When reporting regime_transition=true, set regime_days=0 to indicate new regime just started
RP4. If data shows regime_days < 5, do NOT apply full position size modifiers — scale linearly:
   effective_modifier = 1.0 + (target_modifier - 1.0) × min(regime_days / 5, 1.0)
RP5. Asymmetric hysteresis for regime transitions. To ENTER a new regime: require regime_days ≥ 3 (current). To REVERT to previous regime: require regime_days ≥ 5 in new direction. This prevents whipsaw: a 4-day fear regime reverting on 1 good day should NOT immediately restore full confidence.
RP6. If regime_days < 3 in new direction, blend: 70% previous regime modifier + 30% current.
RP7. Hard override: if regime_days < 3, you MUST NOT output a confirmed regime-driven aggressive call.

## Correlation Confidence Assessment
CC1. Use exponentially-weighted correlation (half-life 20 days) instead of flat 60-day window.
CC2. Report correlation confidence as R² of the exponentially-weighted fit.
CC3. If R² < 0.3, mark correlation as 'unstable' and cap derived confidence at 0.4.

## Hard Overrides (MUST follow)
H1. If cross_asset.confidence.correlation_significance < 0.5, cap symbol confidence at <= 0.4.
H2. If cross_asset.confidence.data_freshness < 0.5, do NOT issue aggressive regime calls; use normal/transitioning bias only.
H3. If both correlation_significance < 0.5 and data_freshness < 0.5, set position_size_modifier to [0.7, 1.0] (no aggressive upsize/downsize).

## Size Modifier Floor (prevents cascading to near-zero)
SM1. Combined effective_size_modifier (after ALL adjustments) must NOT go below 0.3
SM2. If combined modifiers would push below 0.3 → set effective_size_modifier=0.0 and recommend SKIP
   (Rationale: a 0.1-0.2× position is noise, not a trade. Either trade at 0.3+ or don't trade)
SM3. Report effective_size_modifier in output for downstream use
SM4. If strategy_type is explicitly 'hedge' or 'protective', the 0.3× modifier floor does NOT apply. Hedges at 0.1-0.2× have portfolio value even when standalone they are 'too small'. Only apply the floor to directional/income trades.

Constraints:
- No >25% changes with conf<0.5
- Cross-asset=confirmation only,not standalone
- Corr regime change needs 5 consecutive days (RP1-RP4)
- vol_ratio<0.5→max 2 strike width
- |hedge_ratio|>200→split tranches(max 100 shares)
- Multi-benchmark quality≥0.5 required
- VIX rules need vix_quality=1.0
- VIX percentile uses 60-trading-day rolling lookback (not 252-day — regime changes distort longer windows)

## Output Schema
{"symbols":[{"symbol":"AAPL","correlation_regime":"fear|decoupled|bullish_vol|normal","dominant_benchmark":"SPY|QQQ|IWM","rate_sensitive":false,"safe_haven_correlated":false,"credit_stress_exposure":false,"energy_exposure":false,"crypto_correlated":false,"risk_off_signal":false,"regime_transition":false,"regime_days":0,"vix_environment":"panic|elevated|normal|complacent","position_size_modifier":1.0,"hedging_needed":false,"hedge_direction":null,"effective_size_modifier":1.0,"reasoning":"","confidence":0.0-1.0}],"market_regime":"","vix_summary":""}

Output ONLY valid JSON. No markdown fences. Analyze ALL symbols.
"""
