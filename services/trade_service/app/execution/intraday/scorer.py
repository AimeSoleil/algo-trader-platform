"""Entry quality scorer — rates intraday entry opportunities by IV, price, liquidity, and time."""
from __future__ import annotations

from typing import Any

from shared.config import get_settings
from shared.models.intraday import EntryScore, IntradayFeatures
from shared.utils import get_logger

from .strategy_profiles import StrategyProfile

logger = get_logger(__name__)


def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def _parse_time_windows(windows: list[str]) -> list[tuple[float, float]]:
    """Parse ["10:00-11:30", "14:00-15:30"] into [(10.0, 11.5), (14.0, 15.5)]."""
    parsed: list[tuple[float, float]] = []
    for w in windows:
        parts = w.split("-")
        if len(parts) != 2:
            continue
        start_parts = parts[0].strip().split(":")
        end_parts = parts[1].strip().split(":")
        if len(start_parts) != 2 or len(end_parts) != 2:
            continue
        start = int(start_parts[0]) + int(start_parts[1]) / 60.0
        end = int(end_parts[0]) + int(end_parts[1]) / 60.0
        parsed.append((start, end))
    return parsed


class EntryQualityScorer:
    """Scores entry quality using IV, price action, liquidity, and time-of-day signals."""

    def score(
        self,
        features: IntradayFeatures,
        plan: dict[str, Any],
        profile: StrategyProfile,
        market_time: float,
    ) -> EntryScore:
        direction = plan.get("direction", "neutral")

        iv_score, iv_reasons = self._score_iv(features, profile)
        price_score, price_reasons = self._score_price(features, direction)
        liquidity_score, liq_reasons = self._score_liquidity(features)
        time_score, time_reasons = self._score_time(market_time)

        reasons = iv_reasons + price_reasons + liq_reasons + time_reasons

        weighted = (
            iv_score * profile.iv_weight
            + price_score * profile.price_weight
            + liquidity_score * profile.liquidity_weight
        )
        total = weighted * time_score

        return EntryScore(
            symbol=features.symbol,
            total=round(total, 4),
            iv_score=round(iv_score, 4),
            price_score=round(price_score, 4),
            liquidity_score=round(liquidity_score, 4),
            time_score=round(time_score, 4),
            reasons=reasons,
        )

    # ── IV sub-score ──────────────────────────────────────

    def _score_iv(
        self, f: IntradayFeatures, profile: StrategyProfile
    ) -> tuple[float, list[str]]:
        reasons: list[str] = []
        zscore = f.iv_zscore_intraday
        momentum = f.iv_momentum_5m

        if profile.preferred_iv == "high":
            score = _clamp(0.5 + zscore * 0.25)
            if abs(momentum) < 0.02:
                score = _clamp(score + 0.1)
                reasons.append("IV momentum slowing, good credit entry")
            if zscore > 0.5:
                reasons.append(
                    f"IV elevated (zscore={zscore:+.2f}), favorable for credit premium"
                )

        elif profile.preferred_iv == "low":
            score = _clamp(0.5 - zscore * 0.25)
            if zscore < -0.5:
                score = _clamp(score + 0.1)
                reasons.append("IV near day low, favorable for debit entry")
            if zscore < 0:
                reasons.append(
                    f"IV depressed (zscore={zscore:+.2f}), favorable for debit purchase"
                )

        elif profile.preferred_iv == "extreme":
            score = _clamp(abs(zscore) * 0.4)
            if abs(zscore) > 1.0:
                reasons.append(
                    f"IV extreme (zscore={zscore:+.2f}), favorable for neutral strategy"
                )

        else:  # moderate
            score = _clamp(1.0 - abs(zscore) * 0.3)
            if abs(zscore) < 0.5:
                reasons.append("IV near mean, favorable for hedge entry")

        return score, reasons

    # ── Price sub-score ───────────────────────────────────

    def _score_price(
        self, f: IntradayFeatures, direction: str
    ) -> tuple[float, list[str]]:
        reasons: list[str] = []

        if direction == "bullish":
            score = _clamp(0.5 - f.vwap_deviation * 5.0)
            if f.price_momentum_5m > 0.001:
                score = _clamp(score + 0.15)
                reasons.append("positive price momentum supports bullish entry")
            if f.volume_surge_ratio > 1.5:
                score = _clamp(score + 0.1)
                reasons.append("volume surge confirms bullish move")
            if f.vwap_deviation < 0:
                reasons.append("price below VWAP, good bullish entry zone")

        elif direction == "bearish":
            score = _clamp(0.5 + f.vwap_deviation * 5.0)
            if f.price_momentum_5m < -0.001:
                score = _clamp(score + 0.15)
                reasons.append("negative price momentum supports bearish entry")
            if f.vwap_deviation > 0:
                reasons.append("price above VWAP, good bearish entry zone")

        else:  # neutral
            score = _clamp(1.0 - abs(f.vwap_deviation) * 10.0)
            if abs(f.vwap_deviation) < 0.005:
                reasons.append("price near VWAP, ideal for neutral strategy")

        return score, reasons

    # ── Liquidity sub-score ───────────────────────────────

    def _score_liquidity(
        self, f: IntradayFeatures
    ) -> tuple[float, list[str]]:
        reasons: list[str] = []

        spread_score = _clamp(1.0 - f.avg_bid_ask_spread_pct * 10.0)
        volume_bonus = min(f.option_volume_ratio * 2.0, 0.3)
        score = _clamp(spread_score + volume_bonus)

        if f.avg_bid_ask_spread_pct > 0.15:
            score = _clamp(score * 0.5)
            reasons.append("wide spreads, poor liquidity")

        if f.option_volume_ratio > 0.1:
            reasons.append(
                f"option volume ratio {f.option_volume_ratio:.2f}, decent flow"
            )

        return score, reasons

    # ── Time sub-score ────────────────────────────────────

    def _score_time(self, market_time: float) -> tuple[float, list[str]]:
        reasons: list[str] = []

        optimizer_cfg = get_settings().trade_service.intraday_optimizer
        blackout_min = optimizer_cfg.blackout_minutes_after_open
        preferred_windows = optimizer_cfg.preferred_windows

        market_open = 9.5  # 09:30

        # Blackout period after open
        if market_time < market_open + blackout_min / 60.0:
            reasons.append("blackout period")
            return 0.0, reasons

        # Last 30 minutes
        if market_time >= 15.5:
            reasons.append("end of day, wider spreads typical")
            return 0.7, reasons

        # Preferred windows
        windows = _parse_time_windows(preferred_windows)
        for start, end in windows:
            if start <= market_time <= end:
                reasons.append("inside preferred trading window")
                return 1.0, reasons

        return 0.8, reasons
