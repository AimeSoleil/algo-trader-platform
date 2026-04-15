"""Strategy profiles — weight presets for entry quality scoring by strategy type."""
from __future__ import annotations

from dataclasses import dataclass

from shared.models.blueprint import StrategyType


@dataclass(frozen=True)
class StrategyProfile:
    """Scoring weight configuration for a strategy category."""

    name: str
    iv_weight: float          # weight for IV timing sub-score
    price_weight: float       # weight for price action sub-score
    liquidity_weight: float   # weight for bid-ask / volume sub-score
    preferred_iv: str         # "high" | "low" | "extreme" | "moderate"


# ── Four strategy categories ──────────────────────────────

CREDIT = StrategyProfile(
    name="credit",
    iv_weight=0.45,
    price_weight=0.30,
    liquidity_weight=0.25,
    preferred_iv="high",
)

DEBIT = StrategyProfile(
    name="debit",
    iv_weight=0.35,
    price_weight=0.40,
    liquidity_weight=0.25,
    preferred_iv="low",
)

NEUTRAL = StrategyProfile(
    name="neutral",
    iv_weight=0.55,
    price_weight=0.20,
    liquidity_weight=0.25,
    preferred_iv="extreme",
)

HEDGE = StrategyProfile(
    name="hedge",
    iv_weight=0.30,
    price_weight=0.40,
    liquidity_weight=0.30,
    preferred_iv="moderate",
)


# ── Mapping: StrategyType → profile ───────────────────────
# Credit strategies: sell premium when IV is high
# Debit strategies: buy premium when IV is low
# Neutral: profit from extreme IV moves
# Hedge: protective positions, price matters most

_STRATEGY_PROFILE_MAP: dict[StrategyType, StrategyProfile] = {
    # Credit
    StrategyType.IRON_CONDOR: CREDIT,
    StrategyType.IRON_BUTTERFLY: CREDIT,
    # Debit
    StrategyType.SINGLE_LEG: DEBIT,
    StrategyType.BUTTERFLY: DEBIT,
    StrategyType.CALENDAR_SPREAD: DEBIT,
    StrategyType.DIAGONAL_SPREAD: DEBIT,
    # Neutral
    StrategyType.STRADDLE: NEUTRAL,
    StrategyType.STRANGLE: NEUTRAL,
    # Hedge
    StrategyType.COVERED_CALL: HEDGE,
    StrategyType.PROTECTIVE_PUT: HEDGE,
    StrategyType.COLLAR: HEDGE,
}
# NOTE: VERTICAL_SPREAD is ambiguous (credit or debit depends on direction/side)
# → resolved at runtime by get_profile()


def get_profile(strategy_type: str, direction: str = "neutral") -> StrategyProfile:
    """Resolve a StrategyProfile for a given strategy_type + direction.

    VERTICAL_SPREAD is treated as credit when bearish, debit when bullish,
    falling back to DEBIT if direction is neutral.
    """
    try:
        st = StrategyType(strategy_type)
    except ValueError:
        return DEBIT  # safe fallback

    if st == StrategyType.VERTICAL_SPREAD:
        if direction == "bearish":
            return CREDIT  # bear call spread = credit
        return DEBIT       # bull call spread / bull put spread = debit

    return _STRATEGY_PROFILE_MAP.get(st, DEBIT)
