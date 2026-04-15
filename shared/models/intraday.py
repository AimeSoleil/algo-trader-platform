"""Intraday feature models for the entry optimizer."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class IntradayFeatures:
    """Per-symbol intraday features computed from 5m option snapshots + 1m stock bars."""

    symbol: str
    timestamp: datetime

    # ── IV features (from option_5min_snapshots) ──
    atm_iv: float = 0.0                  # current ATM implied vol
    iv_momentum_5m: float = 0.0          # rate of change over last bar
    iv_zscore_intraday: float = 0.0      # (current - day_mean) / day_std
    iv_hv_spread: float = 0.0            # IV minus realised vol (from 1m bars)
    term_structure_slope_delta: float = 0.0  # change in term structure slope

    # ── Price features (from stock_5min_bars) ──
    vwap_deviation: float = 0.0          # (price - vwap) / vwap  (%)
    price_momentum_5m: float = 0.0       # 5-bar close change %
    volume_surge_ratio: float = 1.0      # current_vol / rolling_avg_vol
    ema20_deviation: float = 0.0         # (price - ema20) / ema20  (%)

    # ── Liquidity features (from latest option snapshot) ──
    avg_bid_ask_spread_pct: float = 0.0  # mean (ask-bid)/mid across ATM strikes
    option_volume_ratio: float = 0.0     # total option vol / stock vol
    top_strike_oi_concentration: float = 0.0  # top-5 OI / total OI

    # ── Meta ──
    bars_available: int = 0              # how many 5m bars were found (data quality)


@dataclass
class EntryScore:
    """Composite entry quality score with component breakdown."""

    symbol: str
    total: float = 0.0         # weighted composite (0-1)
    iv_score: float = 0.0      # IV timing sub-score (0-1)
    price_score: float = 0.0   # price action sub-score (0-1)
    liquidity_score: float = 0.0  # spread/volume sub-score (0-1)
    time_score: float = 0.0    # time-of-day sub-score (0-1)
    reasons: list[str] = field(default_factory=list)


@dataclass
class EntryDecision:
    """Final entry decision for one symbol plan."""

    symbol: str
    score: EntryScore
    action: str = "wait"       # "enter" | "wait" | "skip"
    strategy_type: str = ""
    conditions_met: bool = True        # True when all entry_conditions passed (or none defined)
    conditions_failed: list[str] = field(default_factory=list)  # human-readable failed condition descriptions
    reasons: list[str] = field(default_factory=list)
