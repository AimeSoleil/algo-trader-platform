"""LLM prompt builder for Trading Blueprint generation.

Serializes signal data, positions, and task instructions into a
structured user prompt.  The analysis skill (SKILL.md + references)
is mounted directly into the LLM runtime by the provider — this
module only provides the *data* portion of the prompt.
"""
from __future__ import annotations

import json
from datetime import date
from typing import Any

from shared.models.signal import SignalFeatures

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_blueprint_prompt(
    signal_features: list[SignalFeatures],
    current_positions: dict | None = None,
    previous_execution: dict | None = None,
) -> str:
    """Build the user prompt containing market data and task instructions.

    The analysis workflow, reference documents, and output schema are
    provided by the ``trading-analysis`` skill bundle that is already
    mounted in the LLM runtime.  This function supplies only the
    *concrete market data* for the model to analyse.
    """
    sections: list[str] = []

    # Market Signal Data
    signal_section = _serialize_signals(signal_features)
    sections.append(
        f"## Market Signal Data (computed after {date.today()} close)\n\n{signal_section}"
    )

    # Current Positions
    positions_text = (
        json.dumps(current_positions, indent=2, ensure_ascii=False)
        if current_positions
        else "No open positions."
    )
    sections.append(f"## Current Positions\n\n{positions_text}")

    # Previous Execution Review
    prev_exec_text = (
        json.dumps(previous_execution, indent=2, ensure_ascii=False)
        if previous_execution
        else "No previous execution data available."
    )
    sections.append(f"## Previous Execution Review\n\n{prev_exec_text}")

    # Task
    sections.append(_build_task_section())

    return "\n\n".join(sections) + "\n"


# ---------------------------------------------------------------------------
# Full signal serialization
# ---------------------------------------------------------------------------


def _serialize_signals(features: list[SignalFeatures]) -> str:
    """Serialize every symbol's full indicator set, organized by category."""
    blocks: list[str] = []
    for sf in features:
        blocks.append(_serialize_one_signal(sf))
    return "\n\n".join(blocks)


def _serialize_one_signal(sf: SignalFeatures) -> str:
    """Produce a structured text block for a single symbol."""
    oi = sf.option_indicators
    si = sf.stock_indicators
    ca = sf.cross_asset_indicators

    sections: list[str] = [f"### {sf.symbol}"]

    # -- Price Context --
    sections.append("**Price Context**")
    sections.append(json.dumps({
        "close_price": sf.close_price,
        "daily_return": round(sf.daily_return, 6),
        "volume": sf.volume,
        "signal_score": round(sf.signal_score, 4),
        "signal_type": sf.signal_type,
        "volatility_regime": sf.volatility_regime,
        "suggested_strategies": sf.suggested_strategies,
    }, indent=2, ensure_ascii=False))

    # -- Stock Indicators --
    sections.append("**Stock Indicators — Trend & Momentum**")
    sections.append(json.dumps({
        "trend": si.trend,
        "trend_strength": round(si.trend_strength, 4),
        "rsi_14": round(si.rsi_14, 2),
        "stoch_rsi": round(si.stoch_rsi, 4),
        "rsi_divergence": round(si.rsi_divergence, 4),
        "macd": round(si.macd, 4),
        "macd_signal": round(si.macd_signal, 4),
        "macd_histogram": round(si.macd_histogram, 4),
        "macd_hist_divergence": round(si.macd_hist_divergence, 4),
        "adx_14": round(si.adx_14, 2),
        "ema_20": round(si.ema_20, 2),
        "ema_50": round(si.ema_50, 2),
        "sma_200": round(si.sma_200, 2),
        "bollinger_upper": round(si.bollinger_upper, 2),
        "bollinger_mid": round(si.bollinger_mid, 2),
        "bollinger_lower": round(si.bollinger_lower, 2),
        "bollinger_band_width": round(si.bollinger_band_width, 4),
        "keltner_upper": round(si.keltner_upper, 2),
        "keltner_mid": round(si.keltner_mid, 2),
        "keltner_lower": round(si.keltner_lower, 2),
        "ichimoku_tenkan": round(si.ichimoku_tenkan, 2),
        "ichimoku_kijun": round(si.ichimoku_kijun, 2),
        "ichimoku_span_a": round(si.ichimoku_span_a, 2),
        "ichimoku_span_b": round(si.ichimoku_span_b, 2),
        "linear_reg_slope": round(si.linear_reg_slope, 4),
        "atr_14": round(si.atr_14, 2),
    }, indent=2, ensure_ascii=False))

    sections.append("**Stock Indicators — Volatility**")
    sections.append(json.dumps({
        "hv_20d": round(si.hv_20d, 4),
        "hv_iv_spread": round(si.hv_iv_spread, 4),
        "garch_vol_forecast": round(si.garch_vol_forecast, 4),
    }, indent=2, ensure_ascii=False))

    sections.append("**Stock Indicators — Flow & Volume**")
    sections.append(json.dumps({
        "vwap": round(si.vwap, 2),
        "volume_profile_poc": round(si.volume_profile_poc, 2),
        "volume_profile_val": round(si.volume_profile_val, 2),
        "volume_profile_vah": round(si.volume_profile_vah, 2),
        "cmf_20": round(si.cmf_20, 4),
        "tick_volume_delta": round(si.tick_volume_delta, 4),
    }, indent=2, ensure_ascii=False))

    if si.extreme_flags:
        sections.append(f"**Stock Extreme Flags**: {si.extreme_flags}")
    if si.confidence_scores:
        sections.append(f"**Stock Confidence**: {json.dumps(si.confidence_scores)}")

    # -- Option Indicators --
    sections.append("**Option Indicators — Volatility Surface**")
    sections.append(json.dumps({
        "iv_rank": round(oi.iv_rank, 2),
        "iv_percentile": round(oi.iv_percentile, 2),
        "current_iv": round(oi.current_iv, 4),
        "historical_iv_30d": round(oi.historical_iv_30d, 4),
        "iv_skew": round(oi.iv_skew, 4),
        "term_structure_slope": round(oi.term_structure_slope, 4),
        "atm_iv": {k: round(v, 4) for k, v in oi.atm_iv.items()},
        "vol_surface_fit_error": round(oi.vol_surface_fit_error, 6),
    }, indent=2, ensure_ascii=False))

    sections.append("**Option Indicators — Greeks & Risk**")
    sections.append(json.dumps({
        "delta_exposure_profile": {k: round(v, 4) for k, v in oi.delta_exposure_profile.items()},
        "gamma_peak_strike": round(oi.gamma_peak_strike, 2),
        "theta_decay_rate": round(oi.theta_decay_rate, 4),
        "vanna": round(oi.vanna, 4),
        "charm": round(oi.charm, 4),
        "portfolio_greeks": {k: round(v, 4) for k, v in oi.portfolio_greeks.items()},
    }, indent=2, ensure_ascii=False))

    sections.append("**Option Indicators — Chain Structure**")
    sections.append(json.dumps({
        "pcr_volume": round(oi.pcr_volume, 4),
        "pcr_oi": round(oi.pcr_oi, 4),
        "oi_concentration_top5": round(oi.oi_concentration_top5, 4),
        "bid_ask_spread_ratio": round(oi.bid_ask_spread_ratio, 4),
        "option_volume_imbalance": round(oi.option_volume_imbalance, 4),
    }, indent=2, ensure_ascii=False))

    sections.append("**Option Indicators — Spreads & Arbitrage**")
    sections.append(json.dumps({
        "vertical_spread_risk_reward": round(oi.vertical_spread_risk_reward, 4),
        "calendar_spread_theta_capture": round(oi.calendar_spread_theta_capture, 4),
        "butterfly_pricing_error": round(oi.butterfly_pricing_error, 4),
        "box_spread_arbitrage": round(oi.box_spread_arbitrage, 4),
    }, indent=2, ensure_ascii=False))

    if oi.extreme_flags:
        sections.append(f"**Option Extreme Flags**: {oi.extreme_flags}")
    if oi.confidence_scores:
        sections.append(f"**Option Confidence**: {json.dumps(oi.confidence_scores)}")

    # -- Cross-Asset --
    sections.append("**Cross-Asset Indicators**")
    sections.append(json.dumps({
        "stock_iv_correlation": round(ca.stock_iv_correlation, 4),
        "option_vs_stock_volume_ratio": round(ca.option_vs_stock_volume_ratio, 4),
        "delta_adjusted_hedge_ratio": round(ca.delta_adjusted_hedge_ratio, 4),
    }, indent=2, ensure_ascii=False))
    if ca.confidence_scores:
        sections.append(f"**Cross-Asset Confidence**: {json.dumps(ca.confidence_scores)}")

    return "\n".join(sections)


# ---------------------------------------------------------------------------
# Task section
# ---------------------------------------------------------------------------


def _build_task_section() -> str:
    return f"""## Task

Generate a Trading Blueprint for the next trading day ({_next_trading_day()}).

Requirements:
1. Follow the trading-analysis skill workflow and apply loaded references.
2. Select 1-3 optimal underlyings from the watchlist based on the signal data.
3. For each underlying, design a concrete option strategy with fully defined legs.
4. Every strategy MUST include stop-loss exit conditions.
5. Apply portfolio-level risk controls.
6. Output strict JSON conforming to the blueprint schema — no extra keys, no comments, no markdown fences."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _next_trading_day() -> str:
    """Return the ISO date string for the next trading day (skip weekends)."""
    from datetime import timedelta
    d = date.today() + timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d.isoformat()
