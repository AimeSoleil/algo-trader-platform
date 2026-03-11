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
# System prompt (shared by all LLM providers)
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a professional options quantitative strategist at an institutional trading desk.

The trading-analysis skill is mounted in your environment.  Read its SKILL.md, \
follow the workflow, load references based on the market context in the data, \
and produce a next-day Trading Blueprint.

Rules:
1. Output ONLY valid JSON — no markdown fences, no comments, no extra text.
2. Every condition must be mechanically evaluable with concrete numeric thresholds.
3. Every option leg must be fully defined (expiry, strike, option_type, side, quantity).
4. Every symbol_plan MUST include at least one stop-loss exit condition.
5. The reasoning field must reference which indicators and reference analyses drove the decision.
6. Respect all portfolio-level risk limits from the risk-management reference.
7. **Position-aware analysis**: The prompt may include a "Current Portfolio" section. \
If open positions are present, you MUST: \
(a) evaluate whether to hold, increase, decrease, or close each existing position; \
(b) ensure aggregate portfolio Greeks stay within limits after any proposed changes; \
(c) document how existing exposure influenced your decision in the reasoning field. \
Do NOT ignore existing positions — every new plan must be justified relative to current exposure. \
If no positions are provided or the portfolio is flat, focus on fresh entry opportunities.
"""

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

    # Current Positions & Portfolio Context
    sections.append(_build_positions_section(current_positions))

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


def _build_positions_section(current_positions: dict | None) -> str:
    """Build the Current Positions section with portfolio-aware context."""
    if not current_positions or current_positions.get("count", 0) == 0:
        return (
            "## Current Portfolio\n\n"
            "No open positions. The portfolio is flat — all capital is available "
            "for new entries.\n\n"
            "*Position-related requirements in the Task section can be skipped.*"
        )

    source = current_positions.get("source", "unknown")
    count = current_positions.get("count", 0)
    positions = current_positions.get("positions", [])
    aggregates = current_positions.get("aggregates", {})

    lines: list[str] = ["## Current Portfolio"]

    # Source note
    if source == "portfolio_service":
        lines.append(f"\n*Source: live portfolio — {count} open position(s).*")
    elif source == "previous_blueprint":
        bp_date = current_positions.get("blueprint_date", "?")
        lines.append(
            f"\n*Source: inferred from {bp_date} blueprint — {count} position(s) "
            f"entered but not yet exited. Treat these as current exposure.*"
        )
    else:
        lines.append(f"\n*{count} open position(s).*")

    # Aggregates
    if aggregates:
        lines.append("\n**Portfolio Aggregates**")
        lines.append(json.dumps(
            {k: round(v, 4) if isinstance(v, float) else v for k, v in aggregates.items()},
            indent=2,
            ensure_ascii=False,
        ))

    # Individual positions
    lines.append("\n**Open Positions**")
    lines.append(json.dumps(positions, indent=2, ensure_ascii=False))

    return "\n".join(lines)


def _build_task_section() -> str:
    return f"""## Task

Generate a Trading Blueprint for the next trading day ({_next_trading_day()}).

Requirements:
1. Follow the trading-analysis skill workflow and apply loaded references.
2. Select 1-3 optimal underlyings from the watchlist based on the signal data.
3. For each underlying, design a concrete option strategy with fully defined legs.
4. Every strategy MUST include stop-loss exit conditions.
5. Apply portfolio-level risk controls.
6. **Position-Aware Analysis** — If the Current Portfolio section above contains open positions:
   a. Assess whether to HOLD, INCREASE, DECREASE, or CLOSE each existing position.
   b. If increasing, ensure the added size does not breach max_total_positions or delta/gamma limits.
   c. If the position has unrealized losses exceeding stop thresholds, recommend closing or hedging.
   d. If the position is profitable, evaluate whether to take partial profits or roll to extend.
   e. For underlyings with NO existing position, treat as fresh entry candidates.
   If no positions are shown, skip this requirement.
7. **Risk Management for Existing Exposure** — If there are open positions, before proposing new trades:
   a. Check aggregate portfolio Greeks (delta, gamma, theta, vega) stay within limits.
   b. Avoid concentrating too much exposure in a single underlying.
   c. Factor in existing theta decay and margin usage.
   d. Document in ``reasoning`` how existing positions influenced the decision.
   If no positions are shown, skip this requirement.
8. Output strict JSON conforming to the blueprint schema — no extra keys, no comments, no markdown fences."""


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
