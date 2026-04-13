"""LLM prompt builder for Trading Blueprint generation.

Serializes signal data, positions, and task instructions into a
structured user prompt.  Analysis knowledge is inlined directly in
each agent's system prompt (see agents/ directory).
"""
from __future__ import annotations

import json
from datetime import date
from typing import Any

from shared.data_quality import is_option_all_degraded, is_stock_all_degraded
from shared.models.signal import SignalFeatures
from shared.utils import today_trading

# ---------------------------------------------------------------------------
# System prompt (shared by all LLM providers)
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a professional options quantitative strategist at an institutional trading desk.

Generate a next-day Trading Blueprint by analysing the market signal data provided.

Rules:
1. Output ONLY valid, standard JSON — no markdown fences, no comments, no extra text.
   Use double-quoted keys and string values. Do NOT use single quotes, trailing commas,
   or language-specific literals like True/False/None.
2. Every condition must be mechanically evaluable with concrete numeric thresholds.
3. Every option leg must be fully defined (expiry, strike, option_type, side, quantity).
4. Every symbol_plan MUST include at least one stop-loss exit condition.
5. The reasoning field must reference which indicators drove the decision.
6. Respect all portfolio-level risk limits (see risk constraints in data).
7. **Position-aware analysis**: The prompt may include a "Current Portfolio" section. \
If open positions are present, you MUST: \
(a) evaluate whether to hold, increase, decrease, or close each existing position; \
(b) ensure aggregate portfolio Greeks stay within limits after any proposed changes; \
(c) document how existing exposure influenced your decision in the reasoning field. \
Do NOT ignore existing positions — every new plan must be justified relative to current exposure. \
If no positions are provided or the portfolio is flat, focus on fresh entry opportunities.
8. **Data quality awareness**: If a symbol's data includes a "data_quality" section with \
`complete: false`, you MUST: \
(a) note which indicators are degraded in the reasoning field; \
(b) reduce confidence proportionally — score < 0.5 should cap confidence at 0.5; \
(c) prefer conservative strategies (smaller position sizes, wider stops) for low-quality data symbols; \
(d) never rely on degraded indicators for entry/exit conditions.
"""

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_blueprint_prompt(
    signal_features: list[SignalFeatures],
    current_positions: dict | None = None,
    previous_execution: dict | None = None,
    *,
    signal_date: date | None = None,
) -> str:
    """Build the user prompt containing market data and task instructions."""
    sections: list[str] = []

    # Market Signal Data
    data_date = signal_date or today_trading()
    signal_section = _serialize_signals(signal_features)
    sections.append(
        f"## Market Signal Data (computed after {data_date} close)\n\n{signal_section}"
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
    sections.append(_build_task_section(signal_date=signal_date))

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


def _prune_defaults(d: dict[str, Any]) -> dict[str, Any]:
    """Remove only empty values while preserving meaningful numeric zeros."""
    return {
        k: v for k, v in d.items()
        if v is not None and v != {} and v != "" and v != []
    }


def _serialize_one_signal(sf: SignalFeatures) -> str:
    """Produce a compact JSON block for a single symbol."""
    oi = sf.option_indicators
    si = sf.stock_indicators
    ca = sf.cross_asset_indicators

    # -- Stock Trend --
    stock_trend: dict[str, Any] = {
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
    }
    if si.extreme_flags:
        stock_trend["extreme_flags"] = si.extreme_flags
    if si.confidence_scores:
        stock_trend["confidence"] = si.confidence_scores

    # -- Option Chain --
    option_chain: dict[str, Any] = {
        "pcr_volume": round(oi.pcr_volume, 4),
        "pcr_oi": round(oi.pcr_oi, 4),
        "oi_concentration_top5": round(oi.oi_concentration_top5, 4),
        "bid_ask_spread_ratio": round(oi.bid_ask_spread_ratio, 4),
        "option_volume_imbalance": round(oi.option_volume_imbalance, 4),
    }
    if oi.extreme_flags:
        option_chain["extreme_flags"] = oi.extreme_flags
    if oi.confidence_scores:
        option_chain["confidence"] = oi.confidence_scores

    # -- Cross-Asset --
    cross_asset: dict[str, Any] = {
        "stock_iv_correlation": round(ca.stock_iv_correlation, 4),
        "option_vs_stock_volume_ratio": round(ca.option_vs_stock_volume_ratio, 4),
        "delta_adjusted_hedge_ratio": round(ca.delta_adjusted_hedge_ratio, 4),
        "spy_beta": round(ca.spy_beta, 4),
        "sector_relative_strength": round(ca.sector_relative_strength, 4),
        "earnings_proximity_days": ca.earnings_proximity_days,
        "spy_correlation_20d": round(ca.index_correlation_20d, 4),
        "qqq_beta": round(ca.qqq_beta, 4),
        "qqq_correlation_20d": round(ca.qqq_correlation_20d, 4),
        "iwm_beta": round(ca.iwm_beta, 4),
        "iwm_correlation_20d": round(ca.iwm_correlation_20d, 4),
        "tlt_correlation_20d": round(ca.tlt_correlation_20d, 4),
        "vix_level": round(ca.vix_level, 4),
        "vix_percentile_52w": round(ca.vix_percentile_52w, 4),
        "vix_correlation_20d": round(ca.vix_correlation_20d, 4),
    }
    if ca.confidence_scores:
        cross_asset["confidence"] = ca.confidence_scores

    data: dict[str, Any] = {
        "price": {
            "close_price": sf.close_price,
            "daily_return": round(sf.daily_return, 6),
            "volume": sf.volume,
            "volatility_regime": sf.volatility_regime,
        },
        "stock_trend": _prune_defaults(stock_trend),
        "stock_vol": _prune_defaults({
            "hv_20d": round(si.hv_20d, 4),
            "hv_iv_spread": round(si.hv_iv_spread, 4),
            "garch_vol_forecast": round(si.garch_vol_forecast, 4),
        }),
        "stock_flow": _prune_defaults({
            "vwap": round(si.vwap, 2),
            "volume_profile_poc": round(si.volume_profile_poc, 2),
            "volume_profile_val": round(si.volume_profile_val, 2),
            "volume_profile_vah": round(si.volume_profile_vah, 2),
            "cmf_20": round(si.cmf_20, 4),
            "tick_volume_delta": round(si.tick_volume_delta, 4),
        }),
        "option_vol_surface": _prune_defaults({
            "iv_rank": round(oi.iv_rank, 2),
            "iv_percentile": round(oi.iv_percentile, 2),
            "current_iv": round(oi.current_iv, 4),
            "historical_iv_30d": round(oi.historical_iv_30d, 4),
            "iv_skew": round(oi.iv_skew, 4),
            "term_structure_slope": round(oi.term_structure_slope, 4),
            "atm_iv": {k: round(v, 4) for k, v in oi.atm_iv.items()},
            "vol_surface_fit_error": round(oi.vol_surface_fit_error, 6),
        }),
        "option_greeks": _prune_defaults({
            "delta_exposure_profile": {k: round(v, 4) for k, v in oi.delta_exposure_profile.items()},
            "gamma_peak_strike": round(oi.gamma_peak_strike, 2),
            "theta_decay_rate": round(oi.theta_decay_rate, 4),
            "vanna": round(oi.vanna, 4),
            "charm": round(oi.charm, 4),
            "portfolio_greeks": {k: round(v, 4) for k, v in oi.portfolio_greeks.items()},
        }),
        "option_chain": _prune_defaults(option_chain),
        "option_spreads": _prune_defaults({
            "vertical_spread_risk_reward": round(oi.vertical_spread_risk_reward, 4),
            "calendar_spread_theta_capture": round(oi.calendar_spread_theta_capture, 4),
            "butterfly_pricing_error": round(oi.butterfly_pricing_error, 4),
            "box_spread_arbitrage": round(oi.box_spread_arbitrage, 4),
        }),
        "cross_asset": _prune_defaults(cross_asset),
    }

    # ── Exclude sections for fully-degraded data categories ──
    degraded = sf.data_quality.degraded_indicators
    excluded_categories: list[str] = []
    if is_stock_all_degraded(degraded):
        for key in ("stock_trend", "stock_vol", "stock_flow"):
            data.pop(key, None)
        excluded_categories.append("stock")
    if is_option_all_degraded(degraded):
        for key in ("option_vol_surface", "option_greeks", "option_chain", "option_spreads"):
            data.pop(key, None)
        excluded_categories.append("option")

    data = {k: v for k, v in data.items() if v}

    if not sf.data_quality.complete:
        dq_info: dict[str, Any] = {
            "complete": False,
            "score": round(sf.data_quality.score, 4),
            "warnings": sf.data_quality.warnings,
            "degraded_indicators": sf.data_quality.degraded_indicators,
        }
        if excluded_categories:
            dq_info["excluded_categories"] = excluded_categories
        data["data_quality"] = dq_info

    compact = json.dumps(data, separators=(',', ':'), ensure_ascii=False)
    return f"### {sf.symbol}\n{compact}"


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
    if source == "trade_service_portfolio":
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


def _build_task_section(*, is_chunk: bool = False, signal_date: date | None = None) -> str:
    from shared.config import get_settings
    target_date = _next_trading_day(from_date=signal_date)
    if is_chunk:
        benchmark_str = ", ".join(get_settings().common.watchlist.for_trade_benchmark)
        return (
            f"## Task\n\n"
            f"Generate a Trading Blueprint for **{target_date}**.\n\n"
            f"This is a **subset** of the full watchlist processed in parallel. "
            f"Benchmark symbols ({benchmark_str}) are included for market context only — "
            f"do NOT generate plans for them unless they present actionable setups.\n\n"
            f"Follow the trading-analysis skill workflow. Generate trading plans for "
            f"ALL non-benchmark symbols provided in the signal data. Design concrete "
            f"strategies with fully defined legs for each.\n\n"
            f"If the Current Portfolio section shows open positions, evaluate each for "
            f"hold / increase / decrease / close before proposing new trades.\n\n"
            f"Output ONLY raw JSON — no markdown code fences, no commentary.\n\n"
        )
    return (
        f"## Task\n\n"
        f"Generate a Trading Blueprint for **{target_date}**.\n\n"
        f"Follow the trading-analysis skill workflow. Select 1-3 optimal underlyings "
        f"from the signal data, design concrete strategies with fully defined legs.\n\n"
        f"If the Current Portfolio section shows open positions, evaluate each for "
        f"hold / increase / decrease / close before proposing new trades.\n\n"
        f"Output ONLY raw JSON — no markdown code fences, no commentary."
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _next_trading_day(from_date: date | None = None) -> str:
    """Return the ISO date string for the next trading day (skip weekends)."""
    from shared.utils import next_trading_day
    return next_trading_day(from_date=from_date).isoformat()
