"""期权交易级过滤器 — Stage 3（仅用于策略类指标）

此过滤器用于 signal_service 的策略类指标计算（vertical spread、butterfly 等），
不影响分析类指标（IV skew、vol surface、term structure、PCR 等）。

两种使用模式：
  1. ``apply_trading_filter(df)`` — 基于配置阈值过滤 DataFrame
  2. ``filter_by_is_tradeable(df)`` — 直接使用 data_service 标记的 is_tradeable 列

推荐：优先使用 ``filter_by_is_tradeable``（利用 data_service Stage 2 标记），
当需要更严格交易级条件时使用 ``apply_trading_filter``。
"""
from __future__ import annotations

import pandas as pd

from shared.config import get_settings
from shared.utils import get_logger

from shared.models.filter import FilterResult

logger = get_logger("signal_option_filters")


def filter_by_is_tradeable(option_data: pd.DataFrame) -> pd.DataFrame:
    """使用 data_service Stage 2 预标记的 is_tradeable 列过滤。

    如果 DataFrame 中没有 is_tradeable 列，返回原始数据（向后兼容）。
    """
    if "is_tradeable" not in option_data.columns:
        return option_data
    return option_data[option_data["is_tradeable"] == True].copy()  # noqa: E712


def apply_trading_filter(
    option_data: pd.DataFrame,
    underlying_price: float = 0.0,
    *,
    cfg=None,
) -> tuple[pd.DataFrame, FilterResult]:
    """Stage 3: 交易级过滤 — 用于策略类指标，不影响分析类指标。

    过滤规则（基于 signal_service.filters.options.trading 配置）：
    - volume ≥ min_volume
    - open_interest ≥ min_open_interest
    - bid-ask spread / mid ≤ max_relative_spread
    - |delta| ∈ [min_delta, max_delta]
    - DTE ∈ [min_dte, max_dte]

    如果过滤后行数不足 10 行，回退到使用 is_tradeable 列（如果可用），
    或返回原始数据以避免策略指标全部为零。

    Parameters
    ----------
    option_data : pd.DataFrame
        期权链 DataFrame，含 delta / volume / open_interest / bid / ask / expiry 列。
    underlying_price : float
        标的当前价格（当前未使用，预留扩展）。

    Returns
    -------
    tuple[pd.DataFrame, FilterResult]
    """
    result = FilterResult(total_input=len(option_data))

    if option_data.empty:
        return option_data, result

    if cfg is None:
        cfg = get_settings().signal_service.filters.options.trading
    df = option_data.copy()

    mask = pd.Series(True, index=df.index)

    # Volume filter
    if "volume" in df.columns:
        mask &= df["volume"].fillna(0) >= cfg.min_volume

    # Open interest filter
    if "open_interest" in df.columns:
        mask &= df["open_interest"].fillna(0) >= cfg.min_open_interest

    # Bid-ask spread filter
    if "bid" in df.columns and "ask" in df.columns:
        mid = (df["bid"].fillna(0) + df["ask"].fillna(0)) / 2
        spread_ratio = (df["ask"].fillna(0) - df["bid"].fillna(0)) / mid.replace(0, float("nan"))
        mask &= spread_ratio.fillna(1.0) <= cfg.max_relative_spread

    # Delta filter
    if "delta" in df.columns:
        abs_delta = df["delta"].fillna(0).abs()
        mask &= abs_delta >= cfg.min_delta
        mask &= abs_delta <= cfg.max_delta

    # DTE filter
    if "expiry" in df.columns:
        now_date = pd.Timestamp.utcnow().normalize()
        dte = (pd.to_datetime(df["expiry"], errors="coerce") - now_date).dt.days
        mask &= dte.fillna(0) >= cfg.min_dte
        mask &= dte.fillna(9999) <= cfg.max_dte

    filtered = df[mask]
    result.filtered = len(option_data) - len(filtered)

    # Fallback: if too few rows survive, try is_tradeable column
    MIN_ROWS_FOR_STRATEGY = 10
    if len(filtered) < MIN_ROWS_FOR_STRATEGY:
        fallback = filter_by_is_tradeable(option_data)
        if len(fallback) >= MIN_ROWS_FOR_STRATEGY:
            result.details["fallback"] = "is_tradeable"
            result.filtered = len(option_data) - len(fallback)
            return fallback, result
        # Both too sparse — return original to avoid all-zero indicators
        result.details["fallback"] = "no_filter"
        result.filtered = 0
        return option_data.copy(), result

    result.details["method"] = "config_based"
    return filtered, result
