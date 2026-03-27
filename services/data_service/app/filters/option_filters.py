"""期权数据过滤器 — Stage 1 清洁 + Stage 2 可交易标记

两阶段设计：
  Stage 1 (clean_option_chain):
      仅剔除不可用的坏数据（IV=0 / IV>阈值），
      保留所有 OTM / 低流动性合约以维护 IV smile / skew / term structure 完整。

  Stage 2 (mark_tradeable):
      不剔除任何合约，仅对满足流动性 / 价差 / delta 等条件的合约
      设置 is_tradeable = True，供信号服务按需过滤。
"""
from __future__ import annotations

from shared.config import get_settings
from shared.models.option import OptionChainSnapshot, OptionContract
from shared.utils import get_logger
from shared.utils.time import today_trading

from shared.models.filter import FilterResult

logger = get_logger("option_filters")


# ── Stage 1: 数据清洁 ─────────────────────────────────────


def clean_option_chain(snapshot: OptionChainSnapshot, *, cfg=None) -> tuple[OptionChainSnapshot, dict]:
    """Stage 1: 清洁过滤 — 仅剔除真正不可用的坏数据。

    规则（全部基于 config 可配置）：
    - IV ≤ 0（greeks 无法计算，无意义）
    - IV > max_iv（数据垃圾，如 yfinance 返回 50000%）

    注意：DTE 过滤在 option_fetcher 的到期日循环中完成（控制要不要 *请求* 那个到期日），
    不属于此处的合约级清洁。

    Returns
    -------
    tuple[OptionChainSnapshot, dict]
        就地修改后的快照 + 清洁统计指标。
    """
    if cfg is None:
        cfg = get_settings().data_service.filters.options.cleaning

    kept: list[OptionContract] = []
    removed_bad_iv = 0

    for c in snapshot.contracts:
        iv = c.greeks.iv
        if iv <= 0 or iv > cfg.max_iv:
            removed_bad_iv += 1
            continue
        kept.append(c)

    snapshot.contracts = kept
    stats = {"removed_bad_iv": removed_bad_iv}
    return snapshot, stats


# ── Stage 2: 可交易标记 ───────────────────────────────────


def mark_tradeable(snapshot: OptionChainSnapshot, *, cfg=None) -> tuple[OptionChainSnapshot, dict]:
    """Stage 2: 可交易标记 — 不剔除合约，仅设置 is_tradeable 标志。

    满足以下 **全部** 条件的合约标记为 is_tradeable = True：
    - volume ≥ min_volume
    - open_interest ≥ min_open_interest
    - bid-ask spread / mid ≤ max_relative_spread
    - strike / underlying_price ∈ [min_strike_ratio, max_strike_ratio]
    - |delta| ≥ min_delta_threshold

    不满足条件的合约保留（供分析类指标使用），但 is_tradeable = False。

    Returns
    -------
    tuple[OptionChainSnapshot, dict]
        就地修改后的快照 + 标记统计指标。
    """
    if cfg is None:
        cfg = get_settings().data_service.filters.options.tradeable_marking
    marked_count = 0

    for c in snapshot.contracts:
        tradeable = True

        # Volume check
        if c.volume < cfg.min_volume:
            tradeable = False

        # Open interest check
        if c.open_interest < cfg.min_open_interest:
            tradeable = False

        # Bid-ask spread check
        mid = c.mid_price
        if mid > 0 and c.spread / mid > cfg.max_relative_spread:
            tradeable = False

        # Strike range check (relative to underlying)
        if snapshot.underlying_price > 0:
            ratio = c.strike / snapshot.underlying_price
            if ratio < cfg.min_strike_ratio or ratio > cfg.max_strike_ratio:
                tradeable = False

        # Delta threshold check
        if abs(c.greeks.delta) < cfg.min_delta_threshold:
            tradeable = False

        # Stale trade check: contracts not traded recently are illiquid
        if c.last_trade_date is not None:
            days_since_trade = (today_trading() - c.last_trade_date).days
            if days_since_trade > cfg.max_stale_trade_days:
                tradeable = False

        c.is_tradeable = tradeable
        if tradeable:
            marked_count += 1

    stats = {"tradeable_marked": marked_count, "total_after_clean": len(snapshot.contracts)}
    return snapshot, stats


# ── Pipeline 入口 ──────────────────────────────────────────


def apply_option_pipeline(
    snapshot: OptionChainSnapshot,
) -> tuple[OptionChainSnapshot, FilterResult]:
    """执行完整的期权过滤流水线：clean → mark_tradeable。

    Parameters
    ----------
    snapshot : OptionChainSnapshot
        已经过 greeks 计算的期权链快照。

    Returns
    -------
    tuple[OptionChainSnapshot, FilterResult]
        处理后的快照 + FilterResult（可观测性指标）。
    """
    result = FilterResult(total_input=len(snapshot.contracts))

    # Stage 1: Clean
    snapshot, clean_stats = clean_option_chain(snapshot)
    result.removed = clean_stats.get("removed_bad_iv", 0)

    # Stage 2: Mark tradeable
    snapshot, mark_stats = mark_tradeable(snapshot)
    result.marked_tradeable = mark_stats.get("tradeable_marked", 0)

    result.details = {**clean_stats, **mark_stats}
    result.log("pipeline", "option", snapshot.underlying)

    return snapshot, result
