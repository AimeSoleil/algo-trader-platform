"""Data Service — 数据转换工具"""
from __future__ import annotations


def contracts_to_rows(
    snapshot,
    top_expiries: int | None = None,
    include_snapshot_date: bool = False,
) -> list[dict]:
    """将 OptionChainSnapshot 转为可入库的 dict 列表

    Parameters
    ----------
    snapshot : OptionChainSnapshot
        期权链快照对象
    top_expiries : int | None
        仅保留最近 N 个到期日的合约（None 表示全部）
    include_snapshot_date : bool
        是否添加 snapshot_date 字段（option_daily 表需要）
    """
    contracts = snapshot.contracts
    if top_expiries and top_expiries > 0:
        expiries = sorted({c.expiry for c in contracts})[:top_expiries]
        contracts = [contract for contract in contracts if contract.expiry in expiries]

    rows = [
        {
            "underlying": contract.underlying,
            "symbol": contract.symbol,
            "timestamp": snapshot.timestamp,
            "expiry": contract.expiry,
            "strike": contract.strike,
            "option_type": contract.option_type.value,
            "last_price": contract.last_price,
            "bid": contract.bid,
            "ask": contract.ask,
            "volume": contract.volume,
            "open_interest": contract.open_interest,
            "iv": contract.greeks.iv,
            "delta": contract.greeks.delta,
            "gamma": contract.greeks.gamma,
            "theta": contract.greeks.theta,
            "vega": contract.greeks.vega,
        }
        for contract in contracts
    ]

    if include_snapshot_date:
        for row in rows:
            row["snapshot_date"] = snapshot.timestamp.date()
    return rows
