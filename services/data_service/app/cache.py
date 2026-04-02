"""盘中实时缓存 — L1 内存"""
from __future__ import annotations

from shared.utils import get_logger

logger = get_logger("market_cache")


class MarketHoursCache:
    """盘中缓存 — L1 内存

    - latest_quotes + latest_option_chains → API / Execution Service 读取
    """

    def __init__(self) -> None:
        # L1: 内存 — 最新快照
        self.latest_quotes: dict[str, dict] = {}
        self.latest_option_chains: dict[str, list[dict]] = {}

    # ── L1: 实时更新 ──────────────────────────────────────

    def update_quote(self, symbol: str, quote: dict) -> None:
        """更新股票行情到 L1 内存（供 API 查询）"""
        self.latest_quotes[symbol] = quote

    def update_option_chain(self, symbol: str, contracts: list[dict]) -> None:
        """更新期权链到 L1 内存"""
        self.latest_option_chains[symbol] = contracts

    # ── L1: 读取 ──────────────────────────────────────────

    def get_realtime_quote(self, symbol: str) -> dict | None:
        """Execution Service 调用：获取最新行情"""
        return self.latest_quotes.get(symbol)

    def get_realtime_option_chain(self, symbol: str) -> list[dict] | None:
        """Execution Service 调用：获取最新期权链"""
        return self.latest_option_chains.get(symbol)

    def clear_l1(self) -> None:
        """清空 L1 内存缓存（收盘后调用）"""
        self.latest_quotes.clear()
        self.latest_option_chains.clear()


# ── 模块级单例 ─────────────────────────────────────────────
cache = MarketHoursCache()
