"""盘中双层缓存 — L1 内存 + L2 Parquet 文件"""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from shared.utils import get_logger

logger = get_logger("market_cache")


class MarketHoursCache:
    """盘中缓存 — L1 内存 + L2 Parquet（期权链专用）

    - L1 (内存): latest_quotes + latest_option_chains → API / Execution Service 读取
    - L2 (Parquet): 盘中期权链累积 → 盘后 batch_flush 入库 option_5min_snapshots
    - 股票行情由盘后 Celery pipeline 直接写入 DB，不经过 L2
    """

    def __init__(self, cache_dir: str | None = None):
        self.cache_dir = Path(cache_dir or os.environ.get("CACHE_DIR", "/tmp/algo_trader_cache"))
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # L1: 内存 — 最新快照
        self.latest_quotes: dict[str, dict] = {}
        self.latest_option_chains: dict[str, list[dict]] = {}

        # L2: 期权链 buffer（达到阈值刷盘到 Parquet）
        self._option_buffer: list[dict] = []
        self._flush_threshold = 500

    # ── L1: 实时更新 ──────────────────────────────────────

    def update_quote(self, symbol: str, quote: dict) -> None:
        """更新股票行情到 L1 内存（供 API 查询；不写 L2）"""
        self.latest_quotes[symbol] = quote

    def update_option_chain(self, symbol: str, contracts: list[dict]) -> None:
        """更新期权链到 L1 + 追加到 L2 buffer"""
        self.latest_option_chains[symbol] = contracts
        self._option_buffer.extend(contracts)
        if len(self._option_buffer) >= self._flush_threshold:
            self._flush_to_file("option")

    # ── L1: 读取 ──────────────────────────────────────────

    def get_realtime_quote(self, symbol: str) -> dict | None:
        """Execution Service 调用：获取最新行情"""
        return self.latest_quotes.get(symbol)

    def get_realtime_option_chain(self, symbol: str) -> list[dict] | None:
        """Execution Service 调用：获取最新期权链"""
        return self.latest_option_chains.get(symbol)

    # ── L2: 文件刷盘 ──────────────────────────────────────

    def _flush_to_file(self, data_type: str = "option") -> None:
        """将期权链 buffer 刷到 Parquet 文件"""
        buffer = self._option_buffer
        if not buffer:
            return

        today = date.today().isoformat()
        filepath = self.cache_dir / f"{data_type}_{today}.parquet"

        try:
            df = pd.DataFrame(buffer)
            if filepath.exists():
                existing = pq.read_table(str(filepath)).to_pandas()
                df = pd.concat([existing, df], ignore_index=True)

            table = pa.Table.from_pandas(df)
            pq.write_table(table, str(filepath))

            buffer.clear()
            logger.info("cache.flushed_to_file", data_type=data_type, file=str(filepath), rows=len(df))
        except Exception as e:
            logger.error("cache.flush_failed", data_type=data_type, error=str(e))

    def flush_all(self) -> None:
        """刷盘期权链残余 buffer（盘后批量入库前调用）"""
        self._flush_to_file("option")

    def get_parquet_path(self, data_type: str, trading_date: date | None = None) -> Path:
        """获取指定日期的 Parquet 文件路径"""
        d = (trading_date or date.today()).isoformat()
        return self.cache_dir / f"{data_type}_{d}.parquet"

    def read_parquet(self, data_type: str, trading_date: date | None = None) -> pd.DataFrame | None:
        """读取指定日期的 Parquet 缓存文件"""
        filepath = self.get_parquet_path(data_type, trading_date)
        if filepath.exists():
            return pq.read_table(str(filepath)).to_pandas()
        return None

    def clear_parquet(self, data_type: str, trading_date: date | None = None) -> None:
        """入库成功后清理 Parquet 文件"""
        filepath = self.get_parquet_path(data_type, trading_date)
        if filepath.exists():
            filepath.unlink()
            logger.info("cache.parquet_cleared", file=str(filepath))

    def clear_l1(self) -> None:
        """清空 L1 内存缓存（收盘后调用）"""
        self.latest_quotes.clear()
        self.latest_option_chains.clear()
