"""过滤器结果模型 — 所有 service 共用

FilterResult 统一记录各阶段过滤指标，供可观测性日志与 API 查询使用。
三阶段过滤的计数器独立：
  - removed: Stage 1 清洁阶段剔除的数量
  - marked_tradeable: Stage 2 标记为可交易的数量
  - filtered: Stage 3 交易级过滤剔除的数量
"""
from __future__ import annotations

from dataclasses import dataclass, field

from shared.utils import get_logger

logger = get_logger("filters")


@dataclass
class FilterResult:
    """单次过滤流水线的执行结果摘要。

    Attributes
    ----------
    total_input : int
        输入的合约/行数。
    removed : int
        Stage 1 清洁阶段剔除的数量（坏数据）。
    marked_tradeable : int
        Stage 2 标记为 is_tradeable=True 的数量。
    filtered : int
        Stage 3 交易级过滤剔除的数量（仅策略指标阶段使用）。
    details : dict
        额外统计明细（如 removed_bad_iv、fallback 方式等）。
    """

    total_input: int = 0
    removed: int = 0
    marked_tradeable: int = 0
    filtered: int = 0
    details: dict = field(default_factory=dict)

    @property
    def output_count(self) -> int:
        """经过清洁 + 交易级过滤后的最终输出数量。"""
        return self.total_input - self.removed - self.filtered

    def log(self, stage: str, asset_type: str, symbol: str) -> None:
        """输出结构化日志。

        Parameters
        ----------
        stage : str
            日志事件名，如 ``"pipeline"``（data-service）或 ``"trading"``（signal-service）。
        asset_type : str
            资产类型，如 ``"option"`` 或 ``"stock"``。
        symbol : str
            标的代码。
        """
        logger.info(
            f"filter.{stage}_result",
            asset_type=asset_type,
            symbol=symbol,
            total_input=self.total_input,
            removed=self.removed,
            marked_tradeable=self.marked_tradeable,
            filtered=self.filtered,
            output_count=self.output_count,
            details=self.details,
        )
