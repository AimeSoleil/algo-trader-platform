"""LLM Provider 统一接口"""
from __future__ import annotations

from abc import ABC, abstractmethod

from shared.models.blueprint import LLMTradingBlueprint
from shared.models.signal import SignalFeatures


class LLMProviderBase(ABC):
    """LLM 适配器基类"""

    @abstractmethod
    async def generate_blueprint(
        self,
        signal_features: list[SignalFeatures],
        current_positions: dict | None = None,
        previous_execution: dict | None = None,
    ) -> LLMTradingBlueprint:
        """生成次日交易蓝图"""
        ...

    @abstractmethod
    async def health_check(self) -> bool:
        """检查 LLM 服务是否可用"""
        ...
