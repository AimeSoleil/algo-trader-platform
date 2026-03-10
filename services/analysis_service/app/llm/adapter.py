"""LLM 适配器 — 统一接口 + 回退逻辑"""
from __future__ import annotations

from shared.config import get_settings
from shared.models.blueprint import LLMTradingBlueprint
from shared.models.signal import SignalFeatures
from shared.utils import get_logger

from services.analysis_service.app.llm.base import LLMProviderBase
from services.analysis_service.app.llm.openai_provider import OpenAIProvider

logger = get_logger("llm_adapter")


class LLMAdapter:
    """
    LLM 统一适配器：
    - 根据配置选择 primary provider
    - primary 失败时回退到 secondary
    """

    def __init__(self):
        settings = get_settings()
        self.primary_name = settings.llm.provider
        self.primary = self._create_provider(self.primary_name)
        self.secondary = self._create_secondary(self.primary_name)

    def _create_provider(self, name: str) -> LLMProviderBase:
        if name == "openai":
            return OpenAIProvider()
        elif name == "copilot":
            from services.analysis_service.app.llm.copilot_provider import CopilotProvider
            return CopilotProvider()
        else:
            raise ValueError(f"Unknown LLM provider: {name}")

    def _create_secondary(self, primary_name: str) -> LLMProviderBase | None:
        """创建回退 provider"""
        if primary_name == "openai":
            try:
                from services.analysis_service.app.llm.copilot_provider import CopilotProvider
                return CopilotProvider()
            except ImportError:
                return None
        elif primary_name == "copilot":
            return OpenAIProvider()
        return None

    async def generate_blueprint(
        self,
        signal_features: list[SignalFeatures],
        current_positions: dict | None = None,
        previous_execution: dict | None = None,
    ) -> LLMTradingBlueprint:
        """生成蓝图：primary provider → 失败则回退 secondary"""
        try:
            blueprint = await self.primary.generate_blueprint(
                signal_features, current_positions, previous_execution
            )
            logger.info("llm_adapter.success", provider=self.primary_name)
            return blueprint
        except Exception as e:
            logger.warning(
                "llm_adapter.primary_failed",
                provider=self.primary_name,
                error=str(e),
            )
            if self.secondary:
                logger.info("llm_adapter.fallback_to_secondary")
                blueprint = await self.secondary.generate_blueprint(
                    signal_features, current_positions, previous_execution
                )
                return blueprint
            raise
