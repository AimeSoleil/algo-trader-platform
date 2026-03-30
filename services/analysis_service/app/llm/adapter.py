"""LLM adapter — agentic-only orchestration entrypoint."""
from __future__ import annotations

from datetime import date
from time import perf_counter

from shared.models.blueprint import LLMTradingBlueprint
from shared.models.signal import SignalFeatures
from shared.utils import get_logger

logger = get_logger("llm_adapter")


class LLMAdapter:
    """Unified adapter that delegates blueprint generation to AgentOrchestrator."""

    def __init__(self):
        self._orchestrator = None  # lazy-init

    def _get_orchestrator(self):
        """Lazy-init the AgentOrchestrator."""
        if self._orchestrator is None:
            from services.analysis_service.app.llm.agents.orchestrator import AgentOrchestrator
            self._orchestrator = AgentOrchestrator()
            logger.info("llm_adapter.orchestrator_initialized")
        return self._orchestrator

    async def generate_blueprint(
        self,
        signal_features: list[SignalFeatures],
        current_positions: dict | None = None,
        previous_execution: dict | None = None,
        *,
        signal_date: date | None = None,
    ) -> LLMTradingBlueprint:
        """Generate blueprint via the multi-agent pipeline only."""
        return await self._generate_agentic(
            signal_features,
            current_positions,
            previous_execution,
            signal_date=signal_date,
        )

    async def _generate_agentic(
        self,
        signal_features: list[SignalFeatures],
        current_positions: dict | None,
        previous_execution: dict | None,
        *,
        signal_date: date | None = None,
    ) -> LLMTradingBlueprint:
        """Multi-agent pipeline; failures propagate to caller."""
        started = perf_counter()
        try:
            orchestrator = self._get_orchestrator()
            blueprint = await orchestrator.generate(
                signal_features=signal_features,
                current_positions=current_positions,
                previous_execution=previous_execution,
                signal_date=signal_date,
            )
            elapsed_ms = round((perf_counter() - started) * 1000, 2)
            logger.info(
                "llm_adapter.agentic_success",
                plans=len(blueprint.symbol_plans),
                elapsed_ms=elapsed_ms,
            )
            return blueprint
        except Exception as e:
            elapsed_ms = round((perf_counter() - started) * 1000, 2)
            logger.warning(
                "llm_adapter.agentic_failed",
                error=str(e),
                elapsed_ms=elapsed_ms,
            )
            raise
