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

    async def generate_single_symbol(
        self,
        signal_features: list[SignalFeatures],
        current_positions: dict | None = None,
        previous_execution: dict | None = None,
        *,
        signal_date: date | None = None,
    ) -> LLMTradingBlueprint:
        """Simplified single-LLM-call path for manual single-symbol analysis.

        Instead of running the full 6-agent → synthesizer → critic pipeline
        (7-9 LLM calls), this method builds one comprehensive prompt and
        makes a single LLM call.  Ideal for quick manual checks on one symbol.
        """
        from services.analysis_service.app.llm.agents.orchestrator import (
            _create_agent_provider,
        )
        from services.analysis_service.app.llm.json_utils import parse_llm_json
        from services.analysis_service.app.llm.prompts import (
            SYSTEM_PROMPT,
            build_blueprint_prompt,
        )

        started = perf_counter()
        provider = _create_agent_provider()

        user_prompt = build_blueprint_prompt(
            signal_features=signal_features,
            current_positions=current_positions,
            previous_execution=previous_execution,
            signal_date=signal_date,
        )

        logger.info(
            "llm_adapter.single_symbol_started",
            symbols=len(signal_features),
            provider=provider.name,
        )

        from shared.config import get_settings
        settings = get_settings()
        max_tokens = settings.analysis_service.llm.openai.max_tokens

        result = await provider.generate(
            instructions=SYSTEM_PROMPT,
            user_prompt=user_prompt,
            max_tokens=max_tokens,
        )

        blueprint = LLMTradingBlueprint(**parse_llm_json(result.content))

        # Fill in provider metadata
        blueprint = blueprint.model_copy(update={
            "model_provider": provider.name,
            "reasoning_context": {
                "pipeline": "single_symbol",
                "provider": provider.name,
                "input_tokens": result.input_tokens,
                "output_tokens": result.output_tokens,
            },
        })

        elapsed_ms = round((perf_counter() - started) * 1000, 2)
        logger.info(
            "llm_adapter.single_symbol_success",
            plans=len(blueprint.symbol_plans),
            input_tokens=result.input_tokens,
            output_tokens=result.output_tokens,
            elapsed_ms=elapsed_ms,
        )
        return blueprint
