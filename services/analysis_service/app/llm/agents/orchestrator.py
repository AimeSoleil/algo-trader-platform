"""Multi-agent orchestrator for the analysis pipeline.

Coordinates 6 specialist agents → Synthesizer → Critic in a structured
pipeline, replacing the old chunk-split-merge approach.

The Orchestrator creates the LLM provider based on ``settings.analysis_service.llm.provider``
and injects it into every agent — no agent hardcodes which LLM to call.

Flow:
    6 Analysis Agents (parallel) → Synthesizer → Critic → [optional revision] → Final Blueprint
"""
from __future__ import annotations

import asyncio
import json
from datetime import date
from time import perf_counter
from typing import Any

from shared.config import get_settings
from shared.data_quality import should_circuit_break_analysis
from shared.metrics import llm_request_duration
from shared.models.blueprint import LLMTradingBlueprint
from shared.models.signal import SignalFeatures
from shared.utils import get_logger

from services.analysis_service.app.llm.agents.base_agent import AgentLLMProvider, LLMUsageTracker
from services.analysis_service.app.llm.agents.chain_agent import ChainAgent
from services.analysis_service.app.llm.agents.cross_asset_agent import CrossAssetAgent
from services.analysis_service.app.llm.agents.critic_agent import CriticAgent
from services.analysis_service.app.llm.agents.flow_agent import FlowAgent
from services.analysis_service.app.llm.agents.spread_agent import SpreadAgent
from services.analysis_service.app.llm.agents.synthesizer_agent import SynthesizerAgent
from services.analysis_service.app.llm.agents.trend_agent import TrendAgent
from services.analysis_service.app.llm.agents.volatility_agent import VolatilityAgent
from services.analysis_service.app.llm.prompts import _serialize_one_signal

logger = get_logger("agent_orchestrator")

# Maximum revision rounds now read from config: settings.analysis_service.llm.max_critic_revisions


def _create_agent_provider(provider_name: str | None = None) -> AgentLLMProvider:
    """Instantiate the correct ``AgentLLMProvider`` based on config.

    Parameters
    ----------
    provider_name:
        ``"openai"`` or ``"copilot"``.  Defaults to ``settings.analysis_service.llm.provider``.
    """
    if provider_name is None:
        provider_name = get_settings().analysis_service.llm.provider

    if provider_name == "copilot":
        from services.analysis_service.app.llm.agents._copilot_agent_provider import (
            CopilotAgentProvider,
        )
        return CopilotAgentProvider()

    # Default / "openai"
    from services.analysis_service.app.llm.agents._openai_agent_provider import (
        OpenAIAgentProvider,
    )
    return OpenAIAgentProvider()


class AgentOrchestrator:
    """Orchestrate multi-agent blueprint generation.

    Usage::

        orch = AgentOrchestrator()
        blueprint = await orch.generate(signals, positions, prev_exec)
    """

    def __init__(self, *, provider: AgentLLMProvider | None = None):
        # Specialist agents (stateless — safe to reuse)
        self._trend = TrendAgent()
        self._volatility = VolatilityAgent()
        self._flow = FlowAgent()
        self._chain = ChainAgent()
        self._spread = SpreadAgent()
        self._cross_asset = CrossAssetAgent()

        # Synthesis & review
        self._synthesizer = SynthesizerAgent()
        self._critic = CriticAgent()

        # Provider — lazy-created on first use if not supplied
        self._provider = provider

    async def generate(
        self,
        signal_features: list[SignalFeatures],
        current_positions: dict | None = None,
        previous_execution: dict | None = None,
        *,
        signal_date: date | None = None,
    ) -> LLMTradingBlueprint:
        """Run the full multi-agent pipeline.

        Parameters
        ----------
        signal_features:
            All symbols' signal features.
        current_positions:
            Current portfolio positions (or None).
        previous_execution:
            Yesterday's execution summary (or None).

        Returns
        -------
        LLMTradingBlueprint
            The final reviewed blueprint.
        """
        started = perf_counter()

        # ── Resolve provider (lazy-create on first call) ──
        provider = self._provider
        if provider is None:
            provider = _create_agent_provider()
            self._provider = provider

        settings = get_settings()
        chunk_size = settings.analysis_service.llm.orchestrator_chunk_size
        max_parallel = settings.analysis_service.llm.orchestrator_max_parallel
        trade_benchmark_syms = set(
            s.upper() for s in settings.common.watchlist.for_trade_benchmark
        )
        trade_syms = set(
            s.upper() for s in settings.common.watchlist.for_trade
        )

        logger.info(
            "orchestrator.started",
            symbols=len(signal_features),
            provider=provider.name,
            chunk_size=chunk_size,
        )

        # ── Split: trade targets vs benchmark-only ──
        # A symbol in both for_trade AND for_trade_benchmark is a trade target.
        benchmark_only_features = [
            sf for sf in signal_features
            if sf.symbol.upper() in trade_benchmark_syms
            and sf.symbol.upper() not in trade_syms
        ]
        trade_features = [
            sf for sf in signal_features
            if sf.symbol.upper() in trade_syms
        ]

        # ── Circuit-break: drop symbols with both stock + option fully degraded ──
        circuit_broken = [
            sf for sf in trade_features
            if should_circuit_break_analysis(sf.data_quality.degraded_indicators)
        ]
        if circuit_broken:
            broken_syms = [sf.symbol for sf in circuit_broken]
            logger.warning(
                "orchestrator.circuit_break",
                symbols=broken_syms,
                reason="both stock:all and option:all degraded — skipping LLM analysis",
            )
            trade_features = [
                sf for sf in trade_features
                if not should_circuit_break_analysis(sf.data_quality.degraded_indicators)
            ]
            # Also remove from the full signal_features list for single-pass path
            signal_features = [
                sf for sf in signal_features
                if not should_circuit_break_analysis(sf.data_quality.degraded_indicators)
            ]

        if not trade_features:
            logger.warning("orchestrator.no_trade_features", reason="all trade symbols circuit-broken or empty")
            # Return an empty blueprint
            from shared.models.blueprint import LLMTradingBlueprint
            return LLMTradingBlueprint(symbol_plans=[])

        # ── Decide: single pass vs chunked ──
        usage_tracker = LLMUsageTracker()

        if len(trade_features) <= chunk_size:
            # Small enough — run everything in one pass (no chunking overhead)
            blueprint = await self._generate_single_pass(
                signal_features=signal_features,
                current_positions=current_positions,
                previous_execution=previous_execution,
                provider=provider,
                signal_date=signal_date,
                usage_tracker=usage_tracker,
            )
        else:
            # Chunk trade symbols, inject benchmark-only into each chunk
            chunks: list[list[SignalFeatures]] = []
            for i in range(0, len(trade_features), chunk_size):
                chunk = benchmark_only_features + trade_features[i : i + chunk_size]
                chunks.append(chunk)

            logger.info(
                "orchestrator.chunking",
                total_symbols=len(signal_features),
                benchmark_only_symbols=len(benchmark_only_features),
                benchmark_symbols=[sf.symbol for sf in benchmark_only_features],
                trade_symbols=len(trade_features),
                chunks=len(chunks),
                chunk_size=chunk_size,
                max_parallel=max_parallel,
            )

            # Run chunks with concurrency limit
            sem = asyncio.Semaphore(max_parallel)
            chunk_trackers: list[LLMUsageTracker] = []

            async def _run_chunk(idx: int, chunk_features: list[SignalFeatures]):
                chunk_tracker = LLMUsageTracker()
                chunk_trackers.append(chunk_tracker)
                trade_syms_in_chunk = [
                    sf.symbol for sf in chunk_features
                    if sf.symbol.upper() not in trade_benchmark_syms
                    or sf.symbol.upper() in trade_syms
                ]
                benchmark_syms_in_chunk = [
                    sf.symbol for sf in chunk_features
                    if sf.symbol.upper() in trade_benchmark_syms
                    and sf.symbol.upper() not in trade_syms
                ]
                async with sem:
                    logger.info(
                        "orchestrator.chunk_started",
                        chunk=idx,
                        symbols=len(chunk_features),
                        trade_symbols=trade_syms_in_chunk,
                        benchmark_symbols=benchmark_syms_in_chunk,
                    )
                    return await self._generate_single_pass(
                        signal_features=chunk_features,
                        current_positions=current_positions,
                        previous_execution=previous_execution,
                        provider=provider,
                        signal_date=signal_date,
                        is_chunk=True,
                        usage_tracker=chunk_tracker,
                    )

            chunk_blueprints = await asyncio.gather(
                *[_run_chunk(i, c) for i, c in enumerate(chunks)]
            )

            # Merge: concatenate symbol_plans, combine reasoning_context
            blueprint = chunk_blueprints[0]
            for other in chunk_blueprints[1:]:
                blueprint.symbol_plans.extend(other.symbol_plans)

            # De-duplicate benchmark plans — keep only from first chunk
            seen_symbols: set[str] = set()
            deduped_plans = []
            for plan in blueprint.symbol_plans:
                sym = plan.underlying.upper()
                if sym in seen_symbols:
                    continue
                seen_symbols.add(sym)
                deduped_plans.append(plan)
            blueprint.symbol_plans = deduped_plans

            # Merge reasoning contexts
            all_contexts = [bp.reasoning_context for bp in chunk_blueprints if bp.reasoning_context]
            blueprint.reasoning_context = {
                "pipeline": "agentic_chunked",
                "provider": provider.name,
                "chunks": len(chunks),
                "chunk_contexts": all_contexts,
            }

            logger.info(
                "orchestrator.chunks_merged",
                total_plans=len(blueprint.symbol_plans),
                chunks=len(chunks),
            )

            # Merge chunk trackers into the main usage tracker
            for ct in chunk_trackers:
                usage_tracker.merge(ct)

        elapsed_ms = round((perf_counter() - started) * 1000, 2)

        # ── Log LLM usage summary ──
        usage_summary = usage_tracker.summary()
        logger.info(
            "orchestrator.llm_usage_summary",
            elapsed_ms=elapsed_ms,
            **usage_summary["total"],
            agents=usage_summary["agents"],
        )

        logger.info(
            "orchestrator.completed",
            plans=len(blueprint.symbol_plans),
            elapsed_ms=elapsed_ms,
        )
        return blueprint

    async def _generate_single_pass(
        self,
        signal_features: list[SignalFeatures],
        current_positions: dict | None,
        previous_execution: dict | None,
        provider: AgentLLMProvider,
        *,
        signal_date: date | None = None,
        is_chunk: bool = False,
        usage_tracker: LLMUsageTracker | None = None,
    ) -> LLMTradingBlueprint:
        """Run the full specialist → synthesizer → critic pipeline on one set of signals."""
        # ── Step 0: Serialize signals once ──
        serialized = self._serialize_signals(signal_features)
        signals_summary = [
            {
                "symbol": sf.symbol,
                "close_price": sf.close_price,
                "volume": sf.volume,
                "volatility_regime": sf.volatility_regime,
            }
            for sf in signal_features
        ]

        # ── Step 1: Run 6 specialist agents in parallel ──
        agent_outputs = await self._run_specialists(serialized, provider=provider, usage_tracker=usage_tracker)

        logger.info(
            "orchestrator.specialists_done",
            agents_succeeded=len(agent_outputs),
            is_chunk=is_chunk,
        )

        # ── Step 2: Synthesize ──
        blueprint = await self._synthesizer.synthesize(
            agent_outputs=agent_outputs,
            signals_summary=signals_summary,
            current_positions=current_positions,
            previous_execution=previous_execution,
            provider=provider,
            signal_date=signal_date,
            usage_tracker=usage_tracker,
        )

        logger.info(
            "orchestrator.synthesis_done",
            plans=len(blueprint.symbol_plans),
            is_chunk=is_chunk,
        )

        # ── Step 3: Critic review loop ──
        max_revisions = get_settings().analysis_service.llm.max_critic_revisions
        critic_history: list[dict] = []
        for revision in range(max_revisions):
            verdict = await self._critic.review(
                blueprint_json=blueprint.model_dump(mode="json"),
                agent_outputs=agent_outputs,
                signals_summary=signals_summary,
                provider=provider,
                usage_tracker=usage_tracker,
            )

            critic_history.append({
                "revision": revision,
                "verdict": verdict.verdict,
                "summary": verdict.summary,
                "issues": [i.model_dump() for i in verdict.issues],
            })

            logger.info(
                "orchestrator.critic_verdict",
                revision=revision,
                verdict=verdict.verdict,
                issues=len(verdict.issues),
            )

            if verdict.verdict == "pass":
                break

            # Revision needed — feed critic feedback back to synthesizer
            logger.info(
                "orchestrator.revision_requested",
                revision=revision + 1,
                error_count=sum(1 for i in verdict.issues if i.severity == "error"),
            )

            blueprint = await self._synthesizer.synthesize(
                agent_outputs=agent_outputs,
                signals_summary=signals_summary,
                current_positions=current_positions,
                previous_execution=previous_execution,
                critic_feedback=verdict.summary + "\n\nIssues:\n" + json.dumps(
                    [i.model_dump() for i in verdict.issues],
                    indent=2,
                    ensure_ascii=False,
                ),
                provider=provider,
                signal_date=signal_date,
                usage_tracker=usage_tracker,
            )

        # ── Attach reasoning context for auditability ──
        blueprint.reasoning_context = {
            "pipeline": "agentic" if not is_chunk else "agentic_chunk",
            "provider": provider.name,
            "signals_summary": signals_summary,
            "agent_outputs": agent_outputs,
            "critic_history": critic_history,
        }

        return blueprint

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _serialize_signals(self, features: list[SignalFeatures]) -> list[dict[str, Any]]:
        """Serialize all signal features into dicts for agent consumption.

        Uses the prompt serializer but parses back to dict so agents
        can extract their relevant subsets.
        """
        results = []
        for sf in features:
            # Re-use the compact serialization from prompts module
            text_block = _serialize_one_signal(sf)
            # Extract JSON from "### SYMBOL\n{...}" format
            lines = text_block.split("\n", 1)
            if len(lines) == 2:
                try:
                    data = json.loads(lines[1])
                    data["symbol"] = sf.symbol
                    results.append(data)
                except json.JSONDecodeError:
                    # Fallback: minimal data
                    results.append({
                        "symbol": sf.symbol,
                        "price": {"close_price": sf.close_price},
                    })
            else:
                results.append({
                    "symbol": sf.symbol,
                    "price": {"close_price": sf.close_price},
                })
        return results

    async def _run_specialists(
        self,
        serialized_signals: list[dict[str, Any]],
        *,
        provider: AgentLLMProvider,
        usage_tracker: LLMUsageTracker | None = None,
    ) -> dict[str, Any]:
        """Run all 6 specialist agents in parallel, collecting results.

        Failed agents are logged but do NOT fail the pipeline — the
        synthesizer will work with whatever analyses are available.
        """
        agents = {
            "trend": self._trend,
            "volatility": self._volatility,
            "flow": self._flow,
            "chain": self._chain,
            "spread": self._spread,
            "cross_asset": self._cross_asset,
        }

        async def _run_one(name: str, agent):
            try:
                result = await agent.analyze(serialized_signals, provider=provider, usage_tracker=usage_tracker)
                return name, result.model_dump(mode="json")
            except Exception as e:
                logger.warning(
                    f"orchestrator.agent_failed",
                    agent=name,
                    error=str(e),
                )
                return name, None

        tasks = [_run_one(name, agent) for name, agent in agents.items()]
        results = await asyncio.gather(*tasks)

        outputs = {}
        for name, result in results:
            if result is not None:
                outputs[name] = result

        return outputs
