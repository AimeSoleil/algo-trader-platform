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
import copy
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

        # ── Split: trade targets vs benchmark ──
        # Benchmark features are always injected into every chunk for
        # cross-asset context, but only trade symbols get plans.
        benchmark_features = [
            sf for sf in signal_features
            if sf.symbol.upper() in trade_benchmark_syms
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
            # Also remove from the full signal_features list for single-pass path,
            # but always keep benchmark symbols so cross-asset context is preserved.
            signal_features = [
                sf for sf in signal_features
                if sf.symbol.upper() in trade_benchmark_syms
                or not should_circuit_break_analysis(sf.data_quality.degraded_indicators)
            ]

        if not trade_features:
            logger.warning("orchestrator.no_trade_features", reason="all trade symbols circuit-broken or empty")
            # Return an empty blueprint
            from shared.models.blueprint import LLMTradingBlueprint
            return LLMTradingBlueprint(symbol_plans=[])

        # ── Decide: single pass vs chunked ──
        usage_tracker = LLMUsageTracker()

        # Compute trade symbol names for the synthesizer prompt
        trade_symbol_names = [sf.symbol for sf in trade_features]

        if len(trade_features) <= chunk_size:
            # Small enough — run everything in one pass (no chunking overhead)
            blueprint = await self._generate_single_pass(
                signal_features=signal_features,
                current_positions=current_positions,
                previous_execution=previous_execution,
                provider=provider,
                signal_date=signal_date,
                usage_tracker=usage_tracker,
                trade_symbols=trade_symbol_names,
            )
        else:
            # Chunk trade symbols; inject ALL benchmark symbols into each chunk
            # for cross-asset context.  Dedup in case a symbol is both trade
            # and benchmark (e.g. SPY, QQQ, IWM).
            benchmark_sym_set = {sf.symbol.upper() for sf in benchmark_features}
            chunks: list[list[SignalFeatures]] = []
            for i in range(0, len(trade_features), chunk_size):
                trade_slice = trade_features[i : i + chunk_size]
                # Benchmark first, then trade symbols not already in benchmark
                seen: set[str] = set()
                deduped_chunk: list[SignalFeatures] = []
                for sf in benchmark_features + trade_slice:
                    key = sf.symbol.upper()
                    if key not in seen:
                        seen.add(key)
                        deduped_chunk.append(sf)
                chunks.append(deduped_chunk)

            logger.info(
                "orchestrator.chunking",
                total_symbols=len(signal_features),
                benchmark_symbols=[sf.symbol for sf in benchmark_features],
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
                    if sf.symbol.upper() in trade_syms
                ]
                benchmark_syms_in_chunk = [
                    sf.symbol for sf in chunk_features
                    if sf.symbol.upper() in trade_benchmark_syms
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
                        trade_symbols=trade_syms_in_chunk,
                    )

            chunk_blueprints = await asyncio.gather(
                *[_run_chunk(i, c) for i, c in enumerate(chunks)]
            )

            # Merge: concatenate symbol_plans, combine reasoning_context
            blueprint = chunk_blueprints[0]
            for other in chunk_blueprints[1:]:
                blueprint.symbol_plans.extend(other.symbol_plans)

            # Keep only plans for trade symbols (drop benchmark-only plans)
            # and de-duplicate (a symbol in multiple chunks keeps the first).
            seen_symbols: set[str] = set()
            deduped_plans = []
            for plan in blueprint.symbol_plans:
                sym = plan.underlying.upper()
                if sym in seen_symbols:
                    continue
                if sym not in trade_syms:
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

        # Safety net: drop any benchmark-only plans the LLM may still emit
        blueprint.symbol_plans = [
            p for p in blueprint.symbol_plans
            if p.underlying.upper() in trade_syms
        ]

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
        trade_symbols: list[str] | None = None,
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

        # Compact copy for synthesizer/critic prompts (strip reasoning, trim benchmarks)
        trade_sym_set = set(s.upper() for s in trade_symbols) if trade_symbols else set()
        compact_outputs = self._compact_for_synthesis(agent_outputs, trade_sym_set)

        logger.info(
            "orchestrator.specialists_done",
            agents_succeeded=len(agent_outputs),
            is_chunk=is_chunk,
        )

        # ── Step 2: Synthesize ──
        agent_models_cfg = get_settings().analysis_service.llm.agent_models_override
        synth_model = agent_models_cfg.synthesizer or None
        critic_model = agent_models_cfg.critic or None

        blueprint = await self._synthesizer.synthesize(
            agent_outputs=compact_outputs,
            signals_summary=signals_summary,
            current_positions=current_positions,
            previous_execution=previous_execution,
            provider=provider,
            signal_date=signal_date,
            usage_tracker=usage_tracker,
            trade_symbols=trade_symbols,
            model=synth_model,
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
                agent_outputs=compact_outputs,
                signals_summary=signals_summary,
                provider=provider,
                usage_tracker=usage_tracker,
                model=critic_model,
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
                agent_outputs=compact_outputs,
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
                trade_symbols=trade_symbols,
                model=synth_model,
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

    # Keys preserved for benchmark-only symbols (compact cross-asset context)
    _BENCHMARK_KEEP_KEYS = frozenset({
        "symbol", "confidence",
        # trend
        "regime", "trend_direction", "trend_strength",
        # volatility
        "vol_regime", "iv_rank_zone", "hv_iv_assessment",
        # flow
        "flow_signal", "volume_anomaly", "vwap_bias",
        # chain
        "pcr_signal", "gamma_pin_active", "institutional_flow",
        # spread — not useful for benchmark context
        # cross_asset
        "correlation_regime", "risk_off_signal", "vix_environment",
        "safe_haven_correlated", "credit_stress_exposure",
        "energy_exposure", "crypto_correlated",
    })

    def _compact_for_synthesis(
        self,
        agent_outputs: dict[str, Any],
        trade_syms: set[str],
    ) -> dict[str, Any]:
        """Return a token-efficient copy of *agent_outputs* for the synthesizer/critic.

        Optimizations applied (original dict is NOT mutated):
        1. ``reasoning`` text is stripped from every symbol analysis — the
           synthesizer forms its own reasoning from structured fields.
        2. ``strategies`` lists are stripped — the synthesizer generates
           its own strategy selection from structured regime/signal fields.
        3. Benchmark-only symbols are trimmed to a small set of key
           signals (direction, regime, confidence) — full detail is not
           needed to generate trade plans.
        4. Top-level ``market_*_summary`` fields are stripped — the
           synthesizer derives its own market assessment.
        """
        compact = {}
        for agent_name, output in agent_outputs.items():
            if not isinstance(output, dict):
                compact[agent_name] = output
                continue

            out = {}
            for key, value in output.items():
                # Strip top-level summary fields (market_trend_summary, market_vol_summary, etc.)
                if key.startswith("market_") and key.endswith("_summary"):
                    continue
                if key == "symbols" and isinstance(value, list):
                    trimmed_symbols = []
                    for sym_data in value:
                        if not isinstance(sym_data, dict):
                            trimmed_symbols.append(sym_data)
                            continue

                        sym = sym_data.get("symbol", "").upper()
                        # Strip reasoning + strategies (high token cost, low synthesis value)
                        entry = {
                            k: v for k, v in sym_data.items()
                            if k not in ("reasoning", "strategies")
                        }

                        if sym not in trade_syms:
                            # Benchmark-only: keep only essential signals
                            entry = {
                                k: v for k, v in entry.items()
                                if k in self._BENCHMARK_KEEP_KEYS
                            }

                        trimmed_symbols.append(entry)
                    out["symbols"] = trimmed_symbols
                else:
                    out[key] = value
            compact[agent_name] = out
        return compact

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

        # Resolve per-agent model overrides from config
        agent_models_cfg = get_settings().analysis_service.llm.agent_models_override

        async def _run_one(name: str, agent):
            try:
                model_override = getattr(agent_models_cfg, name, "") or None
                result = await agent.analyze(
                    serialized_signals,
                    provider=provider,
                    usage_tracker=usage_tracker,
                    model=model_override,
                )
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
