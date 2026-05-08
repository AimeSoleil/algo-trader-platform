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
import statistics
import uuid
from datetime import date
from time import perf_counter
from typing import Any

from shared.config import get_settings
from shared.data_quality import DataQualityConfig, OPTION_MIN_ROWS, should_circuit_break_analysis
from shared.metrics import llm_request_duration
from shared.models.blueprint import LLMTradingBlueprint
from shared.redis_pool import get_redis
from shared.models.signal import SignalFeatures
from shared.utils import get_logger

from services.analysis_service.app.llm.agents.base_agent import AgentLLMProvider, LLMUsageTracker
from services.analysis_service.app.llm.agents.chain_agent import ChainAgent
from services.analysis_service.app.llm.agents.cross_asset_agent import CrossAssetAgent
from services.analysis_service.app.llm.agents.critic_agent import CriticAgent
from services.analysis_service.app.llm.agents.flow_agent import FlowAgent
from services.analysis_service.app.llm.agents.portfolio_selector import PlanCandidate, PortfolioSelector
from services.analysis_service.app.llm.agents.post_merge_portfolio_agent import PostMergePortfolioAgent
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
        ``"openai"``, ``"copilot"``, ``"qiniu"`` or ``"closeai"``.
        Defaults to ``settings.analysis_service.llm.provider``.
    """
    if provider_name is None:
        provider_name = get_settings().analysis_service.llm.provider

    if provider_name == "copilot":
        from services.analysis_service.app.llm.agents._copilot_agent_provider import (
            CopilotAgentProvider,
        )
        return CopilotAgentProvider()

    if provider_name == "qiniu":
        from services.analysis_service.app.llm.agents._qiniu_agent_provider import (
            QiniuAgentProvider,
        )
        return QiniuAgentProvider()

    if provider_name == "closeai":
        from services.analysis_service.app.llm.agents._closeai_agent_provider import (
            CloseAIAgentProvider,
        )
        return CloseAIAgentProvider()

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
        self._portfolio_selector = PortfolioSelector()
        self._post_merge_portfolio_agent = PostMergePortfolioAgent()

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

        # ── Validate signal features (D6) ──
        signal_features = self._validate_signal_features(signal_features)
        source_signal_features = list(signal_features)

        # ── Resolve provider (lazy-create on first call) ──
        provider = self._provider
        if provider is None:
            provider = _create_agent_provider()
            self._provider = provider

        settings = get_settings()
        chunk_size = settings.analysis_service.llm.orchestrator_chunk_size
        max_parallel = settings.analysis_service.llm.orchestrator_max_parallel
        precision_first_cfg = getattr(settings.analysis_service.llm, "precision_first", None)
        precision_first_enabled = bool(getattr(precision_first_cfg, "enabled", False))
        allowed_strategy_types = list(getattr(precision_first_cfg, "allowed_strategy_types", []) or [])
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
        # Benchmark features no longer enter every chunk as full signal rows.
        # They are compressed once into a market snapshot and passed through
        # agent context instead, while only trade symbols remain in chunk data.
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

        trade_features, pre_synthesis_filter = self._apply_pre_synthesis_candidate_filter(
            trade_features,
            settings=settings,
            precision_first_enabled=precision_first_enabled,
            allowed_strategy_types=allowed_strategy_types,
        )
        if pre_synthesis_filter["dropped_symbol_count"]:
            logger.info(
                "orchestrator.pre_synthesis_filter",
                kept_symbols=pre_synthesis_filter["kept_symbol_count"],
                dropped_symbols=pre_synthesis_filter["dropped_symbols"],
            )

        trade_features, pre_synthesis_triage = self._apply_pre_synthesis_coarse_ranking(
            trade_features,
            settings=settings,
            precision_first_enabled=precision_first_enabled,
            allowed_strategy_types=allowed_strategy_types,
        )
        if pre_synthesis_triage["monitor_symbol_count"]:
            logger.info(
                "orchestrator.pre_synthesis_triage",
                escalate_symbols=pre_synthesis_triage["escalate_symbols"],
                monitor_symbols=pre_synthesis_triage["monitor_symbols"],
                target_shortlist_size=pre_synthesis_triage["target_shortlist_size"],
            )

        active_trade_symbols = {sf.symbol.upper() for sf in trade_features}
        signal_features = [
            sf for sf in signal_features
            if sf.symbol.upper() in trade_benchmark_syms
            or sf.symbol.upper() in active_trade_symbols
        ]

        if not trade_features:
            logger.warning(
                "orchestrator.no_trade_features",
                reason="all trade symbols filtered before LLM analysis",
            )
            return self._build_empty_blueprint(
                signal_features=source_signal_features,
                signal_date=signal_date,
                provider_name=provider.name,
                model_version=self._configured_model_version(settings, provider.name),
                reasoning_context={
                    "pipeline": "agentic_empty",
                    "provider": provider.name,
                    "pre_synthesis_filter": pre_synthesis_filter,
                    "pre_synthesis_triage": pre_synthesis_triage,
                },
            )

        # ── Decide: single pass vs chunked ──
        usage_tracker = LLMUsageTracker()

        # Compute trade symbol names for the synthesizer prompt
        trade_symbol_names = [sf.symbol for sf in trade_features]
        market_snapshot = self._build_market_snapshot(benchmark_features)

        if len(trade_features) <= chunk_size:
            # Small enough — run everything in one pass (no chunking overhead)
            blueprint = await self._generate_single_pass(
                signal_features=trade_features,
                current_positions=current_positions,
                previous_execution=previous_execution,
                provider=provider,
                signal_date=signal_date,
                analysis_chunk_id=f"single-{uuid.uuid4().hex[:8]}",
                usage_tracker=usage_tracker,
                trade_symbols=trade_symbol_names,
                market_snapshot=market_snapshot,
            )
        else:
            # Chunk trade symbols only; benchmark context is shared as one
            # market snapshot instead of being re-serialized into every chunk.
            chunks: list[list[SignalFeatures]] = []
            for i in range(0, len(trade_features), chunk_size):
                trade_slice = trade_features[i : i + chunk_size]
                chunks.append(trade_slice)

            logger.info(
                "orchestrator.chunking",
                total_symbols=len(signal_features),
                benchmark_symbols=(market_snapshot or {}).get("symbols", []),
                trade_symbols=len(trade_features),
                chunks=len(chunks),
                chunk_size=chunk_size,
                max_parallel=max_parallel,
            )

            # Run chunks with concurrency limit
            sem = asyncio.Semaphore(max_parallel)
            chunk_trackers: dict[int, LLMUsageTracker] = {}

            async def _run_chunk(idx: int, chunk_features: list[SignalFeatures]):
                chunk_tracker = LLMUsageTracker()
                chunk_trackers[idx] = chunk_tracker  # Safe: each coroutine writes unique key
                analysis_chunk_id = f"chunk-{idx:03d}-{uuid.uuid4().hex[:8]}"
                trade_syms_in_chunk = [
                    sf.symbol for sf in chunk_features
                    if sf.symbol.upper() in trade_syms
                ]
                async with sem:
                    logger.info(
                        "orchestrator.chunk_started",
                        analysis_chunk_id=analysis_chunk_id,
                        chunk=idx,
                        symbols=len(chunk_features),
                        trade_symbols=trade_syms_in_chunk,
                        benchmark_symbols=(market_snapshot or {}).get("symbols", []),
                    )
                    return await self._generate_single_pass(
                        signal_features=chunk_features,
                        current_positions=current_positions,
                        previous_execution=previous_execution,
                        provider=provider,
                        signal_date=signal_date,
                        is_chunk=True,
                        analysis_chunk_id=analysis_chunk_id,
                        usage_tracker=chunk_tracker,
                        trade_symbols=trade_syms_in_chunk,
                        market_snapshot=market_snapshot,
                    )

            chunk_blueprints = await asyncio.gather(
                *[_run_chunk(i, c) for i, c in enumerate(chunks)]
            )

            blueprint = chunk_blueprints[0]
            quality_by_symbol = {
                sf.symbol.upper(): sf.data_quality.score
                for sf in signal_features
            }
            chunk_limits = [
                {
                    "chunk_index": idx,
                    "chunk_id": (bp.reasoning_context or {}).get("analysis_chunk_id"),
                    "max_total_positions": bp.max_total_positions,
                    "max_daily_loss": bp.max_daily_loss,
                    "max_margin_usage": bp.max_margin_usage,
                    "portfolio_delta_limit": bp.portfolio_delta_limit,
                    "portfolio_gamma_limit": bp.portfolio_gamma_limit,
                }
                for idx, bp in enumerate(chunk_blueprints)
            ]
            merged_plan_candidates: list[PlanCandidate] = []
            original_order = 0
            for idx, bp in enumerate(chunk_blueprints):
                chunk_id = (bp.reasoning_context or {}).get("analysis_chunk_id")
                agent_outputs = (bp.reasoning_context or {}).get("agent_outputs")
                for plan in bp.symbol_plans:
                    merged_plan_candidates.append(PlanCandidate(
                        plan=plan,
                        chunk_index=idx,
                        original_order=original_order,
                        quality_score=quality_by_symbol.get(plan.underlying.upper(), plan.data_quality_score),
                        chunk_id=chunk_id,
                        agent_outputs=agent_outputs if isinstance(agent_outputs, dict) else None,
                    ))
                    original_order += 1

            llm_post_merge_review: dict[str, Any] | None = None
            llm_post_merge_metadata: dict[str, Any] = {"status": "skipped", "reason": "provider_missing_generate"}
            if hasattr(provider, "generate") and len(merged_plan_candidates) > 1:
                agent_models_cfg = settings.analysis_service.llm.agent_models_override
                post_merge_model = agent_models_cfg.post_merge or None
                candidate_summaries, selector_context = self._portfolio_selector.build_review_inputs(
                    candidates=merged_plan_candidates,
                    max_total_positions=min(bp.max_total_positions for bp in chunk_blueprints),
                    trade_symbols=trade_syms,
                    current_positions=current_positions,
                    precision_first_enabled=precision_first_enabled,
                    allowed_strategy_types=allowed_strategy_types,
                )
                try:
                    review = await self._post_merge_portfolio_agent.review(
                        candidate_summaries=candidate_summaries,
                        current_positions=current_positions,
                        previous_execution=previous_execution,
                        chunk_limit_proposals=chunk_limits,
                        selector_metadata=selector_context,
                        max_total_positions=min(bp.max_total_positions for bp in chunk_blueprints),
                        provider=provider,
                        usage_tracker=usage_tracker,
                        model=post_merge_model,
                    )
                    llm_post_merge_review = review.model_dump(mode="json")
                    llm_post_merge_metadata = {
                        "status": "applied",
                        "ranking": review.ranking,
                        "selected_symbols": review.selected_symbols,
                        "portfolio_summary": review.portfolio_summary,
                        "risk_notes": review.risk_notes,
                        "conflict_explanations": [item.model_dump() for item in review.conflict_explanations],
                    }
                except Exception as exc:
                    logger.warning(
                        "orchestrator.post_merge_review_failed",
                        error_type=type(exc).__name__,
                        error=str(exc),
                    )
                    llm_post_merge_metadata = {
                        "status": "failed",
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    }

            selected_plans, final_limits, selection_metadata = self._portfolio_selector.select(
                candidates=merged_plan_candidates,
                max_total_positions=min(bp.max_total_positions for bp in chunk_blueprints),
                trade_symbols=trade_syms,
                chunk_limits=chunk_limits,
                risk_policy=settings.trade_service.risk.blueprint_limits,
                current_positions=current_positions,
                previous_execution=previous_execution,
                llm_review=llm_post_merge_review,
                precision_first_enabled=precision_first_enabled,
                allowed_strategy_types=allowed_strategy_types,
            )
            selection_metadata["llm_review"] = {
                **selection_metadata.get("llm_review", {}),
                **llm_post_merge_metadata,
            }

            blueprint.symbol_plans = selected_plans
            blueprint.max_total_positions = final_limits["max_total_positions"]
            blueprint.max_daily_loss = final_limits["max_daily_loss"]
            blueprint.max_margin_usage = final_limits["max_margin_usage"]
            blueprint.portfolio_delta_limit = final_limits["portfolio_delta_limit"]
            blueprint.portfolio_gamma_limit = final_limits["portfolio_gamma_limit"]

            # Merge reasoning contexts
            all_contexts = [bp.reasoning_context for bp in chunk_blueprints if bp.reasoning_context]
            blueprint.reasoning_context = {
                "pipeline": "agentic_chunked",
                "provider": provider.name,
                "chunks": len(chunks),
                "chunk_contexts": all_contexts,
                "post_merge_phase": selection_metadata,
            }

            logger.info(
                "orchestrator.chunks_merged",
                total_plans=len(blueprint.symbol_plans),
                chunks=len(chunks),
                trimmed_plans=selection_metadata["deduped_plan_count"] - selection_metadata["output_plan_count"],
                max_total_positions=blueprint.max_total_positions,
                ranking_method=selection_metadata["ranking_method"],
            )

            # Merge chunk trackers into the main usage tracker
            for ct in chunk_trackers.values():
                usage_tracker.merge(ct)

        # Safety net: drop any benchmark-only plans the LLM may still emit
        blueprint.symbol_plans = [
            p for p in blueprint.symbol_plans
            if p.underlying.upper() in trade_syms
        ]
        blueprint.reasoning_context = {
            **(blueprint.reasoning_context or {}),
            "pre_synthesis_filter": pre_synthesis_filter,
            "pre_synthesis_triage": pre_synthesis_triage,
        }

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
        analysis_chunk_id: str,
        usage_tracker: LLMUsageTracker | None = None,
        trade_symbols: list[str] | None = None,
        market_snapshot: dict[str, Any] | None = None,
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
        logger.info(
            "orchestrator.phase_started",
            phase="specialists",
            analysis_chunk_id=analysis_chunk_id,
            agents=6,
            symbols=len(signal_features),
            market_snapshot_symbols=len((market_snapshot or {}).get("symbols", [])),
            is_chunk=is_chunk,
        )
        specialists_t0 = perf_counter()
        agent_outputs = await self._run_specialists(
            serialized,
            provider=provider,
            usage_tracker=usage_tracker,
            analysis_chunk_id=analysis_chunk_id,
            market_snapshot=market_snapshot,
        )
        logger.info(
            "orchestrator.phase_completed",
            phase="specialists",
            analysis_chunk_id=analysis_chunk_id,
            agents_succeeded=len(agent_outputs),
            elapsed_s=round(perf_counter() - specialists_t0, 1),
        )
        logger.debug("orchestrator.agent_outputs", outputs=agent_outputs)

        expected_agents = ("trend", "volatility", "flow", "chain", "spread", "cross_asset")
        availability = {}
        for agent_name in expected_agents:
            out = agent_outputs.get(agent_name)
            if not isinstance(out, dict):
                # agent raised an exception — see orchestrator.agent_failed warnings above
                availability[agent_name] = {"has_data": False, "symbols": 0, "failed": True}
                continue
            symbols_list = out.get("symbols", [])
            symbol_count = len(symbols_list) if isinstance(symbols_list, list) else 0
            availability[agent_name] = {
                "has_data": symbol_count > 0,
                "symbols": symbol_count,
                "failed": False,
            }
        agents_failed = sum(1 for v in availability.values() if v["failed"])
        logger.info(
            "orchestrator.specialist_data_availability",
            analysis_chunk_id=analysis_chunk_id,
            is_chunk=is_chunk,
            input_signals=len(serialized),
            agents_failed=agents_failed,
            availability=availability,
        )

        # Compact copy for synthesizer/critic prompts (strip reasoning, trim benchmarks)
        trade_sym_set = set(s.upper() for s in trade_symbols) if trade_symbols else set()
        compact_outputs = self._compact_for_synthesis(agent_outputs, trade_sym_set)

        # ── Step 1b: Compute consensus & market condition ──
        consensus = self._compute_consensus(agent_outputs, trade_sym_set)
        market_condition = self._classify_market_condition(agent_outputs)

        logger.info(
            "orchestrator.consensus_computed",
            analysis_chunk_id=analysis_chunk_id,
            symbols=len(consensus),
            market_condition=market_condition,
            consensus_snapshot={
                sym: {
                    "dir": c["consensus_direction"],
                    "agree": c["agreement_count"],
                    "event_risk_count": c.get("event_risk_agent_count", 0),
                    "event_risk_capped": c.get("event_risk_capped", False),
                    "eff_conf_raw": c.get("effective_confidence_raw", 0.0),
                    "eff_conf": c.get("effective_confidence", 0.0),
                }
                for sym, c in list(consensus.items())[:5]  # log first 5
            },
        )

        # Inject consensus into compact outputs for synthesizer context
        compact_outputs["_consensus"] = consensus
        compact_outputs["_market_condition"] = market_condition

        logger.info(
            "orchestrator.specialists_done",
            analysis_chunk_id=analysis_chunk_id,
            agents_succeeded=len(agent_outputs),
            is_chunk=is_chunk,
        )

        # ── Step 2: Synthesize ──
        agent_models_cfg = get_settings().analysis_service.llm.agent_models_override
        synth_model = agent_models_cfg.synthesizer or None
        critic_model = agent_models_cfg.critic or None

        logger.info(
            "orchestrator.phase_started",
            phase="synthesizer",
            is_chunk=is_chunk,
            has_critic_feedback=False,
        )
        synth_t0 = perf_counter()
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
            "orchestrator.phase_completed",
            phase="synthesizer",
            plans=len(blueprint.symbol_plans),
            is_chunk=is_chunk,
            elapsed_s=round(perf_counter() - synth_t0, 1),
        )

        # ── Step 3: Critic review loop ──
        max_revisions = get_settings().analysis_service.llm.max_critic_revisions
        critic_history: list[dict] = []
        for revision in range(max_revisions):
            logger.info(
                "orchestrator.phase_started",
                phase="critic",
                revision=revision,
                max_revisions=max_revisions,
            )
            critic_t0 = perf_counter()
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
                "orchestrator.phase_completed",
                phase="critic",
                revision=revision,
                verdict=verdict.verdict,
                issues=len(verdict.issues),
                elapsed_s=round(perf_counter() - critic_t0, 1),
            )

            if verdict.verdict == "pass":
                break

            # Revision needed — feed critic feedback back to synthesizer
            logger.info(
                "orchestrator.phase_started",
                phase="synthesizer_revision",
                revision=revision + 1,
                error_count=sum(1 for i in verdict.issues if i.severity == "error"),
            )
            rev_t0 = perf_counter()
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
            logger.info(
                "orchestrator.phase_completed",
                phase="synthesizer_revision",
                revision=revision + 1,
                plans=len(blueprint.symbol_plans),
                elapsed_s=round(perf_counter() - rev_t0, 1),
            )

        # ── Attach reasoning context for auditability ──
        blueprint.reasoning_context = {
            "pipeline": "agentic" if not is_chunk else "agentic_chunk",
            "provider": provider.name,
            "analysis_chunk_id": analysis_chunk_id,
            "signals_summary": signals_summary,
            "market_snapshot": market_snapshot,
            "agent_outputs": agent_outputs,
            "critic_history": critic_history,
        }

        return blueprint

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _validate_signal_features(self, signal_features: list) -> list:
        """Validate and filter signal features, removing invalid entries."""
        valid = []
        for sf in signal_features:
            if isinstance(sf, dict):
                symbol = sf.get("symbol", "")
                price = sf.get("close_price", 0)
            else:
                symbol = getattr(sf, "symbol", "")
                price = getattr(sf, "close_price", 0)

            if not symbol:
                logger.warning("orchestrator.signal_filtered", reason="missing symbol")
                continue
            if price is None or price <= 0:
                logger.warning("orchestrator.signal_filtered", symbol=symbol, reason=f"invalid close_price={price}")
                continue
            valid.append(sf)

        if len(valid) < 1:
            raise ValueError(f"Insufficient valid signal features: {len(valid)} (minimum 1 required)")

        if len(valid) < len(signal_features):
            logger.warning(
                "orchestrator.signals_filtered",
                original=len(signal_features),
                valid=len(valid),
                dropped=len(signal_features) - len(valid),
            )

        return valid

    def _apply_pre_synthesis_candidate_filter(
        self,
        trade_features: list[SignalFeatures],
        *,
        settings: Any,
        precision_first_enabled: bool,
        allowed_strategy_types: list[str],
    ) -> tuple[list[SignalFeatures], dict[str, Any]]:
        """Drop symbols that already fail hard deterministic gates before LLM fan-out."""
        dq_cfg = DataQualityConfig.from_settings(settings)
        kept: list[SignalFeatures] = []
        dropped: list[dict[str, Any]] = []

        for sf in trade_features:
            reasons, eligible_strategy_types = self._pre_synthesis_filter_reasons(
                sf,
                dq_cfg=dq_cfg,
                precision_first_enabled=precision_first_enabled,
                allowed_strategy_types=allowed_strategy_types,
            )
            if reasons:
                dropped.append({
                    "symbol": sf.symbol,
                    "reasons": reasons,
                    "eligible_strategy_types": eligible_strategy_types,
                })
                continue
            kept.append(sf)

        return kept, {
            "precision_first_enabled": precision_first_enabled,
            "allowed_strategy_types": allowed_strategy_types,
            "input_symbol_count": len(trade_features),
            "kept_symbol_count": len(kept),
            "dropped_symbol_count": len(dropped),
            "dropped_symbols": dropped,
        }

    def _apply_pre_synthesis_coarse_ranking(
        self,
        trade_features: list[SignalFeatures],
        *,
        settings: Any,
        precision_first_enabled: bool,
        allowed_strategy_types: list[str],
    ) -> tuple[list[SignalFeatures], dict[str, Any]]:
        """Rank surviving trade symbols and split them into monitor vs escalate."""
        target_shortlist_size, coarse_weights = self._pre_synthesis_coarse_ranking_config(settings)
        if not trade_features:
            return [], {
                "target_shortlist_size": target_shortlist_size,
                "shortlist_limit": 0,
                "input_symbol_count": 0,
                "escalate_symbol_count": 0,
                "monitor_symbol_count": 0,
                "escalate_symbols": [],
                "monitor_symbols": [],
                "ranked_symbols": [],
                "weights": coarse_weights,
            }

        dq_cfg = DataQualityConfig.from_settings(settings)
        ranked_entries: list[dict[str, Any]] = []
        for sf in trade_features:
            ranked_entries.append({
                "feature": sf,
                **self._coarse_rank_signal(
                    sf,
                    dq_cfg=dq_cfg,
                    precision_first_enabled=precision_first_enabled,
                    allowed_strategy_types=allowed_strategy_types,
                    weights=coarse_weights,
                ),
            })

        ranked_entries.sort(
            key=lambda entry: (
                -entry["coarse_score"],
                -entry["components"]["data_quality_score"],
                -entry["components"]["eligible_strategy_count"],
                -entry["components"]["option_coverage_score"],
                -entry["components"]["liquidity_score"],
                entry["symbol"],
            )
        )

        shortlist_limit = min(len(ranked_entries), target_shortlist_size)
        escalate_entries = ranked_entries[:shortlist_limit]
        monitor_entries = ranked_entries[shortlist_limit:]
        ranked_symbols: list[dict[str, Any]] = []

        for rank, entry in enumerate(ranked_entries, start=1):
            ranked_symbols.append({
                "rank": rank,
                "symbol": entry["symbol"],
                "action": "escalate" if rank <= shortlist_limit else "monitor",
                "coarse_score": entry["coarse_score"],
                "eligible_strategy_types": entry["eligible_strategy_types"],
                "components": entry["components"],
                "decision_reason": self._coarse_ranking_decision_reason(
                    entry,
                    rank=rank,
                    shortlist_limit=shortlist_limit,
                ),
            })

        return [entry["feature"] for entry in escalate_entries], {
            "target_shortlist_size": target_shortlist_size,
            "shortlist_limit": shortlist_limit,
            "input_symbol_count": len(trade_features),
            "escalate_symbol_count": len(escalate_entries),
            "monitor_symbol_count": len(monitor_entries),
            "escalate_symbols": [entry["symbol"] for entry in escalate_entries],
            "monitor_symbols": [entry["symbol"] for entry in monitor_entries],
            "ranked_symbols": ranked_symbols,
            "weights": coarse_weights,
        }

    def _coarse_rank_signal(
        self,
        signal_feature: SignalFeatures,
        *,
        dq_cfg: DataQualityConfig,
        precision_first_enabled: bool,
        allowed_strategy_types: list[str],
        weights: dict[str, float],
    ) -> dict[str, Any]:
        """Build a deterministic coarse ranking score for one symbol."""
        eligible_strategy_types = self._eligible_precision_first_strategy_types(
            signal_feature,
            allowed_strategy_types,
        )
        liquidity_hard_threshold = self._pre_synthesis_bid_ask_hard_threshold(
            allowed_strategy_types if precision_first_enabled else []
        )
        option_row_score = min(
            signal_feature.data_quality.option_row_count / max(1, dq_cfg.option_full_rows),
            1.0,
        )
        liquidity_score = max(
            0.0,
            1.0 - (signal_feature.option_indicators.bid_ask_spread_ratio / liquidity_hard_threshold),
        )
        strategy_score = 1.0
        if precision_first_enabled and allowed_strategy_types:
            strategy_score = len(eligible_strategy_types) / max(1, len(allowed_strategy_types))
        earnings_score = self._coarse_ranking_earnings_score(
            signal_feature.cross_asset_indicators.earnings_proximity_days,
        )
        weight_sum = sum(max(0.0, value) for value in weights.values()) or 1.0
        coarse_score = round(
            (
                weights["data_quality"] * signal_feature.data_quality.score
                + weights["option_coverage"] * option_row_score
                + weights["liquidity"] * liquidity_score
                + weights["strategy_eligibility"] * strategy_score
                + weights["earnings_buffer"] * earnings_score
            ) / weight_sum,
            6,
        )

        return {
            "symbol": signal_feature.symbol.upper(),
            "coarse_score": coarse_score,
            "eligible_strategy_types": eligible_strategy_types,
            "components": {
                "data_quality_score": round(signal_feature.data_quality.score, 6),
                "option_coverage_score": round(option_row_score, 6),
                "liquidity_score": round(liquidity_score, 6),
                "strategy_eligibility_score": round(strategy_score, 6),
                "earnings_score": round(earnings_score, 6),
                "eligible_strategy_count": len(eligible_strategy_types),
                "option_row_count": signal_feature.data_quality.option_row_count,
                "bid_ask_spread_ratio": round(signal_feature.option_indicators.bid_ask_spread_ratio, 6),
                "earnings_proximity_days": signal_feature.cross_asset_indicators.earnings_proximity_days,
            },
        }

    def _pre_synthesis_filter_reasons(
        self,
        signal_feature: SignalFeatures,
        *,
        dq_cfg: DataQualityConfig,
        precision_first_enabled: bool,
        allowed_strategy_types: list[str],
    ) -> tuple[list[dict[str, str]], list[str]]:
        """Return hard pre-synthesis drop reasons for one symbol."""
        reasons: list[dict[str, str]] = []
        data_quality = signal_feature.data_quality
        option_indicators = signal_feature.option_indicators
        earnings_days = signal_feature.cross_asset_indicators.earnings_proximity_days

        if data_quality.score < dq_cfg.skip_threshold:
            reasons.append({
                "rule": "data_quality_skip_threshold",
                "description": (
                    f"data_quality.score={data_quality.score:.4f} < {dq_cfg.skip_threshold:.2f}"
                ),
            })

        if data_quality.option_row_count < OPTION_MIN_ROWS:
            reasons.append({
                "rule": "insufficient_option_rows",
                "description": (
                    f"option_row_count={data_quality.option_row_count} < {OPTION_MIN_ROWS}"
                ),
            })

        bid_ask_hard_threshold = self._pre_synthesis_bid_ask_hard_threshold(
            allowed_strategy_types if precision_first_enabled else []
        )
        if option_indicators.bid_ask_spread_ratio > bid_ask_hard_threshold:
            reasons.append({
                "rule": "bid_ask_hard_block",
                "description": (
                    "bid_ask_spread_ratio="
                    f"{option_indicators.bid_ask_spread_ratio:.4f} > {bid_ask_hard_threshold:.2f}"
                ),
            })

        eligible_strategy_types = self._eligible_precision_first_strategy_types(
            signal_feature,
            allowed_strategy_types,
        )
        if precision_first_enabled and allowed_strategy_types and not eligible_strategy_types:
            earnings_desc = "unknown" if earnings_days is None else str(earnings_days)
            reasons.append({
                "rule": "precision_first_no_eligible_strategy",
                "description": (
                    "no hard-eligible precision-first strategy remains for "
                    f"earnings_proximity_days={earnings_desc} and "
                    f"term_structure_slope={signal_feature.option_indicators.term_structure_slope:.4f}"
                ),
            })

        return reasons, eligible_strategy_types

    def _eligible_precision_first_strategy_types(
        self,
        signal_feature: SignalFeatures,
        allowed_strategy_types: list[str],
    ) -> list[str]:
        """Keep only precision-first strategies that pass symbol-level hard context gates."""
        earnings_days = signal_feature.cross_asset_indicators.earnings_proximity_days
        slope = signal_feature.option_indicators.term_structure_slope
        eligible: list[str] = []

        for strategy_type in allowed_strategy_types:
            normalized = str(strategy_type).lower()

            if normalized == "calendar_spread":
                if slope <= 0.0:
                    continue
                if isinstance(earnings_days, int) and earnings_days <= 5:
                    continue
            elif normalized in {"single_leg", "vertical_spread", "iron_condor"}:
                if isinstance(earnings_days, int) and earnings_days <= 1:
                    continue
            elif normalized in {"butterfly", "iron_butterfly"}:
                if isinstance(earnings_days, int) and earnings_days <= 3:
                    continue

            eligible.append(normalized)

        return eligible

    def _pre_synthesis_coarse_ranking_config(self, settings: Any) -> tuple[int, dict[str, float]]:
        """Resolve Stage-A shortlist size and score weights from config with safe fallbacks."""
        default_shortlist_size = LLMTradingBlueprint.model_fields["max_total_positions"].default
        coarse_ranking_cfg = getattr(settings.analysis_service.llm, "coarse_ranking", None)
        shortlist_size_value = getattr(coarse_ranking_cfg, "shortlist_size", default_shortlist_size)
        try:
            shortlist_size = max(1, int(shortlist_size_value))
        except (TypeError, ValueError):
            shortlist_size = 5

        weights_cfg = getattr(coarse_ranking_cfg, "weights", None)
        return shortlist_size, {
            "data_quality": float(getattr(weights_cfg, "data_quality", 0.4)),
            "option_coverage": float(getattr(weights_cfg, "option_coverage", 0.2)),
            "liquidity": float(getattr(weights_cfg, "liquidity", 0.2)),
            "strategy_eligibility": float(getattr(weights_cfg, "strategy_eligibility", 0.1)),
            "earnings_buffer": float(getattr(weights_cfg, "earnings_buffer", 0.1)),
        }

    def _coarse_ranking_decision_reason(
        self,
        entry: dict[str, Any],
        *,
        rank: int,
        shortlist_limit: int,
    ) -> str:
        """Build a concise human-readable explanation for triage decisions."""
        if rank <= shortlist_limit:
            return f"rank {rank} inside shortlist cutoff {shortlist_limit}"

        weakest_components = sorted(
            (
                ("data_quality", entry["components"]["data_quality_score"]),
                ("option_coverage", entry["components"]["option_coverage_score"]),
                ("liquidity", entry["components"]["liquidity_score"]),
                ("strategy_eligibility", entry["components"]["strategy_eligibility_score"]),
                ("earnings_buffer", entry["components"]["earnings_score"]),
            ),
            key=lambda item: item[1],
        )[:2]
        weakest_text = ", ".join(f"{label}={value:.2f}" for label, value in weakest_components)
        return f"rank {rank} below shortlist cutoff {shortlist_limit}; weaker {weakest_text}"

    def _coarse_ranking_earnings_score(self, earnings_days: int | None) -> float:
        """Reward cleaner earnings buffers without hard-dropping non-event names."""
        if earnings_days is None:
            return 0.75
        if earnings_days <= 1:
            return 0.0
        if earnings_days <= 3:
            return 0.35
        if earnings_days <= 5:
            return 0.6
        return 1.0

    def _pre_synthesis_bid_ask_hard_threshold(self, allowed_strategy_types: list[str]) -> float:
        """Mirror deterministic liquidity hard blocks conservatively at symbol level."""
        normalized = {str(strategy_type).lower() for strategy_type in allowed_strategy_types}
        if normalized.intersection({"iron_condor", "calendar_spread"}):
            return 0.30
        return 0.25

    def _configured_model_version(self, settings: Any, provider_name: str) -> str:
        """Return the configured model name for the active provider."""
        provider_settings = getattr(settings.analysis_service.llm, provider_name, None)
        model_name = getattr(provider_settings, "model", "")
        return model_name or "unknown"

    def _build_empty_blueprint(
        self,
        *,
        signal_features: list[SignalFeatures],
        signal_date: date | None,
        provider_name: str,
        model_version: str,
        reasoning_context: dict[str, Any] | None = None,
    ) -> LLMTradingBlueprint:
        """Build a valid empty blueprint when no symbol survives pre-LLM gating."""
        if not signal_features:
            raise ValueError("signal_features must not be empty when building an empty blueprint")

        return LLMTradingBlueprint(
            trading_date=signal_date or signal_features[0].date,
            generated_at=max(sf.computed_at for sf in signal_features),
            model_provider=provider_name,
            model_version=model_version,
            market_regime="neutral",
            symbol_plans=[],
            reasoning_context=reasoning_context,
        )

    _MARKET_SNAPSHOT_SECTION_KEYS = {
        "price": ("close_price", "daily_return", "volume", "volatility_regime"),
        "stock_trend": ("trend", "trend_strength", "rsi_14", "adx_14"),
        "option_vol_surface": ("iv_rank", "iv_percentile", "term_structure_slope"),
        "cross_asset": ("vix_level", "vix_percentile_52w"),
    }

    def _build_market_snapshot(
        self,
        benchmark_features: list[SignalFeatures],
    ) -> dict[str, Any] | None:
        """Compress benchmark signals into one shared market snapshot."""
        if not benchmark_features:
            return None

        serialized = self._serialize_signals(benchmark_features)
        benchmarks: list[dict[str, Any]] = []

        for sig in serialized:
            if not isinstance(sig, dict):
                continue

            entry: dict[str, Any] = {"symbol": sig.get("symbol", "UNKNOWN")}
            for section, keys in self._MARKET_SNAPSHOT_SECTION_KEYS.items():
                raw = sig.get(section)
                if not isinstance(raw, dict):
                    continue
                compact = {
                    key: raw[key]
                    for key in keys
                    if key in raw and raw[key] not in (None, "", [], {})
                }
                if compact:
                    entry[section] = compact

            benchmarks.append(entry)

        if not benchmarks:
            return None

        return {
            "symbols": [item["symbol"] for item in benchmarks],
            "benchmarks": benchmarks,
        }

    # Keys preserved for benchmark-only symbols (compact cross-asset context)
    _BENCHMARK_KEEP_KEYS = frozenset({
        "symbol", "confidence",
        # trend
        "regime", "trend_direction", "trend_strength",
        "divergence_detected", "divergence_type", "false_positive_risk",
        "trade_allowed", "confidence_cap", "simple_structures_only", "blocked_reasons",
        # volatility
        "vol_regime", "iv_rank_zone", "hv_iv_assessment",
        "iv_percentile_divergence", "garch_divergence_direction",
        "event_risk_present", "liquidity_status", "trade_allowed",
        "confidence_cap", "simple_structures_only", "blocked_reasons",
        # flow
        "flow_signal", "volume_anomaly", "vwap_bias",
        "position_size_modifier", "false_breakout_risk",
        "event_risk_present", "liquidity_status", "trade_allowed",
        "confidence_cap", "simple_structures_only", "blocked_reasons",
        "confirming_indicators_count",
        # chain
        "pcr_signal", "gamma_pin_active", "institutional_flow",
        "hard_block", "liquidity_ok", "front_expiry_dte",
        "liquidity_tier", "event_risk_present", "net_delta_exposure",
        "trade_allowed", "confidence_cap", "simple_structures_only", "blocked_reasons",
        "confirming_indicators_count",
        # spread
        "best_spread_type", "effective_rr", "liquidity_status",
        "event_risk_present", "trade_allowed", "confidence_cap",
        "simple_structures_only", "blocked_reasons",
        # cross_asset
        "correlation_regime", "risk_off_signal", "vix_environment",
        "gex_regime", "master_override", "regime_transition",
        "regime_days", "position_size_modifier", "effective_size_modifier",
        "hedging_needed",
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

    def _compute_consensus(
        self,
        agent_outputs: dict[str, Any],
        trade_syms: set[str],
    ) -> dict[str, dict[str, Any]]:
        """Pre-compute directional consensus across agents per symbol.

        Returns a dict mapping symbol → {direction_counts, consensus_direction,
        consensus_strength, agreement_count} for injection into the synthesizer.
        """
        # Collect directional signals per symbol from each agent.
        # Confidence is adjusted by risk priority rules from prompt logic.
        symbol_directions: dict[str, list[tuple[str, str, float, float]]] = {}  # sym → [(agent, direction, raw_conf, adjusted_conf)]
        symbol_event_risk_count: dict[str, int] = {}
        symbol_cross_regime: dict[str, str] = {}

        direction_field_map = {
            "trend": ("trend_direction", "confidence"),
            "flow": ("flow_signal", "confidence"),
            "chain": ("pcr_signal", "confidence"),
            "cross_asset": ("correlation_regime", "confidence"),
        }

        flow_direction_map = {
            "strong_buy": "bullish", "moderate_buy": "bullish",
            "strong_sell": "bearish", "moderate_sell": "bearish",
            "neutral": "neutral", "conflicting": "neutral",
        }
        pcr_direction_map = {
            "contrarian_bullish": "bullish", "contrarian_bearish": "bearish", "neutral": "neutral",
            "directional_bullish": "bullish", "directional_bearish": "bearish",
        }
        cross_direction_map = {
            "fear": "bearish", "bullish_vol": "bullish", "decoupled": "neutral",
            "normal": "neutral", "event_driven": "neutral",
        }

        for agent_name, output in agent_outputs.items():
            if not isinstance(output, dict):
                continue
            symbols_list = output.get("symbols", [])
            if not isinstance(symbols_list, list):
                continue

            field_info = direction_field_map.get(agent_name)
            if not field_info:
                continue
            dir_field, conf_field = field_info

            for sym_data in symbols_list:
                if not isinstance(sym_data, dict):
                    continue
                sym = sym_data.get("symbol", "").upper()
                if sym not in trade_syms:
                    continue

                raw_dir = sym_data.get(dir_field, "neutral")
                conf = sym_data.get(conf_field, 0.5)
                try:
                    raw_conf = float(conf)
                except (TypeError, ValueError):
                    raw_conf = 0.5
                adjusted_conf = raw_conf

                if agent_name == "cross_asset":
                    symbol_cross_regime[sym] = str(raw_dir)

                # Event risk penalty: de-prioritize directional conviction.
                if bool(sym_data.get("event_risk_present", False)):
                    symbol_event_risk_count[sym] = symbol_event_risk_count.get(sym, 0) + 1
                    adjusted_conf *= 0.8

                # Confirming-indicators weighting (Flow/Chain only).
                if agent_name in {"flow", "chain"}:
                    ci = sym_data.get("confirming_indicators_count")
                    if isinstance(ci, int):
                        if ci <= 1:
                            adjusted_conf *= 0.8
                        elif ci >= 3:
                            adjusted_conf *= 1.1

                adjusted_conf = max(0.0, min(1.0, adjusted_conf))

                # Normalize direction to bullish/bearish/neutral
                if agent_name == "flow":
                    direction = flow_direction_map.get(raw_dir, "neutral")
                elif agent_name == "chain":
                    direction = pcr_direction_map.get(raw_dir, "neutral")
                elif agent_name == "cross_asset":
                    direction = cross_direction_map.get(raw_dir, "neutral")
                else:
                    direction = raw_dir if raw_dir in ("bullish", "bearish", "neutral") else "neutral"

                symbol_directions.setdefault(sym, []).append((agent_name, direction, raw_conf, adjusted_conf))

        # Compute confidence-weighted consensus
        consensus: dict[str, dict[str, Any]] = {}
        for sym, directions in symbol_directions.items():
            counts = {"bullish": 0, "bearish": 0, "neutral": 0}
            raw_weights = {"bullish": 0.0, "bearish": 0.0, "neutral": 0.0}
            weights = {"bullish": 0.0, "bearish": 0.0, "neutral": 0.0}
            for _, d, raw_c, adj_c in directions:
                counts[d] = counts.get(d, 0) + 1
                raw_weights[d] = raw_weights.get(d, 0.0) + raw_c
                weights[d] = weights.get(d, 0.0) + adj_c

            bullish_weight = weights["bullish"]
            bearish_weight = weights["bearish"]

            # Require 1.5× confidence-weight advantage for directional consensus
            if bullish_weight > bearish_weight * 1.5:
                consensus_dir = "bullish"
            elif bearish_weight > bullish_weight * 1.5:
                consensus_dir = "bearish"
            else:
                consensus_dir = "neutral"

            # Effective confidence = 25th percentile of agreeing agents' confidence
            agreeing_confs = [adj_c for _, d, _, adj_c in directions if d == consensus_dir]
            if agreeing_confs:
                effective_confidence = round(
                    statistics.quantiles(agreeing_confs, n=4)[0]
                    if len(agreeing_confs) >= 2
                    else agreeing_confs[0],
                    3,
                )
            else:
                effective_confidence = 0.0

            agreeing_raw_confs = [raw_c for _, d, raw_c, _ in directions if d == consensus_dir]
            if agreeing_raw_confs:
                effective_confidence_raw = round(
                    statistics.quantiles(agreeing_raw_confs, n=4)[0]
                    if len(agreeing_raw_confs) >= 2
                    else agreeing_raw_confs[0],
                    3,
                )
            else:
                effective_confidence_raw = 0.0

            # Event-risk confidence cap from prompt logic:
            # - >=3 agents event-risk -> directional confidence cap
            # - >=2 event-risk + cross-asset event-driven -> same cap
            event_count = symbol_event_risk_count.get(sym, 0)
            cross_regime = symbol_cross_regime.get(sym, "normal")
            event_risk_capped = False
            if event_count >= 3 or (event_count >= 2 and cross_regime == "event_driven"):
                effective_confidence = min(effective_confidence, 0.5)
                event_risk_capped = True

            max_dir = max(counts, key=counts.get)
            agreement = counts[max_dir]
            total = sum(counts.values())

            consensus[sym] = {
                "direction_counts": counts,
                "consensus_direction": consensus_dir,
                "agreement_count": agreement,
                "total_agents": total,
                "consensus_strength": round(agreement / max(total, 1), 2),
                "event_risk_agent_count": event_count,
                "event_risk_capped": event_risk_capped,
                "confidence_weight": {
                    "bullish": round(bullish_weight, 3),
                    "bearish": round(bearish_weight, 3),
                    "neutral": round(weights["neutral"], 3),
                },
                "confidence_weight_raw": {
                    "bullish": round(raw_weights["bullish"], 3),
                    "bearish": round(raw_weights["bearish"], 3),
                    "neutral": round(raw_weights["neutral"], 3),
                },
                "effective_confidence_raw": effective_confidence_raw,
                "effective_confidence": effective_confidence,
            }

        return consensus

    def _classify_market_condition(
        self,
        agent_outputs: dict[str, Any],
    ) -> str:
        """Classify current market condition from cross-asset and trend agent outputs.

        Returns one of: trending_calm, trending_volatile, range_calm,
        range_volatile, crisis, recovery.
        """
        cross = agent_outputs.get("cross_asset", {})
        if not isinstance(cross, dict):
            return "unknown"

        market_regime = cross.get("market_regime", "neutral")

        # Early return for recovery state
        if market_regime == "recovery":
            return "recovery"

        # Extract VIX environment from symbols (majority vote, fallback="normal")
        vix_counts = {
            "panic": 0,
            "elevated": 0,
            "normal": 0,
            "complacent": 0,
        }
        for sym_data in cross.get("symbols", []):
            if isinstance(sym_data, dict):
                env = sym_data.get("vix_environment", "normal")
                if env in vix_counts:
                    vix_counts[env] += 1

        vix_env = max(vix_counts, key=vix_counts.get) if any(vix_counts.values()) else "normal"

        # Extract trend info from trend agent
        trend = agent_outputs.get("trend", {})

        # Simple classification logic
        is_crisis = vix_env in ("panic",) or market_regime == "event_driven"
        is_elevated_vol = vix_env in ("elevated", "panic")
        is_calm = vix_env in ("normal", "complacent")

        # Check if market is trending from trend agent
        trending_count = 0
        range_count = 0
        for sym_data in trend.get("symbols", []):
            if isinstance(sym_data, dict):
                regime = sym_data.get("regime", "neutral")
                if regime in ("trending_up", "trending_down"):
                    trending_count += 1
                elif regime in ("range_bound", "squeeze"):
                    range_count += 1

        is_trending = trending_count > range_count

        if is_crisis:
            return "crisis"
        elif market_regime == "transitioning":
            return "range_volatile" if is_elevated_vol else "range_calm"
        elif market_regime == "risk_off":
            return "range_volatile" if is_elevated_vol else "range_calm"
        elif market_regime == "risk_on" and is_trending and is_calm:
            return "trending_calm"
        elif is_elevated_vol and is_trending:
            return "trending_volatile"
        elif is_elevated_vol and not is_trending:
            return "range_volatile"
        elif is_calm and is_trending:
            return "trending_calm"
        elif is_calm and not is_trending:
            return "range_calm"
        else:
            return "range_calm"  # default

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
        analysis_chunk_id: str,
        market_snapshot: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Run all 6 specialist agents in parallel, collecting results.

        Any failed specialist triggers a pipeline circuit-break.
        """
        redis = get_redis()
        current_task = asyncio.current_task()
        run_id = f"{int(perf_counter() * 1000)}-{id(current_task)}"
        failure_key = f"analysis:orchestrator:specialists_failed:{run_id}"

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
            start_ts = perf_counter()
            if await redis.exists(failure_key):
                raise RuntimeError("specialist circuit-break already active")

            try:
                model_override = getattr(agent_models_cfg, name, "") or None
                logger.info(
                    "orchestrator.agent_started",
                    analysis_chunk_id=analysis_chunk_id,
                    agent=name,
                    provider=provider.name,
                    model_override=model_override,
                )
                context = {"market_snapshot": market_snapshot} if market_snapshot else None
                result = await agent.analyze(
                    serialized_signals,
                    context=context,
                    provider=provider,
                    usage_tracker=usage_tracker,
                    model=model_override,
                    analysis_chunk_id=analysis_chunk_id,
                )
                elapsed_ms = int((perf_counter() - start_ts) * 1000)
                logger.info(
                    "orchestrator.agent_finished",
                    analysis_chunk_id=analysis_chunk_id,
                    agent=name,
                    provider=provider.name,
                    elapsed_ms=elapsed_ms,
                    success=True,
                )
                return name, result.model_dump(mode="json")
            except Exception as e:
                import traceback as _tb
                elapsed_ms = int((perf_counter() - start_ts) * 1000)
                logger.warning(
                    f"orchestrator.agent_failed",
                    analysis_chunk_id=analysis_chunk_id,
                    agent=name,
                    provider=provider.name,
                    elapsed_ms=elapsed_ms,
                    error_type=type(e).__name__,
                    error=str(e),
                    traceback=_tb.format_exc(limit=5),
                )
                await redis.set(failure_key, name, ex=120)
                raise RuntimeError(f"specialist_failed:{name}") from e

        llm_cfg = get_settings().analysis_service.llm
        gate_limit = max(1, int(llm_cfg.specialist_parallel_limit))
        gate_agents = {a.strip() for a in llm_cfg.specialist_parallel_agents if a and a.strip()}
        gate_targets = gate_agents.intersection(agents.keys())
        sem = asyncio.Semaphore(gate_limit)

        logger.info(
            "orchestrator.specialist_parallel_gate",
            analysis_chunk_id=analysis_chunk_id,
            gate_limit=gate_limit,
            gate_agents=sorted(gate_targets),
        )

        async def _run_one_gated(name: str, agent):
            if await redis.exists(failure_key):
                raise RuntimeError("specialist circuit-break already active")
            if name in gate_targets:
                async with sem:
                    return await _run_one(name, agent)
            return await _run_one(name, agent)

        await redis.delete(failure_key)
        tasks = [
            asyncio.create_task(_run_one_gated(name, agent), name=name)
            for name, agent in agents.items()
        ]
        outputs: dict[str, Any] = {}

        try:
            for done in asyncio.as_completed(tasks):
                name, result = await done
                outputs[name] = result
            return outputs
        except Exception as exc:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

            failed_agent = await redis.get(failure_key)
            failed_agents = [failed_agent] if failed_agent else []
            if not failed_agents:
                failed_agents = [t.get_name() for t in tasks if t.done() and not t.cancelled() and t.exception()]

            logger.error(
                "orchestrator.pipeline_circuit_break",
                analysis_chunk_id=analysis_chunk_id,
                phase="specialists",
                failed_agents=failed_agents,
                failed_count=len(failed_agents),
                error=str(exc),
            )
            raise RuntimeError(
                "specialist agent failed: " + ", ".join(sorted(failed_agents))
            ) from exc
        finally:
            await redis.delete(failure_key)
