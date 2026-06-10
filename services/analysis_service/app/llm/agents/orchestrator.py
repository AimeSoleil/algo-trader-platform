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

from pydantic import ValidationError

from shared.config import get_settings
from shared.data_quality import DataQualityConfig, should_circuit_break_analysis
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
from services.analysis_service.app.trade_gate_semantics import classify_reason_token

logger = get_logger("agent_orchestrator")

# Maximum revision rounds now read from config: settings.analysis_service.llm.max_critic_revisions


def _create_agent_provider(provider_name: str | None = None) -> AgentLLMProvider:
    """Instantiate the correct ``AgentLLMProvider`` based on config.

    Parameters
    ----------
    provider_name:
        ``"openai"``, ``"closeai"`` or ``"deepseek"``.
        Defaults to ``settings.analysis_service.llm.provider``.
    """
    if provider_name is None:
        provider_name = get_settings().analysis_service.llm.provider

    if provider_name == "closeai":
        from services.analysis_service.app.llm.agents._closeai_agent_provider import (
            CloseAIAgentProvider,
        )
        return CloseAIAgentProvider()

    if provider_name == "deepseek":
        from services.analysis_service.app.llm.agents._deepseek_agent_provider import (
            DeepSeekAgentProvider,
        )
        return DeepSeekAgentProvider()

    if provider_name != "openai":
        raise ValueError(f"Unsupported LLM provider: {provider_name}")

    # Default / "openai"
    from services.analysis_service.app.llm.agents._openai_agent_provider import (
        OpenAIAgentProvider,
    )
    return OpenAIAgentProvider()


class AgentOrchestrator:
    """Orchestrate multi-agent blueprint generation.

    Usage::

        orch = AgentOrchestrator()
        blueprint = await orch.generate(signals)
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

    @staticmethod
    def _find_validation_error(exc: BaseException | None) -> ValidationError | None:
        """Return the first ValidationError found in the cause/context chain."""
        current = exc
        seen: set[int] = set()
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            if isinstance(current, ValidationError):
                return current
            current = current.__cause__ or current.__context__
        return None

    @staticmethod
    def _failed_agents_from_exception(exc: Exception) -> list[str]:
        """Extract specialist names from orchestrator failure text when present."""
        prefix = "specialist agent failed:"
        message = str(exc).strip()
        if not message.startswith(prefix):
            return []
        raw_names = message[len(prefix):].strip()
        if not raw_names:
            return []
        return [name.strip() for name in raw_names.split(",") if name.strip()]

    def _build_skipped_batch_blueprint(
        self,
        *,
        signal_features: list[SignalFeatures],
        signal_date: date | None,
        provider_name: str,
        model_version: str,
        analysis_chunk_id: str,
        batch_index: int | None,
        is_chunk: bool,
        trade_symbols: list[str],
        error: Exception,
        failed_agents: list[str] | None = None,
    ) -> LLMTradingBlueprint:
        """Build an empty blueprint for a skipped non-validation batch failure."""
        return self._build_empty_blueprint(
            signal_features=signal_features,
            signal_date=signal_date,
            provider_name=provider_name,
            model_version=model_version,
            reasoning_context={
                "pipeline": "agentic_chunk_skipped" if is_chunk else "agentic_single_skipped",
                "provider": provider_name,
                "analysis_chunk_id": analysis_chunk_id,
                "batch_index": batch_index,
                "is_chunk": is_chunk,
                "trade_symbols": trade_symbols,
                "input_symbols": len(signal_features),
                "skip_reason": "batch_exception",
                "error_type": type(error).__name__,
                "error": str(error),
                "failed_agents": failed_agents if failed_agents is not None else self._failed_agents_from_exception(error),
            },
        )

    async def generate(
        self,
        signal_features: list[SignalFeatures],
        *,
        signal_date: date | None = None,
    ) -> LLMTradingBlueprint:
        """Run the full multi-agent pipeline.

        Parameters
        ----------
        signal_features:
            All symbols' signal features.

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
        try:
            max_output_plans = max(1, int(getattr(settings.analysis_service.llm, "max_output_plans", 10)))
        except (TypeError, ValueError):
            max_output_plans = 10
        precision_first_cfg = getattr(settings.analysis_service.llm, "precision_first", None)
        precision_first_enabled = bool(getattr(precision_first_cfg, "enabled", False))
        allowed_strategy_types = list(getattr(precision_first_cfg, "allowed_strategy_types", []) or [])
        trade_benchmark_syms = set(
            s.upper() for s in settings.common.watchlist.for_trade_benchmark
        )
        trade_syms = set(
            s.upper() for s in settings.common.watchlist.for_data_signal
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
        if pre_synthesis_triage["ranked_symbol_count"]:
            logger.info(
                "orchestrator.pre_synthesis_ranking",
                ranked_symbol_count=pre_synthesis_triage["ranked_symbol_count"],
                analysis_order=pre_synthesis_triage["analysis_order"],
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

        used_chunk_merge = False

        if len(trade_features) <= chunk_size:
            # Small enough — run everything in one pass (no chunking overhead)
            analysis_chunk_id = f"single-{uuid.uuid4().hex[:8]}"
            try:
                blueprint = await self._generate_single_pass(
                    signal_features=trade_features,
                    provider=provider,
                    signal_date=signal_date,
                    analysis_chunk_id=analysis_chunk_id,
                    usage_tracker=usage_tracker,
                    trade_symbols=trade_symbol_names,
                    market_snapshot=market_snapshot,
                )
            except Exception as exc:
                validation_error = self._find_validation_error(exc)
                if validation_error is not None:
                    if validation_error is exc:
                        raise
                    raise validation_error from exc

                logger.warning(
                    "orchestrator.batch_skipped",
                    analysis_chunk_id=analysis_chunk_id,
                    chunk=0,
                    is_chunk=False,
                    symbols=len(trade_features),
                    trade_symbols=trade_symbol_names,
                    error_type=type(exc).__name__,
                    error=str(exc),
                    failed_agents=self._failed_agents_from_exception(exc),
                )
                blueprint = self._build_skipped_batch_blueprint(
                    signal_features=trade_features,
                    signal_date=signal_date,
                    provider_name=provider.name,
                    model_version=self._configured_model_version(settings, provider.name),
                    analysis_chunk_id=analysis_chunk_id,
                    batch_index=0,
                    is_chunk=False,
                    trade_symbols=trade_symbol_names,
                    error=exc,
                )
        else:
            used_chunk_merge = True
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
            model_version = self._configured_model_version(settings, provider.name)

            async def _run_chunk(
                idx: int,
                chunk_features: list[SignalFeatures],
                *,
                analysis_chunk_id: str,
                trade_syms_in_chunk: list[str],
                chunk_tracker: LLMUsageTracker,
            ) -> tuple[int, LLMTradingBlueprint]:
                async with sem:
                    try:
                        logger.info(
                            "orchestrator.chunk_started",
                            analysis_chunk_id=analysis_chunk_id,
                            chunk=idx,
                            symbols=len(chunk_features),
                            trade_symbols=trade_syms_in_chunk,
                            benchmark_symbols=(market_snapshot or {}).get("symbols", []),
                        )
                        blueprint = await self._generate_single_pass(
                            signal_features=chunk_features,
                            provider=provider,
                            signal_date=signal_date,
                            is_chunk=True,
                            analysis_chunk_id=analysis_chunk_id,
                            usage_tracker=chunk_tracker,
                            trade_symbols=trade_syms_in_chunk,
                            market_snapshot=market_snapshot,
                        )
                        return idx, blueprint
                    except Exception as exc:
                        validation_error = self._find_validation_error(exc)
                        if validation_error is not None:
                            failed_agents = self._failed_agents_from_exception(exc)
                            logger.warning(
                                "orchestrator.batch_validation_skipped",
                                analysis_chunk_id=analysis_chunk_id,
                                chunk=idx,
                                is_chunk=True,
                                symbols=len(chunk_features),
                                error_type=type(validation_error).__name__,
                                error=str(validation_error),
                                failed_agents=failed_agents,
                            )
                            return idx, self._build_skipped_batch_blueprint(
                                signal_features=chunk_features,
                                signal_date=signal_date,
                                provider_name=provider.name,
                                model_version=model_version,
                                analysis_chunk_id=analysis_chunk_id,
                                batch_index=idx,
                                is_chunk=True,
                                trade_symbols=trade_syms_in_chunk,
                                error=validation_error,
                                failed_agents=failed_agents,
                            )

                        logger.warning(
                            "orchestrator.batch_skipped",
                            analysis_chunk_id=analysis_chunk_id,
                            chunk=idx,
                            is_chunk=True,
                            symbols=len(chunk_features),
                            trade_symbols=trade_syms_in_chunk,
                            error_type=type(exc).__name__,
                            error=str(exc),
                            failed_agents=self._failed_agents_from_exception(exc),
                        )
                        return idx, self._build_skipped_batch_blueprint(
                            signal_features=chunk_features,
                            signal_date=signal_date,
                            provider_name=provider.name,
                            model_version=model_version,
                            analysis_chunk_id=analysis_chunk_id,
                            batch_index=idx,
                            is_chunk=True,
                            trade_symbols=trade_syms_in_chunk,
                            error=exc,
                        )

            tasks = []
            for idx, chunk_features in enumerate(chunks):
                chunk_tracker = LLMUsageTracker()
                chunk_trackers[idx] = chunk_tracker
                analysis_chunk_id = f"chunk-{idx:03d}-{uuid.uuid4().hex[:8]}"
                trade_syms_in_chunk = [
                    sf.symbol for sf in chunk_features
                    if sf.symbol.upper() in trade_syms
                ]
                tasks.append(asyncio.create_task(
                    _run_chunk(
                        idx,
                        chunk_features,
                        analysis_chunk_id=analysis_chunk_id,
                        trade_syms_in_chunk=trade_syms_in_chunk,
                        chunk_tracker=chunk_tracker,
                    ),
                    name=f"chunk-{idx}",
                ))

            chunk_blueprints_by_index: dict[int, LLMTradingBlueprint] = {}
            for done in asyncio.as_completed(tasks):
                idx, chunk_blueprint = await done
                chunk_blueprints_by_index[idx] = chunk_blueprint

            chunk_blueprints = [
                chunk_blueprints_by_index[idx]
                for idx in sorted(chunk_blueprints_by_index)
            ]

            blueprint = await self.merge_chunk_blueprints(
                chunk_blueprints=chunk_blueprints,
                signal_features=signal_features,
                signal_date=signal_date,
                provider=provider,
                usage_tracker=usage_tracker,
            )

            # Merge chunk trackers into the main usage tracker
            for ct in chunk_trackers.values():
                usage_tracker.merge(ct)

        if not used_chunk_merge:
            # Safety net: drop any benchmark-only plans the LLM may still emit.
            # Chunk merge already applies the same filtering and final cap.
            blueprint.symbol_plans = [
                p for p in blueprint.symbol_plans
                if p.underlying.upper() in trade_syms
            ]
            blueprint.symbol_plans = blueprint.symbol_plans[:max_output_plans]
            blueprint.max_total_positions = max_output_plans
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

    async def generate_chunk_blueprint(
        self,
        signal_features: list[SignalFeatures],
        *,
        benchmark_features: list[SignalFeatures] | None = None,
        signal_date: date | None = None,
        analysis_chunk_id: str | None = None,
    ) -> LLMTradingBlueprint:
        """Generate one blueprint chunk without performing final cross-chunk merge."""
        chunk_features = self._validate_signal_features(signal_features)
        valid_benchmark_features = [
            sf for sf in (benchmark_features or [])
            if getattr(sf, "symbol", "") and getattr(sf, "close_price", 0)
        ]

        provider = self._provider
        if provider is None:
            provider = _create_agent_provider()
            self._provider = provider

        usage_tracker = LLMUsageTracker()
        market_snapshot = self._build_market_snapshot(valid_benchmark_features)
        resolved_chunk_id = analysis_chunk_id or f"fanout-{uuid.uuid4().hex[:8]}"
        blueprint = await self._generate_single_pass(
            signal_features=chunk_features,
            provider=provider,
            signal_date=signal_date,
            is_chunk=True,
            analysis_chunk_id=resolved_chunk_id,
            usage_tracker=usage_tracker,
            trade_symbols=[sf.symbol for sf in chunk_features],
            market_snapshot=market_snapshot,
        )

        usage_summary = usage_tracker.summary()
        logger.info(
            "orchestrator.celery_chunk_completed",
            analysis_chunk_id=resolved_chunk_id,
            plans=len(blueprint.symbol_plans),
            input_tokens=usage_summary["total"]["input_tokens"],
            output_tokens=usage_summary["total"]["output_tokens"],
            total_tokens=usage_summary["total"]["total_tokens"],
        )
        return blueprint

    async def merge_chunk_blueprints(
        self,
        *,
        chunk_blueprints: list[LLMTradingBlueprint],
        signal_features: list[SignalFeatures],
        signal_date: date | None = None,
        provider: AgentLLMProvider | None = None,
        usage_tracker: LLMUsageTracker | None = None,
    ) -> LLMTradingBlueprint:
        """Merge multiple chunk blueprints into one final reviewed blueprint."""
        settings = get_settings()
        try:
            max_output_plans = max(1, int(getattr(settings.analysis_service.llm, "max_output_plans", 10)))
        except (TypeError, ValueError):
            max_output_plans = 10
        precision_first_cfg = getattr(settings.analysis_service.llm, "precision_first", None)
        precision_first_enabled = bool(getattr(precision_first_cfg, "enabled", False))
        allowed_strategy_types = list(getattr(precision_first_cfg, "allowed_strategy_types", []) or [])
        trade_symbols = set(
            symbol.upper() for symbol in settings.common.watchlist.for_data_signal
        )

        resolved_provider = provider or self._provider
        if resolved_provider is None:
            resolved_provider = _create_agent_provider()
            self._provider = resolved_provider

        if not chunk_blueprints:
            return self._build_empty_blueprint(
                signal_features=signal_features,
                signal_date=signal_date,
                provider_name=resolved_provider.name,
                model_version=self._configured_model_version(settings, resolved_provider.name),
                reasoning_context={
                    "pipeline": "agentic_chunked",
                    "provider": resolved_provider.name,
                    "chunks": 0,
                    "chunk_contexts": [],
                    "post_merge_phase": {},
                },
            )

        blueprint = chunk_blueprints[0].model_copy(deep=True)
        quality_by_symbol = {
            sf.symbol.upper(): sf.data_quality.score
            for sf in signal_features
        }
        signal_feature_by_symbol = {
            sf.symbol.upper(): sf
            for sf in signal_features
        }
        chunk_limits = [
            {
                "chunk_index": idx,
                "chunk_id": (bp.reasoning_context or {}).get("analysis_chunk_id"),
                "max_total_positions": bp.max_total_positions,
            }
            for idx, bp in enumerate(chunk_blueprints)
        ]
        merged_plan_candidates: list[PlanCandidate] = []
        original_order = 0
        for idx, chunk_blueprint in enumerate(chunk_blueprints):
            chunk_id = (chunk_blueprint.reasoning_context or {}).get("analysis_chunk_id")
            agent_outputs = (chunk_blueprint.reasoning_context or {}).get("agent_outputs")
            for plan in chunk_blueprint.symbol_plans:
                merged_plan_candidates.append(PlanCandidate(
                    plan=plan,
                    chunk_index=idx,
                    original_order=original_order,
                    quality_score=quality_by_symbol.get(plan.underlying.upper(), plan.data_quality_score),
                    chunk_id=chunk_id,
                    agent_outputs=agent_outputs if isinstance(agent_outputs, dict) else None,
                    signal_feature=signal_feature_by_symbol.get(plan.underlying.upper()),
                ))
                original_order += 1

        review_candidate_count = max(
            1,
            len({candidate.plan.underlying.upper() for candidate in merged_plan_candidates}),
        )

        llm_post_merge_review: dict[str, Any] | None = None
        llm_post_merge_metadata: dict[str, Any] = {"status": "skipped", "reason": "provider_missing_generate"}
        if hasattr(resolved_provider, "generate") and len(merged_plan_candidates) > 1:
            agent_models_cfg = settings.analysis_service.llm.agent_models_override
            post_merge_model = agent_models_cfg.post_merge or None
            candidate_summaries, selector_context = self._portfolio_selector.build_review_inputs(
                candidates=merged_plan_candidates,
                trade_symbols=trade_symbols,
                precision_first_enabled=precision_first_enabled,
                allowed_strategy_types=allowed_strategy_types,
            )
            try:
                review = await self._post_merge_portfolio_agent.review(
                    candidate_summaries=candidate_summaries,
                    chunk_limit_proposals=chunk_limits,
                    selector_metadata=selector_context,
                    candidate_count=review_candidate_count,
                    provider=resolved_provider,
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
                validation_error = self._find_validation_error(exc)
                if validation_error is not None:
                    if validation_error is exc:
                        raise
                    raise validation_error from exc
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

        selected_plans, selection_metadata = self._portfolio_selector.select(
            candidates=merged_plan_candidates,
            trade_symbols=trade_symbols,
            chunk_limits=chunk_limits,
            max_output_plans=max_output_plans,
            llm_review=llm_post_merge_review,
            precision_first_enabled=precision_first_enabled,
            allowed_strategy_types=allowed_strategy_types,
        )
        selection_metadata["llm_review"] = {
            **selection_metadata.get("llm_review", {}),
            **llm_post_merge_metadata,
        }

        blueprint.symbol_plans = selected_plans
        output_targets = selection_metadata.get("output_targets", {})
        max_total_positions_target = output_targets.get("max_total_positions", {})
        blueprint.max_total_positions = int(max_total_positions_target.get("value", max_output_plans))

        all_contexts = [bp.reasoning_context for bp in chunk_blueprints if bp.reasoning_context]
        blueprint.reasoning_context = {
            "pipeline": "agentic_chunked",
            "provider": resolved_provider.name,
            "chunks": len(chunk_blueprints),
            "chunk_contexts": all_contexts,
            "post_merge_phase": selection_metadata,
        }

        blueprint.symbol_plans = [
            plan for plan in blueprint.symbol_plans
            if plan.underlying.upper() in trade_symbols
        ]

        logger.info(
            "orchestrator.chunks_merged",
            total_plans=len(blueprint.symbol_plans),
            chunks=len(chunk_blueprints),
            trimmed_plans=selection_metadata["deduped_plan_count"] - selection_metadata["output_plan_count"],
            max_total_positions=blueprint.max_total_positions,
            ranking_method=selection_metadata["ranking_method"],
        )
        return blueprint

    async def _generate_single_pass(
        self,
        signal_features: list[SignalFeatures],
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
        signals_summary = serialized

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

        agent_outputs = self._normalize_specialist_outputs(agent_outputs, serialized)

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
            provider=provider,
            signal_date=signal_date,
            usage_tracker=usage_tracker,
            trade_symbols=trade_symbols,
            model=synth_model,
            apply_output_cap=not is_chunk,
        )

        if not blueprint.symbol_plans:
            fallback_feedback = self._empty_synthesis_fallback_feedback(
                compact_outputs,
                signals_summary,
                trade_symbols,
            )
            if fallback_feedback:
                logger.info(
                    "orchestrator.phase_started",
                    phase="synthesizer_empty_fallback",
                    is_chunk=is_chunk,
                    symbols=len(trade_symbols or []),
                )
                fallback_t0 = perf_counter()
                blueprint = await self._synthesizer.synthesize(
                    agent_outputs=compact_outputs,
                    signals_summary=signals_summary,
                    critic_feedback=fallback_feedback,
                    provider=provider,
                    signal_date=signal_date,
                    usage_tracker=usage_tracker,
                    trade_symbols=trade_symbols,
                    model=synth_model,
                    apply_output_cap=not is_chunk,
                )
                logger.info(
                    "orchestrator.phase_completed",
                    phase="synthesizer_empty_fallback",
                    plans=len(blueprint.symbol_plans),
                    is_chunk=is_chunk,
                    elapsed_s=round(perf_counter() - fallback_t0, 1),
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
                apply_output_cap=not is_chunk,
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
        """Rank surviving trade symbols but keep all of them for LLM analysis."""
        coarse_weights = self._pre_synthesis_coarse_ranking_config(settings)
        if not trade_features:
            return [], {
                "input_symbol_count": 0,
                "analysis_symbol_count": 0,
                "ranked_symbol_count": 0,
                "analysis_order": [],
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

        ranked_symbols: list[dict[str, Any]] = []

        for rank, entry in enumerate(ranked_entries, start=1):
            ranked_symbols.append({
                "rank": rank,
                "symbol": entry["symbol"],
                "action": "analyze",
                "coarse_score": entry["coarse_score"],
                "eligible_strategy_types": entry["eligible_strategy_types"],
                "components": entry["components"],
                "decision_reason": self._coarse_ranking_decision_reason(
                    entry,
                    rank=rank,
                ),
            })

        analysis_order = [entry["symbol"] for entry in ranked_entries]
        return [entry["feature"] for entry in ranked_entries], {
            "input_symbol_count": len(trade_features),
            "analysis_symbol_count": len(ranked_entries),
            "ranked_symbol_count": len(ranked_entries),
            "analysis_order": analysis_order,
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
        trend_signal_score = self._coarse_ranking_trend_signal_score(signal_feature)
        stock_liquidity_score = self._coarse_ranking_stock_liquidity_score(signal_feature)
        weight_sum = sum(max(0.0, value) for value in weights.values()) or 1.0
        coarse_score = round(
            (
                weights["data_quality"] * signal_feature.data_quality.score
                + weights["option_coverage"] * option_row_score
                + weights["liquidity"] * liquidity_score
                + weights["strategy_eligibility"] * strategy_score
                + weights["earnings_buffer"] * earnings_score
                + weights["trend_signal"] * trend_signal_score
                + weights["stock_liquidity"] * stock_liquidity_score
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
                "trend_signal_score": round(trend_signal_score, 6),
                "stock_liquidity_score": round(stock_liquidity_score, 6),
                "eligible_strategy_count": len(eligible_strategy_types),
                "option_row_count": signal_feature.data_quality.option_row_count,
                "bid_ask_spread_ratio": round(signal_feature.option_indicators.bid_ask_spread_ratio, 6),
                "adx_z_score": round(signal_feature.stock_indicators.adx_z_score, 6),
                "liquidity_threshold": round(signal_feature.stock_indicators.liquidity_threshold, 6),
                "volume": signal_feature.volume,
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
        earnings_days = signal_feature.cross_asset_indicators.earnings_proximity_days

        if data_quality.score < dq_cfg.skip_threshold:
            reasons.append({
                "rule": "data_quality_skip_threshold",
                "description": (
                    f"data_quality.score={data_quality.score:.4f} < {dq_cfg.skip_threshold:.2f}"
                ),
            })

        trend_guard_reason = self._pre_synthesis_trend_guard_reason(signal_feature)
        if trend_guard_reason is not None:
            reasons.append(trend_guard_reason)

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

    def _pre_synthesis_trend_guard_reason(
        self,
        signal_feature: SignalFeatures,
    ) -> dict[str, str] | None:
        """Drop symbols trapped in extreme-trend counter-trend/reversal contexts.

        This is intentionally narrow: only fire when the signal context points to an
        extreme established trend, reversal tension is explicit, and there is no
        aligned trend-following confirmation left for the LLM to express.
        """
        stock = signal_feature.stock_indicators
        trend_direction = str(getattr(stock, "trend", "neutral") or "neutral").lower()
        if trend_direction not in {"bullish", "bearish"}:
            return None

        adx_z_score = float(getattr(stock, "adx_z_score", 0.0) or 0.0)
        if adx_z_score <= 1.5:
            return None

        rsi_divergence = float(getattr(stock, "rsi_divergence", 0.0) or 0.0)
        macd_hist_divergence = float(getattr(stock, "macd_hist_divergence", 0.0) or 0.0)
        reversal_tension = (
            (trend_direction == "bullish" and rsi_divergence > 0)
            or (trend_direction == "bearish" and rsi_divergence < 0)
        ) and macd_hist_divergence < 0
        if not reversal_tension:
            return None

        close_price = float(signal_feature.close_price)
        aligned_confirmations = 0

        keltner_upper = float(getattr(stock, "keltner_upper", 0.0) or 0.0)
        keltner_lower = float(getattr(stock, "keltner_lower", 0.0) or 0.0)
        if trend_direction == "bullish" and keltner_upper > 0 and close_price > keltner_upper:
            aligned_confirmations += 1
        if trend_direction == "bearish" and keltner_lower > 0 and close_price < keltner_lower:
            aligned_confirmations += 1

        tenkan = float(getattr(stock, "ichimoku_tenkan", 0.0) or 0.0)
        kijun = float(getattr(stock, "ichimoku_kijun", 0.0) or 0.0)
        span_a = float(getattr(stock, "ichimoku_span_a", 0.0) or 0.0)
        span_b = float(getattr(stock, "ichimoku_span_b", 0.0) or 0.0)
        cloud_top = max(span_a, span_b)
        cloud_bottom = min(span_a, span_b)
        if trend_direction == "bullish" and tenkan > kijun and cloud_top > 0 and close_price > cloud_top:
            aligned_confirmations += 1
        if trend_direction == "bearish" and tenkan < kijun and cloud_bottom > 0 and close_price < cloud_bottom:
            aligned_confirmations += 1

        linreg = float(getattr(stock, "linear_reg_slope", 0.0) or 0.0)
        if trend_direction == "bullish" and linreg > 0:
            aligned_confirmations += 1
        if trend_direction == "bearish" and linreg < 0:
            aligned_confirmations += 1

        if aligned_confirmations > 0:
            return None

        return {
            "rule": "trend_extreme_counter_trend_context",
            "description": (
                f"stock_indicators.adx_z_score={adx_z_score:.2f} signals an extreme {trend_direction} trend, "
                f"but reversal tension is explicit (rsi_divergence={rsi_divergence:.1f}, "
                f"macd_hist_divergence={macd_hist_divergence:.1f}) and there are no aligned trend-following confirmations. "
                "Pre-synthesis Trend hard-gate drops this symbol rather than sending a likely counter-trend setup into LLM analysis."
            ),
        }

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

    def _pre_synthesis_coarse_ranking_config(self, settings: Any) -> dict[str, float]:
        """Resolve deterministic ranking weights from config with safe fallbacks."""
        coarse_ranking_cfg = getattr(settings.analysis_service.llm, "coarse_ranking", None)
        weights_cfg = getattr(coarse_ranking_cfg, "weights", None)
        return {
            "data_quality": float(getattr(weights_cfg, "data_quality", 0.4)),
            "option_coverage": float(getattr(weights_cfg, "option_coverage", 0.2)),
            "liquidity": float(getattr(weights_cfg, "liquidity", 0.2)),
            "strategy_eligibility": float(getattr(weights_cfg, "strategy_eligibility", 0.1)),
            "earnings_buffer": float(getattr(weights_cfg, "earnings_buffer", 0.1)),
            "trend_signal": float(getattr(weights_cfg, "trend_signal", 0.05)),
            "stock_liquidity": float(getattr(weights_cfg, "stock_liquidity", 0.05)),
        }

    def _coarse_ranking_trend_signal_score(self, signal_feature: SignalFeatures) -> float:
        """Reward clearer precomputed trend regimes without dominating coarse ranking."""
        adx_z = float(getattr(signal_feature.stock_indicators, "adx_z_score", 0.0) or 0.0)
        normalized = min(abs(adx_z) / 1.8, 1.0)
        return round(normalized, 6)

    def _coarse_ranking_stock_liquidity_score(self, signal_feature: SignalFeatures) -> float:
        """Score current stock liquidity against the Trend/Flow liquidity threshold."""
        liquidity_threshold = float(getattr(signal_feature.stock_indicators, "liquidity_threshold", 0.0) or 0.0)
        if liquidity_threshold <= 0:
            return 0.5
        current_volume = max(float(signal_feature.volume), 0.0)
        return round(min(current_volume / liquidity_threshold, 1.0), 6)

    def _coarse_ranking_decision_reason(
        self,
        entry: dict[str, Any],
        *,
        rank: int,
    ) -> str:
        """Build a concise human-readable explanation for ordering decisions."""
        components = (
            ("data_quality", entry["components"]["data_quality_score"]),
            ("option_coverage", entry["components"]["option_coverage_score"]),
            ("liquidity", entry["components"]["liquidity_score"]),
            ("strategy_eligibility", entry["components"]["strategy_eligibility_score"]),
            ("earnings_buffer", entry["components"]["earnings_score"]),
            ("trend_signal", entry["components"]["trend_signal_score"]),
            ("stock_liquidity", entry["components"]["stock_liquidity_score"]),
        )
        strongest_components = sorted(components, key=lambda item: item[1], reverse=True)[:2]
        weakest_components = sorted(
            components,
            key=lambda item: item[1],
        )[:1]
        strongest_text = ", ".join(f"{label}={value:.2f}" for label, value in strongest_components)
        weakest_text = ", ".join(f"{label}={value:.2f}" for label, value in weakest_components)
        return f"priority rank {rank}; strongest {strongest_text}; weakest {weakest_text}"

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
        """Reference liquidity threshold used to normalize coarse ranking scores."""
        normalized = {str(strategy_type).lower() for strategy_type in allowed_strategy_types}
        if normalized.intersection({"iron_condor", "calendar_spread"}):
            return 0.45
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
            max_total_positions=0,
            symbol_plans=[],
            reasoning_context=reasoning_context,
        )

    _MARKET_SNAPSHOT_SECTION_KEYS = {
        "price": ("close_price", "daily_return", "volume", "volatility_regime"),
        "stock_trend": ("trend", "trend_strength", "rsi_14", "adx_14"),
        "option_vol_surface": ("iv_rank", "iv_percentile", "term_structure_slope"),
        "cross_asset": ("vix_level", "vix_percentile_60d"),
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

    _EMITTED_STRATEGY_TYPE_ALIASES = {
        "single_leg": "single_leg",
        "single_leg_call": "single_leg",
        "single_leg_put": "single_leg",
        "vertical": "vertical_spread",
        "vertical_spread": "vertical_spread",
        "call_vertical_spread": "vertical_spread",
        "put_vertical_spread": "vertical_spread",
        "bull_put_spread": "vertical_spread",
        "bear_call_spread": "vertical_spread",
        "credit_spread": "vertical_spread",
        "calendar": "calendar_spread",
        "calendar_spread": "calendar_spread",
        "reverse_calendar": "diagonal_spread",
        "diagonal_spread": "diagonal_spread",
        "iron_condor": "iron_condor",
        "iron_butterfly": "iron_butterfly",
        "butterfly": "butterfly",
        "straddle": "straddle",
        "short_straddle": "straddle",
        "long_straddle": "straddle",
        "strangle": "strangle",
        "short_strangle": "strangle",
        "long_strangle": "strangle",
        "box_arb": "box_arb",
    }

    _EMITTED_STRATEGY_CANDIDATE_KEYS = {
        "vertical_spread": ("vertical",),
        "iron_condor": ("iron_condor",),
        "iron_butterfly": ("butterfly",),
        "butterfly": ("butterfly",),
        "calendar_spread": ("calendar",),
        "diagonal_spread": ("calendar", "reverse_calendar"),
        "box_arb": ("box_arb",),
    }

    def _canonical_emitted_strategy_type(self, strategy_type: Any) -> str | None:
        if not isinstance(strategy_type, str):
            return None

        normalized = str(strategy_type).strip().lower().replace("-", "_").replace(" ", "_")
        while "__" in normalized:
            normalized = normalized.replace("__", "_")
        return self._EMITTED_STRATEGY_TYPE_ALIASES.get(normalized)

    def _append_emitted_strategy_type(self, emitted: list[str], strategy_type: Any) -> None:
        canonical = self._canonical_emitted_strategy_type(strategy_type)
        if canonical is None or canonical in emitted:
            return
        emitted.append(canonical)

    def _emitted_strategy_types_for_synthesis(
        self,
        agent_name: str,
        symbol_analysis: dict[str, Any],
    ) -> list[str]:
        emitted: list[str] = []

        if agent_name == "spread":
            self._append_emitted_strategy_type(emitted, symbol_analysis.get("best_spread_type"))

        if agent_name == "chain":
            for strategy_type in symbol_analysis.get("suggested_strategies", []) or []:
                self._append_emitted_strategy_type(emitted, strategy_type)

        strategies = symbol_analysis.get("strategies")
        if isinstance(strategies, list):
            for strategy in strategies:
                if isinstance(strategy, dict):
                    self._append_emitted_strategy_type(emitted, strategy.get("strategy_type"))

        return emitted

    def _compact_symbol_entry(
        self,
        compact_outputs: dict[str, Any],
        agent_name: str,
        symbol: str,
    ) -> dict[str, Any] | None:
        agent_output = compact_outputs.get(agent_name)
        if not isinstance(agent_output, dict):
            return None

        symbols = agent_output.get("symbols")
        if not isinstance(symbols, list):
            return None

        symbol_upper = str(symbol).strip().upper()
        return next(
            (
                item for item in symbols
                if isinstance(item, dict) and str(item.get("symbol") or "").strip().upper() == symbol_upper
            ),
            None,
        )

    def _empty_synthesis_fallback_feedback(
        self,
        compact_outputs: dict[str, Any],
        signals_summary: list[dict[str, Any]],
        trade_symbols: list[str] | None,
    ) -> str | None:
        signal_by_symbol = {
            str(signal.get("symbol") or "").strip().upper(): signal
            for signal in signals_summary
            if isinstance(signal, dict) and str(signal.get("symbol") or "").strip()
        }
        if not signal_by_symbol:
            return None

        settings = get_settings()
        precision_first = getattr(settings.analysis_service.llm, "precision_first", None)
        min_acceptable_confidence = getattr(settings.analysis_service.llm, "min_acceptable_confidence", 0.35)
        try:
            min_acceptable_confidence_value = max(0.0, min(1.0, float(min_acceptable_confidence)))
        except (TypeError, ValueError):
            min_acceptable_confidence_value = 0.35
        min_acceptable_confidence_text = f"{min_acceptable_confidence_value:.2f}".rstrip("0").rstrip(".")
        precision_first_enabled = bool(getattr(precision_first, "enabled", False))
        allowed_strategy_types = {
            str(strategy_type).strip().lower()
            for strategy_type in getattr(precision_first, "allowed_strategy_types", []) or []
            if str(strategy_type).strip()
        }

        target_symbols = [
            str(symbol).strip().upper()
            for symbol in (trade_symbols or signal_by_symbol.keys())
            if str(symbol).strip()
        ]
        if not target_symbols:
            return None

        lines = [
            "Your previous synthesis returned zero symbol_plans.",
            "Do not omit an already-emitted candidate solely because a stronger execution candidate was never emitted by any specialist.",
            f"Only preserve an emitted candidate when it already survives all hard gates, remains inside the allowed structure scope, and can keep confidence >= {min_acceptable_confidence_text}.",
        ]

        usable_symbols = 0
        for symbol in target_symbols:
            emitted: list[str] = []
            for agent_name in ("trend", "volatility", "chain", "spread"):
                symbol_entry = self._compact_symbol_entry(compact_outputs, agent_name, symbol)
                if not isinstance(symbol_entry, dict):
                    continue
                for strategy_type in symbol_entry.get("emitted_strategy_types", []) or []:
                    canonical = self._canonical_emitted_strategy_type(strategy_type)
                    if canonical is None:
                        continue
                    if precision_first_enabled and allowed_strategy_types and canonical not in allowed_strategy_types:
                        continue
                    if canonical not in emitted:
                        emitted.append(canonical)

            if not emitted:
                continue

            signal = signal_by_symbol.get(symbol)
            if not isinstance(signal, dict):
                continue
            option_spreads = signal.get("option_spreads", {})
            execution_candidates = option_spreads.get("execution_candidates", {}) if isinstance(option_spreads, dict) else {}
            if not isinstance(execution_candidates, dict):
                execution_candidates = {}

            candidate_notes: list[str] = []
            for strategy_type in emitted:
                candidate_keys = self._EMITTED_STRATEGY_CANDIDATE_KEYS.get(strategy_type, ())
                if not candidate_keys:
                    continue
                for candidate_key in candidate_keys:
                    candidate = execution_candidates.get(candidate_key)
                    if not isinstance(candidate, dict) or candidate.get("candidate_available") is not True:
                        continue
                    note_parts = [f"{strategy_type} via {candidate_key}"]
                    effective_rr = candidate.get("effective_rr")
                    if isinstance(effective_rr, (int, float)):
                        note_parts.append(f"effective_rr={float(effective_rr):.2f}")
                    theta_capture = candidate.get("effective_theta_capture_per_day")
                    if isinstance(theta_capture, (int, float)):
                        note_parts.append(f"theta/day={float(theta_capture):.3f}")
                    worst_leg_ratio = candidate.get("worst_leg_bid_ask_spread_ratio")
                    if isinstance(worst_leg_ratio, (int, float)):
                        note_parts.append(f"worst_leg_spread={float(worst_leg_ratio):.4f}")
                    expiry_dte = candidate.get("expiry_dte")
                    if isinstance(expiry_dte, int):
                        note_parts.append(f"expiry_dte={expiry_dte}")
                    candidate_notes.append(", ".join(note_parts))
                    break

            usable_symbols += 1
            lines.append(f"- {symbol}: emitted strategy families = {', '.join(emitted)}.")
            if candidate_notes:
                lines.append(f"  Available emitted candidate evidence: {'; '.join(candidate_notes)}.")

        if usable_symbols == 0:
            return None

        lines.append(
            "If no emitted candidate clears those hard constraints, returning an empty blueprint remains allowed."
        )
        return "\n".join(lines)

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
                        emitted_strategy_types = self._emitted_strategy_types_for_synthesis(agent_name, sym_data)
                        # Strip reasoning + strategies (high token cost, low synthesis value)
                        entry = {
                            k: v for k, v in sym_data.items()
                            if k not in ("reasoning", "strategies")
                        }

                        if sym in trade_syms and emitted_strategy_types:
                            entry["emitted_strategy_types"] = emitted_strategy_types

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

    def _normalize_specialist_outputs(
        self,
        agent_outputs: dict[str, Any],
        serialized_signals: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if not isinstance(agent_outputs, dict):
            return agent_outputs

        signal_by_symbol = {
            str(item.get("symbol") or "").strip().upper(): item
            for item in serialized_signals
            if isinstance(item, dict) and str(item.get("symbol") or "").strip()
        }

        normalized_outputs = dict(agent_outputs)
        changed = False

        flow_output = agent_outputs.get("flow")
        if isinstance(flow_output, dict):
            flow_symbols = flow_output.get("symbols")
            if isinstance(flow_symbols, list):
                normalized_flow_symbols: list[Any] = []
                flow_changed = False
                for symbol_analysis in flow_symbols:
                    if not isinstance(symbol_analysis, dict):
                        normalized_flow_symbols.append(symbol_analysis)
                        continue

                    normalized = self._normalize_flow_symbol_analysis(symbol_analysis)
                    flow_changed = flow_changed or normalized != symbol_analysis
                    normalized_flow_symbols.append(normalized)

                if flow_changed:
                    normalized_flow_output = dict(flow_output)
                    normalized_flow_output["symbols"] = normalized_flow_symbols
                    normalized_outputs["flow"] = normalized_flow_output
                    changed = True

        spread_output = agent_outputs.get("spread")
        if isinstance(spread_output, dict):
            spread_symbols = spread_output.get("symbols")
            if isinstance(spread_symbols, list):
                normalized_spread_symbols: list[Any] = []
                spread_changed = False
                for symbol_analysis in spread_symbols:
                    if not isinstance(symbol_analysis, dict):
                        normalized_spread_symbols.append(symbol_analysis)
                        continue

                    symbol = str(symbol_analysis.get("symbol") or "").strip().upper()
                    normalized = self._normalize_spread_symbol_analysis(
                        symbol_analysis,
                        signal_by_symbol.get(symbol),
                    )
                    spread_changed = spread_changed or normalized != symbol_analysis
                    normalized_spread_symbols.append(normalized)

                if spread_changed:
                    normalized_spread_output = dict(spread_output)
                    normalized_spread_output["symbols"] = normalized_spread_symbols
                    normalized_outputs["spread"] = normalized_spread_output
                    changed = True

        chain_output = agent_outputs.get("chain")
        if not isinstance(chain_output, dict):
            return normalized_outputs if changed else agent_outputs

        symbol_entries = chain_output.get("symbols")
        if not isinstance(symbol_entries, list):
            return normalized_outputs if changed else agent_outputs

        normalized_symbols: list[Any] = []
        chain_changed = False
        for symbol_analysis in symbol_entries:
            if not isinstance(symbol_analysis, dict):
                normalized_symbols.append(symbol_analysis)
                continue

            symbol = str(symbol_analysis.get("symbol") or "").strip().upper()
            normalized = self._normalize_chain_symbol_analysis(
                symbol_analysis,
                signal_by_symbol.get(symbol),
            )
            chain_changed = chain_changed or normalized != symbol_analysis
            normalized_symbols.append(normalized)

        if not chain_changed:
            return normalized_outputs if changed else agent_outputs

        normalized_chain_output = dict(chain_output)
        normalized_chain_output["symbols"] = normalized_symbols
        normalized_outputs["chain"] = normalized_chain_output
        return normalized_outputs

    def _normalize_spread_symbol_analysis(
        self,
        symbol_analysis: dict[str, Any],
        signal: dict[str, Any] | None,
    ) -> dict[str, Any]:
        normalized = dict(symbol_analysis)
        blocked_reasons = [
            str(reason).strip().lower()
            for reason in normalized.get("blocked_reasons", [])
            if str(reason).strip()
        ]
        if blocked_reasons:
            normalized["blocked_reasons"] = blocked_reasons

        if not isinstance(signal, dict):
            return normalized

        best_spread_type = normalized.get("best_spread_type")
        candidate = self._selected_execution_candidate(signal, best_spread_type)
        if not isinstance(candidate, dict) or candidate.get("candidate_available") is not True:
            return normalized

        worst_leg_ratio = candidate.get("worst_leg_bid_ask_spread_ratio")
        try:
            worst_leg_ratio_value = float(worst_leg_ratio)
        except (TypeError, ValueError):
            return normalized

        if worst_leg_ratio_value > 0.20:
            normalized["liquidity_status"] = "illiquid"
        elif worst_leg_ratio_value >= 0.10:
            normalized["liquidity_status"] = "wide"
        else:
            normalized["liquidity_status"] = "adequate"

        return normalized

    def _normalize_flow_symbol_analysis(
        self,
        symbol_analysis: dict[str, Any],
    ) -> dict[str, Any]:
        normalized = dict(symbol_analysis)
        blocked_reasons = [
            str(reason).strip().lower()
            for reason in normalized.get("blocked_reasons", [])
            if str(reason).strip()
        ]
        if blocked_reasons:
            normalized["blocked_reasons"] = blocked_reasons

        false_breakout_risk = str(normalized.get("false_breakout_risk") or "").strip().lower()
        flow_signal = str(normalized.get("flow_signal") or "").strip().lower()
        if false_breakout_risk == "high" and flow_signal in {"neutral", "conflicting", ""}:
            normalized["confidence_cap"] = None

        return normalized

    def _normalize_chain_symbol_analysis(
        self,
        symbol_analysis: dict[str, Any],
        signal: dict[str, Any] | None,
    ) -> dict[str, Any]:
        normalized = dict(symbol_analysis)
        blocked_reasons = [
            str(reason).strip().lower()
            for reason in normalized.get("blocked_reasons", [])
            if str(reason).strip()
        ]
        if blocked_reasons:
            normalized["blocked_reasons"] = blocked_reasons

        if (
            normalized.get("trade_allowed") is False
            and blocked_reasons
            and all(classify_reason_token(reason) == "soft" for reason in blocked_reasons)
        ):
            normalized["trade_allowed"] = True

        if not isinstance(signal, dict):
            return normalized

        normalized = self._normalize_chain_gamma_pin_fields(normalized, signal)

        if normalized.get("hard_block"):
            return normalized

        liquidity_tier = str(normalized.get("liquidity_tier") or "").upper()
        if liquidity_tier == "L5":
            return normalized

        supportive_candidates = self._supportive_execution_candidates(signal)
        if not supportive_candidates:
            return normalized

        liquidity_tier = self._reconcile_chain_liquidity_tier(
            liquidity_tier,
            signal,
            supportive_candidates,
        )
        normalized["liquidity_tier"] = liquidity_tier

        if liquidity_tier in {"L1", "L2", "L3", "L4"}:
            normalized["liquidity_ok"] = True

        if "insufficient_leg_liquidity" in blocked_reasons:
            blocked_reasons = [reason for reason in blocked_reasons if reason != "insufficient_leg_liquidity"]
            normalized["blocked_reasons"] = blocked_reasons
            if normalized.get("trade_allowed") is False and (
                not blocked_reasons or all(classify_reason_token(reason) == "soft" for reason in blocked_reasons)
            ):
                normalized["trade_allowed"] = True

        return normalized

    def _normalize_chain_gamma_pin_fields(
        self,
        symbol_analysis: dict[str, Any],
        signal: dict[str, Any],
    ) -> dict[str, Any]:
        normalized = dict(symbol_analysis)

        try:
            front_expiry_dte = int(normalized.get("front_expiry_dte"))
        except (TypeError, ValueError):
            option_vol_surface = signal.get("option_vol_surface", {})
            if isinstance(option_vol_surface, dict):
                try:
                    front_expiry_dte = int(option_vol_surface.get("front_expiry_dte"))
                except (TypeError, ValueError):
                    front_expiry_dte = None
            else:
                front_expiry_dte = None

        try:
            pin_strength = float(normalized.get("pin_strength"))
        except (TypeError, ValueError):
            pin_strength = None

        option_greeks = signal.get("option_greeks", {})
        if isinstance(option_greeks, dict):
            try:
                gamma_peak_strike = float(option_greeks.get("gamma_peak_strike"))
            except (TypeError, ValueError):
                gamma_peak_strike = None
        else:
            gamma_peak_strike = None

        price = signal.get("price", {})
        if isinstance(price, dict):
            try:
                spot = float(price.get("close_price"))
            except (TypeError, ValueError):
                spot = None
        else:
            spot = None

        gamma_pin_active = (
            front_expiry_dte is not None
            and front_expiry_dte <= 5
            and pin_strength is not None
            and pin_strength > 0.45
            and gamma_peak_strike is not None
            and spot is not None
            and spot > 0
            and abs(gamma_peak_strike - spot) / spot <= 0.012
        )

        normalized["gamma_pin_active"] = gamma_pin_active
        normalized["gamma_pin_strike"] = gamma_peak_strike if gamma_pin_active else None
        return normalized

    def _reconcile_chain_liquidity_tier(
        self,
        liquidity_tier: str,
        signal: dict[str, Any],
        supportive_candidates: list[dict[str, Any]],
    ) -> str:
        """Correct clearly over-conservative Chain liquidity tiers using explicit upstream evidence."""
        if liquidity_tier != "L4" or len(supportive_candidates) < 2:
            return liquidity_tier

        option_chain = signal.get("option_chain", {})
        if not isinstance(option_chain, dict):
            return liquidity_tier

        liquidity_profile = option_chain.get("liquidity_profile", {})
        if not isinstance(liquidity_profile, dict):
            return liquidity_tier

        profile_name = str(liquidity_profile.get("profile_name") or "").strip().lower()
        if profile_name == "deep_liquidity":
            return "L3"

        return liquidity_tier

    def _supportive_execution_candidates(self, signal: dict[str, Any]) -> list[dict[str, Any]]:
        option_chain = signal.get("option_chain", {})
        if not isinstance(option_chain, dict):
            option_chain = {}
        liquidity_profile = option_chain.get("liquidity_profile", {})
        if not isinstance(liquidity_profile, dict):
            liquidity_profile = {}
        max_worst_leg_ratio = liquidity_profile.get("max_worst_leg_bid_ask_spread_ratio", 0.20)
        try:
            ratio_cap = float(max_worst_leg_ratio)
        except (TypeError, ValueError):
            ratio_cap = 0.20

        option_spreads = signal.get("option_spreads", {})
        if not isinstance(option_spreads, dict):
            return []
        execution_candidates = option_spreads.get("execution_candidates", {})
        if not isinstance(execution_candidates, dict):
            return []

        supportive: list[dict[str, Any]] = []
        for candidate in execution_candidates.values():
            if not isinstance(candidate, dict):
                continue
            if candidate.get("candidate_available") is not True:
                continue
            worst_ratio = candidate.get("worst_leg_bid_ask_spread_ratio")
            try:
                worst_ratio_value = float(worst_ratio)
            except (TypeError, ValueError):
                continue
            if worst_ratio_value <= ratio_cap:
                supportive.append(candidate)
        return supportive

    def _selected_execution_candidate(
        self,
        signal: dict[str, Any],
        strategy_type: Any,
    ) -> dict[str, Any] | None:
        canonical = self._canonical_emitted_strategy_type(strategy_type)
        if canonical is None:
            return None

        option_spreads = signal.get("option_spreads", {})
        if not isinstance(option_spreads, dict):
            return None
        execution_candidates = option_spreads.get("execution_candidates", {})
        if not isinstance(execution_candidates, dict):
            return None

        for candidate_key in self._EMITTED_STRATEGY_CANDIDATE_KEYS.get(canonical, ()):
            candidate = execution_candidates.get(candidate_key)
            if isinstance(candidate, dict):
                return candidate
        return None

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
