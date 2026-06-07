from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from shared.models.blueprint import SymbolPlan


@dataclass(frozen=True)
class PlanCandidate:
    plan: SymbolPlan
    chunk_index: int
    original_order: int
    quality_score: float
    chunk_id: str | None = None
    agent_outputs: dict[str, Any] | None = None
    signal_feature: Any | None = None


@dataclass(frozen=True)
class PositionContext:
    open_underlyings: set[str]
    counts_by_underlying: dict[str, int]
    direction_by_underlying: dict[str, str]
    total_positions: int
    direction_counts: dict[str, int]


class PortfolioSelector:
    """Deterministic post-merge selector for chunked blueprints."""

    _DEFAULT_SIMPLE_STRUCTURE_TYPES = frozenset({"single_leg", "vertical_spread", "iron_condor", "calendar_spread"})

    _STRATEGY_IMPACT_WEIGHTS = {
        "single_leg": 1.0,
        "vertical_spread": 0.75,
        "calendar_spread": 0.65,
        "diagonal_spread": 0.7,
        "iron_condor": 0.55,
        "iron_butterfly": 0.55,
        "butterfly": 0.6,
        "straddle": 0.95,
        "strangle": 0.95,
        "covered_call": 0.65,
        "protective_put": 0.7,
        "collar": 0.6,
    }

    _PRECISION_FIRST_COMPLEXITY_PENALTIES = {
        "single_leg": 0.0,
        "vertical_spread": 0.05,
        "covered_call": 0.12,
        "protective_put": 0.12,
        "collar": 0.16,
        "calendar_spread": 0.2,
        "diagonal_spread": 0.22,
        "butterfly": 0.24,
        "iron_condor": 0.28,
        "iron_butterfly": 0.28,
        "straddle": 0.22,
        "strangle": 0.22,
    }

    _PRECISION_FIRST_AGENT_NAMES = ("trend", "volatility", "flow", "chain", "spread")
    _EVENT_RISK_AGENT_NAMES = ("trend", "volatility", "flow", "chain", "spread", "cross_asset")
    _SIGNAL_TYPE_AGENT_NAMES = ("trend", "volatility", "cross_asset")

    _EXECUTION_CANDIDATE_STRATEGY_KEYS = {
        "vertical_spread": ("vertical",),
        "iron_condor": ("iron_condor",),
        "calendar_spread": ("calendar",),
        "diagonal_spread": ("calendar", "reverse_calendar"),
        "butterfly": ("butterfly",),
        "iron_butterfly": ("butterfly",),
    }

    def build_review_inputs(
        self,
        *,
        candidates: list[PlanCandidate],
        trade_symbols: set[str],
        precision_first_enabled: bool = False,
        allowed_strategy_types: list[str] | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        position_context = PositionContext(
            open_underlyings=set(),
            counts_by_underlying={},
            direction_by_underlying={},
            total_positions=0,
            direction_counts={"bullish": 0, "bearish": 0, "neutral": 0},
        )
        normalized_allowed_strategy_types = self._normalize_allowed_strategy_types(allowed_strategy_types)
        candidate_summaries = [
            self._review_candidate_summary(
                candidate,
                position_context,
                precision_first_enabled=precision_first_enabled,
                allowed_strategy_types=normalized_allowed_strategy_types,
            )
            for candidate in candidates
        ]
        selector_metadata = {
            "candidate_count": len(candidates),
            "raw_candidate_entry_count": len(candidates),
            "unique_candidate_symbol_count": len({candidate.plan.underlying.upper() for candidate in candidates}),
            "trade_symbols": sorted(trade_symbols),
            "ranking_scope": "symbol_level",
            "candidate_entries_may_repeat_symbols": True,
            "ranking_method": self._ranking_method(precision_first_enabled),
            "precision_first_enabled": precision_first_enabled,
            "allowed_strategy_types": normalized_allowed_strategy_types,
            "deterministic_sort_priority": self._deterministic_sort_priority(precision_first_enabled),
            "available_ranking_signals": [
                "symbol",
                "strategy_type",
                "direction",
                "machine_readable_gate_ok",
                "confidence",
                "data_quality_score",
                "max_contracts",
                "candidate_ref",
                "selector_base_score",
                "portfolio_impact_score",
                "portfolio_impact_breakdown",
                "execution_candidate_score",
                "execution_candidate_breakdown",
                "precision_first_score",
                "precision_first_breakdown",
                "master_override",
                "effective_size_modifier",
                "arb_opportunity",
                "arb_priority",
                "event_risk_present",
                "event_risk_agents",
                "earnings_proximity_days",
                "signal_type",
                "single_indicator_agents",
            ],
        }
        return candidate_summaries, selector_metadata

    def select(
        self,
        *,
        candidates: list[PlanCandidate],
        trade_symbols: set[str],
        chunk_limits: list[dict[str, Any]],
        max_output_plans: int = 10,
        llm_review: dict[str, Any] | None = None,
        precision_first_enabled: bool = False,
        allowed_strategy_types: list[str] | None = None,
    ) -> tuple[list[SymbolPlan], dict[str, Any]]:
        position_context = PositionContext(
            open_underlyings=set(),
            counts_by_underlying={},
            direction_by_underlying={},
            total_positions=0,
            direction_counts={"bullish": 0, "bearish": 0, "neutral": 0},
        )
        llm_rank_positions = self._llm_rank_positions(llm_review)
        llm_selected_symbols = self._llm_selected_symbols(llm_review)
        normalized_allowed_strategy_types = self._normalize_allowed_strategy_types(allowed_strategy_types)
        filtered_candidates = [
            candidate for candidate in candidates
            if candidate.plan.underlying.upper() in trade_symbols
        ]
        eligible_candidates: list[PlanCandidate] = []
        machine_readable_filtered_candidates: list[PlanCandidate] = []
        for candidate in filtered_candidates:
            if self._machine_readable_gate_ok(
                candidate,
                allowed_strategy_types=normalized_allowed_strategy_types,
            ):
                eligible_candidates.append(candidate)
                continue
            machine_readable_filtered_candidates.append(candidate)

        candidates_by_symbol: dict[str, list[PlanCandidate]] = {}
        for candidate in eligible_candidates:
            candidates_by_symbol.setdefault(candidate.plan.underlying.upper(), []).append(candidate)

        deduped_candidates: list[PlanCandidate] = []
        decisions: list[dict[str, Any]] = []
        duplicate_symbols: dict[str, dict[str, Any]] = {}

        for symbol, symbol_candidates in candidates_by_symbol.items():
            ranked_candidates = sorted(
                symbol_candidates,
                key=lambda candidate: self._candidate_sort_key(
                    candidate,
                    position_context,
                    llm_rank_positions,
                    precision_first_enabled=precision_first_enabled,
                    allowed_strategy_types=normalized_allowed_strategy_types,
                ),
                reverse=True,
            )
            winner = ranked_candidates[0]
            deduped_candidates.append(winner)
            winner_score = self._candidate_score(
                winner,
                position_context,
                precision_first_enabled=precision_first_enabled,
                allowed_strategy_types=normalized_allowed_strategy_types,
            )
            winner_impact = self._portfolio_impact_score(winner, position_context)
            winner_breakdown = self._portfolio_impact_breakdown(winner, position_context)
            winner_precision_score = self._precision_first_score(
                winner,
                precision_first_enabled=precision_first_enabled,
                allowed_strategy_types=normalized_allowed_strategy_types,
            )
            winner_precision_breakdown = self._precision_first_breakdown(
                winner,
                precision_first_enabled=precision_first_enabled,
                allowed_strategy_types=normalized_allowed_strategy_types,
            )
            winner_llm_rank = llm_rank_positions.get(symbol)

            losers = ranked_candidates[1:]
            duplicate_symbols[symbol] = {
                "candidate_count": len(symbol_candidates),
                "selected_chunk_index": winner.chunk_index,
                "selected_chunk_id": winner.chunk_id,
                "selected_score": winner_score,
                "selected_portfolio_impact_score": winner_impact,
                "selected_portfolio_impact_breakdown": winner_breakdown,
                "selected_precision_first_score": winner_precision_score,
                "selected_precision_first_breakdown": winner_precision_breakdown,
                "selected_llm_rank": winner_llm_rank,
                "dropped_chunk_indexes": [candidate.chunk_index for candidate in losers],
                "dropped_chunk_ids": [candidate.chunk_id for candidate in losers],
            }

            decisions.append({
                "symbol": symbol,
                "action": "kept",
                "reason": "best_candidate_after_dedup",
                "score": winner_score,
                "confidence": round(winner.plan.confidence, 6),
                "data_quality_score": round(winner.quality_score, 6),
                "portfolio_impact_score": winner_impact,
                "portfolio_impact_breakdown": winner_breakdown,
                "precision_first_score": winner_precision_score,
                "precision_first_breakdown": winner_precision_breakdown,
                "llm_rank": winner_llm_rank,
                "chunk_index": winner.chunk_index,
                "chunk_id": winner.chunk_id,
                "duplicates_dropped": len(losers),
            })

        ranked_plans = sorted(
            deduped_candidates,
            key=lambda candidate: self._candidate_sort_key(
                candidate,
                position_context,
                llm_rank_positions,
                precision_first_enabled=precision_first_enabled,
                allowed_strategy_types=normalized_allowed_strategy_types,
            ),
            reverse=True,
        )
        try:
            normalized_max_output_plans = max(1, int(max_output_plans))
        except (TypeError, ValueError):
            normalized_max_output_plans = 10

        selected_candidates = ranked_plans[:normalized_max_output_plans]
        selected_plans = [candidate.plan for candidate in selected_candidates]
        selected_symbols = [plan.underlying.upper() for plan in selected_plans]
        filtered_symbols = [candidate.plan.underlying.upper() for candidate in ranked_plans[normalized_max_output_plans:]]

        metadata = {
            "selector_version": "v3",
            "selection_mode": "dedupe_and_rank_all",
            "ranking_method": self._ranking_method(precision_first_enabled),
            "input_plan_count": len(candidates),
            "trade_candidate_count": len(filtered_candidates),
            "eligible_candidate_count": len(eligible_candidates),
            "deduped_plan_count": len(deduped_candidates),
            "output_plan_count": len(selected_plans),
            "max_output_plans": normalized_max_output_plans,
            "selected_symbols": selected_symbols,
            "filtered_symbols": filtered_symbols,
            "machine_readable_filtered_candidate_count": len(machine_readable_filtered_candidates),
            "machine_readable_filtered_symbols": sorted({
                candidate.plan.underlying.upper() for candidate in machine_readable_filtered_candidates
            }),
            "machine_readable_filtered_candidates": [
                {
                    "symbol": candidate.plan.underlying.upper(),
                    "chunk_index": candidate.chunk_index,
                    "chunk_id": candidate.chunk_id,
                    "precision_first_breakdown": self._precision_first_breakdown(
                        candidate,
                        precision_first_enabled=precision_first_enabled,
                        allowed_strategy_types=normalized_allowed_strategy_types,
                    ),
                }
                for candidate in machine_readable_filtered_candidates
            ],
            "ranked_symbols": [candidate.plan.underlying.upper() for candidate in ranked_plans],
            "precision_first_enabled": precision_first_enabled,
            "allowed_strategy_types": normalized_allowed_strategy_types,
            "deterministic_sort_priority": self._deterministic_sort_priority(precision_first_enabled),
            "llm_review": {
                "used": bool(llm_review),
                "ranking": list(llm_rank_positions.keys()),
                "selected_symbols": sorted(llm_selected_symbols),
                "portfolio_summary": (llm_review or {}).get("portfolio_summary", ""),
                "risk_notes": (llm_review or {}).get("risk_notes", []),
                "conflict_explanations": (llm_review or {}).get("conflict_explanations", []),
            },
            "duplicate_symbols": duplicate_symbols,
            "decisions": decisions,
            "chunk_limit_proposals": chunk_limits,
            "output_targets": {
                "max_total_positions": {
                    "value": normalized_max_output_plans,
                    "source": "configured_max_output_plans",
                    "configured_cap": normalized_max_output_plans,
                    "chunk_proposals": [limit.get("max_total_positions") for limit in chunk_limits],
                },
                "selected_plan_count": {
                    "value": len(selected_plans),
                    "source": "post_merge_selection",
                },
            },
        }
        return selected_plans, metadata

    def _review_candidate_summary(
        self,
        candidate: PlanCandidate,
        position_context: PositionContext,
        *,
        precision_first_enabled: bool,
        allowed_strategy_types: list[str],
    ) -> dict[str, Any]:
        portfolio_impact_breakdown = self._portfolio_impact_breakdown(candidate, position_context)
        execution_candidate_breakdown = self._execution_candidate_breakdown(candidate)
        precision_first_breakdown = self._precision_first_breakdown(
            candidate,
            precision_first_enabled=precision_first_enabled,
            allowed_strategy_types=allowed_strategy_types,
        )
        cross_asset_analysis = self._symbol_agent_analysis(candidate, "cross_asset") or {}
        spread_analysis = self._symbol_agent_analysis(candidate, "spread") or {}
        event_risk_agents = self._candidate_event_risk_agents(candidate)
        single_indicator_agents = self._candidate_single_indicator_agents(candidate)
        effective_size_modifier = self._as_float(cross_asset_analysis.get("effective_size_modifier"))
        if effective_size_modifier is None:
            effective_size_modifier = 1.0
        arb_priority_raw = spread_analysis.get("arb_priority")
        try:
            arb_priority = int(arb_priority_raw) if arb_priority_raw is not None else 0
        except (TypeError, ValueError):
            arb_priority = 0
        return {
            "symbol": candidate.plan.underlying.upper(),
            "strategy_type": candidate.plan.strategy_type.value,
            "direction": candidate.plan.direction.value,
            "machine_readable_gate_ok": self._machine_readable_gate_ok(
                candidate,
                allowed_strategy_types=allowed_strategy_types,
            ),
            "confidence": round(candidate.plan.confidence, 6),
            "data_quality_score": round(candidate.quality_score, 6),
            "max_contracts": candidate.plan.max_contracts,
            "chunk_index": candidate.chunk_index,
            "chunk_id": candidate.chunk_id,
            "original_order": candidate.original_order,
            "candidate_ref": self._candidate_ref(candidate),
            "selector_base_score": self._candidate_score(
                candidate,
                position_context,
                precision_first_enabled=precision_first_enabled,
                allowed_strategy_types=allowed_strategy_types,
            ),
            "portfolio_impact_score": portfolio_impact_breakdown["portfolio_impact_score"],
            "portfolio_impact_breakdown": portfolio_impact_breakdown,
            "execution_candidate_score": execution_candidate_breakdown["execution_candidate_score"],
            "execution_candidate_breakdown": execution_candidate_breakdown,
            "precision_first_score": precision_first_breakdown["precision_first_score"],
            "precision_first_breakdown": precision_first_breakdown,
            "master_override": bool(cross_asset_analysis.get("master_override", False)),
            "effective_size_modifier": round(effective_size_modifier, 6),
            "arb_opportunity": bool(spread_analysis.get("arb_opportunity", False)),
            "arb_priority": arb_priority,
            "event_risk_present": bool(event_risk_agents),
            "event_risk_agents": event_risk_agents,
            "earnings_proximity_days": self._candidate_earnings_proximity_days(candidate),
            "signal_type": "single_indicator" if single_indicator_agents else "multi_indicator",
            "single_indicator_agents": single_indicator_agents,
        }

    def _candidate_score(
        self,
        candidate: PlanCandidate,
        position_context: PositionContext,
        *,
        precision_first_enabled: bool,
        allowed_strategy_types: list[str],
    ) -> float:
        portfolio_impact_score = self._portfolio_impact_score(candidate, position_context)
        precision_first_score = self._precision_first_score(
            candidate,
            precision_first_enabled=precision_first_enabled,
            allowed_strategy_types=allowed_strategy_types,
        )
        return round(
            candidate.plan.confidence * 0.5
            + candidate.quality_score * 0.15
            + portfolio_impact_score * 0.15
            + precision_first_score * 0.20,
            6,
        )

    def _candidate_sort_key(
        self,
        candidate: PlanCandidate,
        position_context: PositionContext,
        llm_rank_positions: dict[str, int],
        *,
        precision_first_enabled: bool,
        allowed_strategy_types: list[str],
    ) -> tuple[float, float, float, float, float, float, int]:
        symbol = candidate.plan.underlying.upper()
        llm_priority = 0.0
        if symbol in llm_rank_positions:
            llm_priority = round(1.0 / (llm_rank_positions[symbol] + 1), 6)
        machine_readable_gate_ok = 1.0 if self._machine_readable_gate_ok(
            candidate,
            allowed_strategy_types=allowed_strategy_types,
        ) else 0.0
        precision_first_score = self._precision_first_score(
            candidate,
            precision_first_enabled=precision_first_enabled,
            allowed_strategy_types=allowed_strategy_types,
        )
        return (
            machine_readable_gate_ok,
            precision_first_score,
            llm_priority,
            self._candidate_score(
                candidate,
                position_context,
                precision_first_enabled=precision_first_enabled,
                allowed_strategy_types=allowed_strategy_types,
            ),
            round(candidate.plan.confidence, 6),
            round(candidate.quality_score, 6),
            self._portfolio_impact_score(candidate, position_context),
            -candidate.original_order,
        )

    def _precision_first_score(
        self,
        candidate: PlanCandidate,
        *,
        precision_first_enabled: bool,
        allowed_strategy_types: list[str],
    ) -> float:
        return self._precision_first_breakdown(
            candidate,
            precision_first_enabled=precision_first_enabled,
            allowed_strategy_types=allowed_strategy_types,
        )["precision_first_score"]

    def _precision_first_breakdown(
        self,
        candidate: PlanCandidate,
        *,
        precision_first_enabled: bool,
        allowed_strategy_types: list[str],
    ) -> dict[str, Any]:
        strategy_type = str(getattr(candidate.plan.strategy_type, "value", candidate.plan.strategy_type)).lower()
        allowed_set = {item.lower() for item in allowed_strategy_types}
        simple_structure_types = self._configured_simple_structure_types(allowed_strategy_types)
        execution_candidate_breakdown = self._execution_candidate_breakdown(candidate)
        execution_candidate_score = execution_candidate_breakdown["execution_candidate_score"]
        execution_candidate_adjustment = round((execution_candidate_score - 0.5) * 0.4, 6)

        if not precision_first_enabled:
            return {
                "precision_first_score": 1.0,
                "strategy_type": strategy_type,
                "allowed_strategy_types": sorted(allowed_set),
                "strategy_scope_penalty": 0.0,
                "complexity_penalty": 0.0,
                "trade_block_penalty": 0.0,
                "confidence_cap_penalty": 0.0,
                "simple_structure_penalty": 0.0,
                "blocked_reason_penalty": 0.0,
                "trade_blocked_agents": [],
                "simple_structure_conflict_agents": [],
                "confidence_caps": {},
                "blocked_reasons": [],
                "total_penalty": 0.0,
                "execution_candidate_score": round(execution_candidate_score, 6),
                "execution_candidate_adjustment": 0.0,
                "execution_candidate_breakdown": execution_candidate_breakdown,
            }

        strategy_scope_penalty = 0.0
        if allowed_set and strategy_type not in allowed_set:
            strategy_scope_penalty = 0.75

        complexity_penalty = self._PRECISION_FIRST_COMPLEXITY_PENALTIES.get(strategy_type, 0.24)
        trade_blocked_agents: list[str] = []
        simple_structure_conflict_agents: list[str] = []
        confidence_caps: dict[str, float] = {}
        blocked_reasons: list[str] = []

        for agent_name in self._PRECISION_FIRST_AGENT_NAMES:
            symbol_analysis = self._symbol_agent_analysis(candidate, agent_name)
            if not symbol_analysis:
                continue

            if symbol_analysis.get("trade_allowed") is False:
                trade_blocked_agents.append(agent_name)

            confidence_cap = symbol_analysis.get("confidence_cap")
            if isinstance(confidence_cap, (int, float)):
                confidence_caps[agent_name] = round(float(confidence_cap), 6)

            if symbol_analysis.get("simple_structures_only") and strategy_type not in simple_structure_types:
                simple_structure_conflict_agents.append(agent_name)

            reasons = symbol_analysis.get("blocked_reasons")
            if isinstance(reasons, list):
                blocked_reasons.extend(
                    str(reason).strip()
                    for reason in reasons
                    if str(reason).strip()
                )

        trade_block_penalty = min(0.75, len(trade_blocked_agents) * 0.35)

        min_confidence_cap = min(confidence_caps.values(), default=None)
        confidence_cap_penalty = 0.0
        if min_confidence_cap is not None and candidate.plan.confidence > min_confidence_cap:
            confidence_cap_penalty = min(0.25, round(candidate.plan.confidence - min_confidence_cap, 6))

        simple_structure_penalty = min(0.3, len(simple_structure_conflict_agents) * 0.14)
        unique_blocked_reasons = sorted(set(blocked_reasons))
        blocked_reason_penalty = min(0.12, len(unique_blocked_reasons) * 0.03)
        total_penalty = min(
            0.98,
            round(
                strategy_scope_penalty
                + complexity_penalty
                + trade_block_penalty
                + confidence_cap_penalty
                + simple_structure_penalty
                + blocked_reason_penalty,
                6,
            ),
        )

        return {
            "precision_first_score": round(max(0.0, min(1.0, 1.0 - total_penalty + execution_candidate_adjustment)), 6),
            "strategy_type": strategy_type,
            "allowed_strategy_types": sorted(allowed_set),
            "strategy_scope_penalty": round(strategy_scope_penalty, 6),
            "complexity_penalty": round(complexity_penalty, 6),
            "trade_block_penalty": round(trade_block_penalty, 6),
            "confidence_cap_penalty": round(confidence_cap_penalty, 6),
            "simple_structure_penalty": round(simple_structure_penalty, 6),
            "blocked_reason_penalty": round(blocked_reason_penalty, 6),
            "trade_blocked_agents": trade_blocked_agents,
            "simple_structure_conflict_agents": simple_structure_conflict_agents,
            "confidence_caps": confidence_caps,
            "blocked_reasons": unique_blocked_reasons,
            "total_penalty": round(total_penalty, 6),
            "execution_candidate_score": round(execution_candidate_score, 6),
            "execution_candidate_adjustment": round(execution_candidate_adjustment, 6),
            "execution_candidate_breakdown": execution_candidate_breakdown,
        }

    def _execution_candidate_breakdown(self, candidate: PlanCandidate) -> dict[str, Any]:
        strategy_type = str(getattr(candidate.plan.strategy_type, "value", candidate.plan.strategy_type)).lower()
        candidate_keys = self._EXECUTION_CANDIDATE_STRATEGY_KEYS.get(strategy_type, ())

        if not candidate_keys:
            return {
                "execution_candidate_score": 0.5,
                "strategy_type": strategy_type,
                "candidate_key": None,
                "candidate_available": False,
                "metric_name": None,
                "metric_value": None,
                "worst_leg_bid_ask_spread_ratio": None,
                "term_structure_slope": None,
                "reason": "strategy_not_execution_candidate_scored",
            }

        spread_execution_inputs = self._spread_execution_inputs(candidate)
        if not spread_execution_inputs:
            return {
                "execution_candidate_score": 0.25,
                "strategy_type": strategy_type,
                "candidate_key": candidate_keys[0],
                "candidate_available": False,
                "metric_name": None,
                "metric_value": None,
                "worst_leg_bid_ask_spread_ratio": None,
                "term_structure_slope": self._term_structure_slope(candidate),
                "reason": "execution_candidates_missing",
            }

        scored_candidates = [
            self._score_execution_candidate(candidate, strategy_type, candidate_key, spread_execution_inputs.get(candidate_key))
            for candidate_key in candidate_keys
            if spread_execution_inputs.get(candidate_key) is not None
        ]
        if not scored_candidates:
            return {
                "execution_candidate_score": 0.25,
                "strategy_type": strategy_type,
                "candidate_key": candidate_keys[0],
                "candidate_available": False,
                "metric_name": None,
                "metric_value": None,
                "worst_leg_bid_ask_spread_ratio": None,
                "term_structure_slope": self._term_structure_slope(candidate),
                "reason": "relevant_execution_candidate_missing",
            }

        best = max(scored_candidates, key=lambda item: item["execution_candidate_score"])
        best["strategy_type"] = strategy_type
        return best

    def _score_execution_candidate(
        self,
        candidate: PlanCandidate,
        strategy_type: str,
        candidate_key: str,
        execution_candidate: Any,
    ) -> dict[str, Any]:
        data = self._normalize_execution_candidate(execution_candidate)
        slope = self._term_structure_slope(candidate)
        candidate_available = bool(data.get("candidate_available", False))
        worst_leg_ratio = self._as_float(data.get("worst_leg_bid_ask_spread_ratio"))
        liquidity_penalty = 0.0
        if worst_leg_ratio is not None:
            if worst_leg_ratio > 0.2:
                liquidity_penalty = 0.3
            elif worst_leg_ratio > 0.1:
                liquidity_penalty = 0.1

        metric_name: str | None = None
        metric_value: float | None = None
        score = 0.25 if candidate_available else 0.15
        reason = "candidate_unavailable"

        if candidate_available:
            if strategy_type == "vertical_spread":
                metric_name = "effective_rr"
                metric_value = self._as_float(data.get("effective_rr"))
                if metric_value is None:
                    metric_name = "raw_rr"
                    metric_value = self._as_float(data.get("raw_rr"))
                if metric_value is None:
                    score = 0.25
                    reason = "vertical_rr_missing"
                elif metric_value < 0.7:
                    score = 0.1
                    reason = "vertical_rr_below_floor"
                elif metric_value >= 1.2:
                    score = 1.0
                    reason = "vertical_rr_strong"
                else:
                    score = 0.55 + ((metric_value - 0.7) / 0.5) * 0.45
                    reason = "vertical_rr_acceptable"
            elif strategy_type == "iron_condor":
                metric_name = "effective_rr"
                metric_value = self._as_float(data.get("effective_rr"))
                if metric_value is None:
                    metric_name = "raw_rr"
                    metric_value = self._as_float(data.get("raw_rr"))
                if metric_value is None:
                    score = 0.25
                    reason = "iron_condor_rr_missing"
                elif 0.4 <= metric_value <= 0.6:
                    score = 1.0
                    reason = "iron_condor_rr_optimal"
                elif 0.3 <= metric_value <= 0.8:
                    score = 0.8
                    reason = "iron_condor_rr_supported"
                elif 0.2 <= metric_value <= 1.0:
                    score = 0.55
                    reason = "iron_condor_rr_marginal"
                else:
                    score = 0.15
                    reason = "iron_condor_rr_outside_band"
            elif strategy_type in {"calendar_spread", "diagonal_spread"} and candidate_key == "calendar":
                metric_name = "effective_theta_capture_per_day"
                metric_value = self._as_float(data.get("effective_theta_capture_per_day"))
                if metric_value is None:
                    score = 0.25
                    reason = "calendar_theta_missing"
                elif slope is not None and slope <= 0:
                    score = 0.1
                    reason = "calendar_term_structure_misaligned"
                elif metric_value <= 0:
                    score = 0.1
                    reason = "calendar_theta_not_positive"
                else:
                    score = 0.55 + min(metric_value / 0.05, 1.0) * 0.45
                    reason = "calendar_theta_supported"
            elif strategy_type == "diagonal_spread" and candidate_key == "reverse_calendar":
                metric_name = "effective_theta_capture_per_day"
                metric_value = self._as_float(data.get("effective_theta_capture_per_day"))
                if metric_value is None:
                    score = 0.25
                    reason = "reverse_calendar_theta_missing"
                elif slope is not None and slope >= -0.03:
                    score = 0.1
                    reason = "reverse_calendar_term_structure_misaligned"
                elif metric_value <= 0:
                    score = 0.1
                    reason = "reverse_calendar_theta_not_positive"
                else:
                    score = 0.55 + min(metric_value / 0.05, 1.0) * 0.45
                    reason = "reverse_calendar_theta_supported"
            elif strategy_type in {"butterfly", "iron_butterfly"}:
                metric_name = "pricing_error"
                metric_value = self._as_float(data.get("pricing_error"))
                if metric_value is None:
                    score = 0.25
                    reason = "butterfly_pricing_missing"
                elif metric_value > 0.12:
                    score = 1.0
                    reason = "butterfly_pricing_strong"
                elif metric_value >= 0.08:
                    score = 0.8
                    reason = "butterfly_pricing_supported"
                else:
                    score = 0.2
                    reason = "butterfly_pricing_below_threshold"

        final_score = max(0.0, min(1.0, round(score - liquidity_penalty, 6)))
        return {
            "execution_candidate_score": final_score,
            "candidate_key": candidate_key,
            "candidate_available": candidate_available,
            "metric_name": metric_name,
            "metric_value": round(metric_value, 6) if metric_value is not None else None,
            "worst_leg_bid_ask_spread_ratio": round(worst_leg_ratio, 6) if worst_leg_ratio is not None else None,
            "term_structure_slope": round(slope, 6) if slope is not None else None,
            "reason": reason,
        }

    def _spread_execution_inputs(self, candidate: PlanCandidate) -> dict[str, dict[str, Any]]:
        signal_feature = candidate.signal_feature
        if signal_feature is None:
            return {}
        option_indicators = getattr(signal_feature, "option_indicators", None)
        if option_indicators is None:
            return {}
        spread_execution_inputs = getattr(option_indicators, "spread_execution_inputs", None)
        if not isinstance(spread_execution_inputs, dict):
            return {}
        normalized: dict[str, dict[str, Any]] = {}
        for name, entry in spread_execution_inputs.items():
            normalized[str(name)] = self._normalize_execution_candidate(entry)
        return normalized

    def _normalize_execution_candidate(self, entry: Any) -> dict[str, Any]:
        if hasattr(entry, "model_dump"):
            return entry.model_dump(mode="json")
        if isinstance(entry, dict):
            return entry
        return {}

    def _term_structure_slope(self, candidate: PlanCandidate) -> float | None:
        signal_feature = candidate.signal_feature
        if signal_feature is None:
            return None
        option_indicators = getattr(signal_feature, "option_indicators", None)
        if option_indicators is None:
            return None
        return self._as_float(getattr(option_indicators, "term_structure_slope", None))

    def _as_float(self, value: Any) -> float | None:
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def _candidate_ref(self, candidate: PlanCandidate) -> str:
        chunk_ref = candidate.chunk_id or f"chunk-{candidate.chunk_index}"
        return f"{candidate.plan.underlying.upper()}::{chunk_ref}::{candidate.original_order}"

    def _candidate_earnings_proximity_days(self, candidate: PlanCandidate) -> int | None:
        signal_feature = candidate.signal_feature
        if signal_feature is None:
            return None
        cross_asset_indicators = getattr(signal_feature, "cross_asset_indicators", None)
        earnings_days = getattr(cross_asset_indicators, "earnings_proximity_days", None)
        if isinstance(earnings_days, int):
            return earnings_days
        try:
            return int(earnings_days) if earnings_days is not None else None
        except (TypeError, ValueError):
            return None

    def _candidate_event_risk_agents(self, candidate: PlanCandidate) -> list[str]:
        event_risk_agents: list[str] = []
        for agent_name in self._EVENT_RISK_AGENT_NAMES:
            symbol_analysis = self._symbol_agent_analysis(candidate, agent_name)
            if symbol_analysis and symbol_analysis.get("event_risk_present"):
                event_risk_agents.append(agent_name)
        return event_risk_agents

    def _candidate_single_indicator_agents(self, candidate: PlanCandidate) -> list[str]:
        single_indicator_agents: list[str] = []
        for agent_name in self._SIGNAL_TYPE_AGENT_NAMES:
            symbol_analysis = self._symbol_agent_analysis(candidate, agent_name)
            if not symbol_analysis:
                continue
            if str(symbol_analysis.get("signal_type") or "").strip().lower() == "single_indicator":
                single_indicator_agents.append(agent_name)
        return single_indicator_agents

    def _candidate_signal_type(self, candidate: PlanCandidate) -> str:
        return "single_indicator" if self._candidate_single_indicator_agents(candidate) else "multi_indicator"

    def _machine_readable_gate_ok(self, candidate: PlanCandidate, *, allowed_strategy_types: list[str]) -> bool:
        strategy_type = str(getattr(candidate.plan.strategy_type, "value", candidate.plan.strategy_type)).lower()
        simple_structure_types = self._configured_simple_structure_types(allowed_strategy_types)

        for agent_name in self._PRECISION_FIRST_AGENT_NAMES:
            symbol_analysis = self._symbol_agent_analysis(candidate, agent_name)
            if not symbol_analysis:
                continue

            if symbol_analysis.get("trade_allowed") is False:
                return False

            confidence_cap = symbol_analysis.get("confidence_cap")
            if isinstance(confidence_cap, (int, float)) and candidate.plan.confidence > float(confidence_cap):
                return False

            if symbol_analysis.get("simple_structures_only") and strategy_type not in simple_structure_types:
                return False

        return True

    def _configured_simple_structure_types(self, allowed_strategy_types: list[str]) -> frozenset[str]:
        normalized = frozenset(
            str(strategy_type).strip().lower()
            for strategy_type in allowed_strategy_types
            if str(strategy_type).strip()
        )
        return normalized or self._DEFAULT_SIMPLE_STRUCTURE_TYPES

    def _symbol_agent_analysis(self, candidate: PlanCandidate, agent_name: str) -> dict[str, Any] | None:
        agent_outputs = candidate.agent_outputs or {}
        agent_output = agent_outputs.get(agent_name)
        if not isinstance(agent_output, dict):
            return None

        symbol_analyses = agent_output.get("symbols")
        if not isinstance(symbol_analyses, list):
            return None

        symbol = candidate.plan.underlying.upper()
        for item in symbol_analyses:
            if not isinstance(item, dict):
                continue
            item_symbol = str(item.get("symbol") or "").strip().upper()
            if item_symbol == symbol:
                return item
        return None

    def _portfolio_impact_score(self, candidate: PlanCandidate, position_context: PositionContext) -> float:
        return self._portfolio_impact_breakdown(candidate, position_context)["portfolio_impact_score"]

    def _portfolio_impact_breakdown(self, candidate: PlanCandidate, position_context: PositionContext) -> dict[str, Any]:
        plan = candidate.plan
        strategy_weight = self._STRATEGY_IMPACT_WEIGHTS.get(plan.strategy_type.value, 0.8)
        symbol = plan.underlying.upper()
        plan_direction = plan.direction.value
        total_positions = max(1, position_context.total_positions)
        symbol_position_count = position_context.counts_by_underlying.get(symbol, 0)
        symbol_existing_direction = position_context.direction_by_underlying.get(symbol, "none")
        same_direction_count = position_context.direction_counts.get(plan_direction, 0)

        strategy_penalty = round(strategy_weight * 0.18, 6)
        if isinstance(plan.max_position_size, (int, float)):
            size_penalty = round(min(0.22, max(0.0, float(plan.max_position_size)) / 1.5 * 0.22), 6)
        else:
            size_penalty = 0.0
        contracts_penalty = round(min(0.18, max(1, int(plan.max_contracts)) / 4 * 0.18), 6)
        existing_underlying_penalty = round(0.12 if symbol_position_count > 0 else 0.0, 6)

        same_direction_penalty = 0.0
        if symbol_position_count > 0:
            if symbol_existing_direction == plan_direction and plan_direction in {"bullish", "bearish"}:
                same_direction_penalty = 0.12
            elif symbol_existing_direction != "none" and symbol_existing_direction != plan_direction:
                same_direction_penalty = -0.05

        concentration_penalty = 0.0
        if plan_direction in {"bullish", "bearish"} and same_direction_count > 0:
            concentration_penalty = min(0.18, (same_direction_count / total_positions) * 0.18)

        total_penalty = max(
            0.0,
            strategy_penalty
            + size_penalty
            + contracts_penalty
            + existing_underlying_penalty
            + same_direction_penalty
            + concentration_penalty,
        )
        portfolio_impact_score = round(max(0.0, 1.0 - total_penalty), 6)

        return {
            "portfolio_impact_score": portfolio_impact_score,
            "strategy_penalty": round(strategy_penalty, 6),
            "size_penalty": round(size_penalty, 6),
            "contracts_penalty": round(contracts_penalty, 6),
            "existing_underlying_penalty": round(existing_underlying_penalty, 6),
            "same_direction_penalty": round(same_direction_penalty, 6),
            "concentration_penalty": round(concentration_penalty, 6),
            "total_penalty": round(total_penalty, 6),
            "position_context": {
                "symbol_existing_direction": symbol_existing_direction,
                "symbol_position_count": symbol_position_count,
                "same_direction_count": same_direction_count,
                "total_positions": position_context.total_positions,
            },
        }

    def _llm_rank_positions(self, llm_review: dict[str, Any] | None) -> dict[str, int]:
        if not llm_review:
            return {}
        ranking = llm_review.get("ranking")
        if not isinstance(ranking, list):
            return {}
        return {
            str(symbol).strip().upper(): idx
            for idx, symbol in enumerate(ranking)
            if str(symbol).strip()
        }

    def _llm_selected_symbols(self, llm_review: dict[str, Any] | None) -> set[str]:
        if not llm_review:
            return set()
        selected_symbols = llm_review.get("selected_symbols")
        if not isinstance(selected_symbols, list):
            return set()
        return {str(symbol).strip().upper() for symbol in selected_symbols if str(symbol).strip()}

    def _normalize_allowed_strategy_types(self, allowed_strategy_types: list[str] | None) -> list[str]:
        return [str(item).lower() for item in (allowed_strategy_types or []) if str(item).strip()]

    def _ranking_method(self, precision_first_enabled: bool) -> str:
        if precision_first_enabled:
            return "precision_first_confidence_quality_portfolio_impact_weighted"
        return "confidence_quality_portfolio_impact_weighted"

    def _deterministic_sort_priority(self, precision_first_enabled: bool) -> list[str]:
        priority = ["selector_base_score", "confidence", "data_quality_score", "portfolio_impact_score", "original_order"]
        if precision_first_enabled:
            return ["machine_readable_gate_ok", "precision_first_score", *priority]
        return priority