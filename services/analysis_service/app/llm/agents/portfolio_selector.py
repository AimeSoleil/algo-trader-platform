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


@dataclass(frozen=True)
class PositionContext:
    open_underlyings: set[str]
    counts_by_underlying: dict[str, int]
    direction_by_underlying: dict[str, str]
    total_positions: int
    direction_counts: dict[str, int]


class PortfolioSelector:
    """Deterministic post-merge selector for chunked blueprints."""

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

    def build_review_inputs(
        self,
        *,
        candidates: list[PlanCandidate],
        trade_symbols: set[str],
        current_positions: dict | None = None,
        precision_first_enabled: bool = False,
        allowed_strategy_types: list[str] | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        position_context = self._build_position_context(current_positions)
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
            "trade_symbols": sorted(trade_symbols),
            "ranking_method": self._ranking_method(precision_first_enabled),
            "precision_first_enabled": precision_first_enabled,
            "allowed_strategy_types": normalized_allowed_strategy_types,
            "deterministic_sort_priority": self._deterministic_sort_priority(precision_first_enabled),
            "current_position_context": {
                "total_positions": position_context.total_positions,
                "direction_counts": position_context.direction_counts,
                "counts_by_underlying": position_context.counts_by_underlying,
                "direction_by_underlying": position_context.direction_by_underlying,
            },
        }
        return candidate_summaries, selector_metadata

    def select(
        self,
        *,
        candidates: list[PlanCandidate],
        trade_symbols: set[str],
        chunk_limits: list[dict[str, Any]],
        risk_policy: Any,
        current_positions: dict | None = None,
        previous_execution: dict | None = None,
        llm_review: dict[str, Any] | None = None,
        precision_first_enabled: bool = False,
        allowed_strategy_types: list[str] | None = None,
    ) -> tuple[list[SymbolPlan], dict[str, Any], dict[str, Any]]:
        position_context = self._build_position_context(current_positions)
        llm_rank_positions = self._llm_rank_positions(llm_review)
        llm_selected_symbols = self._llm_selected_symbols(llm_review)
        normalized_allowed_strategy_types = self._normalize_allowed_strategy_types(allowed_strategy_types)
        filtered_candidates = [
            candidate for candidate in candidates
            if candidate.plan.underlying.upper() in trade_symbols
        ]

        candidates_by_symbol: dict[str, list[PlanCandidate]] = {}
        for candidate in filtered_candidates:
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
        selected_candidates = ranked_plans
        selected_plans = [candidate.plan for candidate in selected_candidates]
        selected_symbols = [plan.underlying.upper() for plan in selected_plans]
        filtered_symbols: list[str] = []

        final_limits = {
            "max_total_positions": len(selected_plans),
            "max_daily_loss": float(risk_policy.max_daily_loss),
            "max_margin_usage": float(risk_policy.max_margin_usage),
            "portfolio_delta_limit": float(risk_policy.portfolio_delta_limit),
            "portfolio_gamma_limit": float(risk_policy.portfolio_gamma_limit),
        }

        metadata = {
            "selector_version": "v3",
            "selection_mode": "dedupe_and_rank_all",
            "ranking_method": self._ranking_method(precision_first_enabled),
            "input_plan_count": len(candidates),
            "trade_candidate_count": len(filtered_candidates),
            "deduped_plan_count": len(deduped_candidates),
            "output_plan_count": len(selected_plans),
            "selected_symbols": selected_symbols,
            "filtered_symbols": filtered_symbols,
            "ranked_symbols": [candidate.plan.underlying.upper() for candidate in ranked_plans],
            "precision_first_enabled": precision_first_enabled,
            "allowed_strategy_types": normalized_allowed_strategy_types,
            "deterministic_sort_priority": self._deterministic_sort_priority(precision_first_enabled),
            "current_position_count": self._position_count(current_positions),
            "current_position_context": {
                "total_positions": position_context.total_positions,
                "direction_counts": position_context.direction_counts,
                "counts_by_underlying": position_context.counts_by_underlying,
                "direction_by_underlying": position_context.direction_by_underlying,
            },
            "previous_execution_present": previous_execution is not None,
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
            "final_limit_sources": {
                "max_total_positions": {
                    "value": len(selected_plans),
                    "source": "selected_plan_count",
                },
                "max_daily_loss": {
                    "value": final_limits["max_daily_loss"],
                    "source": "risk_policy",
                    "chunk_proposals": [limit.get("max_daily_loss") for limit in chunk_limits],
                },
                "max_margin_usage": {
                    "value": final_limits["max_margin_usage"],
                    "source": "risk_policy",
                    "chunk_proposals": [limit.get("max_margin_usage") for limit in chunk_limits],
                },
                "portfolio_delta_limit": {
                    "value": final_limits["portfolio_delta_limit"],
                    "source": "risk_policy",
                    "chunk_proposals": [limit.get("portfolio_delta_limit") for limit in chunk_limits],
                },
                "portfolio_gamma_limit": {
                    "value": final_limits["portfolio_gamma_limit"],
                    "source": "risk_policy",
                    "chunk_proposals": [limit.get("portfolio_gamma_limit") for limit in chunk_limits],
                },
            },
        }
        return selected_plans, final_limits, metadata

    def _review_candidate_summary(
        self,
        candidate: PlanCandidate,
        position_context: PositionContext,
        *,
        precision_first_enabled: bool,
        allowed_strategy_types: list[str],
    ) -> dict[str, Any]:
        portfolio_impact_breakdown = self._portfolio_impact_breakdown(candidate, position_context)
        precision_first_breakdown = self._precision_first_breakdown(
            candidate,
            precision_first_enabled=precision_first_enabled,
            allowed_strategy_types=allowed_strategy_types,
        )
        return {
            "symbol": candidate.plan.underlying.upper(),
            "strategy_type": candidate.plan.strategy_type.value,
            "direction": candidate.plan.direction.value,
            "confidence": round(candidate.plan.confidence, 6),
            "data_quality_score": round(candidate.quality_score, 6),
            "max_position_size": candidate.plan.max_position_size,
            "max_contracts": candidate.plan.max_contracts,
            "chunk_index": candidate.chunk_index,
            "chunk_id": candidate.chunk_id,
            "original_order": candidate.original_order,
            "selector_base_score": self._candidate_score(
                candidate,
                position_context,
                precision_first_enabled=precision_first_enabled,
                allowed_strategy_types=allowed_strategy_types,
            ),
            "portfolio_impact_score": portfolio_impact_breakdown["portfolio_impact_score"],
            "portfolio_impact_breakdown": portfolio_impact_breakdown,
            "precision_first_score": precision_first_breakdown["precision_first_score"],
            "precision_first_breakdown": precision_first_breakdown,
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
        precision_first_score = self._precision_first_score(
            candidate,
            precision_first_enabled=precision_first_enabled,
            allowed_strategy_types=allowed_strategy_types,
        )
        return (
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
            }

        strategy_scope_penalty = 0.0
        if allowed_set and strategy_type not in allowed_set:
            strategy_scope_penalty = 0.75

        complexity_penalty = self._PRECISION_FIRST_COMPLEXITY_PENALTIES.get(strategy_type, 0.24)
        simple_structure_types = allowed_set or {"single_leg", "vertical_spread"}
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
            "precision_first_score": round(max(0.0, 1.0 - total_penalty), 6),
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
        }

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
        size_penalty = round(min(0.22, max(0.0, float(plan.max_position_size)) / 1.5 * 0.22), 6)
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

    def _position_count(self, current_positions: dict | None) -> int:
        if not current_positions:
            return 0
        positions = current_positions.get("positions")
        if isinstance(positions, list):
            return len(positions)
        count = current_positions.get("count")
        if isinstance(count, int):
            return count
        return 0

    def _build_position_context(self, current_positions: dict | None) -> PositionContext:
        if not current_positions:
            return PositionContext(
                open_underlyings=set(),
                counts_by_underlying={},
                direction_by_underlying={},
                total_positions=0,
                direction_counts={"bullish": 0, "bearish": 0, "neutral": 0},
            )
        positions = current_positions.get("positions")
        if not isinstance(positions, list):
            return PositionContext(
                open_underlyings=set(),
                counts_by_underlying={},
                direction_by_underlying={},
                total_positions=0,
                direction_counts={"bullish": 0, "bearish": 0, "neutral": 0},
            )
        open_underlyings: set[str] = set()
        counts_by_underlying: dict[str, int] = {}
        direction_by_underlying: dict[str, str] = {}
        direction_counts = {"bullish": 0, "bearish": 0, "neutral": 0}
        for position in positions:
            if not isinstance(position, dict):
                continue
            symbol = str(position.get("underlying") or position.get("symbol") or "").strip().upper()
            if symbol:
                open_underlyings.add(symbol)
                counts_by_underlying[symbol] = counts_by_underlying.get(symbol, 0) + 1
                direction = self._position_direction(position)
                direction_by_underlying[symbol] = direction
                direction_counts[direction] = direction_counts.get(direction, 0) + 1
        return PositionContext(
            open_underlyings=open_underlyings,
            counts_by_underlying=counts_by_underlying,
            direction_by_underlying=direction_by_underlying,
            total_positions=len(positions),
            direction_counts=direction_counts,
        )

    def _position_direction(self, position: dict[str, Any]) -> str:
        raw_direction = str(
            position.get("direction")
            or position.get("side")
            or position.get("position_side")
            or ""
        ).strip().lower()
        if raw_direction in {"bullish", "long", "buy"}:
            return "bullish"
        if raw_direction in {"bearish", "short", "sell"}:
            return "bearish"

        quantity = position.get("quantity")
        try:
            numeric_quantity = float(quantity)
        except (TypeError, ValueError):
            numeric_quantity = 0.0
        if numeric_quantity > 0:
            return "bullish"
        if numeric_quantity < 0:
            return "bearish"
        return "neutral"

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
            return ["precision_first_score", *priority]
        return priority