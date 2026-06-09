"""Daily blueprint generation — LLM-powered trading plan."""
from __future__ import annotations

import json
from datetime import date
from time import perf_counter
from typing import Any

from sqlalchemy import text

from shared.async_bridge import run_async
from shared.celery_app import celery_app
from shared.config import get_settings
from shared.db.session import get_postgres_session
from shared.models.blueprint import LLMTradingBlueprint
from shared.models.signal import DataQuality, SignalFeatures
from shared.utils import get_logger, resolve_trading_date_arg, today_trading

from services.analysis_service.app.evaluation.rule_checker import check_blueprint
from services.analysis_service.app.llm.agents.orchestrator import AgentOrchestrator
from services.analysis_service.app.trade_gate_semantics import (
    aggregate_trade_gate_summaries,
    format_trade_gate_rollup_text,
    summarize_trade_gate_analyses,
    trade_gate_taxonomy_metadata,
)

from services.analysis_service.app.tasks.helpers import (
    _get_adapter,
    _parse_signal_features,
)

logger = get_logger("analysis_tasks")

_TRADE_GATE_AGENT_NAMES = ("trend", "volatility", "flow", "chain", "spread", "cross_asset")


async def _claim_terminal_notification_slot_async(trading_date: str, outcome: str) -> bool:
    """Claim one terminal notification slot per trading_date.

    Returns True when this execution wins and should send the notification.
    Returns False when another execution has already sent a terminal
    notification (success or failure) for the same trading date.
    """
    from shared.redis_pool import get_redis

    redis = get_redis()
    key = f"analysis:blueprint:terminal_notify:{trading_date}"
    # Keep for 2 days to cover retries/redelivery around the same run window.
    claimed = await redis.set(key, outcome, nx=True, ex=172800)
    return bool(claimed)


def _claim_terminal_notification_slot(trading_date: str, outcome: str) -> bool:
    """Sync wrapper for Celery task context; best-effort on Redis errors."""
    try:
        return bool(run_async(_claim_terminal_notification_slot_async(trading_date, outcome)))
    except Exception as exc:
        logger.warning(
            "blueprint.notify_dedup_check_failed",
            trading_date=trading_date,
            outcome=outcome,
            error=str(exc),
        )
        # Fail open so we still notify when Redis is temporarily unavailable.
        return True


def _serialize_rule_issues(issues) -> list[dict[str, object]]:
    """Convert checker issues into JSON-safe dictionaries."""
    return [
        {
            "severity": issue.severity,
            "rule": issue.rule,
            "symbol": issue.symbol,
            "category": issue.category,
            "description": issue.description,
        }
        for issue in issues
    ]


def _summarize_check_result(check) -> dict[str, object]:
    """Create a stable summary payload for reasoning_context storage."""
    errors = [issue for issue in check.issues if issue.severity == "error"]
    warnings = [issue for issue in check.issues if issue.severity == "warning"]
    return {
        "passed": check.passed,
        "error_count": len(errors),
        "warning_count": len(warnings),
        "issues": _serialize_rule_issues(check.issues),
    }


def _apply_emitted_strategy_scope_guard(
    blueprint: LLMTradingBlueprint,
) -> tuple[LLMTradingBlueprint, list[str], list[dict[str, object]], list[str], bool]:
    """Prune emitted plans outside the configured precision-first strategy whitelist."""
    precision_first = get_settings().analysis_service.llm.precision_first
    allowed_strategy_types = [strategy_type.lower() for strategy_type in precision_first.allowed_strategy_types]
    allowed_set = set(allowed_strategy_types)

    if not precision_first.enabled or not allowed_set:
        return blueprint, [], [], allowed_strategy_types, False

    pruned_symbols: list[str] = []
    pruned_issues: list[dict[str, object]] = []
    surviving_plans = []
    for plan in blueprint.symbol_plans:
        strategy_value = getattr(plan.strategy_type, "value", plan.strategy_type)
        strategy_type = str(strategy_value).lower()
        if strategy_type in allowed_set:
            surviving_plans.append(plan)
            continue

        symbol = plan.underlying.upper()
        pruned_symbols.append(symbol)
        pruned_issues.append(
            {
                "severity": "error",
                "rule": "emitted_strategy_scope_guard",
                "symbol": symbol,
                "category": "strategy_mismatch",
                "description": (
                    f"emitted strategy_type={strategy_type} is outside the precision-first allowlist "
                    f"{sorted(allowed_set)}"
                ),
            }
        )

    if pruned_symbols:
        blueprint = blueprint.model_copy(update={"symbol_plans": surviving_plans})

    return blueprint, sorted(set(pruned_symbols)), pruned_issues, allowed_strategy_types, True


def _symbol_trade_gate_summary(
    agent_outputs: dict[str, object] | None,
    symbol: str,
) -> dict[str, object] | None:
    if not isinstance(agent_outputs, dict):
        return None

    symbol_upper = symbol.upper()
    agent_analyses: dict[str, dict[str, object] | None] = {}
    for agent_name in _TRADE_GATE_AGENT_NAMES:
        agent_output = agent_outputs.get(agent_name)
        if not isinstance(agent_output, dict):
            agent_analyses[agent_name] = None
            continue
        symbols = agent_output.get("symbols")
        if not isinstance(symbols, list):
            agent_analyses[agent_name] = None
            continue
        agent_analyses[agent_name] = next(
            (
                item for item in symbols
                if isinstance(item, dict) and str(item.get("symbol") or "").strip().upper() == symbol_upper
            ),
            None,
        )

    summary = summarize_trade_gate_analyses(agent_analyses)
    if summary["trade_gate_status"] == "clear":
        return None
    return {"symbol": symbol_upper, **summary}


def _agent_symbol_analysis(
    agent_outputs: dict[str, object] | None,
    agent_name: str,
    symbol: str,
) -> dict[str, object] | None:
    if not isinstance(agent_outputs, dict):
        return None

    agent_output = agent_outputs.get(agent_name)
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


def _preview_symbols(symbols: list[str], *, limit: int = 3) -> str:
    cleaned: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        token = str(symbol).strip().upper()
        if not token or token in seen:
            continue
        seen.add(token)
        cleaned.append(token)

    if not cleaned:
        return "the analyzed symbols"
    if len(cleaned) == 1:
        return cleaned[0]
    preview = ", ".join(cleaned[:limit])
    remaining = len(cleaned) - limit
    return f"{preview}, +{remaining} more" if remaining > 0 else preview


def _refine_empty_market_analysis(
    market_analysis: str,
    market_regime: str,
    signal_map: dict[str, dict[str, object]],
    agent_outputs: dict[str, object] | None,
) -> str:
    if not isinstance(agent_outputs, dict) or not signal_map:
        return market_analysis

    trade_symbols = sorted({str(symbol).strip().upper() for symbol in signal_map.keys() if str(symbol).strip()})
    if not trade_symbols:
        return market_analysis

    false_breakout_symbols: list[str] = []
    simple_structure_symbols: list[str] = []
    executability_symbols: list[str] = []

    for symbol in trade_symbols:
        flow_data = _agent_symbol_analysis(agent_outputs, "flow", symbol) or {}
        flow_blocked_reasons = {
            str(reason).strip().lower()
            for reason in flow_data.get("blocked_reasons", [])
            if str(reason).strip()
        }
        if flow_data.get("false_breakout_risk") == "high" or "high_false_breakout_risk" in flow_blocked_reasons:
            false_breakout_symbols.append(symbol)

        if any(
            (_agent_symbol_analysis(agent_outputs, agent_name, symbol) or {}).get("simple_structures_only")
            for agent_name in ("trend", "volatility", "flow", "chain", "spread")
        ):
            simple_structure_symbols.append(symbol)

        chain_data = _agent_symbol_analysis(agent_outputs, "chain", symbol) or {}
        chain_blocked_reasons = {
            str(reason).strip().lower()
            for reason in chain_data.get("blocked_reasons", [])
            if str(reason).strip()
        }
        spread_data = _agent_symbol_analysis(agent_outputs, "spread", symbol) or {}
        spread_blocked_reasons = {
            str(reason).strip().lower()
            for reason in spread_data.get("blocked_reasons", [])
            if str(reason).strip()
        }

        if (
            chain_data.get("hard_block")
            or chain_data.get("trade_allowed") is False
            or chain_data.get("liquidity_ok") is False
            or str(chain_data.get("liquidity_tier") or "").upper() in {"L4", "L5"}
            or "insufficient_leg_liquidity" in chain_blocked_reasons
            or "hard_block_spread" in chain_blocked_reasons
            or spread_data.get("trade_allowed") is False
            or str(spread_data.get("liquidity_status") or "").lower() in {"wide", "illiquid"}
            or "illiquid_spread_proxy" in spread_blocked_reasons
        ):
            executability_symbols.append(symbol)

    if not (false_breakout_symbols or simple_structure_symbols or executability_symbols):
        return market_analysis

    parts: list[str] = []
    cross_asset_output = agent_outputs.get("cross_asset")
    vix_summary = str(cross_asset_output.get("vix_summary") or "").strip() if isinstance(cross_asset_output, dict) else ""
    market_regime_text = f"Market regime remains {str(market_regime or 'neutral').strip() or 'neutral'}."
    parts.append(f"{market_regime_text} {vix_summary}".strip() if vix_summary else market_regime_text)

    if false_breakout_symbols:
        parts.append(
            f"Directional entries were filtered by high false breakout risk for {_preview_symbols(false_breakout_symbols)}."
        )

    if simple_structure_symbols or executability_symbols:
        sentence_parts: list[str] = []
        if simple_structure_symbols:
            sentence_parts.append(
                f"Configured simple-structure gates remained active for {_preview_symbols(simple_structure_symbols)}"
            )
        if executability_symbols:
            sentence_parts.append(
                f"Chain/Spread executability did not confirm a qualifying structure for {_preview_symbols(executability_symbols)} after liquidity, spread, and DTE checks"
            )
        parts.append("; ".join(sentence_parts) + ".")

    return " ".join(parts)


def _build_validation_trade_gate_summary(
    agent_outputs: dict[str, object] | None,
    pruned_symbols: list[str],
) -> dict[str, object]:
    symbol_summaries: list[dict[str, object]] = []
    for symbol in pruned_symbols:
        summary = _symbol_trade_gate_summary(agent_outputs, symbol)
        if summary is not None:
            symbol_summaries.append(summary)
    aggregate = aggregate_trade_gate_summaries(symbol_summaries)
    aggregate["trade_gate_taxonomy"] = trade_gate_taxonomy_metadata()
    return aggregate


def _build_trade_gate_summary_for_symbols(
    agent_outputs: dict[str, object] | None,
    symbols: list[str],
) -> dict[str, object]:
    symbol_summaries: list[dict[str, object]] = []
    seen: set[str] = set()
    for symbol in symbols:
        symbol_upper = str(symbol).strip().upper()
        if not symbol_upper or symbol_upper in seen:
            continue
        seen.add(symbol_upper)
        summary = _symbol_trade_gate_summary(agent_outputs, symbol)
        if summary is not None:
            symbol_summaries.append(summary)
    aggregate = aggregate_trade_gate_summaries(symbol_summaries)
    aggregate["trade_gate_taxonomy"] = trade_gate_taxonomy_metadata()
    return aggregate


def _apply_deterministic_validation(
    blueprint: LLMTradingBlueprint,
    signal_map: dict[str, dict[str, object]],
    agent_outputs: dict[str, object] | None,
) -> tuple[LLMTradingBlueprint, dict[str, object]]:
    """Prune symbol plans with deterministic errors, then re-run validation."""
    original_plan_count = len(blueprint.symbol_plans)
    blueprint, emitted_scope_pruned_symbols, emitted_scope_pruned_issues, allowed_strategy_types, precision_first_enabled = (
        _apply_emitted_strategy_scope_guard(blueprint)
    )

    initial_check = check_blueprint(
        blueprint.model_dump(mode="json"),
        signal_map,
        agent_outputs=agent_outputs,
    )
    initial_errors = [issue for issue in initial_check.issues if issue.severity == "error"]
    validation_pruned_symbols = sorted({issue.symbol.upper() for issue in initial_errors if issue.symbol})
    pruned_symbols = sorted(set(emitted_scope_pruned_symbols) | set(validation_pruned_symbols))

    pruned_symbol_errors = _serialize_rule_issues(
        [issue for issue in initial_errors if issue.symbol and issue.symbol.upper() in validation_pruned_symbols]
    )
    if emitted_scope_pruned_issues:
        pruned_symbol_errors = [*emitted_scope_pruned_issues, *pruned_symbol_errors]

    if validation_pruned_symbols:
        surviving_plans = [
            plan for plan in blueprint.symbol_plans
            if plan.underlying.upper() not in validation_pruned_symbols
        ]
        blueprint = blueprint.model_copy(update={"symbol_plans": surviving_plans})

    final_check = check_blueprint(
        blueprint.model_dump(mode="json"),
        signal_map,
        agent_outputs=agent_outputs,
    )
    summary = _summarize_check_result(final_check)
    summary["initial_error_count"] = len(initial_errors)
    summary["initial_warning_count"] = initial_check.warning_count
    summary["precision_first_enabled"] = precision_first_enabled
    summary["allowed_strategy_types"] = allowed_strategy_types
    summary["emitted_strategy_scope_pruned_symbols"] = emitted_scope_pruned_symbols
    summary["emitted_strategy_scope_pruned_plan_count"] = len(emitted_scope_pruned_issues)
    summary["pruned_symbols"] = pruned_symbols
    summary["pruned_plan_count"] = original_plan_count - len(blueprint.symbol_plans)
    summary["pruned_symbol_errors"] = pruned_symbol_errors
    validation_trade_gate_summary = _build_validation_trade_gate_summary(agent_outputs, validation_pruned_symbols)
    pre_selection_symbols = sorted({str(symbol).strip().upper() for symbol in signal_map.keys() if str(symbol).strip()})
    pre_selection_trade_gate_summary = _build_trade_gate_summary_for_symbols(agent_outputs, pre_selection_symbols)
    summary["pre_selection_trade_gate_summary"] = pre_selection_trade_gate_summary
    if validation_trade_gate_summary.get("symbols") or blueprint.symbol_plans:
        summary["trade_gate_summary"] = validation_trade_gate_summary
    else:
        summary["trade_gate_summary"] = pre_selection_trade_gate_summary
    summary["empty_after_pruning"] = len(blueprint.symbol_plans) == 0
    summary["passed"] = bool(summary["passed"] and blueprint.symbol_plans)
    return blueprint, summary


def _resolve_validation_agent_outputs(
    reasoning_context: dict[str, Any] | None,
) -> dict[str, object] | None:
    """Return specialist outputs for deterministic validation.

    Chunk-merged blueprints keep per-chunk agent outputs under ``chunk_contexts``
    rather than the top-level reasoning context. Aggregate those symbol analyses so
    deterministic validation can still enforce machine-readable agent gates.
    """
    if not isinstance(reasoning_context, dict):
        return None

    agent_outputs = reasoning_context.get("agent_outputs")
    if isinstance(agent_outputs, dict):
        return agent_outputs

    chunk_contexts = reasoning_context.get("chunk_contexts")
    if not isinstance(chunk_contexts, list):
        return None

    merged_outputs: dict[str, dict[str, object]] = {}
    for chunk_context in chunk_contexts:
        if not isinstance(chunk_context, dict):
            continue

        chunk_outputs = chunk_context.get("agent_outputs")
        if not isinstance(chunk_outputs, dict):
            continue

        for agent_name, agent_output in chunk_outputs.items():
            if not isinstance(agent_output, dict):
                continue

            merged_agent_output = merged_outputs.setdefault(agent_name, {})
            for key, value in agent_output.items():
                if key == "symbols" and isinstance(value, list):
                    existing_symbols = merged_agent_output.setdefault("symbols", [])
                    if isinstance(existing_symbols, list):
                        existing_symbols.extend(
                            item for item in value if isinstance(item, dict)
                        )
                    continue

                merged_agent_output.setdefault(key, value)

    return merged_outputs or None


def _is_blueprint_soft_blocked(blueprint: LLMTradingBlueprint) -> bool:
    """Soft-block blueprints with remaining errors or no surviving plans."""
    validation = (blueprint.reasoning_context or {}).get("deterministic_validation", {})
    return bool(validation.get("error_count", 0) > 0 or not blueprint.symbol_plans)


def _summarize_pre_synthesis_outcome(blueprint: LLMTradingBlueprint) -> dict[str, object]:
    """Extract a stable pre-synthesis summary for daily artifact outputs."""
    reasoning_context = blueprint.reasoning_context or {}
    filter_summary = reasoning_context.get("pre_synthesis_filter", {}) or {}
    triage_summary = reasoning_context.get("pre_synthesis_triage", {}) or {}
    deterministic_validation = reasoning_context.get("deterministic_validation", {}) or {}
    analysis_order = list(triage_summary.get("analysis_order", []) or [])
    ranked_symbols: list[dict[str, object]] = []

    for item in triage_summary.get("ranked_symbols", []) or []:
        if not isinstance(item, dict):
            continue
        ranked_symbols.append({
            "symbol": item.get("symbol", "UNKNOWN"),
            "rank": item.get("rank"),
            "coarse_score": item.get("coarse_score"),
            "reason": item.get("decision_reason", "priority-ranked for analysis"),
        })

    return {
        "dropped_symbol_count": int(filter_summary.get("dropped_symbol_count", 0) or 0),
        "analysis_symbol_count": int(triage_summary.get("analysis_symbol_count", len(analysis_order)) or 0),
        "analysis_order": analysis_order,
        "top_ranked_symbols": ranked_symbols[:5],
        "trade_gate_summary": deterministic_validation.get("trade_gate_summary", {}) or {},
    }


def _format_pre_synthesis_summary_text(summary: dict[str, object]) -> str:
    """Format ranking summaries for user-facing daily notifications."""
    analysis_order = summary.get("analysis_order", []) or []
    parts: list[str] = []
    if analysis_order:
        preview = ", ".join(str(symbol) for symbol in analysis_order[:5])
        remaining_count = max(0, len(analysis_order) - 5)
        suffix = f", +{remaining_count} more" if remaining_count else ""
        parts.append(f"Pre-synthesis analysis priority: {preview}{suffix}.")

    trade_gate_text = format_trade_gate_rollup_text(summary.get("trade_gate_summary", {}) or {})
    if trade_gate_text:
        parts.append(trade_gate_text)

    return " ".join(parts)


def _notify_blueprint_failure(trading_date: str, exc: Exception, *, phase: str) -> None:
    """Send one terminal failure notification for the trading date."""
    from shared.notifier.base import NotificationEvent, EventType, Severity
    from shared.notifier.helpers import notify_sync

    if _claim_terminal_notification_slot(trading_date, "failed"):
        try:
            notify_sync(NotificationEvent(
                event_type=EventType.PIPELINE_FAILED,
                title="❌ Blueprint Generation Failed",
                message=f"Blueprint generation failed for {trading_date}. Root cause: {exc}",
                severity=Severity.ERROR,
                payload={
                    "trading_date": trading_date,
                    "phase": phase,
                    "error": str(exc),
                },
            ))
        except Exception as notify_exc:
            logger.warning("blueprint.notify_failed_on_error", error=str(notify_exc))
    else:
        logger.info(
            "blueprint.notify_failed_skipped_dedup",
            trading_date=trading_date,
            phase=phase,
        )


def _notify_blueprint_success(trading_date: str, result: dict) -> None:
    """Send one terminal success notification for the trading date."""
    from shared.notifier.base import NotificationEvent, EventType, Severity
    from shared.notifier.helpers import notify_sync

    if _claim_terminal_notification_slot(trading_date, "success"):
        try:
            notify_sync(NotificationEvent(
                event_type=EventType.PIPELINE_FINISHED,
                title="✅ Daily Pipeline Completed",
                message=f"Blueprint generated for {trading_date}. "
                        f"{result.get('plans_count', 0)} symbol plans, "
                        f"provider: {result.get('provider', 'unknown')}."
                        f" {result.get('pre_synthesis_summary_text', '')}".strip(),
                severity=Severity.INFO,
                payload={
                    "trading_date": trading_date,
                    "blueprint_id": str(result.get("blueprint_id", "")),
                    "plans_count": str(result.get("plans_count", 0)),
                    "analysis_symbol_count": str(
                        (result.get("pre_synthesis_summary") or {}).get("analysis_symbol_count", 0)
                    ),
                },
            ))
        except Exception as notify_exc:
            logger.warning("blueprint.notify_success_failed", error=str(notify_exc))
    else:
        logger.info(
            "blueprint.notify_success_skipped_dedup",
            trading_date=trading_date,
        )


async def _load_signal_features_for_date(td: date) -> list[SignalFeatures]:
    """Load and parse all signal features for a trading date."""
    signal_features: list[SignalFeatures] = []
    async with get_postgres_session() as session:
        result = await session.execute(
            text("SELECT features_json FROM signal_features WHERE date = :date"),
            {"date": td},
        )
        for row in result.fetchall():
            try:
                signal_features.append(_parse_signal_features(row[0]))
            except Exception as exc:
                logger.warning("blueprint.signal_parse_error", error=str(exc))
    return signal_features


def _apply_and_log_deterministic_validation(
    blueprint: LLMTradingBlueprint,
    signal_features: list[SignalFeatures],
    *,
    td: date,
) -> LLMTradingBlueprint:
    """Apply deterministic validation and log the resulting summary."""
    signal_map = {
        sf.symbol.upper(): sf.model_dump(mode="json")
        for sf in signal_features
    }
    agent_outputs = _resolve_validation_agent_outputs(blueprint.reasoning_context)
    blueprint, summary = _apply_deterministic_validation(blueprint, signal_map, agent_outputs)
    ctx = dict(blueprint.reasoning_context or {})
    ctx["deterministic_validation"] = summary
    update_payload: dict[str, object] = {"reasoning_context": ctx}
    if not blueprint.symbol_plans:
        update_payload["market_analysis"] = _refine_empty_market_analysis(
            blueprint.market_analysis,
            blueprint.market_regime,
            signal_map,
            agent_outputs,
        )
    blueprint = blueprint.model_copy(update=update_payload)

    if summary["error_count"] or summary["warning_count"] or summary["pruned_symbols"]:
        logger.warning(
            "blueprint.pipeline_deterministic_validation",
            trading_date=str(td),
            passed=summary["passed"],
            initial_error_count=summary["initial_error_count"],
            initial_warning_count=summary["initial_warning_count"],
            error_count=summary["error_count"],
            warning_count=summary["warning_count"],
            emitted_strategy_scope_pruned_symbols=summary["emitted_strategy_scope_pruned_symbols"],
            pruned_symbols=summary["pruned_symbols"],
            pruned_plan_count=summary["pruned_plan_count"],
            pruned_symbol_errors=summary["pruned_symbol_errors"],
            issues=summary["issues"],
        )
    else:
        logger.info(
            "blueprint.pipeline_deterministic_validation",
            trading_date=str(td),
            passed=summary["passed"],
            error_count=0,
            warning_count=0,
        )
    return blueprint


async def _persist_blueprint(
    blueprint: LLMTradingBlueprint,
    *,
    blueprint_status: str,
) -> None:
    """UPSERT the finalized blueprint row."""
    async with get_postgres_session() as session:
        logger.debug(
            "blueprint.generate.db_write_started",
            log_event="db_write",
            stage="before_write",
            trading_date=str(blueprint.trading_date),
            blueprint_id=blueprint.id,
        )
        await session.execute(
            text(
                "INSERT INTO llm_trading_blueprint "
                "(id, trading_date, generated_at, model_provider, model_version, "
                " blueprint_json, reasoning_json, status) "
                "VALUES (:id, :trading_date, :generated_at, :model_provider, "
                " :model_version, :blueprint_json, :reasoning_json, :status) "
                "ON CONFLICT (trading_date) DO UPDATE SET "
                "  id               = EXCLUDED.id, "
                "  generated_at     = EXCLUDED.generated_at, "
                "  model_provider   = EXCLUDED.model_provider, "
                "  model_version    = EXCLUDED.model_version, "
                "  blueprint_json   = EXCLUDED.blueprint_json, "
                "  reasoning_json   = EXCLUDED.reasoning_json, "
                "  status           = EXCLUDED.status"
            ),
            {
                "id": blueprint.id,
                "trading_date": blueprint.trading_date,
                "generated_at": blueprint.generated_at,
                "model_provider": blueprint.model_provider,
                "model_version": blueprint.model_version,
                "blueprint_json": blueprint.model_dump_json(),
                "reasoning_json": json.dumps(blueprint.reasoning_context, default=str) if blueprint.reasoning_context else None,
                "status": blueprint_status,
            },
        )
        logger.debug(
            "blueprint.generate.db_write_finished",
            log_event="db_write",
            stage="after_write",
            trading_date=str(blueprint.trading_date),
            blueprint_id=blueprint.id,
            provider=blueprint.model_provider,
        )


async def _refresh_blueprint_cache(
    blueprint: LLMTradingBlueprint,
    *,
    blueprint_status: str,
) -> None:
    """Refresh the blueprint cache with delete-on-write fallback."""
    from services.analysis_service.app.cache import (
        invalidate_blueprint_cache_strict,
        set_cached_blueprint_strict,
    )

    blueprint_data = {
        "id": blueprint.id,
        "trading_date": str(blueprint.trading_date),
        "status": blueprint_status,
        "blueprint": blueprint.model_dump(mode="json"),
        "execution_summary": None,
    }
    logger.debug(
        "blueprint.generate.cache_refresh_started",
        log_event="cache_write",
        stage="before_refresh",
        trading_date=str(blueprint.trading_date),
    )
    try:
        await set_cached_blueprint_strict(blueprint.trading_date, blueprint_data)
    except Exception as cache_exc:
        logger.debug("blueprint.cache_refresh_failed", date=str(blueprint.trading_date), error=str(cache_exc))
        try:
            await invalidate_blueprint_cache_strict(blueprint.trading_date)
        except Exception as del_exc:
            logger.debug("blueprint.cache_delete_failed", date=str(blueprint.trading_date), error=str(del_exc))


async def _finalize_blueprint_result(
    blueprint: LLMTradingBlueprint,
    signal_features: list[SignalFeatures],
    *,
    started: float,
) -> dict:
    """Annotate, persist, cache, and summarize a finalized blueprint."""
    _annotate_blueprint_quality(blueprint, signal_features)
    blueprint = blueprint.model_copy(update={"id": blueprint.id})
    soft_blocked = _is_blueprint_soft_blocked(blueprint)
    blueprint_status = "cancelled" if soft_blocked else "pending"

    await _persist_blueprint(blueprint, blueprint_status=blueprint_status)
    await _refresh_blueprint_cache(blueprint, blueprint_status=blueprint_status)

    logger.debug(
        "blueprint.generate.summary",
        log_event="task_summary",
        stage="completed",
        trading_date=str(blueprint.trading_date),
        blueprint_id=blueprint.id,
        plans=len(blueprint.symbol_plans),
        provider=blueprint.model_provider,
        duration_ms=round((perf_counter() - started) * 1000, 2),
    )

    logger.info(
        "blueprint.generated",
        trading_date=str(blueprint.trading_date),
        plans=len(blueprint.symbol_plans),
        provider=blueprint.model_provider,
        status=blueprint_status,
        soft_blocked=soft_blocked,
    )
    pre_synthesis_summary = _summarize_pre_synthesis_outcome(blueprint)
    return {
        "trading_date": str(blueprint.trading_date),
        "blueprint_id": blueprint.id,
        "plans_count": len(blueprint.symbol_plans),
        "provider": blueprint.model_provider,
        "status": blueprint_status,
        "soft_blocked": soft_blocked,
        "pre_synthesis_summary": pre_synthesis_summary,
        "pre_synthesis_summary_text": _format_pre_synthesis_summary_text(pre_synthesis_summary),
        "deterministic_validation": (
            (blueprint.reasoning_context or {}).get("deterministic_validation")
        ),
    }


# ── Common pipeline (steps 2-4) ───────────────────────────────


async def _run_blueprint_pipeline(
    signal_features: list[SignalFeatures],
    td: date,
    progress_cb=None,
):
    """Common pipeline: LLM generation → deterministic validation → return blueprint."""
    logger.debug(
        "blueprint.pipeline_started",
        log_event="pipeline_start",
        stage="start",
        trading_date=str(td),
        signal_rows=len(signal_features),
    )

    # 1) LLM generation
    if progress_cb:
        progress_cb("generating_blueprint")
    adapter = _get_adapter()
    logger.debug(
        "blueprint.pipeline_generation_started",
        log_event="pipeline_stage",
        stage="generating_blueprint",
        trading_date=str(td),
    )
    blueprint = await adapter.generate_blueprint(
        signal_features=signal_features,
        signal_date=td,
    )
    logger.debug(
        "blueprint.pipeline_generation_finished",
        log_event="pipeline_stage",
        stage="generation_finished",
        trading_date=str(td),
        provider=blueprint.model_provider,
        plans=len(blueprint.symbol_plans),
    )

    # 2) Deterministic validation with symbol-level pruning
    return _apply_and_log_deterministic_validation(blueprint, signal_features, td=td)


def _annotate_blueprint_quality(
    blueprint: "LLMTradingBlueprint",
    signal_features: list["SignalFeatures"],
) -> None:
    """Post-process: inject signal data quality into each SymbolPlan.

    Mutates *blueprint* in place — fills data_quality_score,
    data_quality_warnings, signal_data_quality on each plan and sets
    the blueprint-level min_data_quality_score / data_quality_summary.
    """
    # Build lookup: symbol → DataQuality
    quality_map: dict[str, "DataQuality"] = {}
    for sf in signal_features:
        quality_map[sf.symbol.upper()] = sf.data_quality

    quality_scores: list[float] = []
    global_warnings: list[str] = []

    for plan in blueprint.symbol_plans:
        sym = plan.underlying.upper()
        dq = quality_map.get(sym)
        if dq is None:
            # No signal at all for this symbol — worst quality
            plan.data_quality_score = 0.0
            plan.data_quality_warnings = [f"No signal data found for {sym}"]
            quality_scores.append(0.0)
            global_warnings.append(f"{sym}: no signal data")
            continue

        plan.signal_data_quality = dq
        plan.data_quality_score = dq.score
        plan.data_quality_warnings = list(dq.warnings)  # copy

        if not dq.complete:
            global_warnings.append(
                f"{sym}: score={dq.score:.2f}, issues={', '.join(dq.warnings[:3])}"
            )
        quality_scores.append(dq.score)

    blueprint.min_data_quality_score = min(quality_scores) if quality_scores else 1.0
    blueprint.data_quality_summary = global_warnings


# ── Daily blueprint task ──────────────────────────────────────


@celery_app.task(name="analysis_service.tasks.generate_daily_blueprint", bind=True, max_retries=0,
                 soft_time_limit=7200, time_limit=7500)
def generate_daily_blueprint(self, trading_date: str | None = None, prev_result=None) -> dict:
    """
    17:10 Celery 任务：生成次日交易蓝图
    prev_result: 上游任务 (compute_signals) 的结果
    
    Fails immediately on error with detailed notification — no automatic retries.
    Manual retry via Celery or scheduled re-run required.
    """
    resolved_trading_date = resolve_trading_date_arg(trading_date, prev_result)
    logger.debug(
        "blueprint.generate.start",
        log_event="task_start",
        stage="entry",
        task_id=getattr(self.request, "id", None),
        trading_date=trading_date,
        resolved_trading_date=resolved_trading_date,
    )
    result = None
    success = False
    try:
        result = run_async(_generate_blueprint_async(resolved_trading_date))
        success = True
        return result
    except Exception as exc:
        logger.error(
            "blueprint.generate.failed",
            error=str(exc),
            exc_info=True,
        )

        _notify_blueprint_failure(str(resolved_trading_date), exc, phase="generate_daily_blueprint")

        # Re-raise to mark task as failed (no auto-retry)
        raise
    finally:
        # Fire success notification after successful blueprint generation
        # (wrapped in try-except to prevent notification errors from affecting task)
        if success and result is not None:
            _notify_blueprint_success(str(resolved_trading_date), result)


async def _generate_blueprint_async(trading_date_str: str | None = None) -> dict:
    settings = get_settings()
    td = date.fromisoformat(trading_date_str) if trading_date_str else today_trading()
    started = perf_counter()
    logger.debug(
        "blueprint.generate.context",
        log_event="task_context",
        stage="start",
        trading_date=str(td),
        symbols=len(settings.common.watchlist.all),
    )

    # 1) Read all signal features from DB
    signal_features = await _load_signal_features_for_date(td)

    logger.debug(
        "blueprint.generate.signals_loaded",
        log_event="db_read",
        stage="signals_ready",
        trading_date=str(td),
        rows=len(signal_features),
    )

    if not signal_features:
        logger.warning("blueprint.no_signals", date=str(td))
        return {"error": "No signal features available", "date": str(td)}

    # 2-4) Common pipeline
    blueprint = await _run_blueprint_pipeline(signal_features, td)
    return await _finalize_blueprint_result(blueprint, signal_features, started=started)


@celery_app.task(
    name="analysis_service.tasks.generate_daily_blueprint_chunk",
    bind=True,
    max_retries=0,
    soft_time_limit=7200,
    time_limit=7500,
)
def generate_daily_blueprint_chunk(self, symbols: list[str], trading_date: str) -> dict:
    """Analyze one slice of the daily tradable universe and return a partial blueprint."""
    try:
        return run_async(
            _generate_blueprint_chunk_async(
                symbols,
                trading_date,
                task_id=getattr(self.request, "id", None),
            )
        )
    except Exception as exc:
        logger.error(
            "blueprint.chunk.failed",
            task_id=getattr(self.request, "id", None),
            trading_date=trading_date,
            symbols=symbols,
            error=str(exc),
            exc_info=True,
        )
        raise


@celery_app.task(
    name="analysis_service.tasks.finalize_daily_blueprint_chunks",
    bind=True,
    max_retries=0,
    soft_time_limit=7200,
    time_limit=7500,
)
def finalize_daily_blueprint_chunks(self, chunk_results, trading_date: str) -> dict:
    """Merge all analyzed chunks into one final blueprint and persist it."""
    result = None
    success = False
    try:
        result = run_async(_finalize_daily_blueprint_chunks_async(chunk_results, trading_date))
        success = True
        return result
    except Exception as exc:
        logger.error(
            "blueprint.finalize_chunks.failed",
            task_id=getattr(self.request, "id", None),
            trading_date=trading_date,
            error=str(exc),
            exc_info=True,
        )
        _notify_blueprint_failure(trading_date, exc, phase="finalize_daily_blueprint_chunks")
        raise
    finally:
        if success and result is not None:
            _notify_blueprint_success(trading_date, result)


async def _generate_blueprint_chunk_async(
    symbols: list[str],
    trading_date: str,
    *,
    task_id: str | None = None,
) -> dict:
    td = date.fromisoformat(trading_date)
    requested = {symbol.upper() for symbol in symbols}
    started = perf_counter()

    logger.debug(
        "blueprint.chunk.context",
        log_event="task_context",
        stage="start",
        trading_date=str(td),
        symbols=sorted(requested),
    )

    all_signal_features = await _load_signal_features_for_date(td)
    if not all_signal_features:
        return {
            "trading_date": trading_date,
            "chunk_symbols": sorted(requested),
            "chunk_blueprint": None,
            "missing_symbols": sorted(requested),
        }

    settings = get_settings()
    benchmark_symbols = {symbol.upper() for symbol in settings.common.watchlist.for_trade_benchmark}
    chunk_signal_features = [
        sf for sf in all_signal_features
        if sf.symbol.upper() in requested
    ]
    benchmark_features = [
        sf for sf in all_signal_features
        if sf.symbol.upper() in benchmark_symbols
    ]
    missing_symbols = sorted(requested - {sf.symbol.upper() for sf in chunk_signal_features})

    if not chunk_signal_features:
        return {
            "trading_date": trading_date,
            "chunk_symbols": sorted(requested),
            "chunk_blueprint": None,
            "missing_symbols": sorted(requested),
        }

    orchestrator = AgentOrchestrator()
    blueprint = await orchestrator.generate_chunk_blueprint(
        signal_features=chunk_signal_features,
        benchmark_features=benchmark_features,
        signal_date=td,
        analysis_chunk_id=f"daily-{task_id or 'chunk'}",
    )
    blueprint = _apply_and_log_deterministic_validation(blueprint, chunk_signal_features, td=td)

    logger.info(
        "blueprint.chunk.generated",
        trading_date=trading_date,
        symbols=sorted(requested),
        plans=len(blueprint.symbol_plans),
        duration_ms=round((perf_counter() - started) * 1000, 2),
    )
    return {
        "trading_date": trading_date,
        "chunk_symbols": sorted(requested),
        "chunk_blueprint": blueprint.model_dump(mode="json"),
        "missing_symbols": missing_symbols,
    }


async def _finalize_daily_blueprint_chunks_async(chunk_results, trading_date: str) -> dict:
    td = date.fromisoformat(trading_date)
    started = perf_counter()
    all_signal_features = await _load_signal_features_for_date(td)
    if not all_signal_features:
        logger.warning("blueprint.no_signals", date=str(td))
        return {"error": "No signal features available", "date": str(td)}

    chunk_payloads = chunk_results if isinstance(chunk_results, list) else [chunk_results]
    chunk_blueprints: list[LLMTradingBlueprint] = []
    missing_symbols: list[str] = []
    for payload in chunk_payloads:
        if not isinstance(payload, dict):
            continue
        missing_symbols.extend(payload.get("missing_symbols", []) or [])
        chunk_blueprint = payload.get("chunk_blueprint")
        if chunk_blueprint:
            chunk_blueprints.append(LLMTradingBlueprint.model_validate(chunk_blueprint))

    orchestrator = AgentOrchestrator()
    blueprint = await orchestrator.merge_chunk_blueprints(
        chunk_blueprints=chunk_blueprints,
        signal_features=all_signal_features,
        signal_date=td,
    )
    if missing_symbols:
        blueprint.missing_symbols = sorted(set(missing_symbols))

    return await _finalize_blueprint_result(blueprint, all_signal_features, started=started)
