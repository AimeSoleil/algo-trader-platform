"""Daily blueprint generation — LLM-powered trading plan."""
from __future__ import annotations

import json
from datetime import date, timedelta
from time import perf_counter

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

from services.analysis_service.app.tasks.helpers import (
    _fetch_current_positions,
    _get_adapter,
    _parse_signal_features,
)

logger = get_logger("analysis_tasks")


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


def _apply_precision_first_strategy_scope(
    blueprint: LLMTradingBlueprint,
) -> tuple[LLMTradingBlueprint, list[str], list[dict[str, object]], list[str], bool]:
    """Prune plans outside the configured precision-first strategy whitelist."""
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
                "rule": "precision_first_strategy_scope",
                "symbol": symbol,
                "category": "strategy_mismatch",
                "description": (
                    f"strategy_type={strategy_type} is outside the precision-first allowlist "
                    f"{sorted(allowed_set)}"
                ),
            }
        )

    if pruned_symbols:
        blueprint = blueprint.model_copy(update={"symbol_plans": surviving_plans})

    return blueprint, sorted(set(pruned_symbols)), pruned_issues, allowed_strategy_types, True


def _apply_deterministic_validation(
    blueprint: LLMTradingBlueprint,
    signal_map: dict[str, dict[str, object]],
    agent_outputs: dict[str, object] | None,
) -> tuple[LLMTradingBlueprint, dict[str, object]]:
    """Prune symbol plans with deterministic errors, then re-run validation."""
    original_plan_count = len(blueprint.symbol_plans)
    blueprint, scope_pruned_symbols, scope_pruned_issues, allowed_strategy_types, precision_first_enabled = (
        _apply_precision_first_strategy_scope(blueprint)
    )

    initial_check = check_blueprint(
        blueprint.model_dump(mode="json"),
        signal_map,
        agent_outputs=agent_outputs,
    )
    initial_errors = [issue for issue in initial_check.issues if issue.severity == "error"]
    validation_pruned_symbols = sorted({issue.symbol.upper() for issue in initial_errors if issue.symbol})
    pruned_symbols = sorted(set(scope_pruned_symbols) | set(validation_pruned_symbols))

    pruned_symbol_errors = _serialize_rule_issues(
        [issue for issue in initial_errors if issue.symbol and issue.symbol.upper() in validation_pruned_symbols]
    )
    if scope_pruned_issues:
        pruned_symbol_errors = [*scope_pruned_issues, *pruned_symbol_errors]

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
    summary["strategy_scope_pruned_symbols"] = scope_pruned_symbols
    summary["strategy_scope_pruned_plan_count"] = len(scope_pruned_issues)
    summary["pruned_symbols"] = pruned_symbols
    summary["pruned_plan_count"] = original_plan_count - len(blueprint.symbol_plans)
    summary["pruned_symbol_errors"] = pruned_symbol_errors
    summary["empty_after_pruning"] = len(blueprint.symbol_plans) == 0
    summary["passed"] = bool(summary["passed"] and blueprint.symbol_plans)
    return blueprint, summary


def _is_blueprint_soft_blocked(blueprint: LLMTradingBlueprint) -> bool:
    """Soft-block blueprints with remaining errors or no surviving plans."""
    validation = (blueprint.reasoning_context or {}).get("deterministic_validation", {})
    return bool(validation.get("error_count", 0) > 0 or not blueprint.symbol_plans)


def _summarize_pre_synthesis_outcome(blueprint: LLMTradingBlueprint) -> dict[str, object]:
    """Extract a stable pre-synthesis summary for daily artifact outputs."""
    reasoning_context = blueprint.reasoning_context or {}
    filter_summary = reasoning_context.get("pre_synthesis_filter", {}) or {}
    triage_summary = reasoning_context.get("pre_synthesis_triage", {}) or {}
    monitor_symbols: list[dict[str, object]] = []

    for item in triage_summary.get("ranked_symbols", []) or []:
        if item.get("action") != "monitor":
            continue
        monitor_symbols.append({
            "symbol": item.get("symbol", "UNKNOWN"),
            "rank": item.get("rank"),
            "coarse_score": item.get("coarse_score"),
            "reason": item.get("decision_reason", "ranked below shortlist cutoff"),
        })

    return {
        "dropped_symbol_count": int(filter_summary.get("dropped_symbol_count", 0) or 0),
        "escalate_symbol_count": int(triage_summary.get("escalate_symbol_count", 0) or 0),
        "monitor_symbol_count": int(triage_summary.get("monitor_symbol_count", 0) or 0),
        "target_shortlist_size": int(triage_summary.get("target_shortlist_size", 0) or 0),
        "escalate_symbols": list(triage_summary.get("escalate_symbols", []) or []),
        "monitor_symbols": monitor_symbols,
    }


def _format_pre_synthesis_summary_text(summary: dict[str, object]) -> str:
    """Format monitor-symbol explanations for user-facing daily summaries."""
    monitor_symbols = summary.get("monitor_symbols", []) or []
    if not monitor_symbols:
        return ""

    details = "; ".join(
        f"{item['symbol']} ({item['reason']})"
        for item in monitor_symbols
        if isinstance(item, dict) and item.get("symbol") and item.get("reason")
    )
    if not details:
        return ""

    return (
        "Pre-synthesis triage monitor symbols: "
        f"{details}."
    )


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
                    "monitor_symbol_count": str(
                        (result.get("pre_synthesis_summary") or {}).get("monitor_symbol_count", 0)
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


async def _load_previous_execution(td: date) -> dict | None:
    """Load the last completed execution summary for contextual analysis."""
    previous_execution = None
    yesterday = td - timedelta(days=1)
    async with get_postgres_session() as session:
        result = await session.execute(
            text(
                "SELECT execution_summary FROM llm_trading_blueprint "
                "WHERE trading_date = :date AND status = 'completed'"
            ),
            {"date": yesterday},
        )
        row = result.fetchone()
        if row:
            previous_execution = row[0]
    return previous_execution


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
    agent_outputs = (blueprint.reasoning_context or {}).get("agent_outputs")
    blueprint, summary = _apply_deterministic_validation(blueprint, signal_map, agent_outputs)
    ctx = dict(blueprint.reasoning_context or {})
    ctx["deterministic_validation"] = summary
    blueprint = blueprint.model_copy(update={"reasoning_context": ctx})

    if summary["error_count"] or summary["warning_count"] or summary["pruned_symbols"]:
        logger.warning(
            "blueprint.pipeline_deterministic_validation",
            trading_date=str(td),
            passed=summary["passed"],
            error_count=summary["error_count"],
            warning_count=summary["warning_count"],
            pruned_symbols=summary["pruned_symbols"],
            pruned_plan_count=summary["pruned_plan_count"],
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
    """Common pipeline: fetch positions → previous execution → LLM → return blueprint."""
    logger.debug(
        "blueprint.pipeline_started",
        log_event="pipeline_start",
        stage="start",
        trading_date=str(td),
        signal_rows=len(signal_features),
    )

    # 1) Fetch current positions
    if progress_cb:
        progress_cb("fetching_positions")
    logger.debug(
        "blueprint.pipeline_fetch_positions",
        log_event="pipeline_stage",
        stage="fetching_positions",
        trading_date=str(td),
    )
    current_positions = await _fetch_current_positions(td)
    logger.debug(
        "blueprint.pipeline_positions_ready",
        log_event="pipeline_stage",
        stage="positions_ready",
        trading_date=str(td),
        source=current_positions.get("source"),
        count=current_positions.get("count", 0),
    )

    # 2) Previous execution summary
    if progress_cb:
        progress_cb("reading_previous_execution")
    previous_execution = await _load_previous_execution(td)
    logger.debug(
        "blueprint.pipeline_previous_execution",
        log_event="pipeline_stage",
        stage="previous_execution_ready",
        trading_date=str(td),
        has_previous_execution=previous_execution is not None,
    )

    # 3) LLM generation
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
        current_positions=current_positions,
        previous_execution=previous_execution,
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

    # 4) Deterministic validation with symbol-level pruning
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
                 soft_time_limit=2400, time_limit=2700)
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
    soft_time_limit=2400,
    time_limit=2700,
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
    soft_time_limit=2400,
    time_limit=2700,
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

    current_positions = await _fetch_current_positions(td)
    previous_execution = await _load_previous_execution(td)
    orchestrator = AgentOrchestrator()
    blueprint = await orchestrator.generate_chunk_blueprint(
        signal_features=chunk_signal_features,
        benchmark_features=benchmark_features,
        current_positions=current_positions,
        previous_execution=previous_execution,
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

    current_positions = await _fetch_current_positions(td)
    previous_execution = await _load_previous_execution(td)
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
        current_positions=current_positions,
        previous_execution=previous_execution,
        signal_date=td,
    )
    if missing_symbols:
        blueprint.missing_symbols = sorted(set(missing_symbols))

    return await _finalize_blueprint_result(blueprint, all_signal_features, started=started)
