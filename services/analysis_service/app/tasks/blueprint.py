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

from services.analysis_service.app.tasks.helpers import (
    _fetch_current_positions,
    _get_adapter,
    _parse_signal_features,
)

logger = get_logger("analysis_tasks")


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
    return blueprint


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


@celery_app.task(name="analysis_service.tasks.generate_daily_blueprint", bind=True, max_retries=2)
def generate_daily_blueprint(self, trading_date: str | None = None, prev_result=None) -> dict:
    """
    17:10 Celery 任务：生成次日交易蓝图
    prev_result: 上游任务 (compute_signals) 的结果
    """
    resolved_trading_date = resolve_trading_date_arg(trading_date, prev_result)
    logger.debug(
        "blueprint.generate.start",
        log_event="task_start",
        stage="entry",
        task_id=getattr(self.request, "id", None),
        trading_date=trading_date,
        resolved_trading_date=resolved_trading_date,
        retry=getattr(self.request, "retries", 0),
    )
    try:
        return run_async(_generate_blueprint_async(resolved_trading_date))
    except Exception as exc:
        logger.warning(
            "blueprint.generate.retrying",
            error=str(exc),
            retry=getattr(self.request, "retries", 0),
        )
        raise self.retry(exc=exc, countdown=60)


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
    signal_features: list[SignalFeatures] = []
    async with get_postgres_session() as session:
        result = await session.execute(
            text("SELECT features_json FROM signal_features WHERE date = :date"),
            {"date": td},
        )
        for row in result.fetchall():
            try:
                sf = _parse_signal_features(row[0])
                signal_features.append(sf)
            except Exception as e:
                logger.warning("blueprint.signal_parse_error", error=str(e))

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

    _annotate_blueprint_quality(blueprint, signal_features)
    blueprint = blueprint.model_copy(update={"id": blueprint.id})

    # 5) Write to DB (UPSERT)
    async with get_postgres_session() as session:
        logger.debug(
            "blueprint.generate.db_write_started",
            log_event="db_write",
            stage="before_write",
            trading_date=str(td),
            blueprint_id=blueprint.id,
        )
        await session.execute(
            text(
                "INSERT INTO llm_trading_blueprint "
                "(id, trading_date, generated_at, model_provider, model_version, "
                " blueprint_json, reasoning_json, status) "
                "VALUES (:id, :trading_date, :generated_at, :model_provider, "
                " :model_version, :blueprint_json, :reasoning_json, 'pending') "
                "ON CONFLICT (trading_date) DO UPDATE SET "
                "  id               = EXCLUDED.id, "
                "  generated_at     = EXCLUDED.generated_at, "
                "  model_provider   = EXCLUDED.model_provider, "
                "  model_version    = EXCLUDED.model_version, "
                "  blueprint_json   = EXCLUDED.blueprint_json, "
                "  reasoning_json   = EXCLUDED.reasoning_json, "
                "  status           = 'pending'"
            ),
            {
                "id": blueprint.id,
                "trading_date": blueprint.trading_date,
                "generated_at": blueprint.generated_at,
                "model_provider": blueprint.model_provider,
                "model_version": blueprint.model_version,
                "blueprint_json": blueprint.model_dump_json(),
                "reasoning_json": json.dumps(blueprint.reasoning_context, default=str) if blueprint.reasoning_context else None,
            },
        )
        logger.debug(
            "blueprint.generate.db_write_finished",
            log_event="db_write",
            stage="after_write",
            trading_date=str(td),
            blueprint_id=blueprint.id,
            provider=blueprint.model_provider,
        )

    # 6) Write-through cache refresh (best effort) with delete-on-write fallback
    from services.analysis_service.app.cache import (
        invalidate_blueprint_cache_strict,
        set_cached_blueprint_strict,
    )
    blueprint_data = {
        "id": blueprint.id,
        "trading_date": str(blueprint.trading_date),
        "status": "pending",
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
    )
    return {
        "trading_date": str(blueprint.trading_date),
        "blueprint_id": blueprint.id,
        "plans_count": len(blueprint.symbol_plans),
        "provider": blueprint.model_provider,
    }
