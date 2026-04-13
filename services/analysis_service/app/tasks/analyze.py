"""Manual LLM analysis — uses the same full agentic pipeline as the auto flow."""
from __future__ import annotations

import json
import uuid as _uuid
from datetime import date
from time import perf_counter

from sqlalchemy import text

from shared.async_bridge import run_async
from shared.celery_app import celery_app
from shared.db.session import get_postgres_session
from shared.models.signal import SignalFeatures
from shared.utils import get_logger, today_trading

from services.analysis_service.app.tasks.blueprint import (
    _annotate_blueprint_quality,
    _run_blueprint_pipeline,
)
from services.analysis_service.app.tasks.helpers import _parse_signal_features

logger = get_logger("analysis_tasks")


@celery_app.task(
    name="analysis_service.tasks.manual_analyze",
    bind=True,
    max_retries=1,
    soft_time_limit=2400,
    time_limit=2700,
)
def manual_analyze(
    self,
    symbols: list[str] | str,
    trading_date: str | None = None,
) -> dict:
    """Manually trigger LLM analysis for specified symbols.

    Uses the same full agentic pipeline (6 agents → synthesizer → critic)
    as the auto-triggered ``generate_daily_blueprint`` task.
    Reads signal features for the given symbols, generates a blueprint,
    and stores it with ``status='manual'``.
    """
    # Normalize: accept both "AAPL,NVDA" (str) and ["AAPL","NVDA"] (list)
    if isinstance(symbols, str):
        symbols = [s.strip() for s in symbols.split(",") if s.strip()]
    clean = [s.upper() for s in symbols]
    logger.debug(
        "manual_analyze.start",
        log_event="task_start",
        stage="entry",
        task_id=getattr(self.request, "id", None),
        symbols=clean,
        trading_date=trading_date,
        retry=getattr(self.request, "retries", 0),
    )
    try:
        return run_async(_manual_analyze_async(self, clean, trading_date))
    except Exception as exc:
        logger.warning(
            "manual_analyze.retrying",
            symbols=clean,
            error=str(exc),
            retry=getattr(self.request, "retries", 0),
        )
        raise self.retry(exc=exc, countdown=30)


async def _manual_analyze_async(
    task,
    symbols: list[str],
    trading_date_str: str | None = None,
) -> dict:
    td = date.fromisoformat(trading_date_str) if trading_date_str else today_trading()
    started = perf_counter()
    logger.debug(
        "manual_analyze.context",
        log_event="task_context",
        stage="start",
        symbols=symbols,
        trading_date=str(td),
    )

    task.update_state(
        state="PROGRESS",
        meta={"step": "reading_signals", "symbols": symbols},
    )

    # 1) Read signal features for the requested symbols
    signal_features: list[SignalFeatures] = []
    async with get_postgres_session() as session:
        result = await session.execute(
            text(
                "SELECT features_json FROM signal_features "
                "WHERE date = :date AND symbol = ANY(:symbols)"
            ),
            {"date": td, "symbols": symbols},
        )
        for row in result.fetchall():
            try:
                sf = _parse_signal_features(row[0])
                signal_features.append(sf)
            except Exception as e:
                logger.warning("manual_analyze.signal_parse_error", error=str(e))

    logger.debug(
        "manual_analyze.signals_loaded",
        log_event="db_read",
        stage="signals_ready",
        symbols=symbols,
        trading_date=str(td),
        rows=len(signal_features),
    )

    if not signal_features:
        logger.warning("manual_analyze.no_signals", symbols=symbols, date=str(td))
        return {
            "error": f"No signal features for {symbols} on {td}",
            "symbols": symbols,
            "date": str(td),
        }

    # 2) Full agentic pipeline (same as auto-triggered generate_daily_blueprint)
    task.update_state(
        state="PROGRESS",
        meta={"step": "generating_blueprint", "symbols": symbols},
    )

    blueprint = await _run_blueprint_pipeline(signal_features, td)
    _annotate_blueprint_quality(blueprint, signal_features)

    # 3) Write to DB with status='manual'
    manual_id = f"manual-{_uuid.uuid4().hex[:8]}"
    blueprint = blueprint.model_copy(update={"id": manual_id})

    async with get_postgres_session() as session:
        logger.debug(
            "manual_analyze.db_write_started",
            log_event="db_write",
            stage="before_write",
            trading_date=str(td),
            blueprint_id=manual_id,
        )
        await session.execute(
            text(
                "INSERT INTO llm_trading_blueprint "
                "(id, trading_date, generated_at, model_provider, model_version, "
                " blueprint_json, reasoning_json, status) "
                "VALUES (:id, :trading_date, :generated_at, :model_provider, "
                " :model_version, :blueprint_json, :reasoning_json, 'manual') "
                "ON CONFLICT (trading_date) DO UPDATE SET "
                "  id               = EXCLUDED.id, "
                "  generated_at     = EXCLUDED.generated_at, "
                "  model_provider   = EXCLUDED.model_provider, "
                "  model_version    = EXCLUDED.model_version, "
                "  blueprint_json   = EXCLUDED.blueprint_json, "
                "  reasoning_json   = EXCLUDED.reasoning_json, "
                "  status           = 'manual'"
            ),
            {
                "id": manual_id,
                "trading_date": blueprint.trading_date,
                "generated_at": blueprint.generated_at,
                "model_provider": blueprint.model_provider,
                "model_version": blueprint.model_version,
                "blueprint_json": blueprint.model_dump_json(),
                "reasoning_json": json.dumps(blueprint.reasoning_context, default=str) if blueprint.reasoning_context else None,
            },
        )
        logger.debug(
            "manual_analyze.db_write_finished",
            log_event="db_write",
            stage="after_write",
            trading_date=str(td),
            blueprint_id=manual_id,
        )

    # 4) Invalidate cache
    from services.analysis_service.app.cache import invalidate_blueprint_cache

    await invalidate_blueprint_cache(blueprint.trading_date)

    logger.info(
        "manual_analyze.generated",
        symbols=symbols,
        trading_date=str(blueprint.trading_date),
        plans=len(blueprint.symbol_plans),
        provider=blueprint.model_provider,
        id=manual_id,
    )
    logger.debug(
        "manual_analyze.summary",
        log_event="task_summary",
        stage="completed",
        symbols=symbols,
        trading_date=str(blueprint.trading_date),
        blueprint_id=manual_id,
        plans=len(blueprint.symbol_plans),
        provider=blueprint.model_provider,
        duration_ms=round((perf_counter() - started) * 1000, 2),
    )
    validation = (blueprint.reasoning_context or {}).get("deterministic_validation", {})
    soft_blocked = bool(validation.get("error_count", 0) > 0)
    return {
        "trading_date": str(blueprint.trading_date),
        "blueprint_id": manual_id,
        "symbols": symbols,
        "plans_count": len(blueprint.symbol_plans),
        "provider": blueprint.model_provider,
        "status": "manual",
        "soft_blocked": soft_blocked,
        "deterministic_validation": (
            (blueprint.reasoning_context or {}).get("deterministic_validation")
        ),
    }
