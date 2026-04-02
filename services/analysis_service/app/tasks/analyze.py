"""Manual single-symbol LLM analysis."""
from __future__ import annotations

import json
from datetime import date
from time import perf_counter

from sqlalchemy import text

from shared.async_bridge import run_async
from shared.celery_app import celery_app
from shared.db.session import get_postgres_session
from shared.models.signal import SignalFeatures
from shared.utils import get_logger, today_trading

from services.analysis_service.app.tasks.blueprint import _annotate_blueprint_quality
from services.analysis_service.app.tasks.helpers import _get_adapter, _parse_signal_features

logger = get_logger("analysis_tasks")


@celery_app.task(
    name="analysis_service.tasks.manual_analyze",
    bind=True,
    max_retries=1,
)
def manual_analyze(self, symbol: str, trading_date: str | None = None) -> dict:
    """Manually trigger LLM analysis for a single symbol.

    Reads the symbol's signal features from DB, fetches positions,
    generates a blueprint containing only that symbol, and stores it
    with ``status='manual'``.
    """
    logger.debug(
        "manual_analyze.start",
        log_event="task_start",
        stage="entry",
        task_id=getattr(self.request, "id", None),
        symbol=symbol.upper(),
        trading_date=trading_date,
        retry=getattr(self.request, "retries", 0),
    )
    try:
        return run_async(_manual_analyze_async(self, symbol.upper(), trading_date))
    except Exception as exc:
        logger.warning(
            "manual_analyze.retrying",
            symbol=symbol,
            error=str(exc),
            retry=getattr(self.request, "retries", 0),
        )
        raise self.retry(exc=exc, countdown=30)


async def _manual_analyze_async(task, symbol: str, trading_date_str: str | None = None) -> dict:
    td = date.fromisoformat(trading_date_str) if trading_date_str else today_trading()
    started = perf_counter()
    logger.debug(
        "manual_analyze.context",
        log_event="task_context",
        stage="start",
        symbol=symbol,
        trading_date=str(td),
    )

    task.update_state(state="PROGRESS", meta={"step": "reading_signals", "symbol": symbol})

    # 1) Read single symbol's signal features
    signal_features: list[SignalFeatures] = []
    async with get_postgres_session() as session:
        result = await session.execute(
            text(
                "SELECT features_json FROM signal_features "
                "WHERE date = :date AND symbol = :symbol"
            ),
            {"date": td, "symbol": symbol},
        )
        for row in result.fetchall():
            try:
                sf = _parse_signal_features(row[0])
                signal_features.append(sf)
            except Exception as e:
                logger.warning("manual_analyze.signal_parse_error", symbol=symbol, error=str(e))

    logger.debug(
        "manual_analyze.signals_loaded",
        log_event="db_read",
        stage="signals_ready",
        symbol=symbol,
        trading_date=str(td),
        rows=len(signal_features),
    )

    if not signal_features:
        logger.warning("manual_analyze.no_signals", symbol=symbol, date=str(td))
        return {
            "error": f"No signal features for {symbol} on {td}",
            "symbol": symbol,
            "date": str(td),
        }

    # 2) Single LLM call (simplified path for single symbol)
    task.update_state(state="PROGRESS", meta={"step": "generating_blueprint", "symbol": symbol})

    from services.analysis_service.app.tasks.helpers import _fetch_current_positions
    current_positions = await _fetch_current_positions(td)
    adapter = _get_adapter()
    blueprint = await adapter.generate_single_symbol(
        signal_features=signal_features,
        current_positions=current_positions,
        signal_date=td,
    )

    _annotate_blueprint_quality(blueprint, signal_features)

    # 5) Write to DB with status='manual'
    import uuid as _uuid
    manual_id = f"manual-{symbol.lower()}-{_uuid.uuid4().hex[:8]}"
    blueprint = blueprint.model_copy(update={"id": manual_id})
    async with get_postgres_session() as session:
        logger.debug(
            "manual_analyze.db_write_started",
            log_event="db_write",
            stage="before_write",
            symbol=symbol,
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
            symbol=symbol,
            trading_date=str(td),
            blueprint_id=manual_id,
        )

    # Invalidate cache for this date
    from services.analysis_service.app.cache import invalidate_blueprint_cache
    logger.debug(
        "manual_analyze.cache_invalidate_started",
        log_event="cache_invalidate",
        stage="before_invalidate",
        trading_date=str(blueprint.trading_date),
    )
    await invalidate_blueprint_cache(blueprint.trading_date)

    logger.info(
        "manual_analyze.generated",
        symbol=symbol,
        trading_date=str(blueprint.trading_date),
        plans=len(blueprint.symbol_plans),
        provider=blueprint.model_provider,
        id=manual_id,
    )
    logger.debug(
        "manual_analyze.summary",
        log_event="task_summary",
        stage="completed",
        symbol=symbol,
        trading_date=str(blueprint.trading_date),
        blueprint_id=manual_id,
        plans=len(blueprint.symbol_plans),
        provider=blueprint.model_provider,
        duration_ms=round((perf_counter() - started) * 1000, 2),
    )
    return {
        "symbol": symbol,
        "trading_date": str(blueprint.trading_date),
        "blueprint_id": manual_id,
        "plans_count": len(blueprint.symbol_plans),
        "provider": blueprint.model_provider,
        "blueprint": blueprint.model_dump(),
    }
