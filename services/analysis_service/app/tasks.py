"""Analysis Service — Celery 盘后蓝图生成任务"""
from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
from time import perf_counter

from sqlalchemy import text

from shared.celery_app import celery_app
from shared.config import get_settings
from shared.db.session import get_postgres_session
from shared.models.signal import SignalFeatures
from shared.utils import get_logger, today_trading

logger = get_logger("analysis_tasks")


def _run_async(coro):
    """Run an async coroutine safely — works whether or not an event loop exists."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            return pool.submit(asyncio.run, coro).result()
    return asyncio.run(coro)


_adapter = None


def _get_adapter():
    """Return a cached LLMAdapter instance (reuses OpenAI client + skill bundle)."""
    global _adapter
    if _adapter is None:
        from services.analysis_service.app.llm.adapter import LLMAdapter
        _adapter = LLMAdapter()
    return _adapter


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


# ── Daily blueprint task ──────────────────────────────────────


@celery_app.task(name="analysis_service.tasks.generate_daily_blueprint", bind=True, max_retries=2)
def generate_daily_blueprint(self, trading_date: str | None = None, prev_result=None) -> dict:
    """
    17:10 Celery 任务：生成次日交易蓝图
    prev_result: 上游任务 (compute_signals) 的结果
    """
    logger.debug(
        "blueprint.generate.start",
        log_event="task_start",
        stage="entry",
        task_id=getattr(self.request, "id", None),
        trading_date=trading_date,
        retry=getattr(self.request, "retries", 0),
    )
    return _run_async(_generate_blueprint_async(trading_date))


async def _generate_blueprint_async(trading_date_str: str | None = None) -> dict:
    settings = get_settings()
    td = date.fromisoformat(trading_date_str) if trading_date_str else today_trading()
    started = perf_counter()
    logger.debug(
        "blueprint.generate.context",
        log_event="task_context",
        stage="start",
        trading_date=str(td),
        symbols=len(settings.watchlist),
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
                sf = SignalFeatures.model_validate_json(row[0])
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
                "(id, trading_date, generated_at, model_provider, model_version, blueprint_json, status) "
                "VALUES (:id, :trading_date, :generated_at, :model_provider, :model_version, :blueprint_json, 'pending') "
                "ON CONFLICT (trading_date) DO UPDATE SET "
                "blueprint_json = :blueprint_json, generated_at = :generated_at, "
                "model_provider = :model_provider, model_version = :model_version, status = 'pending'"
            ),
            {
                "id": blueprint.id,
                "trading_date": blueprint.trading_date,
                "generated_at": blueprint.generated_at,
                "model_provider": blueprint.model_provider,
                "model_version": blueprint.model_version,
                "blueprint_json": blueprint.model_dump_json(),
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


# ── Manual single-symbol analysis ─────────────────────────────


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
    return _run_async(_manual_analyze_async(self, symbol.upper(), trading_date))


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
                sf = SignalFeatures.model_validate_json(row[0])
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

    # 2-4) Common pipeline with progress callback
    def _progress(step: str):
        task.update_state(state="PROGRESS", meta={"step": step, "symbol": symbol})

    blueprint = await _run_blueprint_pipeline(signal_features, td, progress_cb=_progress)

    # 5) Write to DB with status='manual'
    import uuid as _uuid
    manual_id = f"manual-{symbol.lower()}-{_uuid.uuid4().hex[:8]}"
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
                " blueprint_json, status) "
                "VALUES (:id, :trading_date, :generated_at, :model_provider, "
                " :model_version, :blueprint_json, 'manual')"
            ),
            {
                "id": manual_id,
                "trading_date": blueprint.trading_date,
                "generated_at": blueprint.generated_at,
                "model_provider": blueprint.model_provider,
                "model_version": blueprint.model_version,
                "blueprint_json": blueprint.model_dump_json(),
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


# ── Position fetching with fallback ──────────────────────────


async def _fetch_current_positions(td: date) -> dict:
    """Fetch current positions from Portfolio Service.

    Fallback priority:
      1. Live open positions from ``positions`` table (via portfolio service logic).
      2. If none found — derive positions from yesterday's *completed* blueprint
         (i.e. the plans that were entered but not yet exited).
      3. If neither available — return an empty-positions dict.
    """
    logger.debug(
        "blueprint.fetch_positions.start",
        log_event="positions_fetch",
        stage="start",
        trading_date=str(td),
    )
    # ── Attempt 1: live positions from trade service portfolio module ──
    try:
        from services.trade_service.app.portfolio.service import get_positions

        positions_data = await get_positions()
        logger.debug(
            "blueprint.fetch_positions.portfolio_result",
            log_event="positions_fetch",
            stage="trade_service_portfolio",
            count=positions_data.get("count", 0),
        )
        if positions_data.get("count", 0) > 0:
            logger.info(
                "blueprint.positions_from_portfolio",
                count=positions_data["count"],
            )
            return {
                "source": "trade_service_portfolio",
                **positions_data,
            }
    except Exception as e:
        logger.warning("blueprint.portfolio_fetch_failed", error=str(e))

    # ── Attempt 2: infer from recent blueprint ──
    # Walk back up to 3 days for weekends / holidays
    for lookback in range(4):
        check_date = td - timedelta(days=1 + lookback)
        logger.debug(
            "blueprint.fetch_positions.previous_blueprint_check",
            log_event="positions_fetch",
            stage="previous_blueprint_lookup",
            trading_date=str(td),
            check_date=str(check_date),
        )
        try:
            async with get_postgres_session() as session:
                result = await session.execute(
                    text(
                        "SELECT blueprint_json FROM llm_trading_blueprint "
                        "WHERE trading_date = :date AND status IN ('completed', 'active')"
                    ),
                    {"date": check_date},
                )
                row = result.fetchone()
                if row and row[0]:
                    import json as _json

                    bp_data = _json.loads(row[0]) if isinstance(row[0], str) else row[0]
                    inferred = _infer_positions_from_blueprint(bp_data)
                    if inferred["count"] > 0:
                        logger.info(
                            "blueprint.positions_from_previous_blueprint",
                            blueprint_date=str(check_date),
                            count=inferred["count"],
                        )
                        return {
                            "source": "previous_blueprint",
                            "blueprint_date": str(check_date),
                            **inferred,
                        }
        except Exception as e:
            logger.warning(
                "blueprint.prev_blueprint_fetch_failed",
                date=str(check_date),
                error=str(e),
            )

    logger.info("blueprint.no_existing_positions")
    return {
        "source": "none",
        "count": 0,
        "positions": [],
        "aggregates": {},
    }


def _infer_positions_from_blueprint(bp_data: dict) -> dict:
    """Extract entered-but-not-exited plans from a completed blueprint.

    These serve as a proxy for "current positions" when the portfolio
    service has no live data.
    """
    positions: list[dict] = []
    for plan in bp_data.get("symbol_plans", []):
        if plan.get("is_entered") and not plan.get("is_exited"):
            legs_summary = []
            for leg in plan.get("legs", []):
                legs_summary.append({
                    "expiry": leg.get("expiry"),
                    "strike": leg.get("strike"),
                    "option_type": leg.get("option_type"),
                    "side": leg.get("side"),
                    "quantity": leg.get("quantity", 1),
                })
            positions.append({
                "underlying": plan.get("underlying"),
                "strategy_type": plan.get("strategy_type"),
                "direction": plan.get("direction"),
                "legs": legs_summary,
                "confidence": plan.get("confidence", 0),
                "entry_fill_prices": plan.get("entry_fill_prices", []),
                "realized_pnl": plan.get("realized_pnl", 0),
            })
    logger.debug(
        "blueprint.positions_inferred",
        log_event="positions_infer",
        stage="completed",
        count=len(positions),
    )
    return {"count": len(positions), "positions": positions}
