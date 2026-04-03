"""Pipeline coordination — trigger downstream stages when both pipelines complete."""
from __future__ import annotations

import asyncio
from datetime import datetime

from celery import chain as celery_chain, chord, group

from shared.celery_app import celery_app
from shared.config import get_settings
from shared.pipeline import chunk_symbols
from shared.redis_pool import get_redis
from shared.utils import get_logger

logger = get_logger("data_tasks")

_FLAG_TTL_SECONDS = 86_400  # 24 h


def _options_done_key(td: str) -> str:
    return f"pipeline:options_done:{td}"


def _stock_done_key(td: str) -> str:
    return f"pipeline:stock_done:{td}"


# ── Downstream step names (ordered) ─────────────────────────

_DOWNSTREAM_STEP_NAMES: list[str] = [
    "detect_and_backfill_gaps",
    "compute_daily_signals",
    "generate_daily_blueprint",
]


@celery_app.task(
    name="data_service.tasks.stage_barrier",
    queue="data",
)
def stage_barrier(results, stage_name: str, trading_date: str) -> dict:
    """Chord callback — logs stage completion and forwards trading_date."""
    logger.info(
        "pipeline.stage_completed",
        stage=stage_name,
        trading_date=trading_date,
        chunks=len(results) if isinstance(results, list) else 1,
    )
    return {"stage": stage_name, "trading_date": trading_date}


def _build_downstream_steps(td: str) -> list[tuple[str, object]]:
    """Build downstream pipeline steps: backfill → signals → blueprint."""
    settings = get_settings()
    symbols = settings.common.watchlist.all
    chunk_size = settings.data_service.worker.pipeline.chunk_size
    chunks = chunk_symbols(symbols, chunk_size)

    return [
        (
            "detect_and_backfill_gaps",
            chord(
                group(
                    celery_app.signature(
                        "backfill_service.tasks.detect_gaps_chunk",
                        args=[chunk, td],
                        queue="backfill",
                        immutable=True,
                    )
                    for chunk in chunks
                ),
                stage_barrier.si("detect_and_backfill_gaps", td).set(queue="data"),
            ),
        ),
        (
            "compute_daily_signals",
            chord(
                group(
                    celery_app.signature(
                        "signal_service.tasks.compute_signals_chunk",
                        args=[chunk, td],
                        queue="signal",
                        immutable=True,
                    )
                    for chunk in chunks
                ),
                stage_barrier.si("compute_daily_signals", td).set(queue="data"),
            ),
        ),
        (
            "generate_daily_blueprint",
            celery_app.signature(
                "analysis_service.tasks.generate_daily_blueprint",
                args=[td],
                queue="analysis",
                immutable=True,
            ),
        ),
    ]


@celery_app.task(
    name="data_service.tasks.check_pipelines_and_continue",
    queue="data",
)
def check_pipelines_and_continue(trading_date: str) -> dict:
    """Check whether both stock and options pipelines have completed.

    Called by both ``run_stock_pipeline`` and ``run_options_post_close``
    after they set their respective Redis flags.  When both flags are
    present, this task dispatches the downstream chain (backfill →
    signals → blueprint) and cleans up the flags.
    """
    stock_ready, options_ready = asyncio.run(_check_flags(trading_date))

    if not (stock_ready and options_ready):
        missing = []
        if not stock_ready:
            missing.append("stock")
        if not options_ready:
            missing.append("options")
        logger.info(
            "coordination.waiting",
            trading_date=trading_date,
            missing=missing,
        )
        return {"status": "waiting", "trading_date": trading_date, "missing": missing}

    # ── Both ready — dispatch downstream ──
    logger.info("coordination.both_ready", trading_date=trading_date)

    # Clean up flags
    asyncio.run(_delete_flags(trading_date))

    settings = get_settings()
    stop_after = settings.data_service.worker.pipeline.stop_after

    if stop_after not in _DOWNSTREAM_STEP_NAMES:
        # stop_after is set to a data-stage name — no downstream to run
        logger.info(
            "coordination.no_downstream",
            trading_date=trading_date,
            stop_after=stop_after,
        )
        return {"status": "no_downstream", "trading_date": trading_date}

    all_steps = _build_downstream_steps(trading_date)
    cutoff = _DOWNSTREAM_STEP_NAMES.index(stop_after)
    included = all_steps[: cutoff + 1]
    gated_out = [name for name, _ in all_steps[cutoff + 1:]]

    if gated_out:
        logger.info(
            "coordination.steps_gated_out",
            trading_date=trading_date,
            gated_out=gated_out,
            stop_after=stop_after,
        )

    pipeline = celery_chain(*[sig for _, sig in included])
    result = pipeline.apply_async()

    logger.info(
        "coordination.downstream_started",
        trading_date=trading_date,
        chain_id=str(result.id),
        steps=[name for name, _ in included],
    )
    return {
        "status": "downstream_dispatched",
        "trading_date": trading_date,
        "chain_id": str(result.id),
        "steps": [name for name, _ in included],
    }


# ── Timeout task — scheduled as a fallback ─────────────────


@celery_app.task(
    name="data_service.tasks.coordination_timeout_check",
    queue="data",
)
def coordination_timeout_check(trading_date: str) -> dict:
    """Fallback — triggered after coordination_timeout_minutes.

    If downstream hasn't run yet (flags still exist), log a warning.
    This does NOT auto-dispatch to avoid running on stale/missing data.
    """
    stock_ready, options_ready = asyncio.run(_check_flags(trading_date))

    if stock_ready or options_ready:
        missing = []
        if not stock_ready:
            missing.append("stock")
        if not options_ready:
            missing.append("options")
        logger.warning(
            "coordination.timeout",
            trading_date=trading_date,
            stock_done=stock_ready,
            options_done=options_ready,
            missing=missing,
        )
        return {"status": "timeout", "trading_date": trading_date, "missing": missing}

    # Flags already cleaned up — downstream already ran
    return {"status": "already_completed", "trading_date": trading_date}


# ── Redis helpers ──────────────────────────────────────────


async def _check_flags(td: str) -> tuple[bool, bool]:
    redis = get_redis()
    stock = await redis.exists(_stock_done_key(td))
    options = await redis.exists(_options_done_key(td))
    return bool(stock), bool(options)


async def _delete_flags(td: str) -> None:
    redis = get_redis()
    await redis.delete(_stock_done_key(td), _options_done_key(td))
