"""Pipeline coordination — dispatch downstream stages after post-market capture."""
from __future__ import annotations

import asyncio

from celery import chain as celery_chain, chord, group

from shared.celery_app import celery_app
from shared.config import get_settings
from shared.pipeline import chunk_symbols
from shared.redis_pool import get_redis
from shared.utils import get_logger

logger = get_logger("data_tasks")

_FLAG_TTL_SECONDS = 86_400  # 24 h


def _pipeline_done_key(td: str) -> str:
    return f"pipeline:done:{td}"


# ── Downstream step names (ordered, critical path only) ─────
# Backfill is NOT in this list — it runs as fire-and-forget in parallel
# so it never blocks the signal / analysis stages.

_DOWNSTREAM_STEP_NAMES: list[str] = [
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


# ── Downstream dispatch (called by pipeline finalize) ──────


def _dispatch_backfill(td: str) -> None:
    """Fire-and-forget: fan out gap-detection chunks on the backfill queue.

    Backfill only fills historical gaps and does not produce data that
    signals depend on for the current trading_date, so it runs in
    parallel with the critical path (signals → blueprint).
    """
    settings = get_settings()
    symbols = settings.common.watchlist.all
    chunk_size = settings.data_service.worker.pipeline.chunk_size
    chunks = chunk_symbols(symbols, chunk_size)

    backfill_group = group(
        celery_app.signature(
            "backfill_service.tasks.detect_gaps_chunk",
            args=[chunk, td],
            queue="backfill",
            immutable=True,
        )
        for chunk in chunks
    )
    backfill_group.apply_async()
    logger.info("coordination.backfill_dispatched", trading_date=td, chunks=len(chunks))


def _build_downstream_steps(td: str) -> list[tuple[str, object]]:
    """Build the critical-path downstream steps: signals → blueprint."""
    settings = get_settings()
    symbols = settings.common.watchlist.all
    chunk_size = settings.data_service.worker.pipeline.chunk_size
    chunks = chunk_symbols(symbols, chunk_size)

    return [
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
                stage_barrier.s("compute_daily_signals", td).set(queue="data"),
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


def dispatch_downstream(trading_date: str) -> dict:
    """Dispatch backfill (fire-and-forget) and critical-path chain (signals → blueprint).

    Called directly by the unified post-market pipeline finalize callback.
    """
    logger.info("coordination.dispatching_downstream", trading_date=trading_date)

    # Notify: pipeline started
    from shared.notifier.helpers import notify_sync
    from shared.notifier.base import NotificationEvent, EventType, Severity
    notify_sync(NotificationEvent(
        event_type=EventType.PIPELINE_STARTED,
        title="🚀 Post-Market Pipeline Started",
        message=f"Options aggregation and stock capture completed for {trading_date}. "
                f"Starting downstream: signals → blueprint.",
        severity=Severity.INFO,
        payload={"trading_date": trading_date},
    ))

    # Set done flag for timeout check
    asyncio.run(_set_done_flag(trading_date))

    # Backfill — fire-and-forget, does not block signals / analysis
    _dispatch_backfill(trading_date)

    settings = get_settings()
    stop_after = settings.data_service.worker.pipeline.stop_after

    if stop_after not in _DOWNSTREAM_STEP_NAMES:
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

    If pipeline done flag is not set, downstream likely never ran.
    """
    done = asyncio.run(_check_done_flag(trading_date))

    if not done:
        logger.warning(
            "coordination.timeout",
            trading_date=trading_date,
        )

        from shared.notifier.helpers import notify_sync
        from shared.notifier.base import NotificationEvent, EventType, Severity
        notify_sync(NotificationEvent(
            event_type=EventType.PIPELINE_FAILED,
            title="⚠️ Pipeline Coordination Timeout",
            message=f"Post-market pipeline may not have completed for {trading_date}. "
                    f"Manual intervention may be needed.",
            severity=Severity.ERROR,
            payload={"trading_date": trading_date, "phase": "coordination"},
        ))

        return {"status": "timeout", "trading_date": trading_date}

    return {"status": "already_completed", "trading_date": trading_date}


# ── Redis helpers ──────────────────────────────────────────


async def _set_done_flag(td: str) -> None:
    redis = get_redis()
    await redis.set(_pipeline_done_key(td), "1", ex=_FLAG_TTL_SECONDS)


async def _check_done_flag(td: str) -> bool:
    redis = get_redis()
    return bool(await redis.exists(_pipeline_done_key(td)))
