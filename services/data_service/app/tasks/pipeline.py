"""Post-market pipeline orchestration — gated chain with chord fan-out/fan-in."""
from __future__ import annotations

from celery import chain as celery_chain, chord, group

from shared.celery_app import celery_app
from shared.config import get_settings
from shared.pipeline import chunk_symbols
from shared.utils import get_logger, today_trading

from services.data_service.app.tasks.aggregation import aggregate_option_daily
from services.data_service.app.tasks.capture import (
    capture_post_market_chunk,
    capture_post_market_data,
)

logger = get_logger("data_tasks")


# ── 流水线步骤注册表（有序）─────────────────────────────────

# Valid stop_after values, in execution order.
_PIPELINE_STEP_NAMES: list[str] = [
    "capture_post_market_data",
    "aggregate_option_daily",
    "detect_and_backfill_gaps",
    "compute_daily_signals",
    "generate_daily_blueprint",
]


@celery_app.task(
    name="data_service.tasks.stage_barrier",
    queue="data",
)
def stage_barrier(results, stage_name: str, trading_date: str) -> dict:
    """Chord callback — logs stage completion and forwards trading_date.

    *results* is the list of return values from all chunk tasks in the group.
    This task is intentionally lightweight and lives on the ``data`` queue.
    """
    logger.info(
        "pipeline.stage_completed",
        stage=stage_name,
        trading_date=trading_date,
        chunks=len(results) if isinstance(results, list) else 1,
    )
    return {"stage": stage_name, "trading_date": trading_date}


def _build_pipeline_steps(td: str) -> list[tuple[str, object]]:
    """Return ALL pipeline steps as (name, signature) pairs, in order.

    Chunked stages (capture / backfill / signals) are expressed as
    ``chord(group([chunk_task, ...]), stage_barrier.s())`` so that all chunks
    in a stage execute in parallel, and the next stage only starts when all
    chunks are done.

    Non-chunked stages (aggregate / blueprint) remain single tasks.
    """
    settings = get_settings()
    symbols = settings.common.watchlist.all
    chunk_size = settings.data_service.worker.pipeline.chunk_size
    chunks = chunk_symbols(symbols, chunk_size)

    return [
        (
            "capture_post_market_data",
            chord(
                group(
                    capture_post_market_chunk.si(chunk, td).set(queue="data")
                    for chunk in chunks
                ),
                stage_barrier.si("capture_post_market_data", td).set(queue="data"),
            ),
        ),
        (
            "aggregate_option_daily",
            aggregate_option_daily.si(td).set(queue="data"),
        ),
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


# ── Pipeline 入口 ──────────────────────────────────────────


@celery_app.task(name="data_service.tasks.run_post_market_pipeline", queue="data")
def run_post_market_pipeline(trading_date: str | None = None) -> str:
    """盘后流水线入口（由 Celery Beat 触发）

    Stages with symbol-level work (capture / backfill / signals) are expressed
    as ``chord(group([chunk_tasks...]), barrier)`` so all chunks in a stage
    execute in parallel.  Stages without symbol iteration (aggregate /
    blueprint) run as single tasks.

    The chain is gated by ``data_service.worker.pipeline.stop_after``:
    all steps up to and including *stop_after* execute; later steps are skipped.

    Valid stop_after values (ordered):
      capture_post_market_data → aggregate_option_daily
      → detect_and_backfill_gaps → compute_daily_signals → generate_daily_blueprint
    """
    settings = get_settings()
    stop_after = settings.data_service.worker.pipeline.stop_after
    td = trading_date or today_trading().isoformat()

    if stop_after not in _PIPELINE_STEP_NAMES:
        raise ValueError(
            f"Invalid stop_after value: {stop_after!r}. "
            f"Must be one of: {_PIPELINE_STEP_NAMES}"
        )

    all_steps = _build_pipeline_steps(td)
    cutoff = _PIPELINE_STEP_NAMES.index(stop_after)
    included = all_steps[: cutoff + 1]
    gated_out = [name for name, _ in all_steps[cutoff + 1 :]]

    logger.debug(
        "post_market_pipeline.building",
        log_event="pipeline_start",
        stage="compose_chain",
        trading_date=td,
        stop_after=stop_after,
        included_steps=[name for name, _ in included],
        gated_out_steps=gated_out,
    )
    if gated_out:
        logger.info(
            "post_market_pipeline.steps_gated_out",
            trading_date=td,
            gated_out=gated_out,
            stop_after=stop_after,
        )

    # Build the sequential pipeline.
    # chord objects and plain signatures can be composed via celery_chain.
    pipeline = celery_chain(*[sig for _, sig in included])
    result = pipeline.apply_async()
    logger.info(
        "post_market_pipeline.started",
        trading_date=td,
        task_id=str(result.id),
        stop_after=stop_after,
        steps=len(included),
    )
    return f"Pipeline started: {result.id}"


# ── Post-market data collection only (no signals / blueprint) ──


@celery_app.task(
    name="data_service.tasks.collect_post_market_data",
    bind=True,
    max_retries=3,
    queue="data",
)
def collect_post_market_data(self, trading_date: str | None = None) -> dict:
    """只执行盘后数据采集（1m bars / daily bars / aggregate option daily）。

    Chain:  capture_post_market_data → aggregate_option_daily
    不触发 backfill / signals / blueprint，适合手动补采。
    """
    td = trading_date or today_trading().isoformat()
    logger.info(
        "collect_post_market.building",
        log_event="task_start",
        stage="compose_chain",
        trading_date=td,
        task_id=getattr(self.request, "id", None),
    )

    pipeline = celery_chain(
        capture_post_market_data.si(td).set(queue="data"),
        aggregate_option_daily.si(td).set(queue="data"),
    )

    result = pipeline.apply_async()
    logger.info("collect_post_market.started", trading_date=td, chain_id=str(result.id))
    return {
        "status": "chain_dispatched",
        "trading_date": td,
        "chain_id": str(result.id),
        "steps": [
            "capture_post_market_data",
            "aggregate_option_daily",
        ],
    }
