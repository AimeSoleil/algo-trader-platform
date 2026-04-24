"""Unified post-market pipeline — options aggregation → stock capture → downstream."""
from __future__ import annotations

from time import perf_counter

from celery import chord, group

from shared.async_bridge import run_async
from shared.celery_app import celery_app
from shared.config import get_settings
from shared.pipeline import chunk_symbols
from shared.redis_pool import get_redis
from shared.utils import get_logger, today_trading

from services.data_service.app.tasks.capture import capture_post_market_chunk

logger = get_logger("data_tasks")

_STARTED_FLAG_TTL_SECONDS = 21_600  # 6 h


def _pipeline_started_key(td: str) -> str:
    return f"pipeline:started:{td}"


async def _set_started_flag(td: str) -> None:
    redis = get_redis()
    await redis.set(_pipeline_started_key(td), "1", ex=_STARTED_FLAG_TTL_SECONDS)


async def _check_started_flag(td: str) -> bool:
    redis = get_redis()
    return bool(await redis.exists(_pipeline_started_key(td)))


def _dispatch_post_market_stock_capture(
    trading_date: str,
    aggregation_result: dict,
    finalize_signature,
    *,
    log_event: str,
    started: float,
    mode: str,
) -> str:
    settings = get_settings()
    symbols = settings.common.watchlist.all
    chunk_size = settings.data_service.worker.pipeline.chunk_size
    chunks = chunk_symbols(symbols, chunk_size)

    pipeline = chord(
        group(
            capture_post_market_chunk.si(chunk, trading_date).set(queue="data")
            for chunk in chunks
        ),
        finalize_signature.set(queue="data"),
    )
    result = pipeline.apply_async()

    logger.info(
        log_event,
        trading_date=trading_date,
        mode=mode,
        chunks=len(chunks),
        chord_id=str(result.id),
        options_duration_ms=round((perf_counter() - started) * 1000, 2),
    )
    return str(result.id)


# ── Pipeline 入口 ──────────────────────────────────────────


@celery_app.task(
    name="data_service.tasks.run_post_market_pipeline",
    bind=True,
    max_retries=2,
    queue="data",
)
def run_post_market_pipeline(self, trading_date: str | None = None) -> dict:
    """统一盘后流水线（17:00 ET 由 Beat 触发）

    1. Options aggregation — 5-min snapshots → option_daily + option_iv_daily
    2. Stock capture — fan-out 1m bars + daily bars via chord
    3. Chord callback → dispatch downstream (signals → blueprint)
    """
    td = trading_date or today_trading().isoformat()
    started = perf_counter()

    run_async(_set_started_flag(td))

    logger.info("post_market_pipeline.start", trading_date=td)

    from shared.notifier.helpers import notify_sync
    from shared.notifier.base import NotificationEvent, EventType, Severity
    notify_sync(NotificationEvent(
        event_type=EventType.PIPELINE_STARTED,
        title="🔄 Post-Market Pipeline Started",
        message=f"Post-market pipeline initiated for {td}.",
        severity=Severity.INFO,
        payload={"trading_date": td},
    ))

    try:
        # ── Step 1: Options aggregation (sync, ~80s) ──
        from services.data_service.app.tasks.aggregation import aggregate_option_daily

        agg_result = aggregate_option_daily(td)
        logger.info(
            "post_market_pipeline.options_aggregated",
            trading_date=td,
            daily_rows=agg_result.get("daily_rows", 0),
            iv_underlyings=agg_result.get("iv_underlyings", 0),
        )
        notify_sync(NotificationEvent(
            event_type=EventType.PIPELINE_OPTIONS_AGGREGATED,
            title="📋 Options Data Aggregated",
            message=(
                f"Options aggregated for {td}: "
                f"{agg_result.get('daily_rows', 0)} daily rows, "
                f"{agg_result.get('iv_underlyings', 0)} IV underlyings."
            ),
            severity=Severity.INFO,
            payload={
                "trading_date": td,
                "daily_rows": str(agg_result.get("daily_rows", 0)),
                "iv_underlyings": str(agg_result.get("iv_underlyings", 0)),
            },
        ))

        # ── Step 2: Stock capture (chord → finalize) ──
        chord_id = _dispatch_post_market_stock_capture(
            td,
            agg_result,
            _post_market_finalize.s(td, agg_result),
            log_event="post_market_pipeline.stock_dispatched",
            started=started,
            mode="full_pipeline",
        )

        return {
            "status": "dispatched",
            "trading_date": td,
            "aggregation": agg_result,
            "stock_chord_id": chord_id,
        }

    except Exception as exc:
        logger.error("post_market_pipeline.failed", trading_date=td, error=str(exc))
        raise self.retry(exc=exc, countdown=120) from exc


@celery_app.task(
    name="data_service.tasks.ensure_post_market_pipeline_started",
    queue="data",
)
def ensure_post_market_pipeline_started(trading_date: str | None = None) -> dict:
    """Watchdog fallback for missed post-market dispatch.

    If Beat misses the exact 17:00 trigger but recovers shortly after, this task
    re-dispatches the pipeline only when no start flag exists for the day.
    """
    td = trading_date or today_trading().isoformat()
    already_started = run_async(_check_started_flag(td))

    if already_started:
        logger.info("post_market_pipeline.watchdog_already_started", trading_date=td)
        return {
            "status": "already_started",
            "trading_date": td,
        }

    task = celery_app.send_task(
        "data_service.tasks.run_post_market_pipeline",
        args=[td],
        queue="data",
    )

    logger.warning(
        "post_market_pipeline.watchdog_dispatched",
        trading_date=td,
        task_id=task.id,
        reason="scheduled post-market pipeline start flag missing",
    )

    from shared.notifier.base import EventType, NotificationEvent, Severity
    from shared.notifier.helpers import notify_sync

    notify_sync(NotificationEvent(
        event_type=EventType.PIPELINE_FAILED,
        title="⚠️ Post-Market Pipeline Watchdog Triggered",
        message=(
            f"The scheduled post-market pipeline did not appear to start for {td}. "
            f"A fallback dispatch has been queued automatically."
        ),
        severity=Severity.WARNING,
        payload={"trading_date": td, "task_id": task.id, "mode": "watchdog"},
    ))

    return {
        "status": "watchdog_dispatched",
        "trading_date": td,
        "task_id": task.id,
    }


@celery_app.task(
    name="data_service.tasks.run_post_market_collection_only",
    bind=True,
    max_retries=2,
    queue="data",
)
def run_post_market_collection_only(self, trading_date: str | None = None) -> dict:
    """Manual post-market collection: options aggregation + stock capture only.

    This intentionally skips downstream signal and analysis dispatch.
    """
    td = trading_date or today_trading().isoformat()
    started = perf_counter()

    logger.info("post_market_collection_only.start", trading_date=td)

    from shared.notifier.base import EventType, NotificationEvent, Severity
    from shared.notifier.helpers import notify_sync

    notify_sync(NotificationEvent(
        event_type=EventType.PIPELINE_STARTED,
        title="🔄 Post-Market Data Collection Started",
        message=(
            f"Manual post-market data collection initiated for {td}. "
            f"Downstream signals and blueprint generation are skipped."
        ),
        severity=Severity.INFO,
        payload={"trading_date": td, "mode": "collection_only"},
    ))

    try:
        from services.data_service.app.tasks.aggregation import aggregate_option_daily

        agg_result = aggregate_option_daily(td)
        logger.info(
            "post_market_collection_only.options_aggregated",
            trading_date=td,
            daily_rows=agg_result.get("daily_rows", 0),
            iv_underlyings=agg_result.get("iv_underlyings", 0),
        )
        notify_sync(NotificationEvent(
            event_type=EventType.PIPELINE_OPTIONS_AGGREGATED,
            title="📋 Options Data Aggregated",
            message=(
                f"Options aggregated for {td}: "
                f"{agg_result.get('daily_rows', 0)} daily rows, "
                f"{agg_result.get('iv_underlyings', 0)} IV underlyings."
            ),
            severity=Severity.INFO,
            payload={
                "trading_date": td,
                "daily_rows": str(agg_result.get("daily_rows", 0)),
                "iv_underlyings": str(agg_result.get("iv_underlyings", 0)),
                "mode": "collection_only",
            },
        ))

        chord_id = _dispatch_post_market_stock_capture(
            td,
            agg_result,
            _post_market_collection_only_finalize.s(td, agg_result),
            log_event="post_market_collection_only.stock_dispatched",
            started=started,
            mode="collection_only",
        )

        return {
            "status": "dispatched",
            "trading_date": td,
            "aggregation": agg_result,
            "stock_chord_id": chord_id,
            "downstream": {"status": "skipped", "reason": "manual_collection_only"},
        }

    except Exception as exc:
        logger.error("post_market_collection_only.failed", trading_date=td, error=str(exc))
        raise self.retry(exc=exc, countdown=120) from exc


@celery_app.task(
    name="data_service.tasks._post_market_finalize",
    queue="data",
)
def _post_market_finalize(
    stock_results,
    trading_date: str,
    aggregation_result: dict,
) -> dict:
    """Chord callback — stock capture done, dispatch downstream."""
    stock_chunks = len(stock_results) if isinstance(stock_results, list) else 1
    stock_errors = []
    for r in (stock_results if isinstance(stock_results, list) else [stock_results]):
        if isinstance(r, dict):
            stock_errors.extend(r.get("errors", []))

    logger.info(
        "post_market_pipeline.stock_complete",
        trading_date=trading_date,
        chunks=stock_chunks,
        stock_errors=len(stock_errors),
    )
    from shared.notifier.helpers import notify_sync
    from shared.notifier.base import NotificationEvent, EventType, Severity
    notify_sync(NotificationEvent(
        event_type=EventType.PIPELINE_STOCK_CAPTURED,
        title="📈 Stock Data Captured",
        message=(
            f"Stock capture complete for {trading_date}: "
            f"{stock_chunks} chunk(s), {len(stock_errors)} error(s)."
        ),
        severity=Severity.WARNING if stock_errors else Severity.INFO,
        payload={
            "trading_date": trading_date,
            "chunks": str(stock_chunks),
            "errors": str(len(stock_errors)),
        },
    ))

    # ── Dispatch downstream: backfill + signals → blueprint ──
    from services.data_service.app.tasks.coordination import dispatch_downstream

    downstream_result = dispatch_downstream(trading_date)

    # ── Schedule timeout fallback ──
    _schedule_timeout_check(trading_date)

    return {
        "status": "post_market_complete",
        "trading_date": trading_date,
        "aggregation": aggregation_result,
        "stock_chunks": stock_chunks,
        "stock_errors": stock_errors[:5],
        "downstream": downstream_result,
    }


@celery_app.task(
    name="data_service.tasks._post_market_collection_only_finalize",
    queue="data",
)
def _post_market_collection_only_finalize(
    stock_results,
    trading_date: str,
    aggregation_result: dict,
) -> dict:
    """Chord callback for manual post-market collection without downstream stages."""
    stock_chunks = len(stock_results) if isinstance(stock_results, list) else 1
    stock_errors = []
    for r in (stock_results if isinstance(stock_results, list) else [stock_results]):
        if isinstance(r, dict):
            stock_errors.extend(r.get("errors", []))

    logger.info(
        "post_market_collection_only.complete",
        trading_date=trading_date,
        chunks=stock_chunks,
        stock_errors=len(stock_errors),
    )

    from shared.notifier.base import EventType, NotificationEvent, Severity
    from shared.notifier.helpers import notify_sync

    notify_sync(NotificationEvent(
        event_type=EventType.PIPELINE_FINISHED,
        title="✅ Post-Market Data Collection Completed",
        message=(
            f"Options aggregation and stock capture completed for {trading_date}. "
            f"Signals and blueprint generation were not triggered."
        ),
        severity=Severity.WARNING if stock_errors else Severity.INFO,
        payload={
            "trading_date": trading_date,
            "chunks": str(stock_chunks),
            "errors": str(len(stock_errors)),
            "downstream": "skipped",
        },
    ))

    return {
        "status": "post_market_collection_only_complete",
        "trading_date": trading_date,
        "aggregation": aggregation_result,
        "stock_chunks": stock_chunks,
        "stock_errors": stock_errors[:5],
        "downstream": {"status": "skipped", "reason": "manual_collection_only"},
    }


def _schedule_timeout_check(trading_date: str) -> None:
    """Schedule a coordination timeout check after configured minutes."""
    settings = get_settings()
    timeout_minutes = settings.data_service.worker.pipeline.coordination_timeout_minutes
    timeout_seconds = timeout_minutes * 60

    from services.data_service.app.tasks.coordination import coordination_timeout_check

    coordination_timeout_check.apply_async(
        args=[trading_date],
        countdown=timeout_seconds,
        queue="data",
    )
    logger.info(
        "post_market_pipeline.timeout_scheduled",
        trading_date=trading_date,
        timeout_minutes=timeout_minutes,
    )
