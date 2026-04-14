"""Unified post-market pipeline — options aggregation → stock capture → downstream."""
from __future__ import annotations

from time import perf_counter

from celery import chord, group

from shared.celery_app import celery_app
from shared.config import get_settings
from shared.pipeline import chunk_symbols
from shared.utils import get_logger, today_trading

from services.data_service.app.tasks.capture import capture_post_market_chunk

logger = get_logger("data_tasks")


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
    3. Chord callback → dispatch downstream (backfill + signals → blueprint)
    """
    td = trading_date or today_trading().isoformat()
    started = perf_counter()

    logger.info("post_market_pipeline.start", trading_date=td)

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

        # ── Step 2: Stock capture (chord → finalize) ──
        settings = get_settings()
        symbols = settings.common.watchlist.all
        chunk_size = settings.data_service.worker.pipeline.chunk_size
        chunks = chunk_symbols(symbols, chunk_size)

        pipeline = chord(
            group(
                capture_post_market_chunk.si(chunk, td).set(queue="data")
                for chunk in chunks
            ),
            _post_market_finalize.s(td, agg_result).set(queue="data"),
        )
        result = pipeline.apply_async()

        logger.info(
            "post_market_pipeline.stock_dispatched",
            trading_date=td,
            chunks=len(chunks),
            chord_id=str(result.id),
            options_duration_ms=round((perf_counter() - started) * 1000, 2),
        )

        return {
            "status": "dispatched",
            "trading_date": td,
            "aggregation": agg_result,
            "stock_chord_id": str(result.id),
        }

    except Exception as exc:
        logger.error("post_market_pipeline.failed", trading_date=td, error=str(exc))
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
