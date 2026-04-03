"""Options post-close pipeline — aggregate intraday snapshots and signal readiness."""
from __future__ import annotations

import asyncio

from shared.celery_app import celery_app
from shared.redis_pool import get_redis
from shared.utils import get_logger, today_trading

logger = get_logger("data_tasks")

_FLAG_TTL_SECONDS = 86_400  # 24 h — shared by both pipeline flag setters


def _options_done_key(trading_date: str) -> str:
    return f"pipeline:options_done:{trading_date}"


@celery_app.task(
    name="data_service.tasks.run_options_post_close",
    bind=True,
    max_retries=3,
    queue="data",
)
def run_options_post_close(self, trading_date: str | None = None) -> dict:
    """盘后期权聚合流水线（16:10 ET 由 Beat 触发）

    1. aggregate_option_daily  — 5-min snapshots → option_daily + option_iv_daily
    2. Set Redis flag ``pipeline:options_done:{date}``
    3. Trigger coordination check
    """
    td = trading_date or today_trading().isoformat()
    logger.info("options_post_close.start", trading_date=td)

    # ── Step 1: aggregate ──
    from services.data_service.app.tasks.aggregation import aggregate_option_daily

    agg_result = aggregate_option_daily(td)

    # ── Step 2: set Redis done-flag ──
    asyncio.run(_set_done_flag(td))
    logger.info("options_post_close.flag_set", trading_date=td)

    # ── Step 3: trigger coordination ──
    from services.data_service.app.tasks.coordination import check_pipelines_and_continue

    check_pipelines_and_continue.delay(td)

    return {
        "status": "options_pipeline_complete",
        "trading_date": td,
        "aggregation": agg_result,
    }


async def _set_done_flag(trading_date: str) -> None:
    redis = get_redis()
    await redis.set(_options_done_key(trading_date), "1", ex=_FLAG_TTL_SECONDS)
