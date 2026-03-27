"""Data Service 调度器 — 盘中期权链采集

盘后数据采集（1m bars / daily bar / option chain）已统一由
Celery pipeline (capture_post_market_data) 处理，不再由 APScheduler 触发。
APScheduler 仅负责盘中 5 分钟期权链快照采集。
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from services.data_service.app.cache import MarketHoursCache
from services.data_service.app.converters import contracts_to_rows
from services.data_service.app.fetchers.registry import get_option_fetcher
from services.data_service.app.filters import apply_option_pipeline
from services.data_service.app.storage import apply_intraday_retention
from shared.config.settings import Settings
from shared.distributed_lock import distributed_once
from shared.utils import get_logger, is_market_open

logger = get_logger("scheduler")

_scheduler: AsyncIOScheduler | None = None
_state: SchedulerState | None = None


@dataclass
class SchedulerState:
    settings: Settings
    cache: MarketHoursCache
    outside_market_logged: bool = False


# ── 公共查询接口 ───────────────────────────────────────────

def get_current_mode() -> str:
    """返回当前运行模式描述"""
    return "standard+intraday"


def get_data_service_config() -> dict:
    if _state is None:
        return {}
    return _state.settings.data_service.model_dump()


# ── 盘中期权链采集 ─────────────────────────────────────────

@distributed_once("data:intraday_capture", ttl=240, service="data_service")
async def _capture_intraday(state: SchedulerState) -> None:
    """盘中定时任务：每 5 分钟采集期权链快照 → L1 + L2 缓存

    Wrapped with ``@distributed_once`` so that when multiple data_service
    replicas are running, only one instance executes the capture per tick.
    """
    if not is_market_open():
        if not state.outside_market_logged:
            logger.info("scheduler.intraday_skipped", reason="outside_market_hours")
            state.outside_market_logged = True
        return

    if state.outside_market_logged:
        logger.info("scheduler.intraday_resumed", reason="market_open")
        state.outside_market_logged = False

    symbols = state.settings.common.watchlist
    intraday_cfg = state.settings.data_service.intraday
    captured = 0

    for symbol in symbols:
        # Skip index symbols (e.g. ^VIX) — they have no tradeable option chain
        if symbol.startswith("^"):
            continue
        snapshot = await get_option_fetcher().fetch_current(symbol)
        if snapshot:
            # 两阶段过滤：Stage 1 清洁 → Stage 2 可交易标记
            snapshot, _filter_result = apply_option_pipeline(snapshot)
            rows = contracts_to_rows(snapshot, top_expiries=None)  # capture ALL expiries for aggregation
            state.cache.update_option_chain(symbol, rows)
            captured += 1

    logger.info(
        "scheduler.intraday_captured",
        symbols_count=len(symbols),
        captured=captured,
    )


# ── 调度管理 ───────────────────────────────────────────────

def _register_intraday_job() -> None:
    """注册盘中期权链采集 job"""
    if _scheduler is None or _state is None:
        return

    _scheduler.remove_all_jobs()

    interval = _state.settings.data_service.intraday.capture_every_minutes
    _scheduler.add_job(
        _capture_intraday,
        trigger=IntervalTrigger(minutes=interval),
        args=[_state],
        id="intraday_capture",
        name="intraday_option_chain_capture",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )
    logger.info("scheduler.intraday_job_registered", interval_min=interval)


def start_data_scheduler(cache: MarketHoursCache, settings: Settings) -> None:
    """启动数据调度器

    - 初始化 state（供 API 查询 / 配置）
    - 设置 TimescaleDB retention policy
    - 启动 APScheduler 盘中采集
    """
    global _scheduler, _state

    _state = SchedulerState(
        settings=settings,
        cache=cache,
    )

    # Retention policy 无论模式都应用
    asyncio.create_task(
        apply_intraday_retention(
            settings.data_service.intraday.retention_days.stock_1min,
            settings.data_service.intraday.retention_days.option_5min,
        )
    )

    _scheduler = AsyncIOScheduler(
        timezone=settings.common.timezone,
    )
    _register_intraday_job()
    _scheduler.start()
    logger.info("scheduler.started", mode=get_current_mode())


def stop_scheduler() -> None:
    """停止调度器"""
    global _scheduler, _state
    if _scheduler:
        _scheduler.shutdown(wait=False)
        logger.info("scheduler.stopped")
    _scheduler = None
    _state = None
