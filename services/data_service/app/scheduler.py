"""Data Service 调度器 — 盘中期权链采集（Intraday 可选）

盘后数据采集（1m bars / daily bar / option chain）已统一由
Celery pipeline (capture_post_market_data) 处理，不再由 APScheduler 触发。
APScheduler 仅负责盘中 5 分钟期权链快照采集。
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from services.data_service.app.cache import MarketHoursCache
from services.data_service.app.converters import contracts_to_rows
from services.data_service.app.fetchers.option_fetcher import fetch_option_chain
from services.data_service.app.storage import apply_intraday_retention
from shared.config.settings import Settings
from shared.utils import get_logger

logger = get_logger("scheduler")

_scheduler: AsyncIOScheduler | None = None
_state: SchedulerState | None = None


@dataclass
class SchedulerState:
    settings: Settings
    cache: MarketHoursCache
    intraday_enabled: bool


def _market_tz(settings: Settings) -> ZoneInfo:
    return ZoneInfo(settings.data_service.market_hours.timezone)


def _is_market_open(settings: Settings) -> bool:
    now = datetime.now(_market_tz(settings))
    if now.weekday() >= 5:
        return False

    start_hour, start_min = map(int, settings.data_service.market_hours.start.split(":"))
    end_hour, end_min = map(int, settings.data_service.market_hours.end.split(":"))
    now_minutes = now.hour * 60 + now.minute
    start_minutes = start_hour * 60 + start_min
    end_minutes = end_hour * 60 + end_min
    return start_minutes <= now_minutes <= end_minutes


# ── 公共查询接口 ───────────────────────────────────────────

def get_current_mode() -> str:
    """返回当前运行模式描述"""
    if _state is None:
        return "standard"
    return "standard+intraday" if _state.intraday_enabled else "standard"


def get_data_service_config() -> dict:
    if _state is None:
        return {}
    return _state.settings.data_service.model_dump()


# ── 盘中期权链采集 ─────────────────────────────────────────

async def _capture_intraday(state: SchedulerState) -> None:
    """盘中定时任务：每 5 分钟采集期权链快照 → L1 + L2 缓存"""
    if not _is_market_open(state.settings):
        logger.info("scheduler.intraday_skipped", reason="outside_market_hours")
        return

    symbols = state.settings.watchlist
    intraday_cfg = state.settings.data_service.intraday
    captured = 0

    for symbol in symbols:
        snapshot = await fetch_option_chain(symbol)
        if snapshot:
            rows = contracts_to_rows(snapshot, top_expiries=intraday_cfg.max_option_expiries)
            state.cache.update_option_chain(symbol, rows)
            captured += 1

    logger.info(
        "scheduler.intraday_captured",
        symbols_count=len(symbols),
        captured=captured,
    )


# ── 调度管理 ───────────────────────────────────────────────

def _register_intraday_job() -> None:
    """注册盘中期权链采集 job（仅在 intraday 启用时）"""
    if _scheduler is None or _state is None:
        return

    _scheduler.remove_all_jobs()

    if _state.intraday_enabled:
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

    - 始终初始化 state（供 API 查询模式 / 配置）
    - 始终设置 TimescaleDB retention policy
    - 仅在 intraday_enabled=True 时启动 APScheduler
    """
    global _scheduler, _state

    _state = SchedulerState(
        settings=settings,
        cache=cache,
        intraday_enabled=settings.data_service.intraday_enabled,
    )

    # Retention policy 无论模式都应用
    asyncio.create_task(
        apply_intraday_retention(
            settings.data_service.intraday.hot_storage_retention_days.stock_1min,
            settings.data_service.intraday.hot_storage_retention_days.option_5min,
        )
    )

    if settings.data_service.intraday_enabled:
        _scheduler = AsyncIOScheduler(
            timezone=settings.data_service.market_hours.timezone,
        )
        _register_intraday_job()
        _scheduler.start()
        logger.info("scheduler.started", mode=get_current_mode())
    else:
        logger.info(
            "scheduler.skipped",
            reason="intraday_disabled",
            mode=get_current_mode(),
        )


def set_intraday_enabled(enabled: bool) -> bool:
    """运行时启停 intraday 采集任务"""
    global _scheduler

    if _state is None:
        raise RuntimeError("scheduler not initialized")

    _state.intraday_enabled = enabled
    _state.settings.data_service.intraday_enabled = enabled

    if enabled and _scheduler is None:
        _scheduler = AsyncIOScheduler(
            timezone=_state.settings.data_service.market_hours.timezone,
        )
        _scheduler.start()

    if _scheduler is not None:
        _register_intraday_job()

    if not enabled and _scheduler is not None:
        _scheduler.shutdown(wait=False)
        _scheduler = None

    logger.info("scheduler.intraday_toggled", intraday_enabled=enabled)
    return enabled


def stop_scheduler() -> None:
    """停止调度器"""
    global _scheduler, _state
    if _scheduler:
        _scheduler.shutdown(wait=False)
        logger.info("scheduler.stopped")
    _scheduler = None
    _state = None
