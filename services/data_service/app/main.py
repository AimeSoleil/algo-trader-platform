"""Data Service — FastAPI 入口"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from services.data_service.app.cache import cache
from services.data_service.app.routes import router
from services.data_service.app.scheduler import start_data_scheduler, stop_scheduler
from shared.config import get_settings
from shared.metrics import setup_metrics
from shared.utils import get_logger, setup_logging

logger = get_logger("data_service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / Shutdown

    - 始终初始化 scheduler state（API 查询模式 / 配置 / retention policy）
    - APScheduler 仅在 intraday_enabled=True 时实际启动
    - 盘后数据采集由 Celery pipeline 处理，不依赖 FastAPI 进程
    """
    setup_logging("data_service")
    settings = get_settings()
    logger.debug(
        "data_service.logging_config_snapshot",
        service_name="data_service",
        log_level=settings.logging.level,
        log_format=settings.logging.format,
        to_file=settings.logging.to_file,
        rotate_mode=settings.logging.file_rotate_mode,
    )
    logger.info(
        "data_service.starting",
        watchlist=settings.watchlist,
        intraday_enabled=settings.data_service.intraday_enabled,
    )

    start_data_scheduler(cache, settings)

    yield

    stop_scheduler()
    logger.info("data_service.stopped")


app = FastAPI(
    title="Data Service",
    description="期权链 / 股票数据采集与缓存服务（盘后 Celery + 盘中可选 APScheduler）",
    version="0.2.0",
    lifespan=lifespan,
)
setup_metrics(app)

app.include_router(router, prefix="/api/v1")
