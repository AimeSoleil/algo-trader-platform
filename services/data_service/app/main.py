"""Data Service — FastAPI 入口"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from services.data_service.app.routes import router
from shared.config import get_settings
from shared.metrics import setup_metrics
from shared.redis_pool import close_redis_pool, get_redis
from shared.utils import get_logger, setup_logging

logger = get_logger("data_service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / Shutdown

    - 盘中期权链采集由 Celery Beat 周期任务处理（data_service.tasks.capture_intraday_options）
    - 盘后数据采集由 Celery pipeline 处理，不依赖 FastAPI 进程
    """
    setup_logging("data_service")
    settings = get_settings()
    logger.debug(
        "data_service.logging_config_snapshot",
        service_name="data_service",
        log_level=settings.common.logging.level,
        log_format=settings.common.logging.format,
        to_file=settings.common.logging.to_file,
        rotate_mode=settings.common.logging.file_rotate_mode,
    )
    logger.info(
        "data_service.starting",
        watchlist=settings.common.watchlist.all,
    )

    # Eagerly initialise the shared Redis pool (used by distributed locks)
    get_redis()

    yield

    await close_redis_pool()
    logger.info("data_service.stopped")


app = FastAPI(
    title="Data Service",
    description="期权链 / 股票数据采集与缓存服务（盘后 Celery + 盘中可选 APScheduler）",
    version="0.2.0",
    lifespan=lifespan,
)
setup_metrics(app)

app.include_router(router, prefix="/api/v1")
