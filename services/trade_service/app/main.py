"""Trade Service — merged execution + portfolio FastAPI 入口"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from shared.config import get_settings
from shared.metrics import setup_metrics
from shared.redis_pool import close_redis_pool, get_redis
from shared.utils import get_logger, setup_logging

from services.trade_service.app.execution.routes import router as execution_router
from services.trade_service.app.execution.scheduler import (
    _shutdown_broker,
    _startup_broker,
    start_execution_scheduler,
    stop_execution_scheduler,
)
from services.trade_service.app.models import runtime_state
from services.trade_service.app.portfolio.routes import router as portfolio_router

logger = get_logger("trade_service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging("trade_service")
    settings = get_settings()
    logger.debug(
        "trade_service.logging_config_snapshot",
        service_name="trade_service",
        log_level=settings.common.logging.level,
        log_format=settings.common.logging.format,
        to_file=settings.common.logging.to_file,
        rotate_mode=settings.common.logging.file_rotate_mode,
    )
    logger.info("trade_service.starting", execution_interval=settings.trade_service.execution_interval)

    await _startup_broker()

    # Eagerly initialise the shared Redis pool (used by distributed locks & tick)
    get_redis()

    start_execution_scheduler(runtime_state)

    yield

    stop_execution_scheduler()
    await _shutdown_broker()
    await close_redis_pool()
    logger.info("trade_service.stopped")


app = FastAPI(
    title="Trade Service",
    description="Merged execution and portfolio service",
    version="0.1.0",
    lifespan=lifespan,
)
setup_metrics(app)

app.include_router(execution_router, prefix="/api/v1/trade")
app.include_router(portfolio_router, prefix="/api/v1/trade")


@app.get("/api/v1/health")
async def health_check():
    return {
        "status": "ok",
        "service": "trade_service",
        "runtime_status": runtime_state.status,
        "paused": runtime_state.paused,
    }


@app.get("/health", include_in_schema=False)
async def health_check_legacy_redirect():
    return RedirectResponse(url="/api/v1/health", status_code=307)
