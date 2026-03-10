"""Execution Service — FastAPI 入口"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from shared.config import get_settings
from shared.utils import get_logger, setup_logging

from services.execution_service.app.models import runtime_state
from services.execution_service.app.routes import router
from services.execution_service.app.scheduler import start_execution_scheduler, stop_execution_scheduler

logger = get_logger("execution_service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging("execution_service")
    settings = get_settings()
    logger.info("execution_service.starting", execution_interval=settings.trading.execution_interval)
    start_execution_scheduler(runtime_state)

    yield

    stop_execution_scheduler()
    logger.info("execution_service.stopped")


app = FastAPI(
    title="Execution Service",
    description="蓝图加载与规则执行服务",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(router)
