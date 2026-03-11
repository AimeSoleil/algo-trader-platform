"""Signal Service — FastAPI 入口"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from shared.utils import setup_logging, get_logger

from services.signal_service.app.routes import router

logger = get_logger("signal_service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging("signal_service")
    logger.info("signal_service.starting")
    yield
    logger.info("signal_service.stopped")


app = FastAPI(
    title="Signal Service",
    description="期权/股票特征计算与信号生成",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(router, prefix="/api/v1")


@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "signal_service"}
