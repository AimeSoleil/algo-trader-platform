"""Analysis Service — FastAPI 入口"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from shared.utils import setup_logging, get_logger

from services.analysis_service.app.routes import router

logger = get_logger("analysis_service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging("analysis_service")
    logger.info("analysis_service.starting")
    yield
    logger.info("analysis_service.stopped")


app = FastAPI(
    title="Analysis Service",
    description="LLM 交易蓝图生成服务",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(router, prefix="/api/v1")


@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "analysis_service"}
