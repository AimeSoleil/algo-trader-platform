"""Analysis Service — FastAPI 入口"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from shared.utils import setup_logging, get_logger

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


@app.get("/api/v1/blueprint/{trading_date}")
async def get_blueprint(trading_date: str):
    """查询某天的蓝图"""
    from services.analysis_service.app.queries import query_blueprint
    return await query_blueprint(trading_date)


@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "analysis_service"}
