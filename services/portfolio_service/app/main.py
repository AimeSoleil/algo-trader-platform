"""Portfolio Service — FastAPI 入口"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from shared.utils import setup_logging, get_logger

from services.portfolio_service.app.routes import router

logger = get_logger("portfolio_service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging("portfolio_service")
    logger.info("portfolio_service.starting")
    yield
    logger.info("portfolio_service.stopped")


app = FastAPI(
    title="Portfolio Service",
    description="持仓查询、组合敞口与绩效归因服务",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(router, prefix="/api/v1")


@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "portfolio_service"}
