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
    checks: dict = {"service": "analysis_service"}
    overall = True

    # DB
    try:
        from shared.db.session import get_postgres_session
        from sqlalchemy import text
        async with get_postgres_session() as session:
            await session.execute(text("SELECT 1"))
        checks["db"] = "ok"
    except Exception as e:
        checks["db"] = f"error: {e}"
        overall = False

    # Redis
    try:
        from redis.asyncio import Redis
        from shared.config import get_settings
        settings = get_settings()
        r = Redis.from_url(settings.redis.url)
        await r.ping()
        await r.aclose()
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = f"error: {e}"
        overall = False

    checks["status"] = "ok" if overall else "degraded"
    return checks
