"""Signal Service — FastAPI 入口"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import text

from shared.db.session import get_timescale_session, get_postgres_session
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
    deps: dict[str, str] = {}

    # TimescaleDB
    try:
        async with get_timescale_session() as session:
            await session.execute(text("SELECT 1"))
        deps["timescaledb"] = "ok"
    except Exception as exc:
        deps["timescaledb"] = f"error: {exc}"

    # Postgres
    try:
        async with get_postgres_session() as session:
            await session.execute(text("SELECT 1"))
        deps["postgres"] = "ok"
    except Exception as exc:
        deps["postgres"] = f"error: {exc}"

    status = "ok" if all(v == "ok" for v in deps.values()) else "degraded"
    return {"status": status, "service": "signal_service", "dependencies": deps}
