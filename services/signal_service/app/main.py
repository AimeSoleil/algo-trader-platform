"""Signal Service — FastAPI 入口"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from sqlalchemy import text

from shared.config import get_settings
from shared.db.session import get_timescale_session, get_postgres_session
from shared.metrics import setup_metrics
from shared.utils import setup_logging, get_logger

from services.signal_service.app.routes import router

logger = get_logger("signal_service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging("signal_service")
    settings = get_settings()
    logger.debug(
        "signal_service.logging_config_snapshot",
        service_name="signal_service",
        log_level=settings.logging.level,
        log_format=settings.logging.format,
        to_file=settings.logging.to_file,
        rotate_mode=settings.logging.file_rotate_mode,
    )
    logger.info("signal_service.starting")
    yield
    from shared.redis_pool import close_redis_pool
    await close_redis_pool()
    logger.info("signal_service.stopped")


app = FastAPI(
    title="Signal Service",
    description="期权/股票特征计算与信号生成",
    version="0.1.0",
    lifespan=lifespan,
)
setup_metrics(app)

app.include_router(router, prefix="/api/v1")


@app.get("/api/v1/health")
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

    # Redis
    try:
        from shared.redis_pool import get_redis
        r = get_redis()
        await r.ping()
        deps["redis"] = "ok"
    except Exception as exc:
        deps["redis"] = f"error: {exc}"

    status = "ok" if all(v == "ok" for v in deps.values()) else "degraded"
    return {"status": status, "service": "signal_service", "dependencies": deps}


@app.get("/health", include_in_schema=False)
async def health_check_legacy_redirect():
    return RedirectResponse(url="/api/v1/health", status_code=307)
