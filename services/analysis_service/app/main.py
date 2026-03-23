"""Analysis Service — FastAPI 入口"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from shared.config import get_settings
from shared.metrics import setup_metrics
from shared.utils import setup_logging, get_logger

from services.analysis_service.app.routes import router

logger = get_logger("analysis_service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging("analysis_service")
    settings = get_settings()
    logger.debug(
        "analysis_service.logging_config_snapshot",
        service_name="analysis_service",
        log_level=settings.logging.level,
        log_format=settings.logging.format,
        to_file=settings.logging.to_file,
        rotate_mode=settings.logging.file_rotate_mode,
    )
    logger.info("analysis_service.starting")
    yield
    from shared.redis_pool import close_redis_pool
    await close_redis_pool()
    logger.info("analysis_service.stopped")


app = FastAPI(
    title="Analysis Service",
    description="LLM 交易蓝图生成服务",
    version="0.1.0",
    lifespan=lifespan,
)
setup_metrics(app)

app.include_router(router, prefix="/api/v1")


@app.get("/api/v1/health")
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
        from shared.redis_pool import get_redis
        r = get_redis()
        await r.ping()
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = f"error: {e}"
        overall = False

    checks["status"] = "ok" if overall else "degraded"
    return checks


@app.get("/health", include_in_schema=False)
async def health_check_legacy_redirect():
    return RedirectResponse(url="/api/v1/health", status_code=307)
