"""Monitoring Service — FastAPI 入口"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from shared.config import get_settings
from shared.utils import setup_logging, get_logger

from services.monitoring_service.app.routes import router

logger = get_logger("monitoring_service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging("monitoring_service")
    settings = get_settings()
    logger.debug(
        "monitoring_service.logging_config_snapshot",
        service_name="monitoring_service",
        log_level=settings.logging.level,
        log_format=settings.logging.format,
        to_file=settings.logging.to_file,
        rotate_mode=settings.logging.file_rotate_mode,
    )
    logger.info("monitoring_service.starting")
    yield
    logger.info("monitoring_service.stopped")


app = FastAPI(
    title="Monitoring Service",
    description="系统指标与健康检查服务",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(router, prefix="/api/v1")


@app.get("/metrics")
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "monitoring_service"}
