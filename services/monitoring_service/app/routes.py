"""Monitoring Service — REST API 路由"""
from __future__ import annotations

from sqlalchemy import text
from fastapi import APIRouter

from shared.config import get_settings
from shared.db.session import get_postgres_session, get_timescale_session
from shared.utils import get_logger

from services.monitoring_service.app.metrics import (
    blueprint_loaded_total,
    post_market_pipeline_runs_total,
)

router = APIRouter(tags=["monitoring"])
logger = get_logger("monitoring_routes")


@router.get("/health/services")
async def health_services():
    logger.debug("monitoring.health_services_start", log_event="health_check", stage="start")
    postgres_ok = False
    timescale_ok = False
    postgres_error = ""
    timescale_error = ""

    try:
        async with get_postgres_session() as session:
            await session.execute(text("SELECT 1"))
        postgres_ok = True
    except Exception as exc:
        postgres_error = str(exc)

    try:
        async with get_timescale_session() as session:
            await session.execute(text("SELECT 1"))
        timescale_ok = True
    except Exception as exc:
        timescale_error = str(exc)

    logger.debug(
        "monitoring.health_services_done",
        log_event="health_check",
        stage="completed",
        postgres_ok=postgres_ok,
        timescale_ok=timescale_ok,
    )
    return {
        "postgres": {"ok": postgres_ok, "error": postgres_error or None},
        "timescale": {"ok": timescale_ok, "error": timescale_error or None},
    }


@router.get("/health/schedule")
async def health_schedule():
    settings = get_settings()
    logger.debug("monitoring.health_schedule", log_event="health_check", stage="schedule", has_schedule=bool(settings.schedule))
    return settings.schedule.model_dump()


@router.post("/metrics/blueprint_loaded")
async def metric_blueprint_loaded():
    blueprint_loaded_total.inc()
    logger.debug("monitoring.metric_blueprint_loaded", log_event="metric_update", metric="blueprint_loaded_total")
    return {"status": "ok", "metric": "blueprint_loaded_total"}


@router.post("/metrics/pipeline_stage")
async def metric_pipeline_stage(stage: str, status: str):
    post_market_pipeline_runs_total.labels(stage=stage, status=status).inc()
    logger.debug(
        "monitoring.metric_pipeline_stage",
        log_event="metric_update",
        metric="post_market_pipeline_runs_total",
        stage_label=stage,
        status_label=status,
    )
    return {
        "status": "ok",
        "metric": "post_market_pipeline_runs_total",
        "stage": stage,
        "result": status,
    }
