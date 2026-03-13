"""Gateway business routes — health checks & spec management."""
from __future__ import annotations

from . import docs

import asyncio
from time import perf_counter

from fastapi import APIRouter
from fastapi.responses import RedirectResponse

from shared.utils import get_logger

from .registry import ServiceRegistry

logger = get_logger("gateway")

router = APIRouter()

# Injected at startup via ``configure()``.
_registry: ServiceRegistry | None = None
_get_http: callable = lambda: None  # type: ignore[assignment]


def configure(
    registry: ServiceRegistry,
    http_getter: callable,  # type: ignore[type-arg]
) -> None:
    """Wire runtime dependencies (called once from app factory)."""
    global _registry, _get_http
    _registry = registry
    _get_http = http_getter


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@router.get("/api/v1/health")
async def health():
    return {"status": "ok", "service": "gateway"}


@router.get("/api/v1/health/all")
async def health_all():
    """Check health of all registered services."""
    assert _registry is not None
    started = perf_counter()
    logger.debug(
        "gateway.health_all_start",
        log_event="health_check",
        stage="start",
        services=len(_registry),
    )

    http_client = _get_http()

    async def _check(name: str, url: str):
        try:
            resp = await http_client.get(f"{url}/api/v1/health", timeout=5.0)
            return name, resp.status_code == 200, None
        except Exception as exc:
            return name, False, str(exc)

    checks = await asyncio.gather(
        *[_check(n, e.url) for n, e in _registry.items()]
    )
    services = {name: {"healthy": ok, "error": err} for name, ok, err in checks}
    all_ok = all(s["healthy"] for s in services.values())
    logger.debug(
        "gateway.health_all_done",
        log_event="health_check",
        stage="completed",
        healthy_count=sum(1 for s in services.values() if s["healthy"]),
        total_count=len(services),
        duration_ms=round((perf_counter() - started) * 1000, 2),
    )
    return {"status": "ok" if all_ok else "degraded", "services": services}


# Legacy redirects
@router.get("/health", include_in_schema=False)
async def health_legacy():
    return RedirectResponse(url="/api/v1/health", status_code=307)


@router.get("/health/all", include_in_schema=False)
async def health_all_legacy():
    return RedirectResponse(url="/api/v1/health/all", status_code=307)

@router.get("/specs/refresh")
async def refresh_specs():
    """Invalidate the cached merged OpenAPI spec so it rebuilds on next request."""
    docs.invalidate_cache()
    logger.debug("gateway.refresh_specs_endpoint", log_event="spec_refresh", stage="invalidated")
    return {
        "status": "ok",
        "message": "OpenAPI cache invalidated; next request will rebuild.",
    }
