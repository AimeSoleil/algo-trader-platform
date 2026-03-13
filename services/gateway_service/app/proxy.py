"""Reverse proxy — forwards requests to backend services."""
from __future__ import annotations

from time import perf_counter

import httpx
from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse

from shared.utils import get_logger

from .registry import ServiceRegistry

logger = get_logger("gateway")

router = APIRouter()

# These are injected at app startup via ``configure()``.
_registry: ServiceRegistry | None = None
_get_http: callable = lambda: None  # type: ignore[assignment]


def configure(registry: ServiceRegistry, http_getter: callable) -> None:  # type: ignore[type-arg]
    """Wire runtime dependencies (called once from app factory)."""
    global _registry, _get_http
    _registry = registry
    _get_http = http_getter


@router.api_route(
    "/{service}/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
    include_in_schema=False,
)
async def proxy(service: str, path: str, request: Request):
    """Forward requests to the appropriate backend service."""
    started = perf_counter()
    assert _registry is not None

    entry = _registry.get(service)
    if not entry:
        logger.debug("gateway.proxy_unknown_service", log_event="proxy", service=service, path=path)
        return JSONResponse(status_code=404, content={"error": f"Unknown service: {service}"})

    target_url = f"{entry.url}/{path}"
    if request.url.query:
        target_url += f"?{request.url.query}"

    body = await request.body()
    headers = dict(request.headers)
    headers.pop("host", None)
    logger.debug(
        "gateway.proxy_forward_start",
        log_event="proxy",
        service=service,
        method=request.method,
        path=path,
    )

    http_client: httpx.AsyncClient = _get_http()
    try:
        resp = await http_client.request(
            method=request.method,
            url=target_url,
            content=body,
            headers=headers,
        )
        passthrough = {
            k: v
            for k, v in resp.headers.items()
            if k.lower() not in ("transfer-encoding", "connection")
        }
        logger.debug(
            "gateway.proxy_forward_done",
            log_event="proxy",
            service=service,
            method=request.method,
            path=path,
            status_code=resp.status_code,
            duration_ms=round((perf_counter() - started) * 1000, 2),
        )
        return Response(content=resp.content, status_code=resp.status_code, headers=passthrough)
    except httpx.TimeoutException:
        logger.warning(
            "gateway.proxy_timeout",
            log_event="proxy",
            service=service,
            method=request.method,
            path=path,
            duration_ms=round((perf_counter() - started) * 1000, 2),
        )
        return JSONResponse(status_code=504, content={"error": "upstream timeout"})
    except Exception as exc:
        logger.error(
            "gateway.proxy_error",
            log_event="proxy",
            service=service,
            method=request.method,
            path=path,
            error=str(exc),
            duration_ms=round((perf_counter() - started) * 1000, 2),
        )
        return JSONResponse(status_code=502, content={"error": str(exc)})
