"""API Gateway — Unified Swagger docs & reverse proxy for all platform services."""
from __future__ import annotations

import asyncio
import copy
import json
import os
from contextlib import asynccontextmanager
from time import perf_counter

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import JSONResponse

from shared.config import get_settings
from shared.utils import setup_logging, get_logger

logger = get_logger("gateway")

# ---------------------------------------------------------------------------
# Service registry  (name → internal URL)
# Each entry can be overridden via env var  GATEWAY_{NAME}_URL.
# Defaults suit docker-compose networking.
# ---------------------------------------------------------------------------

_DEFAULTS: dict[str, dict] = {
    "data": {
        "url": "http://algo_data_service:8001",
        "title": "Data Service",
    },
    "signal": {
        "url": "http://algo_signal_service:8002",
        "title": "Signal Service",
    },
    "analysis": {
        "url": "http://algo_analysis_service:8003",
        "title": "Analysis Service",
    },
    "trade": {
        "url": "http://algo_trade_service:8004",
        "title": "Trade Service",
    },
    "monitoring": {
        "url": "http://algo_monitoring_service:8006",
        "title": "Monitoring Service",
    },
}


def _build_registry() -> dict[str, dict]:
    """Build service registry, allowing env-var overrides."""
    registry = {}
    for name, defaults in _DEFAULTS.items():
        env_key = f"GATEWAY_{name.upper()}_URL"
        url = os.environ.get(env_key, defaults["url"])
        registry[name] = {"url": url, "title": defaults["title"]}
    return registry


SERVICE_REGISTRY: dict[str, dict] = _build_registry()

_http_client: httpx.AsyncClient | None = None
_merged_spec: dict = {}


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _http_client
    setup_logging("gateway")
    settings = get_settings()
    logger.debug(
        "gateway.logging_config_snapshot",
        service_name="gateway",
        log_level=settings.logging.level,
        log_format=settings.logging.format,
        to_file=settings.logging.to_file,
        rotate_mode=settings.logging.file_rotate_mode,
    )
    _http_client = httpx.AsyncClient(timeout=30.0)
    logger.info("gateway.starting", services=list(SERVICE_REGISTRY.keys()))

    # Best-effort initial spec fetch (services may not be up yet)
    await _refresh_specs()

    yield

    await _http_client.aclose()
    logger.info("gateway.stopped")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Algo Trader Platform",
    description=(
        "Unified API gateway that consolidates all micro-service endpoints.\n\n"
        "Each service's routes are prefixed with `/{service_name}/…`.\n"
        "Use the **Swagger UI** below to explore and test every endpoint."
    ),
    version="0.1.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Spec aggregation
# ---------------------------------------------------------------------------


async def _refresh_specs() -> None:
    """Fetch /openapi.json from every backend and merge into one spec."""
    global _merged_spec
    started = perf_counter()
    logger.debug("gateway.refresh_specs_start", event="spec_refresh", stage="start", services=len(SERVICE_REGISTRY))

    merged: dict = {
        "openapi": "3.1.0",
        "info": {
            "title": "Algo Trader Platform",
            "description": "Consolidated API documentation for all services",
            "version": "0.1.0",
        },
        "paths": {},
        "components": {"schemas": {}},
        "tags": [],
    }

    async def _fetch_one(name: str, svc: dict):
        try:
            logger.debug("gateway.spec_fetch_start", event="spec_fetch", service=name, url=svc["url"])
            resp = await _http_client.get(f"{svc['url']}/openapi.json")  # type: ignore[union-attr]
            resp.raise_for_status()
            logger.debug("gateway.spec_fetch_done", event="spec_fetch", service=name, status_code=resp.status_code)
            return name, svc, resp.json()
        except Exception as e:
            logger.warning("gateway.spec_fetch_failed", service=name, error=str(e))
            return name, svc, None

    results = await asyncio.gather(
        *[_fetch_one(n, s) for n, s in SERVICE_REGISTRY.items()]
    )

    for name, svc, spec in results:
        if spec is None:
            continue

        tag_name = f"{name} ({svc['title']})"
        merged["tags"].append({"name": tag_name, "description": svc["title"]})

        # Prefix paths with /{service_name}
        for path, methods in spec.get("paths", {}).items():
            prefixed = f"/{name}{path}"
            patched_methods = copy.deepcopy(methods)
            for method_detail in patched_methods.values():
                if isinstance(method_detail, dict):
                    method_detail["tags"] = [tag_name]
                    # Prefix operationId to avoid collisions
                    if "operationId" in method_detail:
                        method_detail["operationId"] = (
                            f"{name}_{method_detail['operationId']}"
                        )
            merged["paths"][prefixed] = patched_methods

        # Merge schemas with service prefix to avoid name collisions
        for schema_name, schema_def in (
            spec.get("components", {}).get("schemas", {}).items()
        ):
            prefixed_name = f"{name}_{schema_name}"
            merged["components"]["schemas"][prefixed_name] = copy.deepcopy(schema_def)

    # Fix $ref pointers in the merged spec
    _fix_refs(merged, results)

    # Add gateway's own endpoints
    merged["tags"].append({"name": "gateway", "description": "Gateway management"})
    merged["paths"]["/health"] = {
        "get": {
            "tags": ["gateway"],
            "summary": "Gateway health check",
            "operationId": "gateway_health",
            "responses": {"200": {"description": "OK"}},
        }
    }
    merged["paths"]["/health/all"] = {
        "get": {
            "tags": ["gateway"],
            "summary": "Check all service health",
            "operationId": "gateway_health_all",
            "responses": {"200": {"description": "OK"}},
        }
    }
    merged["paths"]["/specs/refresh"] = {
        "post": {
            "tags": ["gateway"],
            "summary": "Force-refresh merged OpenAPI spec",
            "operationId": "gateway_refresh_specs",
            "responses": {"200": {"description": "OK"}},
        }
    }

    _merged_spec = merged
    healthy_specs = sum(1 for _name, _svc, spec in results if spec is not None)
    logger.debug(
        "gateway.refresh_specs_done",
        event="spec_refresh",
        stage="completed",
        services=len(SERVICE_REGISTRY),
        healthy_specs=healthy_specs,
        duration_ms=round((perf_counter() - started) * 1000, 2),
    )
    logger.info(
        "gateway.specs_merged",
        total_paths=len(merged["paths"]),
        total_schemas=len(merged["components"]["schemas"]),
    )


def _fix_refs(merged: dict, results: list) -> None:
    """Rewrite $ref pointers to use prefixed schema names."""
    text = json.dumps(merged)
    for name, _svc, spec in results:
        if spec is None:
            continue
        for schema_name in spec.get("components", {}).get("schemas", {}):
            text = text.replace(
                f'"#/components/schemas/{schema_name}"',
                f'"#/components/schemas/{name}_{schema_name}"',
            )
    merged.clear()
    merged.update(json.loads(text))


def _build_service_scoped_spec(service: str, svc: dict, spec: dict) -> dict:
    """Build a gateway-scoped spec for one service (paths prefixed with /{service})."""
    scoped = copy.deepcopy(spec)

    scoped["info"] = {
        "title": f"{svc['title']} (via Gateway)",
        "description": f"Service-scoped API docs for {svc['title']} routed through gateway prefix '/{service}'.",
        "version": spec.get("info", {}).get("version", "0.1.0"),
    }
    scoped["servers"] = [{"url": ""}]

    paths = scoped.get("paths", {})
    prefixed_paths: dict = {}
    for path, methods in paths.items():
        prefixed = f"/{service}{path}"
        patched_methods = copy.deepcopy(methods)
        for method_detail in patched_methods.values():
            if isinstance(method_detail, dict) and "operationId" in method_detail:
                method_detail["operationId"] = f"{service}_{method_detail['operationId']}"
        prefixed_paths[prefixed] = patched_methods
    scoped["paths"] = prefixed_paths

    return scoped


# ---------------------------------------------------------------------------
# Gateway endpoints
# ---------------------------------------------------------------------------


@app.get("/openapi.json", include_in_schema=False)
async def gateway_openapi_json():
    """Merged OpenAPI spec across all services."""
    return _merged_spec


@app.get("/openapi/{service}.json", include_in_schema=False)
async def service_openapi_json(service: str):
    """Gateway-scoped OpenAPI spec for a single service."""
    svc = SERVICE_REGISTRY.get(service)
    if not svc:
        return JSONResponse(status_code=404, content={"error": f"Unknown service: {service}"})

    try:
        resp = await _http_client.get(f"{svc['url']}/openapi.json")  # type: ignore[union-attr]
        resp.raise_for_status()
        spec = resp.json()
    except Exception as exc:
        return JSONResponse(
            status_code=502,
            content={"error": f"Failed to fetch OpenAPI for {service}", "detail": str(exc)},
        )

    return _build_service_scoped_spec(service, svc, spec)


@app.get("/docs", include_in_schema=False)
async def custom_swagger_docs():
    """Swagger UI with service-level OpenAPI switcher."""
    urls = [{"name": "All Services (Merged)", "url": "/openapi.json"}]
    for name, svc in SERVICE_REGISTRY.items():
        urls.append({"name": svc["title"], "url": f"/openapi/{name}.json"})

    return get_swagger_ui_html(
        openapi_url="/openapi.json",
        title="Algo Trader Platform - API Docs",
        swagger_ui_parameters={
            "urls": urls,
            "urls.primaryName": "All Services (Merged)",
            "docExpansion": "none",
            "filter": True,
        },
    )


@app.get("/health")
async def health():
    return {"status": "ok", "service": "gateway"}


@app.get("/health/all")
async def health_all():
    """Check health of all registered services."""
    started = perf_counter()
    logger.debug("gateway.health_all_start", event="health_check", stage="start", services=len(SERVICE_REGISTRY))

    async def _check(name: str, url: str):
        try:
            resp = await _http_client.get(f"{url}/health", timeout=5.0)  # type: ignore[union-attr]
            return name, resp.status_code == 200, None
        except Exception as e:
            return name, False, str(e)

    checks = await asyncio.gather(
        *[_check(n, s["url"]) for n, s in SERVICE_REGISTRY.items()]
    )
    services = {name: {"healthy": ok, "error": err} for name, ok, err in checks}
    all_ok = all(s["healthy"] for s in services.values())
    logger.debug(
        "gateway.health_all_done",
        event="health_check",
        stage="completed",
        healthy_count=sum(1 for s in services.values() if s["healthy"]),
        total_count=len(services),
        duration_ms=round((perf_counter() - started) * 1000, 2),
    )

    return {"status": "ok" if all_ok else "degraded", "services": services}


@app.post("/specs/refresh")
async def refresh_specs():
    """Force-refresh the merged OpenAPI spec."""
    logger.debug("gateway.refresh_specs_endpoint", event="spec_refresh", stage="endpoint_triggered")
    await _refresh_specs()
    return {
        "status": "refreshed",
        "total_paths": len(_merged_spec.get("paths", {})),
    }


# ---------------------------------------------------------------------------
# Reverse proxy  — /{service_name}/{path}
# ---------------------------------------------------------------------------


@app.api_route(
    "/{service}/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
    include_in_schema=False,
)
async def proxy(service: str, path: str, request: Request):
    """Forward requests to the appropriate backend service."""
    started = perf_counter()
    svc = SERVICE_REGISTRY.get(service)
    if not svc:
        logger.debug("gateway.proxy_unknown_service", event="proxy", service=service, path=path)
        return JSONResponse(
            status_code=404,
            content={"error": f"Unknown service: {service}"},
        )

    target_url = f"{svc['url']}/{path}"
    if request.url.query:
        target_url += f"?{request.url.query}"

    body = await request.body()
    headers = dict(request.headers)
    headers.pop("host", None)
    logger.debug(
        "gateway.proxy_forward_start",
        event="proxy",
        service=service,
        method=request.method,
        path=path,
    )

    try:
        resp = await _http_client.request(  # type: ignore[union-attr]
            method=request.method,
            url=target_url,
            content=body,
            headers=headers,
        )
        # Filter out hop-by-hop headers
        passthrough = {
            k: v
            for k, v in resp.headers.items()
            if k.lower() not in ("transfer-encoding", "connection")
        }
        logger.debug(
            "gateway.proxy_forward_done",
            event="proxy",
            service=service,
            method=request.method,
            path=path,
            status_code=resp.status_code,
            duration_ms=round((perf_counter() - started) * 1000, 2),
        )
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=passthrough,
        )
    except httpx.TimeoutException:
        logger.warning(
            "gateway.proxy_timeout",
            event="proxy",
            service=service,
            method=request.method,
            path=path,
            duration_ms=round((perf_counter() - started) * 1000, 2),
        )
        return JSONResponse(status_code=504, content={"error": "upstream timeout"})
    except Exception as e:
        logger.error(
            "gateway.proxy_error",
            event="proxy",
            service=service,
            method=request.method,
            path=path,
            error=str(e),
            duration_ms=round((perf_counter() - started) * 1000, 2),
        )
        return JSONResponse(status_code=502, content={"error": str(e)})
