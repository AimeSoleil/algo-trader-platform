"""API Gateway — Unified Swagger docs & reverse proxy for all platform services."""
from __future__ import annotations

import asyncio
import copy
import json
import os
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

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
    "execution": {
        "url": "http://algo_execution_service:8004",
        "title": "Execution Service",
    },
    "portfolio": {
        "url": "http://algo_portfolio_service:8005",
        "title": "Portfolio Service",
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
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# Override FastAPI's built-in openapi() to return merged spec
def _custom_openapi():
    return _merged_spec


app.openapi = _custom_openapi  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Spec aggregation
# ---------------------------------------------------------------------------


async def _refresh_specs() -> None:
    """Fetch /openapi.json from every backend and merge into one spec."""
    global _merged_spec

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
            resp = await _http_client.get(f"{svc['url']}/openapi.json")  # type: ignore[union-attr]
            resp.raise_for_status()
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


# ---------------------------------------------------------------------------
# Gateway endpoints
# ---------------------------------------------------------------------------


@app.get("/health")
async def health():
    return {"status": "ok", "service": "gateway"}


@app.get("/health/all")
async def health_all():
    """Check health of all registered services."""

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

    return {"status": "ok" if all_ok else "degraded", "services": services}


@app.post("/specs/refresh")
async def refresh_specs():
    """Force-refresh the merged OpenAPI spec."""
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
    svc = SERVICE_REGISTRY.get(service)
    if not svc:
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
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=passthrough,
        )
    except httpx.TimeoutException:
        return JSONResponse(status_code=504, content={"error": "upstream timeout"})
    except Exception as e:
        return JSONResponse(status_code=502, content={"error": str(e)})
