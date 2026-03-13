"""Swagger UI docs — single merged OpenAPI spec for all services."""
from __future__ import annotations

import asyncio
import json
from typing import Any, Callable

from fastapi import APIRouter, Request, Response
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
import httpx

from shared.utils import get_logger

from .registry import ServiceRegistry

logger = get_logger("gateway")

router = APIRouter()

# ---------------------------------------------------------------------------
# Module-level state (injected via ``configure()``)
# ---------------------------------------------------------------------------
_registry: ServiceRegistry | None = None
_http_getter: Callable[..., Any] | None = None  # returns httpx.AsyncClient
_cached_spec: dict | None = None


def configure(registry: ServiceRegistry, http_getter: Callable[..., Any]) -> None:
    """Wire runtime dependencies (called once from app factory)."""
    global _registry, _http_getter
    _registry = registry
    _http_getter = http_getter


def invalidate_cache() -> None:
    """Clear the cached merged spec so the next request rebuilds it."""
    global _cached_spec
    _cached_spec = None


# ---------------------------------------------------------------------------
# Merge logic
# ---------------------------------------------------------------------------

async def _fetch_service_spec(name: str, url: str) -> tuple[str, dict | None]:
    """Fetch a single service's OpenAPI JSON. Returns (name, spec | None)."""
    try:
        client = _http_getter()
        resp = await client.get(f"{url}/openapi.json", timeout=10.0)
        resp.raise_for_status()
        return name, resp.json()
    except Exception:
        logger.warning("Failed to fetch OpenAPI spec for service '%s' at %s", name, url)
        return name, None


async def _build_merged_spec(request: Request) -> dict:
    """Build a single OpenAPI spec merging the gateway and all backend services."""
    assert _registry is not None
    assert _http_getter is not None

    # Start with the gateway's own spec
    spec = get_openapi(
        title=request.app.title,
        version=request.app.version,
        description=request.app.description,
        routes=request.app.routes,
    )
    root_path = request.scope.get("root_path", "") or ""
    spec["servers"] = [{"url": root_path or ""}]
    spec.setdefault("components", {}).setdefault("schemas", {})

    # Fetch all service specs in parallel
    tasks = [
        _fetch_service_spec(name, entry.url)
        for name, entry in _registry.items()
    ]
    results = await asyncio.gather(*tasks)

    for service_name, service_spec in results:
        if service_spec is None:
            continue

        entry = _registry.get(service_name)
        if entry is None:
            continue

        # Build the schema-name prefix from the service title (spaces removed)
        # e.g. "Data Service" → "DataService"
        prefix = entry.title.replace(" ", "")

        # Collect original schema names from this service
        service_schemas: dict[str, Any] = (
            service_spec.get("components", {}).get("schemas", {})
        )
        old_schema_names = list(service_schemas.keys())

        # --- Prefix paths and rewrite $refs via JSON round-trip ---
        service_paths: dict[str, Any] = service_spec.get("paths", {})

        # Build prefixed paths: /api/v1/foo → /data/api/v1/foo
        prefixed_paths: dict[str, Any] = {}
        for path, path_item in service_paths.items():
            new_path = f"/{service_name}{path}"
            # Prefix operationId for each method
            for method in ("get", "post", "put", "patch", "delete", "options", "head", "trace"):
                if method in path_item and "operationId" in path_item[method]:
                    path_item[method]["operationId"] = (
                        f"{service_name}_{path_item[method]['operationId']}"
                    )
            prefixed_paths[new_path] = path_item

        # Rewrite $refs in paths: serialise → replace → deserialise
        paths_json = json.dumps(prefixed_paths)
        for old_name in old_schema_names:
            paths_json = paths_json.replace(
                f"#/components/schemas/{old_name}",
                f"#/components/schemas/{prefix}_{old_name}",
            )
        prefixed_paths = json.loads(paths_json)

        # Rewrite $refs in schemas themselves
        schemas_json = json.dumps(service_schemas)
        for old_name in old_schema_names:
            schemas_json = schemas_json.replace(
                f"#/components/schemas/{old_name}",
                f"#/components/schemas/{prefix}_{old_name}",
            )
        rewritten_schemas: dict[str, Any] = json.loads(schemas_json)

        # Prefix schema keys
        prefixed_schemas = {
            f"{prefix}_{key}": value for key, value in rewritten_schemas.items()
        }

        # Merge into main spec
        spec["paths"].update(prefixed_paths)
        spec["components"]["schemas"].update(prefixed_schemas)

    return spec


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/openapi.json", include_in_schema=False)
async def merged_openapi_json(request: Request):
    """Single merged OpenAPI spec for all services."""
    global _cached_spec
    if _cached_spec is None:
        _cached_spec = await _build_merged_spec(request)
    return JSONResponse(content=_cached_spec)


_CDN = "https://cdn.jsdelivr.net/npm/swagger-ui-dist@5"


@router.get("/docs", include_in_schema=False)
async def swagger_docs(request: Request):
    """Swagger UI pointing at single merged /openapi.json."""
    root_path = request.scope.get("root_path", "") or ""
    spec_url = f"{root_path}/openapi.json" if root_path else "/openapi.json"
    html = f"""\
<!DOCTYPE html>
<html>
<head>
<link type="text/css" rel="stylesheet" href="{_CDN}/swagger-ui.css">
<title>Algo Trader Platform - API Docs</title>
</head>
<body>
<div id="swagger-ui"></div>
<script src="{_CDN}/swagger-ui-bundle.js"></script>
<script>
SwaggerUIBundle({{
    dom_id: '#swagger-ui',
    url: '{spec_url}',
    deepLinking: true,
    docExpansion: "none",
    filter: true,
    layout: "BaseLayout",
    presets: [
        SwaggerUIBundle.presets.apis,
    ],
    plugins: [
        SwaggerUIBundle.plugins.DownloadUrl
    ],
}});
</script>
</body>
</html>"""
    return Response(content=html, media_type="text/html")


@router.get("/doc", include_in_schema=False)
async def swagger_docs_alias(request: Request):
    """Alias — redirect to /docs."""
    return await swagger_docs(request)
