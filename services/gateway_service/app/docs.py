"""Swagger UI docs — custom HTML with service-level spec switcher."""
from __future__ import annotations

import json

from fastapi import APIRouter, Request, Response
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse

from .registry import ServiceRegistry
from .spec_aggregator import SpecAggregator

router = APIRouter()

# Injected at startup via ``configure()``.
_registry: ServiceRegistry | None = None
_aggregator: SpecAggregator | None = None


def configure(registry: ServiceRegistry, aggregator: SpecAggregator) -> None:
    """Wire runtime dependencies (called once from app factory)."""
    global _registry, _aggregator
    _registry = registry
    _aggregator = aggregator


def _build_gateway_openapi(request: Request) -> dict:
    """Build gateway OpenAPI dynamically from current FastAPI routes."""
    schema = get_openapi(
        title=request.app.title,
        version=request.app.version,
        description=request.app.description,
        routes=request.app.routes,
    )

    root_path = request.scope.get("root_path", "") or ""
    schema["servers"] = [{"url": root_path or ""}]
    return schema


# ---------------------------------------------------------------------------
# OpenAPI JSON endpoints
# ---------------------------------------------------------------------------


@router.get("/openapi.json", include_in_schema=False)
async def gateway_openapi_json(request: Request):
    """Gateway OpenAPI spec generated dynamically from runtime routes."""
    return _build_gateway_openapi(request)


@router.get("/openapi/gateway.json", include_in_schema=False)
async def gateway_only_openapi_json(request: Request):
    """Gateway-only OpenAPI alias used as default docs view."""
    return _build_gateway_openapi(request)


@router.get("/openapi/{service}.json", include_in_schema=False)
async def service_openapi_json(service: str):
    """Gateway-scoped OpenAPI spec for a single service."""
    assert _aggregator is not None
    assert _registry is not None

    if service not in _registry:
        return JSONResponse(status_code=404, content={"error": f"Unknown service: {service}"})

    spec = await _aggregator.fetch_service_spec(service)
    if spec is None:
        return JSONResponse(
            status_code=502,
            content={"error": f"Failed to fetch OpenAPI for {service}"},
        )

    return _aggregator.build_scoped_spec(service, spec)


# ---------------------------------------------------------------------------
# Swagger UI page
# ---------------------------------------------------------------------------

_CDN = "https://cdn.jsdelivr.net/npm/swagger-ui-dist@5"
_SWAGGER_CSS = f"{_CDN}/swagger-ui.css"
_SWAGGER_BUNDLE_JS = f"{_CDN}/swagger-ui-bundle.js"
_SWAGGER_PRESET_JS = f"{_CDN}/swagger-ui-standalone-preset.js"


def _build_swagger_html(urls_json: str) -> str:
    """Generate self-contained Swagger UI HTML with a spec-switcher dropdown.

    Both ``swagger-ui-bundle.js`` **and** ``swagger-ui-standalone-preset.js``
    are required: the standalone preset supplies ``StandaloneLayout`` which
    renders the top-bar URL selector that drives the ``urls`` dropdown.
    Without it Swagger UI silently ignores ``urls`` → "No API definition".
    """
    return f"""\
<!DOCTYPE html>
<html>
<head>
<link type="text/css" rel="stylesheet" href="{_SWAGGER_CSS}">
<title>Algo Trader Platform - API Docs</title>
</head>
<body>
<div id="swagger-ui"></div>
<script src="{_SWAGGER_BUNDLE_JS}"></script>
<script src="{_SWAGGER_PRESET_JS}"></script>
<script>
SwaggerUIBundle({{
    dom_id: '#swagger-ui',
    urls: {urls_json},
    "urls.primaryName": "Gateway",
    deepLinking: true,
    docExpansion: "none",
    filter: true,
    layout: "StandaloneLayout",
    presets: [
        SwaggerUIBundle.presets.apis,
        SwaggerUIStandalonePreset
    ],
    plugins: [
        SwaggerUIBundle.plugins.DownloadUrl
    ],
}});
</script>
</body>
</html>"""


@router.get("/docs", include_in_schema=False)
async def swagger_docs(request: Request):
    """Swagger UI with service-level OpenAPI switcher."""
    assert _registry is not None

    root_path = request.scope.get("root_path", "") or ""

    def _rp(path: str) -> str:
        return f"{root_path}{path}" if root_path else path

    urls = [
        {"name": "Gateway", "url": _rp("/openapi.json")},
    ]
    for name, entry in _registry.items():
        urls.append({"name": entry.title, "url": _rp(f"/{name}/openapi.json")})

    html = _build_swagger_html(json.dumps(urls))
    return Response(content=html, media_type="text/html")


@router.get("/doc", include_in_schema=False)
async def swagger_docs_alias(request: Request):
    """Alias for ``/docs`` (compat)."""
    return await swagger_docs(request)
