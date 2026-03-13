"""Swagger UI docs — custom HTML with service-level spec switcher."""
from __future__ import annotations

import json

from fastapi import APIRouter, Request, Response
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


# ---------------------------------------------------------------------------
# OpenAPI JSON endpoints
# ---------------------------------------------------------------------------


@router.get("/openapi.json", include_in_schema=False)
async def merged_openapi_json():
    """Merged OpenAPI spec across all services."""
    assert _aggregator is not None
    await _aggregator.ensure_ready()
    return _aggregator.merged_spec


@router.get("/openapi/gateway.json", include_in_schema=False)
async def gateway_only_openapi_json():
    """Gateway-only OpenAPI spec used as default docs view."""
    return SpecAggregator.gateway_only_spec()


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

_SWAGGER_CSS = "https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css"
_SWAGGER_JS = "https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"


def _build_swagger_html(urls_json: str) -> str:
    """Generate self-contained Swagger UI HTML with a spec-switcher dropdown."""
    return f"""\
<!DOCTYPE html>
<html>
<head>
<link type="text/css" rel="stylesheet" href="{_SWAGGER_CSS}">
<title>Algo Trader Platform - API Docs</title>
</head>
<body>
<div id="swagger-ui"></div>
<script src="{_SWAGGER_JS}"></script>
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
        SwaggerUIBundle.SwaggerUIStandalonePreset
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
    assert _aggregator is not None
    assert _registry is not None
    await _aggregator.ensure_ready()

    root_path = request.scope.get("root_path", "") or ""

    def _rp(path: str) -> str:
        return f"{root_path}{path}" if root_path else path

    urls = [
        {"name": "Gateway", "url": _rp("/openapi/gateway.json")},
        {"name": "All Services (Merged)", "url": _rp("/openapi.json")},
    ]
    for name, entry in _registry.items():
        urls.append({"name": entry.title, "url": _rp(f"/openapi/{name}.json")})

    html = _build_swagger_html(json.dumps(urls))
    return Response(content=html, media_type="text/html")


@router.get("/doc", include_in_schema=False)
async def swagger_docs_alias(request: Request):
    """Alias for ``/docs`` (compat)."""
    return await swagger_docs(request)
