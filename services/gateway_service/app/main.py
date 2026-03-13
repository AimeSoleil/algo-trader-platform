"""API Gateway — App factory, lifespan, and router wiring.

Modules:
    registry        – Service registry (name → URL mapping)
    docs            – Swagger UI HTML generation & spec endpoints
    routes          – Health checks & spec management
    proxy           – Reverse-proxy catch-all
"""
from __future__ import annotations

from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from shared.config import get_settings
from shared.utils import setup_logging, get_logger

from . import docs, proxy, routes
from .registry import ServiceRegistry

logger = get_logger("gateway")

# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------

registry = ServiceRegistry.from_defaults()
_http_client: httpx.AsyncClient | None = None


def _get_http() -> httpx.AsyncClient:
    """Return the shared HTTP client (guaranteed non-None after lifespan start)."""
    assert _http_client is not None, "HTTP client not initialised"
    return _http_client


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(_app: FastAPI):
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

    logger.info("gateway.starting", services=registry.names())

    yield

    await _http_client.aclose()
    _http_client = None
    logger.info("gateway.stopped")


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


def create_app() -> FastAPI:
    """Build and return the fully-wired FastAPI application."""
    application = FastAPI(
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

    application.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Dependency injection into sub-modules
    docs.configure(registry, _get_http)
    routes.configure(registry, _get_http)
    proxy.configure(registry, _get_http)

    # Register routers (order matters: explicit routes before catch-all proxy)
    application.include_router(docs.router)
    application.include_router(routes.router)
    application.include_router(proxy.router)

    return application


app = create_app()
