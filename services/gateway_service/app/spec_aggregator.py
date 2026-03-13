"""OpenAPI spec aggregation — fetch, merge, and scope service specs."""
from __future__ import annotations

import asyncio
import copy
import json
from time import perf_counter

import httpx

from shared.utils import get_logger

from .registry import ServiceEntry, ServiceRegistry

logger = get_logger("gateway")


class SpecAggregator:
    """Fetches individual service OpenAPI specs and produces merged / scoped views."""

    def __init__(self, registry: ServiceRegistry) -> None:
        self._registry = registry
        self._merged: dict = {}
        self._http: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------
    # HTTP client lifecycle (owned externally; injected)
    # ------------------------------------------------------------------

    def set_http_client(self, client: httpx.AsyncClient) -> None:
        self._http = client

    @property
    def merged_spec(self) -> dict:
        return self._merged

    @property
    def is_ready(self) -> bool:
        return bool(self._merged.get("openapi")) and isinstance(
            self._merged.get("paths"), dict
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def refresh(self) -> None:
        """Fetch ``/openapi.json`` from every backend and re-build the merged spec."""
        started = perf_counter()
        logger.debug(
            "gateway.refresh_specs_start",
            log_event="spec_refresh",
            stage="start",
            services=len(self._registry),
        )

        merged = self._empty_merged_shell()

        results = await asyncio.gather(
            *(self._fetch_one(entry) for _name, entry in self._registry.items())
        )

        for entry, spec in results:
            if spec is None:
                continue
            self._merge_one(merged, entry, spec)

        self._fix_refs(merged, results)
        self._add_gateway_endpoints(merged)
        self._merged = merged

        healthy = sum(1 for _, spec in results if spec is not None)
        logger.debug(
            "gateway.refresh_specs_done",
            log_event="spec_refresh",
            stage="completed",
            services=len(self._registry),
            healthy_specs=healthy,
            duration_ms=round((perf_counter() - started) * 1000, 2),
        )
        logger.info(
            "gateway.specs_merged",
            total_paths=len(merged["paths"]),
            total_schemas=len(merged["components"]["schemas"]),
        )

    async def ensure_ready(self) -> None:
        """Lazy one-shot refresh when merged spec is missing (e.g. lifespan skipped)."""
        if self.is_ready:
            return

        created = False
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=30.0)
            created = True
        try:
            await self.refresh()
        finally:
            if created and self._http is not None:
                await self._http.aclose()
                self._http = None

    async def fetch_service_spec(self, name: str) -> dict | None:
        """Fetch the raw OpenAPI spec for a single service. Returns *None* on failure."""
        entry = self._registry.get(name)
        if entry is None:
            return None
        _, spec = await self._fetch_one(entry)
        return spec

    def build_scoped_spec(self, name: str, spec: dict) -> dict:
        """Build a gateway-scoped spec for one service (paths prefixed ``/{name}``)."""
        entry = self._registry.get(name)
        if entry is None:
            raise ValueError(f"Unknown service: {name}")

        scoped = copy.deepcopy(spec)
        scoped["info"] = {
            "title": f"{entry.title} (via Gateway)",
            "description": (
                f"Service-scoped API docs for {entry.title} "
                f"routed through gateway prefix '/{name}'."
            ),
            "version": spec.get("info", {}).get("version", "0.1.0"),
        }
        scoped["servers"] = [{"url": ""}]

        prefixed_paths: dict = {}
        for path, methods in scoped.get("paths", {}).items():
            prefixed = f"/{name}{path}"
            patched = copy.deepcopy(methods)
            for detail in patched.values():
                if isinstance(detail, dict) and "operationId" in detail:
                    detail["operationId"] = f"{name}_{detail['operationId']}"
            prefixed_paths[prefixed] = patched
        scoped["paths"] = prefixed_paths
        return scoped

    # ------------------------------------------------------------------
    # Static helpers
    # ------------------------------------------------------------------

    @staticmethod
    def gateway_only_spec() -> dict:
        """Return a minimal OpenAPI spec covering only gateway management endpoints."""
        return {
            "openapi": "3.1.0",
            "info": {
                "title": "Gateway Service",
                "description": "Gateway management endpoints only",
                "version": "0.1.0",
            },
            "paths": {
                "/api/v1/health": {
                    "get": {
                        "tags": ["gateway"],
                        "summary": "Gateway health check",
                        "operationId": "gateway_health",
                        "responses": {"200": {"description": "OK"}},
                    }
                },
                "/api/v1/health/all": {
                    "get": {
                        "tags": ["gateway"],
                        "summary": "Check all service health",
                        "operationId": "gateway_health_all",
                        "responses": {"200": {"description": "OK"}},
                    }
                },
                "/specs/refresh": {
                    "post": {
                        "tags": ["gateway"],
                        "summary": "Force-refresh merged OpenAPI spec",
                        "operationId": "gateway_refresh_specs",
                        "responses": {"200": {"description": "OK"}},
                    }
                },
            },
            "components": {"schemas": {}},
            "tags": [{"name": "gateway", "description": "Gateway management"}],
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _fetch_one(self, entry: ServiceEntry) -> tuple[ServiceEntry, dict | None]:
        try:
            logger.debug(
                "gateway.spec_fetch_start",
                log_event="spec_fetch",
                service=entry.name,
                url=entry.url,
            )
            resp = await self._http.get(f"{entry.url}/openapi.json")  # type: ignore[union-attr]
            resp.raise_for_status()
            logger.debug(
                "gateway.spec_fetch_done",
                log_event="spec_fetch",
                service=entry.name,
                status_code=resp.status_code,
            )
            return entry, resp.json()
        except Exception as exc:
            logger.warning("gateway.spec_fetch_failed", service=entry.name, error=str(exc))
            return entry, None

    @staticmethod
    def _empty_merged_shell() -> dict:
        return {
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

    @staticmethod
    def _merge_one(merged: dict, entry: ServiceEntry, spec: dict) -> None:
        tag_name = f"{entry.name} ({entry.title})"
        merged["tags"].append({"name": tag_name, "description": entry.title})

        for path, methods in spec.get("paths", {}).items():
            prefixed = f"/{entry.name}{path}"
            patched = copy.deepcopy(methods)
            for detail in patched.values():
                if isinstance(detail, dict):
                    detail["tags"] = [tag_name]
                    if "operationId" in detail:
                        detail["operationId"] = f"{entry.name}_{detail['operationId']}"
            merged["paths"][prefixed] = patched

        for schema_name, schema_def in (
            spec.get("components", {}).get("schemas", {}).items()
        ):
            merged["components"]["schemas"][f"{entry.name}_{schema_name}"] = copy.deepcopy(
                schema_def
            )

    @staticmethod
    def _fix_refs(merged: dict, results: list[tuple[ServiceEntry, dict | None]]) -> None:
        text = json.dumps(merged)
        for entry, spec in results:
            if spec is None:
                continue
            for schema_name in spec.get("components", {}).get("schemas", {}):
                text = text.replace(
                    f'"#/components/schemas/{schema_name}"',
                    f'"#/components/schemas/{entry.name}_{schema_name}"',
                )
        merged.clear()
        merged.update(json.loads(text))

    @staticmethod
    def _add_gateway_endpoints(merged: dict) -> None:
        merged["tags"].append({"name": "gateway", "description": "Gateway management"})
        merged["paths"]["/api/v1/health"] = {
            "get": {
                "tags": ["gateway"],
                "summary": "Gateway health check",
                "operationId": "gateway_health",
                "responses": {"200": {"description": "OK"}},
            }
        }
        merged["paths"]["/api/v1/health/all"] = {
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
