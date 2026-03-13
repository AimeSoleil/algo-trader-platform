"""Gateway OpenAPI helper — per-service fetch and gateway-scoped rendering."""
from __future__ import annotations

import copy

import httpx

from shared.utils import get_logger

from .registry import ServiceEntry, ServiceRegistry

logger = get_logger("gateway")


class SpecAggregator:
    """Fetches individual service OpenAPI specs and produces scoped views."""

    def __init__(self, registry: ServiceRegistry) -> None:
        self._registry = registry
        self._http: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------
    # HTTP client lifecycle (owned externally; injected)
    # ------------------------------------------------------------------

    def set_http_client(self, client: httpx.AsyncClient) -> None:
        self._http = client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def ensure_ready(self, retry_if_unhealthy: bool = False) -> None:
        """Compatibility no-op.

        Merged OpenAPI is intentionally removed; service specs are fetched on demand.
        """
        return

    async def fetch_service_spec(self, name: str) -> dict | None:
        """Fetch the raw OpenAPI spec for a single service. Returns *None* on failure."""
        entry = self._registry.get(name)
        if entry is None:
            return None

        created_client = False
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=30.0)
            created_client = True

        try:
            _, spec = await self._fetch_one(entry)
            return spec
        finally:
            if created_client and self._http is not None:
                await self._http.aclose()
                self._http = None

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
