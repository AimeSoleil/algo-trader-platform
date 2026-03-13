"""Shared Prometheus metrics instrumentation for all services.

Usage in any service's ``main.py``::

    from shared.metrics import setup_metrics
    app = FastAPI(...)
    setup_metrics(app)
"""
from __future__ import annotations

from prometheus_fastapi_instrumentator import Instrumentator


def setup_metrics(app, *, metrics_path: str = "/metrics") -> Instrumentator:
    """Instrument *app* with Prometheus HTTP metrics and expose ``/metrics``.

    Automatically tracks:
    - ``http_request_duration_seconds`` (histogram)
    - ``http_requests_total`` (counter with method/status/handler labels)
    - ``http_request_size_bytes``
    - ``http_response_size_bytes``
    """
    instrumentator = Instrumentator(
        should_group_status_codes=False,
        should_ignore_untemplated=True,
        excluded_handlers=[metrics_path, "/health", "/docs", "/openapi.json"],
    )
    instrumentator.instrument(app)
    instrumentator.expose(app, endpoint=metrics_path, include_in_schema=False)
    return instrumentator
