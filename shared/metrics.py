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


# ---------------------------------------------------------------------------
# LLM pipeline metrics
# ---------------------------------------------------------------------------

try:
    from prometheus_client import Counter, Histogram

    llm_request_duration = Histogram(
        "llm_request_duration_seconds",
        "Latency of individual LLM API calls",
        labelnames=["provider", "agent", "status"],
        buckets=(1, 2, 5, 10, 20, 30, 60, 120, 300, 600),
    )

    llm_tokens_total = Counter(
        "llm_tokens_total",
        "Total tokens consumed by LLM calls",
        labelnames=["provider", "direction"],  # direction: prompt | completion
    )

    llm_retries_total = Counter(
        "llm_retries_total",
        "LLM call retry attempts",
        labelnames=["provider", "error_type"],
    )

    llm_fallback_total = Counter(
        "llm_fallback_total",
        "Times the LLM adapter fell back to secondary provider",
    )

    llm_circuit_open_total = Counter(
        "llm_circuit_open_total",
        "Times a provider circuit breaker opened",
        labelnames=["provider"],
    )

except ImportError:
    # prometheus_client not installed — provide no-op stubs
    class _NoOp:
        def labels(self, *a, **kw): return self
        def observe(self, *a, **kw): pass
        def inc(self, *a, **kw): pass

    llm_request_duration = _NoOp()
    llm_tokens_total = _NoOp()
    llm_retries_total = _NoOp()
    llm_fallback_total = _NoOp()
    llm_circuit_open_total = _NoOp()
