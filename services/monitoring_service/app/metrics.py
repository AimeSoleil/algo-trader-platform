"""Monitoring Service — Prometheus 指标定义"""
from __future__ import annotations

from prometheus_client import Counter, Gauge

blueprint_loaded_total = Counter(
    "blueprint_loaded_total",
    "Number of loaded trading blueprints",
)

post_market_pipeline_runs_total = Counter(
    "post_market_pipeline_runs_total",
    "Post-market pipeline run count",
    labelnames=("stage", "status"),
)

cache_memory_symbols = Gauge(
    "cache_memory_symbols",
    "Number of symbols currently cached in memory",
)

margin_usage_ratio = Gauge(
    "margin_usage_ratio",
    "Current portfolio margin usage ratio",
)
