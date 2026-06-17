"""Notifier base — abstract backend and event definitions."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


class Severity:
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class EventType:
    PIPELINE_STARTED = "pipeline_started"
    PIPELINE_OPTIONS_AGGREGATED = "pipeline_options_aggregated"
    PIPELINE_STOCK_CAPTURED = "pipeline_stock_captured"
    PIPELINE_DOWNSTREAM_DISPATCHED = "pipeline_downstream_dispatched"
    PIPELINE_SIGNALS_COMPUTED = "pipeline_signals_computed"
    PIPELINE_FAILED = "pipeline_failed"
    PIPELINE_FINISHED = "pipeline_finished"
    MANUAL_ANALYSIS_STARTED = "manual_analysis_started"
    MANUAL_ANALYSIS_FINISHED = "manual_analysis_finished"
    EARNINGS_CACHE_REFRESH_STARTED = "earnings_cache_refresh_started"
    EARNINGS_CACHE_REFRESH_FINISHED = "earnings_cache_refresh_finished"
    EARNINGS_CACHE_REFRESH_FAILED = "earnings_cache_refresh_failed"
    TRADE_EXECUTED = "trade_executed"
    TRADE_ERROR = "trade_error"
    DAILY_REPORT = "daily_report"


@dataclass
class NotificationEvent:
    event_type: str
    title: str
    message: str
    severity: str = Severity.INFO
    payload: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class NotifierBackend(ABC):
    """Abstract notification backend — subclass to add new platforms."""

    @abstractmethod
    async def send(self, event: NotificationEvent) -> bool:
        """Send a notification. Return True on success, False on failure."""
        raise NotImplementedError
