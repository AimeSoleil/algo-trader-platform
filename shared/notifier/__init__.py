"""Extensible notification module — async, fire-and-forget."""
from shared.notifier.base import EventType, NotificationEvent, NotifierBackend, Severity
from shared.notifier.manager import NotifierManager

__all__ = [
    "EventType",
    "NotificationEvent",
    "NotifierBackend",
    "NotifierManager",
    "Severity",
]
