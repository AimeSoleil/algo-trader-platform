"""Sync helpers for using the notifier from Celery tasks and other sync contexts."""
from __future__ import annotations

from shared.async_bridge import run_async
from shared.notifier.base import NotificationEvent
from shared.notifier.manager import NotifierManager
from shared.utils import get_logger

logger = get_logger("notifier_helpers")

_notifier: NotifierManager | None = None


def get_notifier() -> NotifierManager:
    """Return a lazily-initialised singleton NotifierManager."""
    global _notifier
    if _notifier is None:
        from shared.config import get_settings
        _notifier = NotifierManager.from_settings(get_settings())
    return _notifier


def notify_sync(event: NotificationEvent) -> None:
    """Fire-and-forget notification from synchronous / Celery task context.

    Failures are logged but never raised — the caller is never blocked.
    """
    try:
        manager = get_notifier()
        run_async(manager.notify(event))
    except Exception as exc:
        logger.warning("notifier.sync_send_failed", event_type=event.event_type, error=str(exc))
