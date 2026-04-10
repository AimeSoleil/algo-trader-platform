"""NotifierManager — dispatches events to all enabled backends."""
from __future__ import annotations

import asyncio
from typing import Any, ClassVar

from shared.notifier.base import NotificationEvent, NotifierBackend
from shared.utils import get_logger

logger = get_logger("notifier_manager")


class NotifierManager:
    """Facade that dispatches notifications to multiple backends concurrently.

    All backend failures are caught and logged — never propagated.
    """

    _registry: ClassVar[dict[str, type[NotifierBackend]]] = {}

    def __init__(self, backends: list[NotifierBackend] | None = None) -> None:
        self._backends: list[NotifierBackend] = backends or []

    # ── Registry ──────────────────────────────────────────────

    @classmethod
    def register(cls, type_name: str, backend_cls: type[NotifierBackend]) -> None:
        cls._registry[type_name] = backend_cls

    # ── Dispatch ──────────────────────────────────────────────

    async def notify(self, event: NotificationEvent) -> None:
        """Send *event* to every backend. Failures are logged, never raised."""
        if not self._backends:
            return
        tasks = [self._safe_send(backend, event) for backend in self._backends]
        await asyncio.gather(*tasks)

    async def _safe_send(self, backend: NotifierBackend, event: NotificationEvent) -> None:
        try:
            await backend.send(event)
        except Exception as exc:
            logger.warning(
                "notifier.backend_error",
                backend=type(backend).__name__,
                event_type=event.event_type,
                error=str(exc),
            )

    # ── Factory ───────────────────────────────────────────────

    @classmethod
    def from_settings(cls, settings: Any) -> NotifierManager:
        """Build a NotifierManager from the application settings object.

        Reads ``settings.common.notifier`` and instantiates each enabled
        backend listed in ``backends``.
        """
        from shared.notifier.discord import DiscordNotifier

        # Ensure built-in backends are registered
        if "discord" not in cls._registry:
            cls.register("discord", DiscordNotifier)

        notifier_cfg = settings.common.notifier
        if not notifier_cfg.enabled:
            return cls(backends=[])

        backends: list[NotifierBackend] = []
        for backend_cfg in notifier_cfg.backends:
            if not backend_cfg.enabled:
                continue
            backend_cls = cls._registry.get(backend_cfg.type)
            if backend_cls is None:
                logger.warning("notifier.unknown_backend_type", type=backend_cfg.type)
                continue
            try:
                backend = _instantiate_backend(backend_cls, backend_cfg)
                backends.append(backend)
                logger.info("notifier.backend_loaded", type=backend_cfg.type)
            except Exception as exc:
                logger.warning(
                    "notifier.backend_init_failed",
                    type=backend_cfg.type,
                    error=str(exc),
                )

        return cls(backends=backends)


def _instantiate_backend(backend_cls: type[NotifierBackend], cfg: Any) -> NotifierBackend:
    """Construct a backend from its config. Supports webhook_url + timeout."""
    kwargs: dict[str, Any] = {}
    if cfg.webhook_url:
        kwargs["webhook_url"] = cfg.webhook_url
    if cfg.timeout:
        kwargs["timeout"] = cfg.timeout
    return backend_cls(**kwargs)
