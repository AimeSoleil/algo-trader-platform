"""Discord notification backend — posts rich embeds via webhook."""
from __future__ import annotations

from typing import Any

import httpx

from shared.notifier.base import NotificationEvent, NotifierBackend, Severity
from shared.utils import get_logger

logger = get_logger("notifier_discord")

_SEVERITY_COLORS: dict[str, int] = {
    Severity.INFO: 0x2ECC71,      # green
    Severity.WARNING: 0xF1C40F,   # yellow
    Severity.ERROR: 0xE74C3C,     # red
}


class DiscordNotifier(NotifierBackend):
    """Send notifications to a Discord channel via webhook URL."""

    def __init__(self, webhook_url: str, *, timeout: float = 10.0) -> None:
        self._webhook_url = webhook_url
        self._timeout = timeout

    async def send(self, event: NotificationEvent) -> bool:
        embed: dict[str, Any] = {
            "title": event.title,
            "description": event.message[:4096],
            "color": _SEVERITY_COLORS.get(event.severity, _SEVERITY_COLORS[Severity.INFO]),
            "timestamp": event.timestamp.isoformat(),
            "footer": {"text": f"Event: {event.event_type}"},
        }

        # Add payload fields (Discord embed supports up to 25 fields)
        fields: list[dict[str, Any]] = []
        for key, value in list(event.payload.items())[:25]:
            fields.append({"name": str(key), "value": str(value)[:1024], "inline": True})
        if fields:
            embed["fields"] = fields

        body = {"embeds": [embed]}

        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.post(self._webhook_url, json=body)
                resp.raise_for_status()
            return True
        except Exception as exc:
            logger.warning(
                "discord.send_failed",
                event_type=event.event_type,
                status=getattr(exc, "response", None) and exc.response.status_code,
                error=str(exc),
            )
            return False
