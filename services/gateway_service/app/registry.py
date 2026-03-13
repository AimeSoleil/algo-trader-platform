"""Service registry — maps logical service names to internal URLs."""
from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class ServiceEntry:
    """One registered backend service."""

    name: str
    url: str
    title: str


_DEFAULTS: dict[str, dict[str, str]] = {
    "data": {
        "url": "http://algo_data_service:8001",
        "title": "Data Service",
    },
    "signal": {
        "url": "http://algo_signal_service:8002",
        "title": "Signal Service",
    },
    "analysis": {
        "url": "http://algo_analysis_service:8003",
        "title": "Analysis Service",
    },
    "trade": {
        "url": "http://algo_trade_service:8004",
        "title": "Trade Service",
    },
    "monitoring": {
        "url": "http://algo_monitoring_service:8006",
        "title": "Monitoring Service",
    },
}


@dataclass
class ServiceRegistry:
    """Immutable registry of backend services.

    Each entry can be overridden via the env var ``GATEWAY_{NAME}_URL``.
    """

    _entries: dict[str, ServiceEntry] = field(default_factory=dict, repr=False)

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def from_defaults(cls) -> "ServiceRegistry":
        """Build a registry from built-in defaults + env-var overrides."""
        entries: dict[str, ServiceEntry] = {}
        for name, defaults in _DEFAULTS.items():
            env_key = f"GATEWAY_{name.upper()}_URL"
            url = os.environ.get(env_key, defaults["url"])
            entries[name] = ServiceEntry(name=name, url=url, title=defaults["title"])
        return cls(_entries=entries)

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    def get(self, name: str) -> ServiceEntry | None:
        return self._entries.get(name)

    def items(self):
        return self._entries.items()

    def names(self) -> list[str]:
        return list(self._entries.keys())

    def __len__(self) -> int:
        return len(self._entries)

    def __contains__(self, name: str) -> bool:
        return name in self._entries
