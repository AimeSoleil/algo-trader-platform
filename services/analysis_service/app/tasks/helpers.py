"""Analysis task helpers — LLM adapter and signal parsing."""
from __future__ import annotations

from shared.models.signal import SignalFeatures
from shared.utils import get_logger

logger = get_logger("analysis_tasks")


# ── LLM adapter singleton ─────────────────────────────────

_adapter = None


def _get_adapter():
    """Return a cached LLMAdapter instance."""
    global _adapter
    if _adapter is None:
        from services.analysis_service.app.llm.adapter import LLMAdapter
        _adapter = LLMAdapter()
    return _adapter


# ── Signal parsing ─────────────────────────────────────────

def _parse_signal_features(raw: object) -> SignalFeatures:
    """Parse SignalFeatures from DB value that may be JSON string or Python dict.

    Postgres/SQLAlchemy may return JSONB as ``dict`` directly, while some drivers
    may return serialized JSON text/bytes.
    """
    if isinstance(raw, (str, bytes, bytearray)):
        return SignalFeatures.model_validate_json(raw)
    return SignalFeatures.model_validate(raw)
