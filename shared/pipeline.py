"""Shared pipeline utilities — symbol chunking and stage barriers."""
from __future__ import annotations

from shared.utils import get_logger

logger = get_logger("pipeline")


def chunk_symbols(symbols: list[str], chunk_size: int) -> list[list[str]]:
    """Split *symbols* into equal-sized sub-lists.

    The last chunk may be smaller.  Returns ``[symbols]`` unchanged when
    *chunk_size* >= len(symbols).
    """
    if chunk_size <= 0:
        raise ValueError(f"chunk_size must be > 0, got {chunk_size}")
    return [symbols[i : i + chunk_size] for i in range(0, len(symbols), chunk_size)]
