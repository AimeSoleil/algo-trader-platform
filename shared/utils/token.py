"""Token-related utility helpers."""
from __future__ import annotations


def estimate_prompt_tokens(*parts: str) -> int:
    """Estimate prompt token count from text parts.

    This is a lightweight approximation for pre-request logging.
    Final billing/usage should rely on provider-reported usage tokens.
    """
    char_count = sum(len(part) for part in parts if part)
    if char_count <= 0:
        return 0
    # Common heuristic: ~4 chars per token for English-like content.
    return (char_count + 3) // 4
