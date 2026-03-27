"""Shared JSON parsing utilities for LLM responses.

All LLM providers (OpenAI, Copilot) and all consumers (specialist agents,
synthesizer, critic, legacy providers) funnel through the two helpers here
so that JSON extraction, clean-up, and repair logic lives in one place.

Typical LLM quirks handled:
- Markdown code fences (```json ... ```)
- Single-quoted strings instead of double-quoted
- Trailing commas before } / ]
- Python-style literals (True/False/None)
- Leading/trailing prose around JSON body
"""
from __future__ import annotations

import json
import logging
import re

logger = logging.getLogger("json_utils")


# ── Public API ────────────────────────────────────────────────────────


def extract_json_str(text: str) -> str:
    """Extract and return a clean JSON *string* from raw LLM output.

    The function works in escalating stages:

    1. Strip markdown fences.
    2. Try ``json.loads`` directly — if it works the text is already valid.
    3. Attempt best-effort repair (``_fix_json``) and try again.
    4. Regex-extract the outermost ``{ … }`` block and repeat (2)–(3).
    5. Raise ``ValueError`` if nothing works.
    """
    text = _strip_fences(text)

    # Stage 1 — already valid JSON
    if _is_valid_json(text):
        return text

    # Stage 2 — repair the full text
    fixed = _fix_json(text)
    if _is_valid_json(fixed):
        return fixed

    # Stage 3 — regex-extract outermost JSON object / array
    candidate = _regex_extract(text)
    if candidate is None:
        raise ValueError("No JSON object found in LLM response")

    if _is_valid_json(candidate):
        return candidate

    fixed_candidate = _fix_json(candidate)
    if _is_valid_json(fixed_candidate):
        return fixed_candidate

    # Return the best effort — caller's json.loads will surface a clear error
    return fixed_candidate


def parse_llm_json(text: str) -> dict:
    """Parse LLM output into a *dict*, handling common LLM quirks.

    This is the single entry point that all JSON-consuming code should
    call instead of raw ``json.loads``.  Internally delegates to
    ``extract_json_str`` for cleaning, then ``json.loads`` for parsing.
    """
    cleaned = extract_json_str(text)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        # Log the first 200 chars of both raw and cleaned for debugging
        logger.warning(
            "parse_llm_json: json.loads failed after cleanup. "
            "raw[:200]=%r cleaned[:200]=%r",
            text[:200],
            cleaned[:200],
        )
        raise


# ── Internal helpers ──────────────────────────────────────────────────


def _strip_fences(text: str) -> str:
    """Remove markdown code fences (```json ... ```) and surrounding whitespace."""
    text = text.strip()
    if text.startswith("```"):
        # Drop opening fence line (```json / ``` etc.)
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        # Drop closing fence
        if "```" in text:
            text = text.rsplit("```", 1)[0]
    return text.strip()


def _is_valid_json(text: str) -> bool:
    try:
        json.loads(text)
        return True
    except (json.JSONDecodeError, ValueError):
        return False


_SINGLE_QUOTE_PAIR = re.compile(
    r"""'([^'\\]*(?:\\.[^'\\]*)*)'""",
)


def _fix_json(text: str) -> str:
    """Best-effort repair of common LLM JSON quirks.

    Handles:
    - Single-quoted strings → double-quoted
    - Trailing commas before ``}`` / ``]``
    - Python literals ``True`` / ``False`` / ``None``
    """
    # ── Python literals ──────────────────────────────────────────
    text = re.sub(r"\bTrue\b", "true", text)
    text = re.sub(r"\bFalse\b", "false", text)
    text = re.sub(r"\bNone\b", "null", text)

    # ── Single quotes → double quotes ────────────────────────────
    # Replace pairs of single-quoted strings with double quotes.
    # We iterate character-by-character to avoid breaking apostrophes
    # inside already-double-quoted strings.
    out: list[str] = []
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if ch == '"':
            # Skip entire double-quoted string
            j = i + 1
            while j < n:
                if text[j] == '\\':
                    j += 2
                    continue
                if text[j] == '"':
                    j += 1
                    break
                j += 1
            out.append(text[i:j])
            i = j
        elif ch == "'":
            # Convert single-quoted string to double-quoted
            j = i + 1
            inner: list[str] = []
            while j < n:
                if text[j] == '\\':
                    inner.append(text[j:j + 2])
                    j += 2
                    continue
                if text[j] == "'":
                    j += 1
                    break
                # Escape any unescaped double-quote inside
                if text[j] == '"':
                    inner.append('\\"')
                else:
                    inner.append(text[j])
                j += 1
            out.append('"')
            out.append("".join(inner))
            out.append('"')
            i = j
        else:
            out.append(ch)
            i += 1
    text = "".join(out)

    # ── Trailing commas ──────────────────────────────────────────
    text = re.sub(r",\s*([}\]])", r"\1", text)

    return text


def _regex_extract(text: str) -> str | None:
    """Extract the outermost ``{ … }`` or ``[ … ]`` block via regex."""
    # Prefer object
    match = re.search(r"\{[\s\S]*\}", text)
    if match:
        return match.group(0)
    # Fallback to array
    match = re.search(r"\[[\s\S]*\]", text)
    if match:
        return match.group(0)
    return None
