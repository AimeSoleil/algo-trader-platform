"""Helpers for turning escaped error text into human-readable strings."""
from __future__ import annotations

import re


_UNICODE_ESCAPE_RE = re.compile(r"(?:\\u[0-9a-fA-F]{4}|\\U[0-9a-fA-F]{8})+")


def decode_escaped_unicode(value: object) -> str:
    """Decode literal Unicode escape sequences without altering other escapes."""
    text = str(value)
    if "\\" not in text:
        return text

    def _replace(match: re.Match[str]) -> str:
        escaped = match.group(0)
        try:
            return escaped.encode("ascii").decode("unicode_escape")
        except UnicodeDecodeError:
            return escaped

    return _UNICODE_ESCAPE_RE.sub(_replace, text)