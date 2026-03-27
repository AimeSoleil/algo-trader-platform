"""Unit tests for services.analysis_service.app.llm.json_utils."""
from __future__ import annotations

import json

import pytest

from services.analysis_service.app.llm.json_utils import (
    extract_json_str,
    parse_llm_json,
)


# ── extract_json_str ──────────────────────────────────────────────────


class TestExtractJsonStr:
    """Tests for extract_json_str."""

    def test_valid_json_passthrough(self):
        raw = '{"key": "value", "num": 42}'
        assert extract_json_str(raw) == raw

    def test_markdown_fence_json(self):
        raw = '```json\n{"key": "value"}\n```'
        result = extract_json_str(raw)
        assert json.loads(result) == {"key": "value"}

    def test_markdown_fence_no_lang(self):
        raw = '```\n{"key": "value"}\n```'
        result = extract_json_str(raw)
        assert json.loads(result) == {"key": "value"}

    def test_single_quotes_object(self):
        raw = "{'symbol': 'AAPL', 'score': 0.8}"
        result = extract_json_str(raw)
        parsed = json.loads(result)
        assert parsed == {"symbol": "AAPL", "score": 0.8}

    def test_single_quotes_nested(self):
        raw = "{'outer': {'inner': 'val'}, 'list': ['a', 'b']}"
        result = extract_json_str(raw)
        parsed = json.loads(result)
        assert parsed == {"outer": {"inner": "val"}, "list": ["a", "b"]}

    def test_trailing_comma_object(self):
        raw = '{"a": 1, "b": 2, }'
        result = extract_json_str(raw)
        assert json.loads(result) == {"a": 1, "b": 2}

    def test_trailing_comma_array(self):
        raw = '{"items": [1, 2, 3, ]}'
        result = extract_json_str(raw)
        assert json.loads(result) == {"items": [1, 2, 3]}

    def test_python_literals(self):
        raw = '{"active": True, "deleted": False, "value": None}'
        result = extract_json_str(raw)
        parsed = json.loads(result)
        assert parsed == {"active": True, "deleted": False, "value": None}

    def test_mixed_quirks(self):
        """Single quotes + trailing comma + Python literal together."""
        raw = "{'active': True, 'name': 'test', }"
        result = extract_json_str(raw)
        parsed = json.loads(result)
        assert parsed == {"active": True, "name": "test"}

    def test_prose_around_json(self):
        raw = 'Here is my analysis:\n{"symbol": "AAPL", "score": 0.9}\nHope this helps!'
        result = extract_json_str(raw)
        assert json.loads(result) == {"symbol": "AAPL", "score": 0.9}

    def test_markdown_fence_with_prose(self):
        raw = 'Sure, here you go:\n```json\n{"key": "value"}\n```\nLet me know.'
        result = extract_json_str(raw)
        assert json.loads(result) == {"key": "value"}

    def test_no_json_raises(self):
        with pytest.raises(ValueError, match="No JSON object"):
            extract_json_str("This is just plain text with no JSON at all.")

    def test_double_quoted_string_with_apostrophe(self):
        """Ensure apostrophes inside double-quoted strings are preserved."""
        raw = '{"note": "it\'s working", "count": 1}'
        result = extract_json_str(raw)
        parsed = json.loads(result)
        assert parsed["note"] == "it's working"

    def test_whitespace_handling(self):
        raw = '   \n  {"key": "value"}  \n  '
        result = extract_json_str(raw)
        assert json.loads(result) == {"key": "value"}

    def test_single_quote_with_embedded_double_quote(self):
        """Single-quoted string containing a double quote."""
        raw = """{'msg': 'say "hello"', 'ok': True}"""
        result = extract_json_str(raw)
        parsed = json.loads(result)
        assert parsed["msg"] == 'say "hello"'
        assert parsed["ok"] is True

    def test_literal_escaped_newlines(self):
        """Copilot SDK sometimes returns literal \\n instead of real newlines."""
        raw = '{\\n  "symbols": [\\n    {\\n      "symbol": "NVDA",\\n      "score": 0.8\\n    }\\n  ]\\n}'
        result = extract_json_str(raw)
        parsed = json.loads(result)
        assert parsed["symbols"][0]["symbol"] == "NVDA"
        assert parsed["symbols"][0]["score"] == 0.8

    def test_literal_escaped_tabs(self):
        """Literal \\t outside strings converted to real tabs."""
        raw = '{\\t"key": "value"}'
        result = extract_json_str(raw)
        parsed = json.loads(result)
        assert parsed["key"] == "value"

    def test_escaped_newlines_preserved_inside_strings(self):
        """\\n inside double-quoted string values must stay as JSON \\n escape."""
        raw = '{\\n  "msg": "line1\\nline2",\\n  "count": 1\\n}'
        result = extract_json_str(raw)
        parsed = json.loads(result)
        assert parsed["msg"] == "line1\nline2"
        assert parsed["count"] == 1

    def test_literal_escaped_newlines_with_nested_objects(self):
        """Full Copilot-style response with literal \\n throughout."""
        raw = (
            '{\\n  "symbols": [\\n    {\\n      "symbol": "NVDA",\\n'
            '      "best_spread_type": null,\\n      "risk_reward_ratio": 0.0,\\n'
            '      "mispricing_detected": false\\n    }\\n  ]\\n}'
        )
        result = extract_json_str(raw)
        parsed = json.loads(result)
        assert parsed["symbols"][0]["symbol"] == "NVDA"
        assert parsed["symbols"][0]["best_spread_type"] is None
        assert parsed["symbols"][0]["mispricing_detected"] is False


# ── parse_llm_json ────────────────────────────────────────────────────


class TestParseLlmJson:
    """Tests for parse_llm_json (returns dict)."""

    def test_valid_json(self):
        assert parse_llm_json('{"a": 1}') == {"a": 1}

    def test_single_quotes(self):
        assert parse_llm_json("{'a': 1}") == {"a": 1}

    def test_markdown_fence(self):
        assert parse_llm_json('```json\n{"a": 1}\n```') == {"a": 1}

    def test_python_booleans(self):
        result = parse_llm_json('{"x": True, "y": False, "z": None}')
        assert result == {"x": True, "y": False, "z": None}

    def test_raises_on_garbage(self):
        with pytest.raises((json.JSONDecodeError, ValueError)):
            parse_llm_json("no json here at all")

    def test_complex_blueprint_like(self):
        """Simulate a typical LLM blueprint response with quirks."""
        raw = """\
```json
{
    'trading_date': '2026-03-28',
    'market_bias': 'neutral',
    'symbol_plans': [
        {
            'symbol': 'AAPL',
            'direction': 'bullish',
            'confidence': 0.75,
            'active': True,
        }
    ],
}
```"""
        result = parse_llm_json(raw)
        assert result["trading_date"] == "2026-03-28"
        assert result["symbol_plans"][0]["symbol"] == "AAPL"
        assert result["symbol_plans"][0]["active"] is True
