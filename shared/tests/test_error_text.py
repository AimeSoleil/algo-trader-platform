from __future__ import annotations

from shared.utils import decode_escaped_unicode


def test_decode_escaped_unicode_converts_literal_unicode_sequences():
    message = 'Error code: 402 - {"error":{"message":"\u4f59\u989d\u4e0d\u8db3\uff0c\u8bf7\u5145\u503c\u540e\u518d\u4f7f\u7528"}}'

    decoded = decode_escaped_unicode(message)

    assert "余额不足，请充值后再使用" in decoded


def test_decode_escaped_unicode_leaves_other_escapes_intact():
    message = r"line1\npath=C:\\temp\\file.txt"

    decoded = decode_escaped_unicode(message)

    assert decoded == message