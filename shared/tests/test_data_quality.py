"""Tests for shared.data_quality — analysis-stage helpers."""
from __future__ import annotations

from shared.data_quality import (
    is_option_all_degraded,
    is_stock_all_degraded,
    should_circuit_break_analysis,
)


class TestAnalysisStageHelpers:
    def test_stock_all_degraded_true(self):
        assert is_stock_all_degraded(["stock:all"]) is True

    def test_stock_all_degraded_false_when_partial(self):
        assert is_stock_all_degraded(["stock:ema_50", "stock:macd"]) is False

    def test_option_all_degraded_true(self):
        assert is_option_all_degraded(["option:all"]) is True

    def test_option_all_degraded_false_empty(self):
        assert is_option_all_degraded([]) is False

    def test_circuit_break_both_degraded(self):
        assert should_circuit_break_analysis(["stock:all", "option:all"]) is True

    def test_no_circuit_break_stock_only(self):
        assert should_circuit_break_analysis(["stock:all"]) is False

    def test_no_circuit_break_option_only(self):
        assert should_circuit_break_analysis(["option:all"]) is False

    def test_no_circuit_break_partial(self):
        assert should_circuit_break_analysis(["stock:ema_50", "option:iv_rank"]) is False

    def test_no_circuit_break_empty(self):
        assert should_circuit_break_analysis([]) is False
