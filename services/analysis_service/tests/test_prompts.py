"""Test the LLM prompt serialization pipeline.

Covers cross_asset field completeness, sparse filtering,
and signal data structure.
"""
from __future__ import annotations

import json
from types import SimpleNamespace

from shared.models.signal import (
    CrossAssetIndicators,
    DataQuality,
    OptionIndicators,
    SignalFeatures,
    SpreadExecutionCandidate,
    StockIndicators,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_signal_features(**overrides) -> SignalFeatures:
    """Build a minimal valid SignalFeatures using defaults."""
    defaults = dict(
        symbol="AAPL",
        date="2026-03-24",
        computed_at="2026-03-23T20:00:00",
        close_price=185.50,
        daily_return=0.012,
        volume=50_000_000,
        volatility_regime="normal",
        stock_indicators=StockIndicators(),
        option_indicators=OptionIndicators(),
        cross_asset_indicators=CrossAssetIndicators(),
        data_quality=DataQuality(),
    )
    defaults.update(overrides)
    return SignalFeatures(**defaults)


def test_build_blueprint_prompt_uses_configured_max_plans_and_allowed_strategy_types(monkeypatch):
    from services.analysis_service.app.llm.prompts import build_blueprint_prompt

    settings = SimpleNamespace(
        analysis_service=SimpleNamespace(
            llm=SimpleNamespace(
                max_output_plans=10,
                precision_first=SimpleNamespace(
                    enabled=True,
                    allowed_strategy_types=["single_leg", "vertical_spread", "iron_condor", "calendar_spread"],
                ),
            )
        ),
        common=SimpleNamespace(
            timezone="America/New_York",
            watchlist=SimpleNamespace(for_trade_benchmark=["SPY", "QQQ"]),
        ),
    )
    monkeypatch.setattr(
        "shared.config.get_settings",
        lambda: settings,
    )

    prompt = build_blueprint_prompt([_make_signal_features()])
    assert "Select up to 10 actionable underlyings" in prompt
    assert "You may output symbol_plans ONLY with strategy_type in: single_leg, vertical_spread, iron_condor, calendar_spread" in prompt


# ---------------------------------------------------------------------------
# Cross-Asset fields in serialization
# ---------------------------------------------------------------------------


class TestCrossAssetSerialization:
    """Verify _serialize_one_signal includes all multi-benchmark + VIX fields."""

    def test_all_cross_asset_fields_present(self):
        from services.analysis_service.app.llm.prompts import _serialize_one_signal

        sf = _make_signal_features(
            cross_asset_indicators=CrossAssetIndicators(
                spy_beta=1.2,
                index_correlation_20d=0.85,
                qqq_beta=1.1,
                qqq_correlation_20d=0.78,
                iwm_beta=0.9,
                iwm_correlation_20d=0.65,
                tlt_correlation_20d=-0.3,
                vix_level=18.5,
                vix_percentile_60d=0.45,
                vix_correlation_20d=-0.6,
                regime_days=2,
                regime_transition=True,
                regime_flip_count_10d=1,
                market_shock_return_1d=-0.034,
                market_shock_source="QQQ",
                gex_regime="negative",
            ),
        )

        text = _serialize_one_signal(sf)
        # Parse the JSON portion (after header line)
        lines = text.split("\n", 1)
        assert len(lines) == 2, "Expected header + JSON"
        data = json.loads(lines[1])

        ca = data.get("cross_asset", {})
        # New multi-benchmark fields
        assert "qqq_beta" in ca
        assert "qqq_correlation_20d" in ca
        assert "iwm_beta" in ca
        assert "iwm_correlation_20d" in ca
        assert "tlt_correlation_20d" in ca
        # VIX fields
        assert "vix_level" in ca
        assert "vix_percentile_60d" in ca
        assert "vix_percentile_52w" not in ca
        assert "vix_correlation_20d" in ca
        assert "regime_days" in ca
        assert "regime_transition" in ca
        assert "regime_flip_count_10d" in ca
        assert "market_shock_return_1d" in ca
        assert "market_shock_source" in ca
        assert "gex_regime" in ca

    def test_sparse_filtering_removes_zero_values(self):
        from services.analysis_service.app.llm.prompts import _serialize_one_signal

        sf = _make_signal_features()  # all defaults (mostly 0.0)

        text = _serialize_one_signal(sf)
        lines = text.split("\n", 1)
        assert len(lines) == 2
        data = json.loads(lines[1])

        # stock_trend should not contain fields that are 0.0
        trend = data.get("stock_trend", {})
        # Fields like adx_14=0.0 should be pruned
        for key, val in trend.items():
            if isinstance(val, (int, float)):
                assert val != 0 or key in (
                    # Some fields are semantically meaningful at 0
                    "rsi_divergence", "macd_hist_divergence",
                ), f"Zero-value field {key}={val} should have been pruned"

    def test_same_day_earnings_zero_is_preserved(self):
        from services.analysis_service.app.llm.prompts import _serialize_one_signal

        sf = _make_signal_features(
            cross_asset_indicators=CrossAssetIndicators(earnings_proximity_days=0),
        )

        text = _serialize_one_signal(sf)
        lines = text.split("\n", 1)
        assert len(lines) == 2
        data = json.loads(lines[1])

        cross_asset = data.get("cross_asset", {})
        assert cross_asset.get("earnings_proximity_days") == 0

    def test_zero_flip_count_is_preserved(self):
        from services.analysis_service.app.llm.prompts import _serialize_one_signal

        sf = _make_signal_features(
            cross_asset_indicators=CrossAssetIndicators(regime_flip_count_10d=0, market_shock_return_1d=0.0),
        )

        text = _serialize_one_signal(sf)
        lines = text.split("\n", 1)
        assert len(lines) == 2
        data = json.loads(lines[1])

        cross_asset = data.get("cross_asset", {})
        assert cross_asset.get("regime_flip_count_10d") == 0
        assert cross_asset.get("market_shock_return_1d") == 0.0

    def test_same_day_front_expiry_zero_is_preserved(self):
        from services.analysis_service.app.llm.prompts import _serialize_one_signal

        sf = _make_signal_features(
            option_indicators=OptionIndicators(current_iv=0.3, front_expiry_dte=0),
        )

        text = _serialize_one_signal(sf)
        lines = text.split("\n", 1)
        assert len(lines) == 2
        data = json.loads(lines[1])

        option_vol_surface = data.get("option_vol_surface", {})
        assert option_vol_surface.get("front_expiry_dte") == 0

    def test_header_contains_symbol(self):
        from services.analysis_service.app.llm.prompts import _serialize_one_signal

        sf = _make_signal_features(symbol="TSLA")
        text = _serialize_one_signal(sf)
        assert text.startswith("### TSLA")


# ---------------------------------------------------------------------------
# Signal structure completeness
# ---------------------------------------------------------------------------


class TestSignalStructure:
    def test_stock_trend_prunes_zero_adx_stats(self):
        from services.analysis_service.app.llm.prompts import _serialize_one_signal

        si = StockIndicators(adx_14=25.0, adx_z_score=0.0, adx_change_2d=0.0, rsi_14=55.0)
        sf = _make_signal_features(stock_indicators=si)
        text = _serialize_one_signal(sf)
        data = json.loads(text.split("\n", 1)[1])

        stock_trend = data.get("stock_trend", {})
        assert "adx_z_score" not in stock_trend
        assert "adx_change_2d" not in stock_trend

    def test_stock_flow_serializes_liquidity_threshold(self):
        from services.analysis_service.app.llm.prompts import _serialize_one_signal

        si = StockIndicators(vwap=185.0, cmf_20=0.1, tick_volume_delta=0.2, liquidity_threshold=1_500_000.0)
        sf = _make_signal_features(stock_indicators=si)
        text = _serialize_one_signal(sf)
        data = json.loads(text.split("\n", 1)[1])

        stock_flow = data.get("stock_flow", {})
        assert stock_flow.get("liquidity_threshold") == 1_500_000.0

    def test_all_top_level_sections_present(self):
        from services.analysis_service.app.llm.prompts import _serialize_one_signal

        # Use non-zero values so sparse filtering doesn't prune sections
        si = StockIndicators(adx_14=25.0, rsi_14=55.0, vwap=185.0, cmf_20=0.1, hv_20d=0.25)
        oi = OptionIndicators(iv_rank=0.5, iv_percentile=0.5, current_iv=0.3, pcr_volume=0.8, pcr_oi=0.9)
        sf = _make_signal_features(
            cross_asset_indicators=CrossAssetIndicators(spy_beta=1.1),
            stock_indicators=si,
            option_indicators=oi,
        )
        text = _serialize_one_signal(sf)
        lines = text.split("\n", 1)
        data = json.loads(lines[1])

        # With non-zero values, core sections should be present
        # (option_greeks may be pruned if all greeks are 0.0)
        expected_sections = {
            "price", "stock_trend", "stock_vol", "stock_flow",
            "option_vol_surface", "option_chain",
            "cross_asset",
        }
        assert expected_sections.issubset(set(data.keys())), (
            f"Missing sections: {expected_sections - set(data.keys())}"
        )


# ---------------------------------------------------------------------------
# Degraded-indicator section exclusion
# ---------------------------------------------------------------------------


class TestDegradedSectionExclusion:
    """Verify _serialize_one_signal excludes sections for fully-degraded categories."""

    def _parse_json(self, sf: SignalFeatures) -> dict:
        from services.analysis_service.app.llm.prompts import _serialize_one_signal

        text = _serialize_one_signal(sf)
        return json.loads(text.split("\n", 1)[1])

    def test_stock_all_degraded_excludes_stock_sections(self):
        si = StockIndicators(adx_14=25.0, rsi_14=55.0, vwap=185.0, cmf_20=0.1, hv_20d=0.25)
        oi = OptionIndicators(iv_rank=0.5, current_iv=0.3, pcr_volume=0.8)
        sf = _make_signal_features(
            stock_indicators=si,
            option_indicators=oi,
            cross_asset_indicators=CrossAssetIndicators(spy_beta=1.1),
            data_quality=DataQuality(
                complete=False, score=0.3,
                degraded_indicators=["stock:all"],
            ),
        )
        data = self._parse_json(sf)

        # Stock sections should be absent
        assert "stock_trend" not in data
        assert "stock_vol" not in data
        assert "stock_flow" not in data
        # Option + cross-asset sections should remain
        assert "option_vol_surface" in data
        assert "cross_asset" in data
        # data_quality should note excluded categories
        assert data["data_quality"]["excluded_categories"] == ["stock"]

    def test_option_all_degraded_excludes_option_sections(self):
        si = StockIndicators(adx_14=25.0, rsi_14=55.0, vwap=185.0, cmf_20=0.1, hv_20d=0.25)
        oi = OptionIndicators(iv_rank=0.5, current_iv=0.3, pcr_volume=0.8)
        sf = _make_signal_features(
            stock_indicators=si,
            option_indicators=oi,
            cross_asset_indicators=CrossAssetIndicators(spy_beta=1.1),
            data_quality=DataQuality(
                complete=False, score=0.4,
                degraded_indicators=["option:all"],
            ),
        )
        data = self._parse_json(sf)

        # Option sections should be absent
        assert "option_vol_surface" not in data
        assert "option_greeks" not in data
        assert "option_chain" not in data
        assert "option_spreads" not in data
        # Stock sections should remain
        assert "stock_trend" in data
        assert data["data_quality"]["excluded_categories"] == ["option"]

    def test_no_degradation_keeps_all_sections(self):
        si = StockIndicators(adx_14=25.0, rsi_14=55.0, vwap=185.0, cmf_20=0.1, hv_20d=0.25)
        oi = OptionIndicators(iv_rank=0.5, current_iv=0.3, pcr_volume=0.8)
        sf = _make_signal_features(
            stock_indicators=si,
            option_indicators=oi,
            cross_asset_indicators=CrossAssetIndicators(spy_beta=1.1),
        )
        data = self._parse_json(sf)

        assert "stock_trend" in data
        assert "option_vol_surface" in data
        assert "data_quality" not in data  # complete=True, no section added

    def test_partial_degradation_keeps_all_sections(self):
        si = StockIndicators(adx_14=25.0, rsi_14=55.0)
        sf = _make_signal_features(
            stock_indicators=si,
            data_quality=DataQuality(
                complete=False, score=0.7,
                degraded_indicators=["stock:ema_50", "option:iv_rank"],
            ),
        )
        data = self._parse_json(sf)

        # Partial degradation should NOT exclude sections
        assert "stock_trend" in data


def test_synthesizer_system_prompt_requires_strategy_type_leg_match():
    from services.analysis_service.app.llm.agents.synthesizer_agent import _SYNTHESIZER_SYSTEM_PROMPT

    assert "strategy_type MUST strictly match the actual legs count and structure" in _SYNTHESIZER_SYSTEM_PROMPT
    assert "Never label a 4-leg position as vertical_spread" in _SYNTHESIZER_SYSTEM_PROMPT


def test_system_prompt_describes_option_ratio_with_new_threshold_bands():
    from services.analysis_service.app.llm.prompts import SYSTEM_PROMPT

    assert "(Total option volume × 100) / Stock volume" in SYSTEM_PROMPT
    assert "<0.5 = illiquid-options proxy" in SYSTEM_PROMPT
    assert "0.5–1.5 = normal activity" in SYSTEM_PROMPT
    assert ">2.5 = extreme abnormal volume (requires further validation)" in SYSTEM_PROMPT
    assert "Do not use this metric alone as standalone proof of catalyst risk" in SYSTEM_PROMPT


def test_specialist_prompts_downgrade_option_ratio_to_supporting_signal():
    from services.analysis_service.app.llm.agents.cross_asset_agent import _SYSTEM_PROMPT as cross_asset_prompt
    from services.analysis_service.app.llm.agents.flow_agent import _SYSTEM_PROMPT as flow_prompt
    from services.analysis_service.app.llm.agents.volatility_agent import _SYSTEM_PROMPT as volatility_prompt

    assert "option_vs_stock_volume_ratio<0.5 = illiquid-options proxy" in flow_prompt
    assert "option_vs_stock_volume_ratio>2.5 requires separate event / IV confirmation" in flow_prompt
    assert "option_vs_stock_volume_ratio<0.5 = illiquid-options proxy" in volatility_prompt
    assert "option_vs_stock_volume_ratio>2.5 alone cannot justify event-vol trades" in volatility_prompt
    assert "<0.5 = illiquid-options proxy, 0.5-1.5 = normal, 1.5-2.5 = elevated, >2.5 = extreme abnormal volume" in cross_asset_prompt
    assert ">2.5 requires event / IV confirmation" in cross_asset_prompt


def test_cross_asset_prompt_uses_explicit_phase2_inputs_and_drops_prompt_only_trade_gate():
    from services.analysis_service.app.llm.agents.cross_asset_agent import _SYSTEM_PROMPT as cross_asset_prompt

    assert "cross_asset.regime_flip_count_10d" in cross_asset_prompt
    assert "cross_asset.market_shock_return_1d" in cross_asset_prompt
    assert "cross_asset.market_shock_source" in cross_asset_prompt
    assert "cross_asset.gex_regime" in cross_asset_prompt
    assert "cross_asset.confidence.correlation_significance" in cross_asset_prompt
    assert "cross_asset.confidence.data_freshness" in cross_asset_prompt
    assert '"trade_allowed":' not in cross_asset_prompt
    assert "4-5d: Transition" not in cross_asset_prompt


def test_flow_prompt_avoids_unavailable_volume_baseline_and_multibar_claims():
    from services.analysis_service.app.llm.agents.flow_agent import _SYSTEM_PROMPT as flow_prompt

    assert "Do NOT invent xSMA volume ratios, declining-volume sequences, gap-fill failures, or candle-pattern confirmations" in flow_prompt
    assert "Do NOT assert quiet deterioration from consecutive bars" in flow_prompt
    assert "Do NOT use reversal-candle exhaustion logic" in flow_prompt
    assert "Breakout-like move with BOTH CMF and tick_delta confirmation" in flow_prompt
    assert "Breakout + volume>1.5×SMA + delta confirm" not in flow_prompt
    assert "Gap fill failure + volume<1×SMA" not in flow_prompt


def test_flow_prompt_uses_explicit_liquidity_threshold_and_breakout_boundaries():
    from services.analysis_service.app.llm.agents.flow_agent import _SYSTEM_PROMPT as flow_prompt

    assert "stock_flow.liquidity_threshold" in flow_prompt
    assert "Use stock_flow.liquidity_threshold as the ADV-derived current-bar liquidity hurdle" in flow_prompt
    assert "close > stock_flow.volume_profile_vah + 0.4×ATR" in flow_prompt
    assert "close < stock_flow.volume_profile_val - 0.4×ATR" in flow_prompt
    assert "POC is NOT a breakout boundary" in flow_prompt


def test_flow_prompt_splits_breakout_specific_confirmation_counts_from_global_counts():
    from services.analysis_service.app.llm.agents.flow_agent import _SYSTEM_PROMPT as flow_prompt

    assert "VWAP alignment is a breakout prerequisite and does NOT add to BK confirmation count" in flow_prompt
    assert "BK1-BK3 breakout-specific confirmation counts ONLY CMF and Tick Delta" in flow_prompt
    assert "Count ONLY distinct directional confirmations:" in flow_prompt


def test_flow_prompt_defines_volume_anomaly_null_earnings_behavior_and_hard_cap_precedence():
    from services.analysis_service.app.llm.agents.flow_agent import _SYSTEM_PROMPT as flow_prompt

    assert "volume_anomaly=true if price.volume ≥ 2 × stock_flow.liquidity_threshold" in flow_prompt
    assert "If earnings_proximity_days is null, keep it null and do NOT trigger H1/H2 earnings overrides" in flow_prompt
    assert "Apply the confidence-to-size table first, then clamp by all active hard caps" in flow_prompt


def test_chain_prompt_excludes_gamma_pin_from_directional_confirming_count():
    from services.analysis_service.app.llm.agents.chain_agent import _SYSTEM_PROMPT as chain_prompt

    assert "Gamma pin does NOT count toward directional `confirming_indicators_count`" in chain_prompt
    assert "Do NOT set `trade_allowed=false` for earnings proximity alone" in chain_prompt
    assert "`liquidity_ok`: informational execution-quality flag only" in chain_prompt


def test_flow_prompt_uses_machine_readable_trade_gates_for_hard_no_trade_states():
    from services.analysis_service.app.llm.agents.flow_agent import _SYSTEM_PROMPT as flow_prompt

    assert "H1. earnings_proximity_days≤1 (Imminent Event): event_risk_present=true, flow_signal=neutral, trade_allowed=false" in flow_prompt
    assert "BK1. Breakout-like move with 0 confirming indicators = high false breakout risk, trade_allowed=false" in flow_prompt
    assert "## Confirming Indicators Count (Deterministic)" in flow_prompt
    assert "4-5d" not in flow_prompt


def test_trend_prompt_uses_explicit_precomputed_fields_and_drops_legacy_4_5d_tier():
    from services.analysis_service.app.llm.agents.trend_agent import _SYSTEM_PROMPT as trend_prompt

    assert "stock_trend.adx_z_score" in trend_prompt
    assert "stock_trend.adx_change_2d" in trend_prompt
    assert "stock_flow.liquidity_threshold" in trend_prompt
    assert "Do NOT compute Z-scores, ADX deltas, or divergence flags from scratch" in trend_prompt
    assert "4-5d: Transition" not in trend_prompt
    assert "H2a." not in trend_prompt


def test_trend_prompt_uses_numeric_liquidity_and_corrects_macd_divergence_semantics():
    from services.analysis_service.app.llm.agents.trend_agent import _SYSTEM_PROMPT as trend_prompt

    assert "price.volume < stock_flow.liquidity_threshold" in trend_prompt
    assert "stock_trend.macd_hist_divergence = +1 trend confirmation, -1 contradiction/divergence" in trend_prompt
    assert "divergence same as RSI" not in trend_prompt
    assert "cross_asset.vix_level>45: regime=neutral, trade_allowed=false, confidence=0.2, blocked_reasons=[\"vix_extreme\"]" in trend_prompt


def test_trend_prompt_defines_confirmation_count_signal_type_and_output_derivation():
    from services.analysis_service.app.llm.agents.trend_agent import _SYSTEM_PROMPT as trend_prompt

    assert "## Confirming Indicators Count (Deterministic)" in trend_prompt
    assert '`signal_type="single_indicator"` when the selected regime has exactly 1 extra confirmation' in trend_prompt
    assert '`signal_type="multi_indicator"` when it has >=2 extra confirmations' in trend_prompt
    assert '`false_positive_risk="high"` when signal_type is single_indicator' in trend_prompt
    assert '`trend_strength`: 0.25 neutral / hard override, 0.45 with 1 extra confirmation, 0.65 with 2 extras, 0.85 with 3 extras' in trend_prompt


def test_trend_prompt_preserves_null_iv_rank_behavior():
    from services.analysis_service.app.llm.agents.trend_agent import _SYSTEM_PROMPT as trend_prompt

    assert "If iv_rank is null, keep it null and skip IV-rank-specific squeeze or penalty rules" in trend_prompt
    assert "do NOT replace it with 0" in trend_prompt


def test_spread_prompt_uses_spread_domain_and_real_available_inputs_only():
    from services.analysis_service.app.llm.agents.spread_agent import _SYSTEM_PROMPT as spread_prompt

    assert "Spread & Arbitrage Specialist" in spread_prompt
    assert "Trend & Options Strategist" not in spread_prompt
    assert "option_spreads.vertical_spread_risk_reward" in spread_prompt
    assert "Do NOT fabricate missing legs or recalculate transaction costs beyond the provided execution_candidates fields" in spread_prompt
    assert "TC1." not in spread_prompt
    assert "4-5d: Transition" not in spread_prompt


def test_spread_prompt_defines_proxy_liquidity_term_structure_and_confirming_indicators():
    from services.analysis_service.app.llm.agents.spread_agent import _SYSTEM_PROMPT as spread_prompt

    assert "bid_ask_spread_ratio = mean bid/ask spread ratio across tradeable contracts in the chain" in spread_prompt
    assert "option_spreads.execution_candidates.* = representative 1-lot execution summaries already computed upstream" in spread_prompt
    assert "term_structure_slope = far-expiry ATM IV - front-expiry ATM IV" in spread_prompt
    assert "## Confirming Indicators Count (Deterministic)" in spread_prompt
    assert "Single confirming indicator only = confirming_indicators_count==1" in spread_prompt
    assert "Do NOT reject non-vertical spreads solely because effective_rr is null" in spread_prompt


def test_spread_prompt_drops_master_override_and_aligns_schema_with_runtime_model():
    from services.analysis_service.app.llm.agents.spread_agent import _SYSTEM_PROMPT as spread_prompt

    assert "master_override" not in spread_prompt
    assert '"confirming_indicators_count":0-4' in spread_prompt
    assert '"position_size_modifier":0.0-1.2' in spread_prompt
    assert '"vix_level":0.0' in spread_prompt


def test_stock_signal_serializes_spread_execution_candidates():
    from services.analysis_service.app.llm.prompts import _serialize_one_signal

    oi = OptionIndicators(
        vertical_spread_risk_reward=1.1,
        spread_execution_inputs={
            "vertical": SpreadExecutionCandidate(
                strategy_type="vertical",
                candidate_available=True,
                expiry="2025-02-15",
                expiry_dte=3,
                long_strike=100.0,
                short_strike=102.0,
                estimated_round_trip_cost=14.0,
                raw_rr=1.25,
                effective_rr=0.93,
                worst_leg_bid_ask_spread_ratio=0.08,
            )
        },
    )
    sf = _make_signal_features(option_indicators=oi)

    text = _serialize_one_signal(sf)
    data = json.loads(text.split("\n", 1)[1])

    execution_candidates = data.get("option_spreads", {}).get("execution_candidates", {})
    assert execution_candidates["vertical"]["effective_rr"] == 0.93
    assert execution_candidates["vertical"]["estimated_round_trip_cost"] == 14.0


def test_spread_prompt_consumes_execution_candidates_for_cost_aware_fields():
    from services.analysis_service.app.llm.agents.spread_agent import _SYSTEM_PROMPT as spread_prompt

    assert "option_spreads.execution_candidates.vertical.effective_rr" in spread_prompt
    assert "option_spreads.execution_candidates.calendar.effective_theta_capture_per_day" in spread_prompt
    assert "option_spreads.execution_candidates.box_arb.net_edge_after_cost" in spread_prompt


def test_synthesizer_and_critic_prompts_apply_spread_rr_gate_only_to_verticals():
    from services.analysis_service.app.llm.agents.critic_agent import _CRITIC_SYSTEM_PROMPT as critic_prompt
    from services.analysis_service.app.llm.agents.synthesizer_agent import _SYNTHESIZER_SYSTEM_PROMPT

    assert "Reject vertical_spread only when Spread.effective_rr is explicitly available and <0.7" in _SYNTHESIZER_SYSTEM_PROMPT
    assert "Do NOT exclude iron_condor, butterfly, calendar_spread, or arbitrage setups solely because Spread.effective_rr is null" in _SYNTHESIZER_SYSTEM_PROMPT
    assert "Only vertical_spread may be rejected on Spread R:R" in critic_prompt
    assert "must NOT be rejected solely because Spread.effective_rr is null" in critic_prompt


def test_synthesizer_system_prompt_allows_iron_condor_for_clean_range_bound_setups():
    from services.analysis_service.app.llm.agents.synthesizer_agent import _SYNTHESIZER_SYSTEM_PROMPT

    assert "prefer single_leg or vertical_spread for directional theses" in _SYNTHESIZER_SYSTEM_PROMPT
    assert "iron_condor is also acceptable" in _SYNTHESIZER_SYSTEM_PROMPT
    assert "gamma-pin exception may override HE7" in _SYNTHESIZER_SYSTEM_PROMPT


def test_synthesizer_system_prompt_gates_calendar_to_contango_and_earnings_buffer():
    from services.analysis_service.app.llm.agents.synthesizer_agent import _SYNTHESIZER_SYSTEM_PROMPT

    assert "calendar_spread is also acceptable" in _SYNTHESIZER_SYSTEM_PROMPT
    assert "calendar_spread specifically requires positive term_structure_slope and earnings_proximity_days > 5" in _SYNTHESIZER_SYSTEM_PROMPT


def test_synthesizer_prompt_uses_execution_candidates_for_structure_priority():
    from services.analysis_service.app.llm.agents.synthesizer_agent import _SYNTHESIZER_SYSTEM_PROMPT

    assert "Market Signal Data option_spreads.execution_candidates" in _SYNTHESIZER_SYSTEM_PROMPT
    assert "use the following priority order" in _SYNTHESIZER_SYSTEM_PROMPT


def test_critic_prompt_uses_execution_candidates_for_structure_priority_conflicts():
    from services.analysis_service.app.llm.agents.critic_agent import _CRITIC_SYSTEM_PROMPT as critic_prompt

    assert "Market Signal Data fields in scope: option_spreads.execution_candidates" in critic_prompt
    assert "Execution Candidate Priority" in critic_prompt
    assert "structure_priority_conflict" in critic_prompt
    assert "vertical effective_rr/raw_rr ≥0.7" in critic_prompt
    assert "downgrade to severity=warning and skip structure-priority comparison" in critic_prompt


def test_synthesizer_and_critic_build_prompts_include_full_signal_context_execution_candidates():
    from services.analysis_service.app.llm.agents.critic_agent import CriticAgent
    from services.analysis_service.app.llm.agents.synthesizer_agent import SynthesizerAgent

    signal_context = [
        {
            "symbol": "AAPL",
            "price": {"close_price": 100.0, "volume": 1_000_000, "volatility_regime": "normal"},
            "option_spreads": {
                "execution_candidates": {
                    "vertical": {"effective_rr": 0.92},
                    "calendar": {"effective_theta_capture_per_day": 0.04},
                }
            },
        }
    ]

    synth_prompt = SynthesizerAgent()._build_prompt(
        agent_outputs={},
        signals_summary=signal_context,
        critic_feedback=None,
        signal_date=None,
        trade_symbols=["AAPL"],
    )
    critic_prompt = CriticAgent()._build_prompt(
        blueprint_json={"symbol_plans": []},
        agent_outputs={},
        signals_summary=signal_context,
    )

    expected = '"execution_candidates":{"vertical":{"effective_rr":0.92},"calendar":{"effective_theta_capture_per_day":0.04}}'
    assert expected in synth_prompt
    assert expected in critic_prompt


def test_synthesizer_and_critic_prompts_align_with_manual_trader_risk_contract():
    from services.analysis_service.app.llm.agents.critic_agent import _CRITIC_SYSTEM_PROMPT as critic_prompt
    from services.analysis_service.app.llm.agents.synthesizer_agent import _SYNTHESIZER_SYSTEM_PROMPT

    assert "Trader decides max loss and sizing manually" in _SYNTHESIZER_SYSTEM_PROMPT
    assert "Do NOT emit max_position_size, stop_loss_amount, take_profit_amount, or max_loss_per_trade" in _SYNTHESIZER_SYSTEM_PROMPT
    assert "Do NOT reject a plan solely because stop_loss_amount, take_profit_amount, max_loss_per_trade, or max_position_size is missing" in critic_prompt
    assert "confidence > 0.5 → severity=error" in critic_prompt
    assert "MAX_POSITION_SIZE_CAP: 2.0" not in critic_prompt


def test_critic_prompt_defines_gp1_precedence_null_defaults_and_lc2_relaxation():
    from services.analysis_service.app.llm.agents.critic_agent import _CRITIC_SYSTEM_PROMPT as critic_prompt

    assert "GP1 takes precedence over SE6" in critic_prompt
    assert "Missing numeric modifiers" in critic_prompt
    assert "Missing numeric `confidence_cap` defaults to GLOBAL_MAX_CONFIDENCE" in critic_prompt
    assert "Directional shock exemption" in critic_prompt
    assert "single_leg and vertical_spread: DTE ≥5 and ≤180" in critic_prompt
    assert "adjustment_rules may be empty only for one-shot expiry structures" in critic_prompt


def test_synthesizer_and_critic_prompts_explicitly_consume_cross_asset_event_risk_market_shock_and_finer_gex():
    from services.analysis_service.app.llm.agents.critic_agent import _CRITIC_SYSTEM_PROMPT as critic_prompt
    from services.analysis_service.app.llm.agents.synthesizer_agent import _SYNTHESIZER_SYSTEM_PROMPT

    assert "Cross-Asset: correlation_regime, vix_environment, vix_percentile_60d, gex_regime, event_risk_present" in _SYNTHESIZER_SYSTEM_PROMPT
    assert "market_shock_return_1d" in _SYNTHESIZER_SYSTEM_PROMPT
    assert "market_shock_source" in _SYNTHESIZER_SYSTEM_PROMPT
    assert "Cross-Asset.event_risk_present=true counts toward the event-risk agent count" in _SYNTHESIZER_SYSTEM_PROMPT
    assert 'Cross-Asset.gex_regime="negative" AND abs(Cross-Asset.market_shock_return_1d)>0.03' in _SYNTHESIZER_SYSTEM_PROMPT
    assert "Cross-Asset fields in scope" in critic_prompt
    assert "event_risk_present" in critic_prompt
    assert "market_shock_return_1d" in critic_prompt
    assert "market_shock_source" in critic_prompt
    assert 'Cross-Asset.gex_regime="negative" AND abs(Cross-Asset.market_shock_return_1d)>0.03' in critic_prompt
    assert "Cross-Asset.event_risk_present=true counts toward the event-risk agent total" in critic_prompt


def test_synthesizer_and_critic_prompts_drop_blueprint_risk_cap_contract():
    from services.analysis_service.app.llm.agents.critic_agent import _CRITIC_SYSTEM_PROMPT as critic_prompt
    from services.analysis_service.app.llm.agents.synthesizer_agent import _SYNTHESIZER_SYSTEM_PROMPT

    assert "portfolio_delta_limit must NOT exceed configured global risk policy cap" not in _SYNTHESIZER_SYSTEM_PROMPT
    assert "portfolio_gamma_limit must NOT exceed configured global risk policy cap" not in _SYNTHESIZER_SYSTEM_PROMPT
    assert "max_daily_loss must NOT exceed configured global risk policy cap" not in _SYNTHESIZER_SYSTEM_PROMPT
    assert "max_margin_usage must NOT exceed configured global risk policy cap" not in _SYNTHESIZER_SYSTEM_PROMPT
    assert "Top-level: max_total_positions, max_daily_loss, max_margin_usage, portfolio_delta_limit, portfolio_gamma_limit" not in _SYNTHESIZER_SYSTEM_PROMPT
    assert "Top-level: max_total_positions" in _SYNTHESIZER_SYSTEM_PROMPT
    assert "portfolio_delta_limit must NOT exceed configured global risk policy cap" not in critic_prompt
    assert "portfolio_gamma_limit must NOT exceed configured global risk policy cap" not in critic_prompt
    assert "max_daily_loss must NOT exceed configured global risk policy cap" not in critic_prompt
    assert "max_margin_usage must NOT exceed configured global risk policy cap" not in critic_prompt
    assert "Ignore legacy top-level portfolio cap fields during review" in critic_prompt


def test_volatility_system_prompt_lists_supported_contango_and_backwardation_regimes():
    from services.analysis_service.app.llm.agents.volatility_agent import _SYSTEM_PROMPT

    assert "30-70=normal" in _SYSTEM_PROMPT
    assert "30-70=neutral" not in _SYSTEM_PROMPT
    assert "vol_regime` is NOT `iv_rank_zone" in _SYSTEM_PROMPT
    assert "emit `backwardation_event_risk` even when IV Rank is high or low" in _SYSTEM_PROMPT
    assert "Never emit unsupported triples such as `high_vol_backwardation_event_risk`" in _SYSTEM_PROMPT
    assert "high_vol_contango" in _SYSTEM_PROMPT
    assert "low_vol_contango" in _SYSTEM_PROMPT
    assert "high_vol_backwardation" in _SYSTEM_PROMPT
    assert "low_vol_backwardation" in _SYSTEM_PROMPT
    assert "Do not invent unsupported compounds beyond the listed regimes above" in _SYSTEM_PROMPT


def test_volatility_prompt_enumerates_real_input_fields_and_drops_legacy_4_5d_tier():
    from services.analysis_service.app.llm.agents.volatility_agent import _SYSTEM_PROMPT

    assert "option_vol_surface.front_expiry_dte" in _SYSTEM_PROMPT
    assert "stock_vol.hv_20d" in _SYSTEM_PROMPT
    assert "stock_vol.garch_vol_forecast" in _SYSTEM_PROMPT
    assert "stock_trend.bollinger_band_width" in _SYSTEM_PROMPT
    assert "4-5d: Transition" not in _SYSTEM_PROMPT
    assert "H2a." not in _SYSTEM_PROMPT


def test_volatility_prompt_removes_master_override_and_cross_module_arbitrage_escalation():
    from services.analysis_service.app.llm.agents.volatility_agent import _SYSTEM_PROMPT

    assert "master_override" not in _SYSTEM_PROMPT
    assert "box arbitrage" not in _SYSTEM_PROMPT.lower()
    assert "butterfly pricing error>0.15" not in _SYSTEM_PROMPT


def test_volatility_prompt_defines_signal_count_dte_and_output_derivations_deterministically():
    from services.analysis_service.app.llm.agents.volatility_agent import _SYSTEM_PROMPT

    assert "## Confirming Indicators Count (Deterministic)" in _SYSTEM_PROMPT
    assert 'signal_type="single_indicator" when exactly 1 confirmation remains after hard overrides' in _SYSTEM_PROMPT
    assert "option_vol_surface.front_expiry_dte<10" in _SYSTEM_PROMPT
    assert '`surface_mispricing=true` when option_vol_surface.vol_surface_fit_error > 0.02' in _SYSTEM_PROMPT
    assert '`liquidity_status="low"` when liquidity.bid_ask_spread_ratio > 0.15' in _SYSTEM_PROMPT


def test_volatility_prompt_binds_h3_and_r6_to_front_expiry_dte_windows():
    from services.analysis_service.app.llm.agents.volatility_agent import _SYSTEM_PROMPT

    assert "option_vol_surface.term_structure_slope<0 AND option_vol_surface.front_expiry_dte<10" in _SYSTEM_PROMPT
    assert "option_vol_surface.front_expiry_dte>21" in _SYSTEM_PROMPT
