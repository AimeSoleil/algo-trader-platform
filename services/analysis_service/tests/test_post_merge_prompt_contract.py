from __future__ import annotations

from services.analysis_service.app.llm.agents.post_merge_portfolio_agent import (
    PostMergePortfolioAgent,
    _POST_MERGE_SYSTEM_PROMPT,
)


def test_post_merge_prompt_uses_symbol_level_ranking_and_explains_duplicate_candidates():
    assert "ranking MUST include every candidate symbol exactly once" in _POST_MERGE_SYSTEM_PROMPT
    assert "candidate_summaries may contain multiple candidate entries for the same symbol" in _POST_MERGE_SYSTEM_PROMPT
    assert "The output ranking must still contain that symbol only once" in _POST_MERGE_SYSTEM_PROMPT
    assert "Keep ALL valid plans in the ranking" not in _POST_MERGE_SYSTEM_PROMPT


def test_post_merge_prompt_defines_sorting_pipeline_and_score_direction():
    assert "## SORTING PIPELINE (Apply in Order)" in _POST_MERGE_SYSTEM_PROMPT
    assert "Follow selector_metadata.deterministic_sort_priority exactly as the canonical base ordering logic" in _POST_MERGE_SYSTEM_PROMPT
    assert "Diversification and risk demotions MUST NOT reorder symbols inside these locked head groups" in _POST_MERGE_SYSTEM_PROMPT
    assert "portfolio_impact_score<0.40: +2 positions later" in _POST_MERGE_SYSTEM_PROMPT
    assert "portfolio_impact_score (lowest first)" not in _POST_MERGE_SYSTEM_PROMPT


def test_post_merge_prompt_defines_input_contract_defaults_and_selected_symbols_rule():
    assert "## INPUT CONTRACT" in _POST_MERGE_SYSTEM_PROMPT
    assert "master_override" in _POST_MERGE_SYSTEM_PROMPT
    assert "arb_opportunity" in _POST_MERGE_SYSTEM_PROMPT
    assert "trade_gate_status" in _POST_MERGE_SYSTEM_PROMPT
    assert "trade_gate_summary" in _POST_MERGE_SYSTEM_PROMPT
    assert "trade_gate_taxonomy" in _POST_MERGE_SYSTEM_PROMPT
    assert "max_loss_per_trade" not in _POST_MERGE_SYSTEM_PROMPT
    assert "max_position_size" not in _POST_MERGE_SYSTEM_PROMPT
    assert "Defaults when optional fields are missing" in _POST_MERGE_SYSTEM_PROMPT
    assert "selected_symbols must be derived from the front of ranking" in _POST_MERGE_SYSTEM_PROMPT


def test_post_merge_build_prompt_describes_unique_symbol_count_and_raw_entries():
    prompt = PostMergePortfolioAgent()._build_prompt(
        candidate_summaries=[
            {"symbol": "AAPL", "candidate_ref": "AAPL::chunk-0::0"},
            {"symbol": "AAPL", "candidate_ref": "AAPL::chunk-1::1"},
            {"symbol": "MSFT", "candidate_ref": "MSFT::chunk-0::2"},
        ],
        chunk_limit_proposals=[],
        selector_metadata={"ranking_scope": "symbol_level"},
        candidate_count=2,
    )

    assert "Rank all 2 candidate symbols globally." in prompt
    assert "The candidate list may contain 3 raw candidate entries across those symbols." in prompt
    assert "Your ranking is symbol-level and must cover every candidate symbol exactly once" in prompt