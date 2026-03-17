"""Signal feature chunking and blueprint merging for parallel LLM calls.

When the watchlist is large, sending all signal features in a single LLM
request degrades output quality.  This module splits signals into smaller
chunks (each containing benchmark symbols for market context), enables
parallel LLM calls, and merges the per-chunk blueprints into one
coherent ``LLMTradingBlueprint``.
"""
from __future__ import annotations

from shared.models.blueprint import LLMTradingBlueprint, SymbolPlan
from shared.models.signal import SignalFeatures
from shared.utils import get_logger

logger = get_logger("llm_chunker")


# ---------------------------------------------------------------------------
# Splitting
# ---------------------------------------------------------------------------


def split_signal_features(
    features: list[SignalFeatures],
    chunk_size: int,
    benchmark_symbols: list[str],
) -> list[list[SignalFeatures]]:
    """Split signal features into chunks, each containing benchmark symbols.

    Parameters
    ----------
    features:
        Full list of signal features for all symbols.
    chunk_size:
        Maximum number of *non-benchmark* symbols per chunk.
    benchmark_symbols:
        Symbols (e.g. ``["SPY", "QQQ"]``) that are injected into every
        chunk to provide market context for the LLM.

    Returns
    -------
    list[list[SignalFeatures]]
        A list of chunks.  If the total non-benchmark symbol count is
        ``<= chunk_size``, a single chunk is returned (no splitting).
    """
    benchmark_set = {s.upper() for s in benchmark_symbols}

    benchmarks: list[SignalFeatures] = []
    non_benchmarks: list[SignalFeatures] = []
    for sf in features:
        if sf.symbol.upper() in benchmark_set:
            benchmarks.append(sf)
        else:
            non_benchmarks.append(sf)

    # No splitting needed — fits in one chunk
    if len(non_benchmarks) <= chunk_size:
        logger.debug(
            "chunker.no_split_needed",
            total=len(features),
            non_benchmark=len(non_benchmarks),
            chunk_size=chunk_size,
        )
        return [features]

    # Build chunks: each = benchmarks + slice of non-benchmarks
    chunks: list[list[SignalFeatures]] = []
    for i in range(0, len(non_benchmarks), chunk_size):
        chunk = list(benchmarks) + non_benchmarks[i : i + chunk_size]
        chunks.append(chunk)

    logger.info(
        "chunker.split",
        total_symbols=len(features),
        benchmark_count=len(benchmarks),
        non_benchmark_count=len(non_benchmarks),
        chunk_size=chunk_size,
        num_chunks=len(chunks),
    )
    return chunks


# ---------------------------------------------------------------------------
# Merging
# ---------------------------------------------------------------------------


def merge_blueprints(blueprints: list[LLMTradingBlueprint]) -> LLMTradingBlueprint:
    """Merge per-chunk blueprints into a single coherent blueprint.

    Strategy:
    - ``symbol_plans``: concatenate all, deduplicate by ``underlying``
      keeping the plan with highest ``confidence``.
    - ``market_regime`` / ``market_analysis``: take from first chunk
      (which always contains benchmark symbols).
    - Global risk fields: take the most conservative (min) value across
      all chunks to prevent aggregate risk overflow.
    - Metadata: preserve ``trading_date``, take latest ``generated_at``,
      use first chunk's provider info.
    """
    if len(blueprints) == 1:
        return blueprints[0]

    base = blueprints[0]

    # ── Collect & deduplicate symbol plans ──
    best_plans: dict[str, SymbolPlan] = {}
    for bp in blueprints:
        for plan in bp.symbol_plans:
            key = plan.underlying.upper()
            existing = best_plans.get(key)
            if existing is None or plan.confidence > existing.confidence:
                best_plans[key] = plan

    merged_plans = list(best_plans.values())

    # ── Conservative global risk limits ──
    max_total_positions = min(bp.max_total_positions for bp in blueprints)
    max_daily_loss = min(bp.max_daily_loss for bp in blueprints)
    max_margin_usage = min(bp.max_margin_usage for bp in blueprints)
    portfolio_delta_limit = min(bp.portfolio_delta_limit for bp in blueprints)
    portfolio_gamma_limit = min(bp.portfolio_gamma_limit for bp in blueprints)

    # ── Latest generated_at ──
    latest_generated = max(bp.generated_at for bp in blueprints)

    merged = LLMTradingBlueprint(
        id=base.id,
        trading_date=base.trading_date,
        generated_at=latest_generated,
        model_provider=base.model_provider,
        model_version=base.model_version,
        market_regime=base.market_regime,
        market_analysis=base.market_analysis,
        symbol_plans=merged_plans,
        max_total_positions=max_total_positions,
        max_daily_loss=max_daily_loss,
        max_margin_usage=max_margin_usage,
        portfolio_delta_limit=portfolio_delta_limit,
        portfolio_gamma_limit=portfolio_gamma_limit,
        status=base.status,
    )

    logger.info(
        "chunker.merged",
        chunks=len(blueprints),
        total_plans=len(merged_plans),
        deduped_from=sum(len(bp.symbol_plans) for bp in blueprints),
    )
    return merged
