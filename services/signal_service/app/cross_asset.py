"""Signal Service — 跨资产指标计算（多基准 + VIX 环境）

纯函数模块（无 DB I/O），接收 pandas 数据帧并返回
``CrossAssetIndicators``。方便单元测试和独立调用。

支持的基准：
  SPY  — S&P 500 大盘市场
  QQQ  — Nasdaq-100 科技/成长
  IWM  — Russell 2000 小盘风险
  TLT  — 20+ 年期国债 ETF（利率敏感度）

环境指标：
  ^VIX — CBOE 波动率指数（波动率环境 & 恐慌度）
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from shared.models.signal import CrossAssetIndicators
from shared.utils import get_logger

logger = get_logger("signal_cross_asset")

# ── Tuning constants ───────────────────────────────────────
BETA_WINDOW = 60            # trailing days for beta calculation
BETA_MIN_OVERLAP = 30       # minimum aligned days required for beta
CORR_WINDOW = 20            # trailing days for rolling correlation
IV_CORR_MIN_SAMPLES = 10    # minimum samples for IV-return correlation
VIX_LOOKBACK_WEEKS = 52     # weeks for VIX percentile (≈252 trading days)
VIX_LOOKBACK_DAYS = 252     # trading days for VIX percentile

# Benchmark names that map to model fields
BENCHMARK_FIELD_MAP: dict[str, tuple[str, str]] = {
    "SPY": ("spy_beta", "index_correlation_20d"),
    "QQQ": ("qqq_beta", "qqq_correlation_20d"),
    "IWM": ("iwm_beta", "iwm_correlation_20d"),
    # TLT: correlation only (CAPM beta vs bonds is not meaningful)
    "TLT": (None, "tlt_correlation_20d"),
}


@dataclass(slots=True)
class BetaResult:
    """Intermediate result from benchmark beta / correlation computation."""
    beta: float = 0.0
    correlation_20d: float = 0.0
    overlap_days: int = 0


@dataclass(slots=True)
class VixResult:
    """VIX environment indicators."""
    vix_level: float = 0.0
    vix_percentile_52w: float = 0.0
    vix_correlation_20d: float = 0.0


@dataclass
class MultiBenchmarkResult:
    """Aggregated results from all benchmark computations."""
    benchmarks: dict[str, BetaResult] = field(default_factory=dict)
    vix: VixResult = field(default_factory=VixResult)


# ═══════════════════════════════════════════════════════════
# Benchmark beta & correlation
# ═══════════════════════════════════════════════════════════

def compute_benchmark_beta(
    symbol: str,
    bars_df: pd.DataFrame,
    benchmark_returns: pd.Series,
    benchmark_name: str,
    *,
    compute_beta: bool = True,
) -> BetaResult:
    """Compute CAPM beta (60 d) and 20-day correlation against a benchmark.

    Parameters
    ----------
    symbol : str
        Ticker being evaluated.
    bars_df : DataFrame
        OHLCV bars with a ``timestamp`` column used as join key.
    benchmark_returns : Series
        Date-indexed benchmark daily returns (``close.pct_change().dropna()``).
    benchmark_name : str
        Name of the benchmark (e.g. ``"SPY"``, ``"QQQ"``).
        If *symbol == benchmark_name*, returns identity (1.0/1.0).
    compute_beta : bool
        If False, skip beta calculation (useful for TLT where CAPM beta
        against bonds is not meaningful — only correlation is computed).
    """
    # Identity short-circuit — avoids sampling noise when symbol IS the benchmark
    if symbol == benchmark_name:
        return BetaResult(
            beta=1.0 if compute_beta else 0.0,
            correlation_20d=1.0,
            overlap_days=max(len(bars_df) - 1, 0),
        )

    if benchmark_returns.empty or bars_df.empty:
        return BetaResult()

    sym_returns = (
        bars_df.set_index("timestamp")["close"]
        .pct_change()
        .dropna()
    )
    aligned = pd.DataFrame({
        "sym": sym_returns,
        "bench": benchmark_returns,
    }).dropna()

    overlap = len(aligned)
    if overlap < CORR_WINDOW:
        return BetaResult(overlap_days=overlap)

    result = BetaResult(overlap_days=overlap)

    # ── Beta (60 d) ────────────────────────────────────────
    if compute_beta and overlap >= BETA_MIN_OVERLAP:
        beta_slice = aligned.tail(BETA_WINDOW)
        bench_var = beta_slice["bench"].var()
        if bench_var > 0:
            raw = beta_slice["sym"].cov(beta_slice["bench"]) / bench_var
            result.beta = float(raw) if pd.notna(raw) else 0.0

    # ── 20-day correlation ─────────────────────────────────
    corr_slice = aligned.tail(CORR_WINDOW)
    if corr_slice["bench"].std() > 0 and corr_slice["sym"].std() > 0:
        raw = corr_slice["sym"].corr(corr_slice["bench"])
        result.correlation_20d = float(raw) if pd.notna(raw) else 0.0

    return result


# ═══════════════════════════════════════════════════════════
# VIX environment
# ═══════════════════════════════════════════════════════════

def compute_vix_environment(
    vix_bars: pd.DataFrame,
    bars_df: pd.DataFrame,
) -> VixResult:
    """Compute VIX-based environment indicators.

    Parameters
    ----------
    vix_bars : DataFrame
        ^VIX OHLCV bars with ``timestamp`` and ``close`` columns.
    bars_df : DataFrame
        Target symbol's OHLCV bars for correlation computation.

    Returns
    -------
    VixResult
        ``vix_level``, ``vix_percentile_52w``, ``vix_correlation_20d``.
    """
    if vix_bars.empty:
        return VixResult()

    vix_close = vix_bars.set_index("timestamp")["close"].sort_index()

    # ── VIX level (latest close) ───────────────────────────
    vix_level = float(vix_close.iloc[-1])

    # ── VIX 52-week percentile ─────────────────────────────
    lookback = vix_close.tail(VIX_LOOKBACK_DAYS)
    if len(lookback) >= 20:  # need at least 20 days for meaningful percentile
        vix_pct = float(np.sum(lookback < vix_level) / len(lookback))
    else:
        vix_pct = 0.0

    # ── 20-day correlation: symbol returns vs VIX changes ──
    vix_corr = 0.0
    if not bars_df.empty:
        sym_returns = (
            bars_df.set_index("timestamp")["close"]
            .pct_change()
            .dropna()
        )
        vix_changes = vix_close.pct_change().dropna()
        aligned = pd.DataFrame({
            "sym": sym_returns,
            "vix": vix_changes,
        }).dropna()

        corr_slice = aligned.tail(CORR_WINDOW)
        if (
            len(corr_slice) >= CORR_WINDOW
            and corr_slice["sym"].std() > 0
            and corr_slice["vix"].std() > 0
        ):
            raw = corr_slice["sym"].corr(corr_slice["vix"])
            vix_corr = float(raw) if pd.notna(raw) else 0.0

    return VixResult(
        vix_level=round(vix_level, 2),
        vix_percentile_52w=round(vix_pct, 4),
        vix_correlation_20d=round(vix_corr, 4),
    )


# ═══════════════════════════════════════════════════════════
# IV-stock correlation (unchanged)
# ═══════════════════════════════════════════════════════════

def compute_iv_stock_correlation(
    bar_returns: pd.Series,
    option_df: pd.DataFrame,
) -> float:
    """Correlation between daily stock returns and aggregated IV changes.

    Returns 0.0 when data is insufficient.
    """
    if option_df.empty or "timestamp" not in option_df.columns:
        return 0.0

    avg_iv = (
        option_df[option_df["iv"] > 0]
        .groupby("timestamp")["iv"]
        .mean()
        .sort_index()
    )
    iv_changes = avg_iv.pct_change().dropna()

    if len(bar_returns) <= IV_CORR_MIN_SAMPLES or len(iv_changes) <= IV_CORR_MIN_SAMPLES:
        return 0.0

    sample_size = min(len(bar_returns), len(iv_changes))
    merged = pd.DataFrame({
        "ret": bar_returns.tail(sample_size).reset_index(drop=True),
        "iv": iv_changes.tail(sample_size).reset_index(drop=True),
    })
    if len(merged) <= 5:
        return 0.0
    if merged["ret"].std() == 0 or merged["iv"].std() == 0:
        return 0.0

    return float(merged["ret"].corr(merged["iv"]))


# ═══════════════════════════════════════════════════════════
# Public entry-point
# ═══════════════════════════════════════════════════════════

def build_cross_asset_indicators(
    *,
    symbol: str,
    bars_df: pd.DataFrame,
    bar_returns: pd.Series,
    option_df: pd.DataFrame,
    benchmark_returns: dict[str, pd.Series],
    vix_bars: pd.DataFrame,
    total_volume: int,
    total_option_volume: float,
    hedge_ratio: float,
) -> CrossAssetIndicators:
    """Assemble the full ``CrossAssetIndicators`` object.

    This is the single entry-point that ``tasks.py`` calls — it
    orchestrates all sub-computations and returns a ready-to-use model.

    Parameters
    ----------
    benchmark_returns : dict[str, Series]
        Mapping of benchmark name → daily return Series.
        Expected keys: ``"SPY"``, ``"QQQ"``, ``"IWM"``, ``"TLT"``.
        Missing benchmarks are silently skipped.
    vix_bars : DataFrame
        ^VIX OHLCV bars. Empty DataFrame if VIX data is unavailable.
    """
    iv_corr = compute_iv_stock_correlation(bar_returns, option_df)
    option_vs_stock = total_option_volume / max(float(total_volume), 1.0)

    # ── Multi-benchmark beta & correlation ─────────────────
    benchmark_results: dict[str, BetaResult] = {}
    for bench_name, (beta_field, corr_field) in BENCHMARK_FIELD_MAP.items():
        returns = benchmark_returns.get(bench_name, pd.Series(dtype=float))
        if returns.empty:
            benchmark_results[bench_name] = BetaResult()
            continue

        benchmark_results[bench_name] = compute_benchmark_beta(
            symbol=symbol,
            bars_df=bars_df,
            benchmark_returns=returns,
            benchmark_name=bench_name,
            compute_beta=(beta_field is not None),
        )

    # ── VIX environment ────────────────────────────────────
    vix_result = compute_vix_environment(vix_bars, bars_df)

    # ── Confidence scoring ─────────────────────────────────
    spy_res = benchmark_results.get("SPY", BetaResult())
    total_bench_overlap = sum(r.overlap_days for r in benchmark_results.values())
    max_possible_overlap = BETA_WINDOW * len(BENCHMARK_FIELD_MAP)

    confidence = {
        "corr_quality": round(min(1.0, len(bar_returns) / 100), 4),
        "volume_quality": 1.0 if total_volume > 0 else 0.0,
        "beta_quality": round(min(1.0, spy_res.overlap_days / BETA_WINDOW), 4),
        "multi_benchmark_quality": round(
            min(1.0, total_bench_overlap / max(max_possible_overlap, 1)),
            4,
        ),
        "vix_quality": 1.0 if vix_result.vix_level > 0 else 0.0,
    }

    # ── Assemble ───────────────────────────────────────────
    spy = benchmark_results.get("SPY", BetaResult())
    qqq = benchmark_results.get("QQQ", BetaResult())
    iwm = benchmark_results.get("IWM", BetaResult())
    tlt = benchmark_results.get("TLT", BetaResult())

    return CrossAssetIndicators(
        stock_iv_correlation=round(iv_corr, 6),
        option_vs_stock_volume_ratio=round(option_vs_stock, 6),
        delta_adjusted_hedge_ratio=round(hedge_ratio, 4),

        # Multi-benchmark
        spy_beta=round(spy.beta, 4),
        index_correlation_20d=round(spy.correlation_20d, 4),
        qqq_beta=round(qqq.beta, 4),
        qqq_correlation_20d=round(qqq.correlation_20d, 4),
        iwm_beta=round(iwm.beta, 4),
        iwm_correlation_20d=round(iwm.correlation_20d, 4),
        tlt_correlation_20d=round(tlt.correlation_20d, 4),

        # VIX environment
        vix_level=vix_result.vix_level,
        vix_percentile_52w=vix_result.vix_percentile_52w,
        vix_correlation_20d=vix_result.vix_correlation_20d,

        confidence_scores=confidence,
    )
