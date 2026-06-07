"""Signal Service — 跨资产指标计算（多基准 + VIX 环境）

纯函数模块（无 DB I/O），接收 pandas 数据帧并返回
``CrossAssetIndicators``。方便单元测试和独立调用。

支持的基准：
  SPY  — S&P 500 大盘市场
  QQQ  — Nasdaq-100 科技/成长
  IWM  — Russell 2000 小盘风险
  TLT  — 20+ 年期国债 ETF（利率敏感度）
  GLD  — 黄金 ETF（避险 / 通胀）
  HYG  — 高收益债 ETF（信用风险）
  XLE  — 能源板块 ETF（大宗商品通胀）
  IBIT — 比特币 ETF（加密情绪）

环境指标：
  ^VIX — CBOE 波动率指数（波动率环境 & 恐慌度）
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

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
VIX_LOOKBACK_DAYS = 60      # trading days for VIX percentile (60d avoids regime distortion)
FRESHNESS_MAX_LAG_DAYS = 2  # data older than this is treated as stale
OPTION_CONTRACT_SHARE_MULTIPLIER = 100.0
REGIME_FLIP_LOOKBACK_DAYS = 10
REGIME_TRANSITION_STREAK_DAYS = 3
GEX_NEUTRAL_THRESHOLD = 0.05

# Benchmark names that map to model fields
BENCHMARK_FIELD_MAP: dict[str, tuple[str, str]] = {
    "SPY": ("spy_beta", "index_correlation_20d"),
    "QQQ": ("qqq_beta", "qqq_correlation_20d"),
    "IWM": ("iwm_beta", "iwm_correlation_20d"),
    # TLT: correlation only (CAPM beta vs bonds is not meaningful)
    "TLT": (None, "tlt_correlation_20d"),
    # Non-equity benchmarks: correlation only
    "GLD": (None, "gld_correlation_20d"),
    "HYG": (None, "hyg_correlation_20d"),
    "XLE": (None, "xle_correlation_20d"),
    "IBIT": (None, "ibit_correlation_20d"),
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
    vix_percentile_60d: float = 0.0
    vix_correlation_20d: float = 0.0


@dataclass
class MultiBenchmarkResult:
    """Aggregated results from all benchmark computations."""
    benchmarks: dict[str, BetaResult] = field(default_factory=dict)
    vix: VixResult = field(default_factory=VixResult)


def _score_correlation_significance(benchmark_results: dict[str, BetaResult]) -> float:
    """Estimate confidence penalty from weak/insufficient correlations.

    Returns
    -------
    float
        0.0-1.0 multiplier; lower means weaker statistical support.
    """
    with_data = [r for r in benchmark_results.values() if r.overlap_days > 0]
    if not with_data:
        return 0.0

    weak = 0
    for res in with_data:
        if res.overlap_days < CORR_WINDOW or abs(res.correlation_20d) < 0.2:
            weak += 1

    weak_ratio = weak / len(with_data)
    if weak_ratio >= 0.7:
        return 0.3
    if weak_ratio >= 0.4:
        return 0.6
    return 1.0


def _score_data_freshness(
    trading_date: date | None,
    bars_df: pd.DataFrame,
    benchmark_returns: dict[str, pd.Series],
    vix_bars: pd.DataFrame,
) -> float:
    """Score freshness (0-1) from lag between trading_date and latest source data."""
    if trading_date is None:
        return 1.0

    latest_dates: list[date] = []

    if not bars_df.empty and "timestamp" in bars_df.columns:
        ts = pd.to_datetime(bars_df["timestamp"], errors="coerce").dropna()
        if not ts.empty:
            latest_dates.append(ts.max().date())

    for series in benchmark_returns.values():
        if series.empty:
            continue
        idx = pd.to_datetime(series.index, errors="coerce")
        if len(idx) > 0:
            latest_dates.append(idx.max().date())

    if not vix_bars.empty and "timestamp" in vix_bars.columns:
        vix_ts = pd.to_datetime(vix_bars["timestamp"], errors="coerce").dropna()
        if not vix_ts.empty:
            latest_dates.append(vix_ts.max().date())

    if not latest_dates:
        return 0.0

    max_lag = max((trading_date - d).days for d in latest_dates)
    if max_lag <= 0:
        return 1.0
    if max_lag <= FRESHNESS_MAX_LAG_DAYS:
        return 0.7
    return 0.3


def _compute_option_vs_stock_volume_ratio(
    *,
    total_volume: int,
    total_option_volume: float,
) -> float:
    """Compute share-equivalent option activity relative to stock turnover.

    The input option volume is contract count, while stock volume is share count.
    Convert contracts to share-equivalent turnover via the standard 100-share
    contract multiplier before comparing the two.
    """
    stock_shares = max(float(total_volume), 1.0)
    option_share_equivalent = max(float(total_option_volume), 0.0) * OPTION_CONTRACT_SHARE_MULTIPLIER
    return option_share_equivalent / stock_shares


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
        ``vix_level``, ``vix_percentile_60d``, ``vix_correlation_20d``.
    """
    if vix_bars.empty:
        return VixResult()

    vix_close = vix_bars.set_index("timestamp")["close"].sort_index()

    # ── VIX level (latest close) ───────────────────────────
    vix_level = float(vix_close.iloc[-1])

    # ── VIX 60-day percentile ──────────────────────────────
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
        vix_percentile_60d=round(vix_pct, 4),
        vix_correlation_20d=round(vix_corr, 4),
    )


# ═══════════════════════════════════════════════════════════
# IV-stock correlation (unchanged)
# ═══════════════════════════════════════════════════════════

def compute_iv_stock_correlation(
    bars_df: pd.DataFrame,
    iv_history: pd.Series,
    *,
    window: int = CORR_WINDOW,
) -> float:
    """Fixed-window Pearson correlation between daily stock returns and IV changes.

    Uses the last ``window`` overlapping daily observations between stock returns
    and aggregated daily IV changes. Returns 0.0 when overlap is insufficient.
    """
    if bars_df.empty or "timestamp" not in bars_df.columns or iv_history.empty:
        return 0.0

    stock_returns = (
        bars_df.assign(timestamp=pd.to_datetime(bars_df["timestamp"], errors="coerce"))
        .dropna(subset=["timestamp"])
        .set_index("timestamp")["close"]
        .sort_index()
        .pct_change()
        .dropna()
    )

    iv_series = pd.Series(iv_history, copy=False)
    iv_series.index = pd.to_datetime(iv_series.index, errors="coerce")
    iv_series = iv_series[iv_series.index.notna()].sort_index()
    iv_series = iv_series[iv_series > 0].dropna()
    iv_changes = iv_series.pct_change().dropna()

    if stock_returns.empty or len(iv_changes) < window:
        return 0.0

    merged = pd.DataFrame({
        "ret": stock_returns,
        "iv": iv_changes,
    }).dropna()

    if len(merged) < window:
        return 0.0

    window_slice = merged.tail(window)
    if window_slice["ret"].std() == 0 or window_slice["iv"].std() == 0:
        return 0.0

    return float(window_slice["ret"].corr(window_slice["iv"]))


def _classify_iv_correlation_regime(iv_corr: float) -> str:
    """Map rolling IV-stock correlation to the Cross-Asset regime labels."""
    if iv_corr <= -0.45:
        return "fear"
    if iv_corr >= 0.25:
        return "bullish_vol"
    if -0.25 <= iv_corr <= 0.25:
        return "decoupled"
    return "normal"


def _rolling_iv_regime_series(
    bars_df: pd.DataFrame,
    iv_history: pd.Series,
    *,
    window: int = CORR_WINDOW,
) -> pd.Series:
    """Build a trailing regime-label series from rolling IV-stock correlation windows."""
    if bars_df.empty or "timestamp" not in bars_df.columns or iv_history.empty:
        return pd.Series(dtype="object")

    stock_returns = (
        bars_df.assign(timestamp=pd.to_datetime(bars_df["timestamp"], errors="coerce"))
        .dropna(subset=["timestamp"])
        .set_index("timestamp")["close"]
        .sort_index()
        .pct_change()
        .dropna()
    )

    iv_series = pd.Series(iv_history, copy=False)
    iv_series.index = pd.to_datetime(iv_series.index, errors="coerce")
    iv_series = iv_series[iv_series.index.notna()].sort_index()
    iv_series = iv_series[iv_series > 0].dropna()
    iv_changes = iv_series.pct_change().dropna()

    if stock_returns.empty or len(iv_changes) < window:
        return pd.Series(dtype="object")

    merged = pd.DataFrame({
        "ret": stock_returns,
        "iv": iv_changes,
    }).dropna()

    if len(merged) < window:
        return pd.Series(dtype="object")

    regimes: list[str] = []
    regime_index: list[pd.Timestamp] = []
    for end in range(window, len(merged) + 1):
        window_slice = merged.iloc[end - window:end]
        if window_slice["ret"].std() == 0 or window_slice["iv"].std() == 0:
            regimes.append("normal")
        else:
            raw_corr = window_slice["ret"].corr(window_slice["iv"])
            corr = float(raw_corr) if pd.notna(raw_corr) else 0.0
            regimes.append(_classify_iv_correlation_regime(corr))
        regime_index.append(merged.index[end - 1])

    return pd.Series(regimes, index=regime_index, dtype="object")


def _derive_regime_history_metrics(
    regimes: pd.Series,
    *,
    lookback: int = REGIME_FLIP_LOOKBACK_DAYS,
) -> tuple[int | None, bool, int]:
    """Derive current streak length, transition state, and recent flip count."""
    cleaned = regimes.dropna()
    if cleaned.empty:
        return None, False, 0

    current = cleaned.iloc[-1]
    streak = 1
    for pos in range(len(cleaned) - 2, -1, -1):
        if cleaned.iloc[pos] != current:
            break
        streak += 1

    recent = cleaned.tail(lookback)
    flip_count = 0
    for pos in range(1, len(recent)):
        if recent.iloc[pos] != recent.iloc[pos - 1]:
            flip_count += 1

    regime_transition = flip_count > 0 and streak < REGIME_TRANSITION_STREAK_DAYS
    return streak, regime_transition, flip_count


def _derive_market_shock_context(
    benchmark_returns: dict[str, pd.Series],
) -> tuple[float, str | None]:
    """Pick the latest largest absolute market-proxy move across SPY/QQQ/IWM."""
    candidates: list[tuple[float, float, str]] = []
    for name in ("SPY", "QQQ", "IWM"):
        series = benchmark_returns.get(name)
        if series is None:
            continue

        clean = pd.Series(series, copy=False).dropna()
        if clean.empty:
            continue

        last_move = float(clean.iloc[-1])
        candidates.append((abs(last_move), last_move, name))

    if not candidates:
        return 0.0, None

    _, move, source = max(candidates, key=lambda item: item[0])
    return round(move, 4), source


def _classify_gex_regime(
    gamma_exposure: float | None,
    *,
    threshold: float = GEX_NEUTRAL_THRESHOLD,
) -> str:
    """Classify normalized gamma exposure into a coarse GEX regime."""
    if gamma_exposure is None or pd.isna(gamma_exposure):
        return "neutral"

    gamma_value = float(gamma_exposure)
    if gamma_value >= threshold:
        return "positive"
    if gamma_value <= -threshold:
        return "negative"
    return "neutral"


# ═══════════════════════════════════════════════════════════
# Public entry-point
# ═══════════════════════════════════════════════════════════

def build_cross_asset_indicators(
    *,
    symbol: str,
    bars_df: pd.DataFrame,
    bar_returns: pd.Series,
    iv_history: pd.Series,
    benchmark_returns: dict[str, pd.Series],
    vix_bars: pd.DataFrame,
    total_volume: int,
    total_option_volume: float,
    hedge_ratio: float,
    gamma_exposure: float | None = None,
    gamma_peak_strike: float | None = None,
    trading_date: date | None = None,
    earnings_date: date | None = None,
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
    iv_corr = compute_iv_stock_correlation(bars_df, iv_history)
    option_vs_stock = _compute_option_vs_stock_volume_ratio(
        total_volume=total_volume,
        total_option_volume=total_option_volume,
    )

    # ── Multi-benchmark beta & correlation ─────────────────
    benchmark_results: dict[str, BetaResult] = {}
    for bench_name, (beta_field, corr_field) in BENCHMARK_FIELD_MAP.items():
        # Skip self-benchmark to avoid identity correlation (e.g. IBIT vs IBIT = 1.0)
        if symbol == bench_name:
            benchmark_results[bench_name] = BetaResult()
            continue

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
    correlation_significance = _score_correlation_significance(benchmark_results)
    data_freshness = _score_data_freshness(
        trading_date=trading_date,
        bars_df=bars_df,
        benchmark_returns=benchmark_returns,
        vix_bars=vix_bars,
    )
    cap = min(correlation_significance, data_freshness)

    confidence = {
        "corr_quality": round(min(1.0, len(bar_returns) / 100) * cap, 4),
        "volume_quality": round((1.0 if total_volume > 0 else 0.0) * data_freshness, 4),
        "beta_quality": round(min(1.0, spy_res.overlap_days / BETA_WINDOW) * cap, 4),
        "multi_benchmark_quality": round(
            min(1.0, total_bench_overlap / max(max_possible_overlap, 1)) * cap,
            4,
        ),
        "vix_quality": round((1.0 if vix_result.vix_level > 0 else 0.0) * data_freshness, 4),
        "correlation_significance": round(correlation_significance, 4),
        "data_freshness": round(data_freshness, 4),
    }

    regime_series = _rolling_iv_regime_series(bars_df, iv_history)
    if correlation_significance >= 0.5:
        regime_days, regime_transition, regime_flip_count_10d = _derive_regime_history_metrics(regime_series)
    else:
        regime_days, regime_transition, regime_flip_count_10d = None, False, 0

    market_shock_return_1d, market_shock_source = _derive_market_shock_context(benchmark_returns)
    gex_regime = _classify_gex_regime(gamma_exposure)

    # ── Earnings proximity ──────────────────────────────────
    earnings_proximity: int | None = None
    if earnings_date is not None and trading_date is not None:
        delta_days = (earnings_date - trading_date).days
        earnings_proximity = delta_days if delta_days >= 0 else None

    # ── Assemble ───────────────────────────────────────────
    spy = benchmark_results.get("SPY", BetaResult())
    qqq = benchmark_results.get("QQQ", BetaResult())
    iwm = benchmark_results.get("IWM", BetaResult())
    tlt = benchmark_results.get("TLT", BetaResult())
    gld = benchmark_results.get("GLD", BetaResult())
    hyg = benchmark_results.get("HYG", BetaResult())
    xle = benchmark_results.get("XLE", BetaResult())
    ibit = benchmark_results.get("IBIT", BetaResult())

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
        gld_correlation_20d=round(gld.correlation_20d, 4),
        hyg_correlation_20d=round(hyg.correlation_20d, 4),
        xle_correlation_20d=round(xle.correlation_20d, 4),
        ibit_correlation_20d=round(ibit.correlation_20d, 4),

        # VIX environment
        vix_level=vix_result.vix_level,
        vix_percentile_60d=vix_result.vix_percentile_60d,
        vix_correlation_20d=vix_result.vix_correlation_20d,

        # Earnings
        earnings_proximity_days=earnings_proximity,
        regime_days=regime_days,
        regime_transition=regime_transition,
        regime_flip_count_10d=regime_flip_count_10d,
        market_shock_return_1d=market_shock_return_1d,
        market_shock_source=market_shock_source,
        gex_regime=gex_regime,

        confidence_scores=confidence,
    )
