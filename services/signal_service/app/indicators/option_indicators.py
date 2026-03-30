"""期权指标计算 — 修复设计文档中的所有问题"""
from __future__ import annotations

import math
from datetime import date, timedelta

import numpy as np
import pandas as pd
from scipy import stats
from sqlalchemy import text

from shared.db.session import get_timescale_session
from shared.models.signal import OptionIndicators
from shared.utils import get_logger, today_trading

from .cal_utils import sanitize_float as _sanitize_float

logger = get_logger("option_indicators")

# ── Named constants (extracted from inline magic numbers) ──
DELTA_TARGET_25 = 0.25          # 25-delta 定位目标
MONEYNESS_OTM_PUT = 0.95       # Fallback: OTM put moneyness 阈值
MONEYNESS_OTM_CALL = 1.05      # Fallback: OTM call moneyness 阈值
MIN_IV_SKEW_DELTA = 0.01       # IV skew 中排除 delta 过小的合约
MIN_POLY_FIT_POINTS = 5        # Vol surface 多项式拟合最少数据点


async def get_historical_iv(symbol: str, lookback_days: int | None = None) -> list[float]:
    """从 TimescaleDB 获取历史 IV 数据。

    lookback_days 默认使用 config 中的 signal_service.iv_lookback_days（252 交易日）。

    查询优先级：
    1. ``option_iv_daily`` — 预聚合的每日 IV 汇总（最可靠）
    2. ``option_daily``    — 每日期权快照的 IV 均值
    3. ``option_5min_snapshots`` — 盘中快照兜底
    """
    if lookback_days is None:
        from shared.config import get_settings
        lookback_days = get_settings().signal_service.iv_lookback_days

    start_date = today_trading() - timedelta(days=lookback_days)

    # ── 1) Prefer pre-aggregated IV daily summary ──
    async with get_timescale_session() as session:
        result = await session.execute(
            text(
                "SELECT avg_iv "
                "FROM option_iv_daily "
                "WHERE underlying = :symbol "
                "AND trading_date >= :start_date "
                "AND avg_iv > 0 AND avg_iv < 5 "
                "ORDER BY trading_date"
            ),
            {"symbol": symbol, "start_date": start_date},
        )
        iv_daily_rows = [float(row[0]) for row in result.fetchall() if row[0]]

    if iv_daily_rows:
        return iv_daily_rows

    # ── 2) Fallback: option_daily per-contract snapshots ──
    logger.debug("get_historical_iv.fallback_option_daily", symbol=symbol)
    async with get_timescale_session() as session:
        result = await session.execute(
            text(
                "SELECT AVG(iv) as avg_iv "
                "FROM option_daily "
                "WHERE underlying = :symbol "
                "AND snapshot_date >= :start_date "
                "AND iv > 0 AND iv < 5 "
                "GROUP BY snapshot_date "
                "ORDER BY snapshot_date"
            ),
            {"symbol": symbol, "start_date": start_date},
        )
        daily_rows = [float(row[0]) for row in result.fetchall() if row[0]]

    if daily_rows:
        return daily_rows

    # ── 3) Fallback: intraday 5-min snapshots ──
    logger.debug("get_historical_iv.fallback_intraday", symbol=symbol)
    async with get_timescale_session() as session:
        result = await session.execute(
            text(
                "SELECT AVG(iv) as avg_iv "
                "FROM option_5min_snapshots "
                "WHERE underlying = :symbol "
                "AND timestamp::date >= :start_date "
                "AND iv > 0 AND iv < 5 "
                "GROUP BY timestamp::date "
                "ORDER BY timestamp::date"
            ),
            {"symbol": symbol, "start_date": start_date},
        )
        return [float(row[0]) for row in result.fetchall() if row[0]]


def _sanitize_option_indicators(ind: OptionIndicators) -> OptionIndicators:
    """Sanitize all float fields — replace NaN/Inf with 0.0."""
    updates: dict = {}
    for name, field_info in OptionIndicators.model_fields.items():
        val = getattr(ind, name)
        if isinstance(val, float):
            clean = _sanitize_float(val)
            if clean != val or (isinstance(val, float) and math.isnan(val)):
                updates[name] = clean
        elif isinstance(val, dict):
            cleaned = {k: (_sanitize_float(v) if isinstance(v, float) else v) for k, v in val.items()}
            if cleaned != val:
                updates[name] = cleaned
    return ind.model_copy(update=updates) if updates else ind


async def calculate_iv_rank(symbol: str, current_iv: float, lookback_days: int | None = None, historical_iv: list[float] | None = None) -> float:
    """
    计算 IV Rank（百分位排名）
    修复：使用 scipy.stats.percentileofscore 而非错误的 np.percentile
    """
    if historical_iv is None:
        historical_iv = await get_historical_iv(symbol, lookback_days)

    if not historical_iv or len(historical_iv) < 5:
        logger.warning("iv_rank.insufficient_data", symbol=symbol, data_points=len(historical_iv))
        return 50.0  # 数据不足时返回中性值

    # percentileofscore: 返回 current_iv 在历史分布中的百分位 (0-100)
    rank = stats.percentileofscore(historical_iv, current_iv, kind="rank")
    return round(rank, 2)


def calculate_pcr(option_data: pd.DataFrame) -> tuple[float, float]:
    """
    计算 Put/Call Ratio
    修复：添加除零保护
    """
    calls = option_data[option_data["option_type"] == "call"]
    puts = option_data[option_data["option_type"] == "put"]

    call_volume = calls["volume"].sum()
    put_volume = puts["volume"].sum()
    call_oi = calls["open_interest"].sum()
    put_oi = puts["open_interest"].sum()

    pcr_volume = put_volume / call_volume if call_volume > 0 else 0.0
    pcr_oi = put_oi / call_oi if call_oi > 0 else 0.0

    return round(pcr_volume, 4), round(pcr_oi, 4)


def calculate_iv_skew(option_data: pd.DataFrame, underlying_price: float) -> float:
    """
    计算 IV 偏斜：25-delta put IV - 25-delta call IV（仅使用最近到期日）

    优先使用 delta 列定位 25-delta 合约；delta 数据不可用时回退到
    moneyness 近似（OTM 5%）。
    """
    if option_data.empty or underlying_price <= 0:
        return 0.0

    # Use only the nearest expiry to avoid dilution from far-month contracts
    nearest_expiry = option_data["expiry"].min()
    near = option_data[option_data["expiry"] == nearest_expiry]

    # ── Prefer 25-delta approach when delta data is available ──
    if "delta" in near.columns:
        puts_with_delta = near[
            (near["option_type"] == "put") & (near["delta"] < -MIN_IV_SKEW_DELTA) & (near["iv"] > 0)
        ]
        calls_with_delta = near[
            (near["option_type"] == "call") & (near["delta"] > MIN_IV_SKEW_DELTA) & (near["iv"] > 0)
        ]

        if not puts_with_delta.empty and not calls_with_delta.empty:
            # Find contracts closest to 25-delta
            put_25d = puts_with_delta.iloc[
                (puts_with_delta["delta"] + DELTA_TARGET_25).abs().argsort()[:1]
            ]
            call_25d = calls_with_delta.iloc[
                (calls_with_delta["delta"] - DELTA_TARGET_25).abs().argsort()[:1]
            ]

            if not put_25d.empty and not call_25d.empty:
                put_iv = float(put_25d.iloc[0]["iv"])
                call_iv = float(call_25d.iloc[0]["iv"])
                if put_iv > 0 and call_iv > 0:
                    return round(put_iv - call_iv, 4)

    # ── Fallback: moneyness-based (OTM 5%) ──
    otm_put_strike = underlying_price * MONEYNESS_OTM_PUT
    otm_call_strike = underlying_price * MONEYNESS_OTM_CALL

    puts = near[
        (near["option_type"] == "put") & (near["strike"] <= otm_put_strike) & (near["iv"] > 0)
    ]
    calls = near[
        (near["option_type"] == "call") & (near["strike"] >= otm_call_strike) & (near["iv"] > 0)
    ]

    if puts.empty or calls.empty:
        return 0.0

    put_iv = puts.loc[puts["strike"].idxmax(), "iv"]
    call_iv = calls.loc[calls["strike"].idxmin(), "iv"]

    return round(float(put_iv - call_iv), 4)


def calculate_term_structure(option_data: pd.DataFrame, underlying_price: float) -> dict[str, float]:
    """
    计算期限结构：各到期日ATM IV
    """
    if option_data.empty or underlying_price <= 0:
        return {}

    result = {}
    for expiry, group in option_data.groupby("expiry"):
        # 找最接近 ATM 的合约
        atm = group.iloc[(group["strike"] - underlying_price).abs().argsort()[:1]]
        if not atm.empty and atm.iloc[0]["iv"] > 0:
            result[str(expiry)] = round(float(atm.iloc[0]["iv"]), 4)

    return result


# ═══════════════════════════════════════════════════════════
# Private sub-functions — split from compute_option_indicators
# ═══════════════════════════════════════════════════════════


async def _compute_iv_metrics(
    symbol: str,
    option_data: pd.DataFrame,
    underlying_price: float,
    historical_iv: list[float],
) -> dict:
    """IV 相关分析指标：IV rank/percentile、skew、term structure、vol surface."""
    current_iv = (
        float(option_data[option_data["iv"] > 0]["iv"].mean())
        if not option_data[option_data["iv"] > 0].empty
        else 0.0
    )
    percentile = await calculate_iv_rank(symbol, current_iv, historical_iv=historical_iv)
    iv_skew = calculate_iv_skew(option_data, underlying_price)
    atm_iv = calculate_term_structure(option_data, underlying_price)
    historical_iv_30d = float(np.mean(historical_iv)) if historical_iv else 0.0

    # iv_rank: min-max normalization
    if historical_iv and current_iv > 0:
        iv_min = float(np.min(historical_iv))
        iv_max = float(np.max(historical_iv))
        iv_rank = ((current_iv - iv_min) / max(iv_max - iv_min, 1e-9)) * 100
        iv_rank = float(np.clip(iv_rank, 0.0, 100.0))
    else:
        iv_rank = 50.0

    # Term structure slope
    term_slope = 0.0
    if len(atm_iv) >= 2:
        sorted_expiries = sorted(atm_iv.keys())
        term_slope = atm_iv[sorted_expiries[-1]] - atm_iv[sorted_expiries[0]]

    # Vol surface fit error (quadratic on moneyness)
    fit_errors: list[float] = []
    df = option_data[option_data["strike"] > 0].copy()
    if not df.empty and underlying_price > 0:
        df["moneyness"] = df["strike"] / underlying_price
        for _, group in df.groupby("expiry"):
            valid = group[(group["iv"] > 0) & group["iv"].notna()]
            if len(valid) >= MIN_POLY_FIT_POINTS:
                x = valid["moneyness"].to_numpy(dtype=float)
                y = valid["iv"].to_numpy(dtype=float)
                coeff = np.polyfit(x, y, 2)
                y_hat = np.polyval(coeff, x)
                fit_errors.append(float(np.sqrt(np.mean((y - y_hat) ** 2))))
    vol_surface_fit_error = float(np.mean(fit_errors)) if fit_errors else 0.0

    return {
        "current_iv": round(current_iv, 4),
        "iv_rank": iv_rank,
        "iv_percentile": percentile,
        "historical_iv_30d": round(historical_iv_30d, 4),
        "iv_skew": iv_skew,
        "atm_iv": atm_iv,
        "term_structure_slope": round(term_slope, 4),
        "vol_surface_fit_error": round(vol_surface_fit_error, 6),
    }


def _compute_greek_aggregations(option_data: pd.DataFrame) -> dict:
    """OI 加权的 Greek 聚合：exposure、gamma peak、theta decay、vanna、charm."""
    weighted_oi = option_data["open_interest"].fillna(0).astype(float)
    total_oi = max(float(weighted_oi.sum()), 1.0)

    delta_exposure = float((option_data["delta"].fillna(0).astype(float) * weighted_oi).sum())
    gamma_exposure = float((option_data["gamma"].fillna(0).astype(float) * weighted_oi).sum())
    theta_exposure = float((option_data["theta"].fillna(0).astype(float) * weighted_oi).sum())
    vega_exposure = float((option_data["vega"].fillna(0).astype(float) * weighted_oi).sum())

    call_mask = option_data["option_type"] == "call"
    put_mask = option_data["option_type"] == "put"

    call_delta_exposure = float(
        (option_data.loc[call_mask, "delta"].fillna(0).astype(float)
         * option_data.loc[call_mask, "open_interest"].fillna(0).astype(float)).sum()
    )
    put_delta_exposure = float(
        (option_data.loc[put_mask, "delta"].fillna(0).astype(float)
         * option_data.loc[put_mask, "open_interest"].fillna(0).astype(float)).sum()
    )

    gamma_weighted = (option_data["gamma"].fillna(0).abs().astype(float) * weighted_oi).to_numpy()
    gamma_peak_strike = (
        float(option_data.iloc[int(np.argmax(gamma_weighted))]["strike"])
        if len(gamma_weighted) > 0 else 0.0
    )

    theta_decay_rate = float(
        (option_data["theta"].fillna(0).abs().astype(float) * weighted_oi).sum() / total_oi
    )

    # Vanna — BSM analytical from DB, fallback to crude proxy
    if "vanna" in option_data.columns and option_data["vanna"].notna().any() and (option_data["vanna"] != 0).any():
        vanna = float((option_data["vanna"].fillna(0).astype(float) * weighted_oi).sum() / total_oi)
    else:
        vanna = float(
            (option_data["delta"].fillna(0).astype(float)
             * option_data["iv"].fillna(0).astype(float) * weighted_oi).sum() / total_oi
        )

    # Charm — BSM analytical from DB, fallback to crude proxy
    if "charm" in option_data.columns and option_data["charm"].notna().any() and (option_data["charm"] != 0).any():
        charm = float((option_data["charm"].fillna(0).astype(float) * weighted_oi).sum() / total_oi)
    else:
        now_ts = pd.Timestamp.today()  # tz-naive; avoid tz-aware/tz-naive mismatch
        expiry_ts = pd.to_datetime(option_data["expiry"], errors="coerce")
        if expiry_ts.dt.tz is not None:
            expiry_ts = expiry_ts.dt.tz_convert(None)
        expiry_days = (expiry_ts - now_ts).dt.days.clip(lower=1)
        charm = float(
            (option_data["delta"].fillna(0).astype(float)
             / expiry_days.fillna(1).astype(float) * weighted_oi).sum() / total_oi
        )

    return {
        "delta_exposure_profile": {
            "total": round(delta_exposure, 4),
            "call": round(call_delta_exposure, 4),
            "put": round(put_delta_exposure, 4),
        },
        "portfolio_greeks": {
            "delta": round(delta_exposure, 4),
            "gamma": round(gamma_exposure, 4),
            "theta": round(theta_exposure, 4),
            "vega": round(vega_exposure, 4),
        },
        "gamma_peak_strike": round(gamma_peak_strike, 4),
        "theta_decay_rate": round(theta_decay_rate, 6),
        "vanna": round(vanna, 6),
        "charm": round(charm, 6),
        "total_oi": total_oi,
    }


def _compute_flow_metrics(option_data: pd.DataFrame) -> dict:
    """流动性与资金流指标：PCR、OI 集中度、bid-ask、volume imbalance."""
    pcr_volume, pcr_oi = calculate_pcr(option_data)

    # OI concentration (top 5 strikes / total per expiry)
    oi_concentrations = []
    for _, group in option_data.groupby("expiry"):
        oi_series = group.groupby("strike")["open_interest"].sum().sort_values(ascending=False)
        denom = max(float(oi_series.sum()), 1.0)
        oi_concentrations.append(float(oi_series.head(5).sum()) / denom)
    oi_concentration_top5 = float(np.mean(oi_concentrations)) if oi_concentrations else 0.0

    # Bid-ask spread ratio
    mid = ((option_data["bid"].fillna(0) + option_data["ask"].fillna(0)) / 2).replace(0, np.nan)
    spread_ratio = (
        (option_data["ask"].fillna(0) - option_data["bid"].fillna(0)) / mid
    ).replace([np.inf, -np.inf], np.nan)
    bid_ask_spread_ratio = float(spread_ratio.dropna().mean()) if not spread_ratio.dropna().empty else 0.0

    # Volume imbalance
    call_vol = float(option_data.loc[option_data["option_type"] == "call", "volume"].fillna(0).sum())
    put_vol = float(option_data.loc[option_data["option_type"] == "put", "volume"].fillna(0).sum())
    denom_vol = max(call_vol + put_vol, 1.0)
    option_volume_imbalance = (call_vol - put_vol) / denom_vol

    return {
        "pcr_volume": pcr_volume,
        "pcr_oi": pcr_oi,
        "oi_concentration_top5": round(oi_concentration_top5, 6),
        "bid_ask_spread_ratio": round(bid_ask_spread_ratio, 6),
        "option_volume_imbalance": round(option_volume_imbalance, 6),
    }


def _compute_strategy_metrics(
    option_data: pd.DataFrame,
    underlying_price: float,
    atm_iv: dict[str, float],
) -> dict:
    """策略类指标：vertical spread、butterfly、box spread、calendar spread.

    Uses tradeable-filtered contracts only — illiquid contracts produce
    meaningless bid/ask-based spread prices.
    """
    from services.signal_service.app.filters.option_filters import apply_trading_filter

    tradeable_data, _filter_result = apply_trading_filter(option_data, underlying_price)

    vertical_scores: list[float] = []
    calendar_scores: list[float] = []
    butterfly_errors: list[float] = []
    box_errors: list[float] = []

    for _expiry, group in tradeable_data.groupby("expiry"):
        calls = group[group["option_type"] == "call"].copy()
        puts = group[group["option_type"] == "put"].copy()
        calls["mid"] = (calls["bid"].fillna(0) + calls["ask"].fillna(0)) / 2
        puts["mid"] = (puts["bid"].fillna(0) + puts["ask"].fillna(0)) / 2

        # ── Vertical spread risk/reward ──
        sorted_calls = calls.sort_values("strike")
        for i in range(len(sorted_calls) - 1):
            low = sorted_calls.iloc[i]
            high = sorted_calls.iloc[i + 1]
            width = float(high["strike"] - low["strike"])
            debit = float(low["mid"] - high["mid"])
            max_loss = max(debit, 0.01)
            max_profit = max(width - debit, 0.0)
            vertical_scores.append(max_profit / max_loss)

        # ── Butterfly pricing error ──
        strikes = sorted(calls["strike"].unique())
        for i in range(1, len(strikes) - 1):
            k1, k2, k3 = strikes[i - 1], strikes[i], strikes[i + 1]
            if abs((k2 - k1) - (k3 - k2)) > 1e-9:
                continue
            c1 = calls.loc[calls["strike"] == k1, "mid"]
            c2 = calls.loc[calls["strike"] == k2, "mid"]
            c3 = calls.loc[calls["strike"] == k3, "mid"]
            if c1.empty or c2.empty or c3.empty:
                continue
            market = float(c1.iloc[0] - 2 * c2.iloc[0] + c3.iloc[0])
            butterfly_errors.append(abs(market))

        # ── Box spread arbitrage ──
        common_strikes = sorted(set(calls["strike"].tolist()) & set(puts["strike"].tolist()))
        if len(common_strikes) >= 2:
            k_low, k_high = common_strikes[0], common_strikes[-1]
            c_low = calls.loc[calls["strike"] == k_low, "mid"]
            c_high = calls.loc[calls["strike"] == k_high, "mid"]
            p_low = puts.loc[puts["strike"] == k_low, "mid"]
            p_high = puts.loc[puts["strike"] == k_high, "mid"]
            if not (c_low.empty or c_high.empty or p_low.empty or p_high.empty):
                box_price = float(c_low.iloc[0] - c_high.iloc[0] + p_high.iloc[0] - p_low.iloc[0])
                fair = float(k_high - k_low)
                box_errors.append((fair - box_price) / fair if fair != 0 else 0.0)

    # ── Calendar spread theta capture ──
    if len(atm_iv) >= 2 and not tradeable_data.empty:
        expiry_order = sorted(tradeable_data["expiry"].unique())
        if len(expiry_order) >= 2:
            near = tradeable_data[tradeable_data["expiry"] == expiry_order[0]]
            far = tradeable_data[tradeable_data["expiry"] == expiry_order[-1]]
            near_theta = float(near["theta"].fillna(0).abs().mean()) if not near.empty else 0.0
            far_theta = float(far["theta"].fillna(0).abs().mean()) if not far.empty else 0.0
            calendar_scores.append(max(near_theta - far_theta, 0.0))

    return {
        "vertical_spread_risk_reward": round(float(np.mean(vertical_scores)) if vertical_scores else 0.0, 6),
        "calendar_spread_theta_capture": round(float(np.mean(calendar_scores)) if calendar_scores else 0.0, 6),
        "butterfly_pricing_error": round(float(np.mean(butterfly_errors)) if butterfly_errors else 0.0, 6),
        "box_spread_arbitrage": round(float(np.mean(box_errors)) if box_errors else 0.0, 6),
    }


def _assess_confidence_and_flags(
    iv_metrics: dict,
    flow_metrics: dict,
    greek_metrics: dict,
    option_data: pd.DataFrame,
    historical_iv: list[float],
) -> dict:
    """置信度评分 + 极端值标记 + 降级指标列表."""
    iv_rank = iv_metrics["iv_rank"]
    bid_ask_spread_ratio = flow_metrics["bid_ask_spread_ratio"]
    option_volume_imbalance = flow_metrics["option_volume_imbalance"]
    total_oi = greek_metrics["total_oi"]

    confidence_scores = {
        "iv_regime": round(min(1.0, len(historical_iv) / 30.0), 4),
        "chain_liquidity": round(max(0.0, 1.0 - min(1.0, bid_ask_spread_ratio)), 4),
        "greeks_stability": round(min(1.0, total_oi / 100000.0), 4),
    }

    extreme_flags: list[str] = []
    degraded: list[str] = []

    if iv_rank > 90:
        extreme_flags.append("extreme_high_iv")
    if iv_rank < 10:
        extreme_flags.append("extreme_low_iv")
    if abs(option_volume_imbalance) > 0.6:
        extreme_flags.append("extreme_volume_imbalance")
    if bid_ask_spread_ratio > 0.2:
        extreme_flags.append("poor_liquidity")

    # Data sufficiency checks
    n_rows = len(option_data)
    n_expiries = option_data["expiry"].nunique() if "expiry" in option_data.columns else 0
    n_hist_iv = len(historical_iv)

    if n_hist_iv < 10:
        degraded.extend(["iv_rank", "iv_percentile", "historical_iv_30d"])
    if n_expiries < 2:
        degraded.extend(["term_structure_slope", "calendar_spread_theta_capture"])
    if n_rows < 20:
        degraded.extend(["vol_surface_fit_error", "iv_skew"])
    if degraded:
        extreme_flags.append("partial_option_data")

    confidence_scores["data_coverage"] = round(min(1.0, n_rows / 200), 4)

    return {
        "confidence_scores": confidence_scores,
        "extreme_flags": extreme_flags,
        "degraded_indicators": degraded,
    }


# ═══════════════════════════════════════════════════════════
# Public orchestrator
# ═══════════════════════════════════════════════════════════


async def compute_option_indicators(
    symbol: str,
    option_data: pd.DataFrame,
    underlying_price: float,
    historical_iv: list[float] | None = None,
) -> OptionIndicators:
    """计算完整期权指标集。

    输入:
        symbol           — 标的代码 (e.g. 'NVDA')
        option_data      — 当日期权链快照 DataFrame
        underlying_price — 标的当前价格
        historical_iv    — 可选，预加载的历史 IV 列表

    两类指标分流：
    - 分析类（IV/Greek/flow）使用 **全部合约**
    - 策略类（spread/arb）使用 **可交易合约**
    """
    if option_data.empty:
        ind = OptionIndicators()
        ind.extreme_flags.append("no_option_data")
        ind.degraded_indicators = ["all"]
        ind.confidence_scores = {"iv_regime": 0.0, "chain_liquidity": 0.0, "greeks_stability": 0.0}
        return ind

    # Fetch historical IV once (uses config iv_lookback_days, default 252)
    if historical_iv is None:
        historical_iv = await get_historical_iv(symbol)

    # ── Analysis indicators (all contracts) ──
    iv_metrics = await _compute_iv_metrics(symbol, option_data, underlying_price, historical_iv)
    greek_metrics = _compute_greek_aggregations(option_data)
    flow_metrics = _compute_flow_metrics(option_data)

    # ── Strategy indicators (tradeable contracts only) ──
    strategy_metrics = _compute_strategy_metrics(option_data, underlying_price, iv_metrics["atm_iv"])

    # ── Confidence & flags ──
    quality = _assess_confidence_and_flags(iv_metrics, flow_metrics, greek_metrics, option_data, historical_iv)

    result = OptionIndicators(
        iv_rank=iv_metrics["iv_rank"],
        iv_percentile=iv_metrics["iv_percentile"],
        current_iv=iv_metrics["current_iv"],
        historical_iv_30d=iv_metrics["historical_iv_30d"],
        pcr_volume=flow_metrics["pcr_volume"],
        pcr_oi=flow_metrics["pcr_oi"],
        iv_skew=iv_metrics["iv_skew"],
        term_structure_slope=iv_metrics["term_structure_slope"],
        atm_iv=iv_metrics["atm_iv"],
        vol_surface_fit_error=iv_metrics["vol_surface_fit_error"],
        delta_exposure_profile=greek_metrics["delta_exposure_profile"],
        gamma_peak_strike=greek_metrics["gamma_peak_strike"],
        theta_decay_rate=greek_metrics["theta_decay_rate"],
        vanna=greek_metrics["vanna"],
        charm=greek_metrics["charm"],
        portfolio_greeks=greek_metrics["portfolio_greeks"],
        oi_concentration_top5=flow_metrics["oi_concentration_top5"],
        bid_ask_spread_ratio=flow_metrics["bid_ask_spread_ratio"],
        option_volume_imbalance=flow_metrics["option_volume_imbalance"],
        vertical_spread_risk_reward=strategy_metrics["vertical_spread_risk_reward"],
        calendar_spread_theta_capture=strategy_metrics["calendar_spread_theta_capture"],
        butterfly_pricing_error=strategy_metrics["butterfly_pricing_error"],
        box_spread_arbitrage=strategy_metrics["box_spread_arbitrage"],
        confidence_scores=quality["confidence_scores"],
        extreme_flags=quality["extreme_flags"],
        degraded_indicators=quality["degraded_indicators"],
    )
    return _sanitize_option_indicators(result)
