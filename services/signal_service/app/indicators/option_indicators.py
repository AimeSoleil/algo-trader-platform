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

logger = get_logger("option_indicators")


async def get_historical_iv(symbol: str, lookback_days: int = 30) -> list[float]:
    """从 TimescaleDB 获取历史 IV 数据"""
    start_date = today_trading() - timedelta(days=lookback_days)

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


def _sanitize_float(v: float) -> float:
    """Replace NaN / Inf with 0.0."""
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return 0.0
    return v


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


async def calculate_iv_rank(symbol: str, current_iv: float, lookback_days: int = 30, historical_iv: list[float] | None = None) -> float:
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
    """
    if option_data.empty or underlying_price <= 0:
        return 0.0

    # Use only the nearest expiry to avoid dilution from far-month contracts
    nearest_expiry = option_data["expiry"].min()
    near = option_data[option_data["expiry"] == nearest_expiry]

    # 近似：取 OTM 5% put IV - OTM 5% call IV
    otm_put_strike = underlying_price * 0.95
    otm_call_strike = underlying_price * 1.05

    puts = near[
        (near["option_type"] == "put") & (near["strike"] <= otm_put_strike)
    ]
    calls = near[
        (near["option_type"] == "call") & (near["strike"] >= otm_call_strike)
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


async def compute_option_indicators(symbol: str, option_data: pd.DataFrame, underlying_price: float, historical_iv: list[float] | None = None) -> OptionIndicators:
    """计算完整期权指标集"""
    if option_data.empty:
        return OptionIndicators()

    # Fetch historical IV once
    if historical_iv is None:
        historical_iv = await get_historical_iv(symbol, 30)

    current_iv = float(option_data[option_data["iv"] > 0]["iv"].mean()) if not option_data[option_data["iv"] > 0].empty else 0.0
    percentile = await calculate_iv_rank(symbol, current_iv, historical_iv=historical_iv)
    pcr_volume, pcr_oi = calculate_pcr(option_data)
    iv_skew = calculate_iv_skew(option_data, underlying_price)
    atm_iv = calculate_term_structure(option_data, underlying_price)

    # Use the already-fetched historical_iv
    historical_iv_30d = np.mean(historical_iv) if historical_iv else 0.0

    # iv_rank: min-max normalization — (current - min) / (max - min) * 100
    if historical_iv and current_iv > 0:
        iv_min = float(np.min(historical_iv))
        iv_max = float(np.max(historical_iv))
        iv_rank = ((current_iv - iv_min) / max(iv_max - iv_min, 1e-9)) * 100
        iv_rank = float(np.clip(iv_rank, 0.0, 100.0))
    else:
        iv_rank = 50.0

    # Term structure slope: IV of far expiry - IV of near expiry
    term_slope = 0.0
    if len(atm_iv) >= 2:
        sorted_expiries = sorted(atm_iv.keys())
        term_slope = atm_iv[sorted_expiries[-1]] - atm_iv[sorted_expiries[0]]

    # Volatility surface fit error (quadratic fit on moneyness)
    fit_errors: list[float] = []
    option_data = option_data.copy()
    option_data = option_data[option_data["strike"] > 0]
    if not option_data.empty and underlying_price > 0:
        option_data["moneyness"] = option_data["strike"] / underlying_price
        for _, group in option_data.groupby("expiry"):
            valid = group[(group["iv"] > 0) & group["iv"].notna()]
            if len(valid) >= 5:
                x = valid["moneyness"].to_numpy(dtype=float)
                y = valid["iv"].to_numpy(dtype=float)
                coeff = np.polyfit(x, y, 2)
                y_hat = np.polyval(coeff, x)
                fit_errors.append(float(np.sqrt(np.mean((y - y_hat) ** 2))))
    vol_surface_fit_error = float(np.mean(fit_errors)) if fit_errors else 0.0

    # Exposure and Greek aggregations (OI weighted)
    weighted_oi = option_data["open_interest"].fillna(0).astype(float)
    total_oi = float(weighted_oi.sum()) if float(weighted_oi.sum()) > 0 else 1.0
    delta_exposure = float((option_data["delta"].fillna(0).astype(float) * weighted_oi).sum())
    gamma_exposure = float((option_data["gamma"].fillna(0).astype(float) * weighted_oi).sum())
    theta_exposure = float((option_data["theta"].fillna(0).astype(float) * weighted_oi).sum())
    vega_exposure = float((option_data["vega"].fillna(0).astype(float) * weighted_oi).sum())

    call_delta_exposure = float(
        (
            option_data.loc[option_data["option_type"] == "call", "delta"].fillna(0).astype(float)
            * option_data.loc[option_data["option_type"] == "call", "open_interest"].fillna(0).astype(float)
        ).sum()
    )
    put_delta_exposure = float(
        (
            option_data.loc[option_data["option_type"] == "put", "delta"].fillna(0).astype(float)
            * option_data.loc[option_data["option_type"] == "put", "open_interest"].fillna(0).astype(float)
        ).sum()
    )

    gamma_weighted = (option_data["gamma"].fillna(0).abs().astype(float) * weighted_oi).to_numpy()
    gamma_peak_strike = float(option_data.iloc[int(np.argmax(gamma_weighted))]["strike"]) if len(gamma_weighted) else 0.0

    # Theta decay per day (OI weighted absolute theta)
    theta_decay_rate = float((option_data["theta"].fillna(0).abs().astype(float) * weighted_oi).sum() / total_oi)

    # Vanna / Charm approximations using available columns
    # vanna ≈ delta * iv sensitivity proxy
    vanna = float((option_data["delta"].fillna(0).astype(float) * option_data["iv"].fillna(0).astype(float) * weighted_oi).sum() / total_oi)

    now_ts = pd.Timestamp.utcnow().tz_localize(None)
    expiry_days = (pd.to_datetime(option_data["expiry"], errors="coerce") - now_ts).dt.days.clip(lower=1)
    charm = float((option_data["delta"].fillna(0).astype(float) / expiry_days.fillna(1).astype(float) * weighted_oi).sum() / total_oi)

    # OI concentration (top 5 strikes / total, then average by expiry)
    oi_concentrations = []
    for _, group in option_data.groupby("expiry"):
        oi_series = group.groupby("strike")["open_interest"].sum().sort_values(ascending=False)
        denom = float(oi_series.sum()) if float(oi_series.sum()) > 0 else 1.0
        oi_concentrations.append(float(oi_series.head(5).sum()) / denom)
    oi_concentration_top5 = float(np.mean(oi_concentrations)) if oi_concentrations else 0.0

    # Bid-Ask spread ratio
    mid = ((option_data["bid"].fillna(0) + option_data["ask"].fillna(0)) / 2).replace(0, np.nan)
    spread_ratio = ((option_data["ask"].fillna(0) - option_data["bid"].fillna(0)) / mid).replace([np.inf, -np.inf], np.nan)
    bid_ask_spread_ratio = float(spread_ratio.dropna().mean()) if not spread_ratio.dropna().empty else 0.0

    # Volume imbalance
    call_vol = float(option_data.loc[option_data["option_type"] == "call", "volume"].fillna(0).sum())
    put_vol = float(option_data.loc[option_data["option_type"] == "put", "volume"].fillna(0).sum())
    denom_vol = call_vol + put_vol if (call_vol + put_vol) > 0 else 1.0
    option_volume_imbalance = (call_vol - put_vol) / denom_vol

    # Spread/arbitrage approximations
    vertical_scores = []
    calendar_scores = []
    butterfly_errors = []
    box_errors = []

    for expiry, group in option_data.groupby("expiry"):
        calls = group[group["option_type"] == "call"].copy()
        puts = group[group["option_type"] == "put"].copy()
        calls["mid"] = (calls["bid"].fillna(0) + calls["ask"].fillna(0)) / 2
        puts["mid"] = (puts["bid"].fillna(0) + puts["ask"].fillna(0)) / 2

        sorted_calls = calls.sort_values("strike")
        for i in range(len(sorted_calls) - 1):
            low = sorted_calls.iloc[i]
            high = sorted_calls.iloc[i + 1]
            width = float(high["strike"] - low["strike"])
            debit = float(low["mid"] - high["mid"])
            max_loss = max(debit, 0.01)
            max_profit = max(width - debit, 0.0)
            vertical_scores.append(max_profit / max_loss)

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

    # Calendar spread theta capture (near - far)
    if len(atm_iv) >= 2:
        expiry_order = sorted(option_data["expiry"].unique())
        near = option_data[option_data["expiry"] == expiry_order[0]]
        far = option_data[option_data["expiry"] == expiry_order[-1]]
        near_theta = float(near["theta"].fillna(0).abs().mean()) if not near.empty else 0.0
        far_theta = float(far["theta"].fillna(0).abs().mean()) if not far.empty else 0.0
        calendar_scores.append(max(near_theta - far_theta, 0.0))

    vertical_spread_risk_reward = float(np.mean(vertical_scores)) if vertical_scores else 0.0
    calendar_spread_theta_capture = float(np.mean(calendar_scores)) if calendar_scores else 0.0
    butterfly_pricing_error = float(np.mean(butterfly_errors)) if butterfly_errors else 0.0
    box_spread_arbitrage = float(np.mean(box_errors)) if box_errors else 0.0

    confidence_scores = {
        "iv_regime": round(min(1.0, len(historical_iv) / 30.0), 4),
        "chain_liquidity": round(max(0.0, 1.0 - min(1.0, bid_ask_spread_ratio)), 4),
        "greeks_stability": round(min(1.0, total_oi / 100000.0), 4),
    }

    extreme_flags: list[str] = []
    if iv_rank > 90:
        extreme_flags.append("extreme_high_iv")
    if iv_rank < 10:
        extreme_flags.append("extreme_low_iv")
    if abs(option_volume_imbalance) > 0.6:
        extreme_flags.append("extreme_volume_imbalance")
    if bid_ask_spread_ratio > 0.2:
        extreme_flags.append("poor_liquidity")

    result = OptionIndicators(
        iv_rank=iv_rank,
        iv_percentile=percentile,
        current_iv=round(current_iv, 4),
        historical_iv_30d=round(historical_iv_30d, 4),
        pcr_volume=pcr_volume,
        pcr_oi=pcr_oi,
        iv_skew=iv_skew,
        term_structure_slope=round(term_slope, 4),
        atm_iv=atm_iv,
        vol_surface_fit_error=round(vol_surface_fit_error, 6),
        delta_exposure_profile={
            "total": round(delta_exposure, 4),
            "call": round(call_delta_exposure, 4),
            "put": round(put_delta_exposure, 4),
        },
        gamma_peak_strike=round(gamma_peak_strike, 4),
        theta_decay_rate=round(theta_decay_rate, 6),
        vanna=round(vanna, 6),
        charm=round(charm, 6),
        portfolio_greeks={
            "delta": round(delta_exposure, 4),
            "gamma": round(gamma_exposure, 4),
            "theta": round(theta_exposure, 4),
            "vega": round(vega_exposure, 4),
        },
        oi_concentration_top5=round(oi_concentration_top5, 6),
        bid_ask_spread_ratio=round(bid_ask_spread_ratio, 6),
        option_volume_imbalance=round(option_volume_imbalance, 6),
        vertical_spread_risk_reward=round(vertical_spread_risk_reward, 6),
        calendar_spread_theta_capture=round(calendar_spread_theta_capture, 6),
        butterfly_pricing_error=round(butterfly_pricing_error, 6),
        box_spread_arbitrage=round(box_spread_arbitrage, 6),
        confidence_scores=confidence_scores,
        extreme_flags=extreme_flags,
    )
    return _sanitize_option_indicators(result)
