"""期权希腊字母计算 — 使用 py_vollib Black-Scholes 解析式 + Vanna / Charm"""
from __future__ import annotations

import math

from py_vollib.black_scholes.greeks.analytical import delta, gamma, theta, vega
from py_vollib.black_scholes.implied_volatility import implied_volatility
from scipy.stats import norm

from shared.models.option import OptionChainSnapshot, OptionContract, OptionGreeks, OptionType
from shared.utils import get_logger

logger = get_logger("greeks")

DEFAULT_RISK_FREE_RATE = 0.045

# If yfinance IV is below this threshold we treat it as unreliable and
# attempt to recalculate IV from the option's last traded price using BSM.
IV_RECALC_THRESHOLD = 0.01  # 1%


def _recalc_iv(
    flag: str,
    S: float,
    K: float,
    T: float,
    r: float,
    market_price: float,
) -> float | None:
    """Try to recover IV from the option's market price via BSM inversion.

    Returns the recalculated IV, or ``None`` if the solver fails
    (e.g. price below intrinsic, numerical issues).
    """
    if market_price <= 0 or T <= 0 or S <= 0 or K <= 0:
        return None
    try:
        iv = implied_volatility(market_price, S, K, T, r, flag)
        if iv > 0:
            return iv
    except Exception:  # noqa: BLE001
        pass
    return None


def _compute_vanna_charm(
    S: float, K: float, T: float, r: float, sigma: float,
) -> tuple[float, float]:
    """BSM 解析式计算 Vanna 与 Charm（call/put 共用）。

    Vanna = ∂Δ/∂σ = -n(d1) × d2 / σ
    Charm = -∂Δ/∂T = -n(d1) × [2rT - d2·σ·√T] / (2T·σ·√T)

    Returns (vanna, charm).  如果输入不合法则返回 (0.0, 0.0)。
    """
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0, 0.0

    try:
        sqrt_T = math.sqrt(T)
        d1 = (math.log(S / K) + (r + sigma ** 2 / 2) * T) / (sigma * sqrt_T)
        d2 = d1 - sigma * sqrt_T
        n_d1 = norm.pdf(d1)

        # Vanna: sensitivity of delta to volatility
        van = -n_d1 * d2 / sigma

        # Charm: sensitivity of delta to time (negative ∂Δ/∂T)
        denom = 2.0 * T * sigma * sqrt_T
        if abs(denom) < 1e-15:
            cha = 0.0
        else:
            cha = -n_d1 * (2.0 * r * T - d2 * sigma * sqrt_T) / denom
    except (ValueError, ZeroDivisionError, OverflowError):
        return 0.0, 0.0

    return van, cha


def compute_greeks(
    flag: str,
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
) -> OptionGreeks:
    """计算单个合约的希腊字母（含 Vanna / Charm）。

    Parameters
    ----------
    flag : str
        ``"c"`` for call, ``"p"`` for put.
    S : float
        Underlying price.
    K : float
        Strike price.
    T : float
        Time to expiry in years.
    r : float
        Risk-free interest rate.
    sigma : float
        Implied volatility.
    """
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return OptionGreeks(iv=sigma)

    try:
        d = delta(flag, S, K, T, r, sigma)
        g = gamma(flag, S, K, T, r, sigma)
        t = theta(flag, S, K, T, r, sigma)
        v = vega(flag, S, K, T, r, sigma)
    except (ValueError, ZeroDivisionError):
        return OptionGreeks(iv=sigma)

    # ── Vanna & Charm (BSM analytical) ──
    van, cha = _compute_vanna_charm(S, K, T, r, sigma)

    return OptionGreeks(
        delta=d,
        gamma=g,
        theta=t,
        vega=v,  # py_vollib already returns per-1% IV change
        iv=sigma,
        vanna=van,
        charm=cha,
    )


def enrich_snapshot_greeks(
    snapshot: OptionChainSnapshot,
    risk_free_rate: float = DEFAULT_RISK_FREE_RATE,
) -> OptionChainSnapshot:
    """为快照中的所有合约计算并填充希腊字母。

    Parameters
    ----------
    snapshot : OptionChainSnapshot
        期权链快照，包含 ``underlying_price`` 和 ``contracts``。
    risk_free_rate : float
        无风险利率，默认 ``DEFAULT_RISK_FREE_RATE``。

    Returns
    -------
    OptionChainSnapshot
        同一快照对象（就地修改）。
    """
    S = snapshot.underlying_price
    logger.debug(
        "greeks.enrich_start",
        underlying=snapshot.underlying,
        contracts_count=len(snapshot.contracts),
        risk_free_rate=risk_free_rate,
    )
    valid_count = 0
    iv_recalc_count = 0

    for contract in snapshot.contracts:
        flag = "c" if contract.option_type == OptionType.CALL else "p"
        T = contract.days_to_expiry / 365.0
        sigma = contract.greeks.iv
        K = contract.strike

        # ── IV sanity gate ──
        # yfinance sometimes returns nonsensical IV (e.g. 0.003 for a contract
        # worth $6) when bid=ask=0 (market closed / illiquid).  In that case
        # we recalculate IV from the last traded price via BSM inversion.
        # Also attempt recalculation when IV is exactly 0 (fetcher no longer
        # filters these out — cleaning filter handles removal post-enrichment).
        if sigma < IV_RECALC_THRESHOLD:
            recalced = _recalc_iv(flag, S, K, T, risk_free_rate, contract.last_price)
            if recalced is not None:
                logger.debug(
                    "greeks.iv_recalculated",
                    symbol=contract.symbol,
                    old_iv=sigma,
                    new_iv=recalced,
                    last_price=contract.last_price,
                )
                sigma = recalced
                iv_recalc_count += 1

        contract.greeks = compute_greeks(flag, S, K, T, risk_free_rate, sigma)

        if contract.greeks.delta != 0:
            valid_count += 1

    logger.info(
        "%s | contracts=%d | greeks_valid=%d | iv_recalculated=%d",
        snapshot.underlying,
        len(snapshot.contracts),
        valid_count,
        iv_recalc_count,
    )
    logger.debug(
        "greeks.enrich_done",
        underlying=snapshot.underlying,
        contracts_count=len(snapshot.contracts),
        greeks_valid=valid_count,
        iv_recalculated=iv_recalc_count,
    )

    return snapshot
