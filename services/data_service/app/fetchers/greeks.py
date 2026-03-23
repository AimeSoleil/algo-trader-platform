"""期权希腊字母计算 — 使用 py_vollib Black-Scholes 解析式"""
from __future__ import annotations

from py_vollib.black_scholes.greeks.analytical import delta, gamma, theta, vega, rho
from py_vollib.black_scholes.implied_volatility import implied_volatility

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


def compute_greeks(
    flag: str,
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
) -> OptionGreeks:
    """计算单个合约的希腊字母。

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
        rh = rho(flag, S, K, T, r, sigma)
    except (ValueError, ZeroDivisionError):
        return OptionGreeks(iv=sigma)

    return OptionGreeks(
        delta=d,
        gamma=g,
        theta=t,
        vega=v,  # py_vollib already returns per-1% IV change
        rho=rh,
        iv=sigma,
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
        if 0 < sigma < IV_RECALC_THRESHOLD:
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
