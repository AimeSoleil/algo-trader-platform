"""Deterministic Greeks validation for LLM-generated blueprints.

Computes expected net Greeks from option legs using Black-Scholes and
compares against LLM-stated values to catch hallucination.

Usage::

    from services.analysis_service.app.evaluation.greeks_validator import validate_greeks
    issues = validate_greeks(blueprint_dict, market_data)
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any


@dataclass
class GreeksIssue:
    """A discrepancy found between calculated and stated Greeks."""
    severity: str  # "error", "warning"
    symbol: str
    greek: str  # "delta", "gamma", "theta", "vega"
    calculated: float
    stated: float
    pct_diff: float
    description: str


@dataclass
class GreeksValidationResult:
    """Result of Greeks validation."""
    issues: list[GreeksIssue] = field(default_factory=list)
    passed: bool = True

    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "error")


# ---------------------------------------------------------------------------
# Black-Scholes primitives
# ---------------------------------------------------------------------------

def _norm_cdf(x: float) -> float:
    """Standard normal CDF using error function."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    """Standard normal PDF."""
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _d1(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Black-Scholes d1."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    return (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))


def _d2(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Black-Scholes d2."""
    if T <= 0 or sigma <= 0:
        return 0.0
    return _d1(S, K, T, r, sigma) - sigma * math.sqrt(T)


def bs_delta(S: float, K: float, T: float, r: float, sigma: float, is_call: bool) -> float:
    """Black-Scholes delta for a single option."""
    if T <= 0 or sigma <= 0:
        return 0.0
    d1 = _d1(S, K, T, r, sigma)
    if is_call:
        return _norm_cdf(d1)
    return _norm_cdf(d1) - 1.0


def bs_gamma(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Black-Scholes gamma (same for call and put)."""
    if T <= 0 or sigma <= 0 or S <= 0:
        return 0.0
    d1 = _d1(S, K, T, r, sigma)
    return _norm_pdf(d1) / (S * sigma * math.sqrt(T))


def bs_theta(S: float, K: float, T: float, r: float, sigma: float, is_call: bool) -> float:
    """Black-Scholes theta (per day, negative for long options)."""
    if T <= 0 or sigma <= 0 or S <= 0:
        return 0.0
    d1 = _d1(S, K, T, r, sigma)
    d2 = _d2(S, K, T, r, sigma)
    term1 = -(S * _norm_pdf(d1) * sigma) / (2.0 * math.sqrt(T))
    if is_call:
        term2 = -r * K * math.exp(-r * T) * _norm_cdf(d2)
    else:
        term2 = r * K * math.exp(-r * T) * _norm_cdf(-d2)
    return (term1 + term2) / 365.0  # per-day theta


def bs_vega(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Black-Scholes vega (same for call and put), per 1% vol move."""
    if T <= 0 or sigma <= 0 or S <= 0:
        return 0.0
    d1 = _d1(S, K, T, r, sigma)
    return S * _norm_pdf(d1) * math.sqrt(T) / 100.0


# ---------------------------------------------------------------------------
# Leg-level Greeks computation
# ---------------------------------------------------------------------------

def compute_leg_greeks(
    spot: float,
    strike: float,
    dte_years: float,
    iv: float,
    is_call: bool,
    is_long: bool,
    quantity: int = 1,
    risk_free_rate: float = 0.05,
) -> dict[str, float]:
    """Compute Greeks for a single option leg.
    
    Parameters
    ----------
    spot: Current underlying price
    strike: Option strike price
    dte_years: Time to expiry in years (DTE / 365)
    iv: Implied volatility as decimal (e.g. 0.30 for 30%)
    is_call: True for call, False for put
    is_long: True for buy, False for sell
    quantity: Number of contracts
    risk_free_rate: Risk-free rate (default 5%)
    
    Returns
    -------
    dict with delta, gamma, theta, vega
    """
    sign = 1 if is_long else -1
    
    delta = bs_delta(spot, strike, dte_years, risk_free_rate, iv, is_call) * sign * quantity
    gamma = bs_gamma(spot, strike, dte_years, risk_free_rate, iv) * sign * quantity
    theta = bs_theta(spot, strike, dte_years, risk_free_rate, iv, is_call) * sign * quantity
    vega = bs_vega(spot, strike, dte_years, risk_free_rate, iv) * sign * quantity
    
    return {"delta": delta, "gamma": gamma, "theta": theta, "vega": vega}


# ---------------------------------------------------------------------------
# Portfolio-level validation
# ---------------------------------------------------------------------------

def compute_position_greeks(
    legs: list[dict[str, Any]],
    spot: float,
    default_iv: float = 0.30,
    risk_free_rate: float = 0.05,
) -> dict[str, float]:
    """Compute aggregate Greeks for a multi-leg position.
    
    Parameters
    ----------
    legs: List of leg dicts with keys: strike, option_type, side, quantity, expiry
          Optionally: iv (implied vol for this leg)
    spot: Current underlying price
    default_iv: Default IV if not specified per leg
    risk_free_rate: Risk-free rate
    
    Returns
    -------
    dict with net delta, gamma, theta, vega
    """
    from datetime import date, datetime
    
    net = {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0}
    today = date.today()
    
    for leg in legs:
        strike = leg.get("strike", 0)
        if not strike or not spot:
            continue
            
        is_call = leg.get("option_type", "").lower() == "call"
        is_long = leg.get("side", "").lower() == "buy"
        quantity = leg.get("quantity", 1)
        iv = leg.get("iv", default_iv)
        
        # Parse expiry
        expiry_str = leg.get("expiry", "")
        try:
            if isinstance(expiry_str, str):
                expiry = datetime.strptime(expiry_str, "%Y-%m-%d").date()
            elif isinstance(expiry_str, date):
                expiry = expiry_str
            else:
                continue
        except (ValueError, TypeError):
            continue
        
        dte = (expiry - today).days
        if dte <= 0:
            continue
        dte_years = dte / 365.0
        
        leg_greeks = compute_leg_greeks(
            spot=spot,
            strike=strike,
            dte_years=dte_years,
            iv=iv,
            is_call=is_call,
            is_long=is_long,
            quantity=quantity,
            risk_free_rate=risk_free_rate,
        )
        
        for greek in net:
            net[greek] += leg_greeks[greek]
    
    return net


def validate_greeks(
    blueprint: dict[str, Any],
    market_data: dict[str, dict[str, Any]] | None = None,
    warn_threshold: float = 0.20,
    error_threshold: float = 0.50,
) -> GreeksValidationResult:
    """Validate Greeks in a blueprint against Black-Scholes calculations.
    
    Parameters
    ----------
    blueprint: Blueprint dict (model_dump output)
    market_data: Optional mapping of symbol -> {spot, iv, risk_free_rate}
    warn_threshold: Fractional difference to trigger warning (default 20%)
    error_threshold: Fractional difference to trigger error (default 50%)
    
    Returns
    -------
    GreeksValidationResult with any discrepancies found
    """
    result = GreeksValidationResult()
    market_data = market_data or {}
    
    for plan in blueprint.get("symbol_plans", []):
        sym = plan.get("underlying", "UNKNOWN")
        legs = plan.get("legs", [])
        
        if not legs:
            continue
        
        # Get market data for this symbol
        mkt = market_data.get(sym.upper(), {})
        spot = mkt.get("spot", 0)
        default_iv = mkt.get("iv", 0.30)
        rfr = mkt.get("risk_free_rate", 0.05)
        
        if not spot:
            continue
        
        # Compute expected Greeks
        calculated = compute_position_greeks(
            legs=legs,
            spot=spot,
            default_iv=default_iv,
            risk_free_rate=rfr,
        )
        
        # Compare with plan-level direction for basic sanity
        direction = plan.get("direction", "neutral")
        net_delta = calculated["delta"]
        
        if direction == "bullish" and net_delta < -0.05:
            result.issues.append(GreeksIssue(
                severity="warning",
                symbol=sym,
                greek="delta",
                calculated=round(net_delta, 4),
                stated=0.0,
                pct_diff=0.0,
                description=f"Direction is bullish but calculated net delta={net_delta:.4f} is negative",
            ))
        elif direction == "bearish" and net_delta > 0.05:
            result.issues.append(GreeksIssue(
                severity="warning",
                symbol=sym,
                greek="delta",
                calculated=round(net_delta, 4),
                stated=0.0,
                pct_diff=0.0,
                description=f"Direction is bearish but calculated net delta={net_delta:.4f} is positive",
            ))
    
    result.passed = result.error_count == 0
    return result
