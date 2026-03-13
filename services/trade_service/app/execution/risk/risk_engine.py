from __future__ import annotations

from datetime import datetime, timedelta


def should_run_risk_check(
    last_check_at: datetime | None,
    check_interval_seconds: int,
    now: datetime,
) -> bool:
    if check_interval_seconds <= 0:
        return True
    if last_check_at is None:
        return True
    return (now - last_check_at).total_seconds() >= check_interval_seconds


def evaluate_portfolio_stop_loss(
    total_unrealized_pnl: float,
    portfolio_loss_limit: float,
) -> bool:
    return total_unrealized_pnl <= -abs(portfolio_loss_limit)


def evaluate_position_stop_loss(
    unrealized_pnl: float,
    position_loss_limit: float,
) -> bool:
    return unrealized_pnl <= -abs(position_loss_limit)


def in_cooldown(symbol: str, cooldowns: dict[str, datetime], now: datetime) -> bool:
    expires_at = cooldowns.get(symbol)
    if expires_at is None:
        return False
    if now >= expires_at:
        cooldowns.pop(symbol, None)
        return False
    return True


def mark_cooldown(
    symbol: str,
    cooldowns: dict[str, datetime],
    now: datetime,
    cooldown_seconds: int,
) -> None:
    seconds = max(0, cooldown_seconds)
    cooldowns[symbol] = now + timedelta(seconds=seconds)
