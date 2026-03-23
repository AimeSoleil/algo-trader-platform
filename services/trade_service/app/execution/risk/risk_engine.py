from __future__ import annotations

from datetime import datetime, timedelta

from shared.utils import get_logger

logger = get_logger("risk_engine")


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


# ── Legacy in-memory cooldown (kept for fallback / tests) ──


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


# ── Redis-backed distributed cooldown ──────────────────────

_COOLDOWN_PREFIX = "trade:stoploss_cooldown"


async def in_cooldown_redis(symbol: str) -> bool:
    """Check whether *symbol* is in stop-loss cooldown (Redis-backed).

    Returns ``True`` if the cooldown key exists in Redis, meaning another
    instance (or this one) already placed a stop-loss order recently.
    Falls back to ``False`` if Redis is unavailable (fail-open).
    """
    try:
        from shared.redis_pool import get_redis
        redis = get_redis()
        key = f"{_COOLDOWN_PREFIX}:{symbol.upper()}"
        return await redis.exists(key) == 1
    except Exception:
        logger.warning("risk_engine.cooldown_check_failed", symbol=symbol)
        return False


async def mark_cooldown_redis(symbol: str, cooldown_seconds: int) -> None:
    """Set a Redis cooldown key with TTL for *symbol*.

    Once set, ``in_cooldown_redis`` will return ``True`` until the key
    expires — even from other service replicas.
    """
    try:
        from shared.redis_pool import get_redis
        redis = get_redis()
        key = f"{_COOLDOWN_PREFIX}:{symbol.upper()}"
        await redis.set(key, "1", ex=max(1, cooldown_seconds))
    except Exception:
        logger.warning("risk_engine.cooldown_mark_failed", symbol=symbol)
