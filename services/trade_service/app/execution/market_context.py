"""Shared market-context builder for the execution layer."""
from __future__ import annotations

import json
from typing import Any

from shared.redis_pool import get_redis
from shared.utils import get_logger, now_market
from services.trade_service.app.helpers import safe_float

logger = get_logger("market_context")


async def build_market_context(
    symbol: str,
    trading_date_iso: str,
) -> dict[str, Any] | None:
    """Build an enriched market-context dict for *symbol* from Redis.

    Returns ``None`` when no quote is available (symbol not trading).

    Keys populated:
        underlying_price, price, bid, ask, time,
        iv, iv_rank, delta, volume  (from signal features cache, if present).

    .. note::
        # TODO(后续考虑-1): Check quote/signal timestamp freshness.  When the
        # optimizer runs from a Celery task (not the tick loop), Redis data may
        # be stale (>60 s old).  A future enhancement should compare the quote
        # timestamp against ``now_market()`` and return ``None`` for symbols
        # whose data is too old, rather than scoring on stale values.
    """
    redis = get_redis()

    # ── Quote ──
    quote = await redis.hgetall(f"market:quote:{symbol}")
    if not quote:
        return None

    quote_price = safe_float(quote.get("price"), 0.0)
    if quote_price <= 0:
        return None

    mt = now_market()
    market_time_decimal = mt.hour + mt.minute / 60.0

    ctx: dict[str, Any] = {
        "underlying_price": quote_price,
        "price": quote_price,
        "bid": safe_float(quote.get("bid"), 0.0),
        "ask": safe_float(quote.get("ask"), 0.0),
        "time": market_time_decimal,
    }

    # ── Enrich from signal features cache ──
    sig_key = f"signal:features:{symbol.upper()}:{trading_date_iso}"
    sig_data = await redis.get(sig_key)
    if sig_data:
        try:
            sig = json.loads(sig_data)
            opt = sig.get("option_indicators", {})
            ctx.setdefault("iv", opt.get("current_iv", 0.0))
            ctx.setdefault("iv_rank", opt.get("iv_rank", 0.0))
            ctx.setdefault("delta", opt.get("delta_exposure_profile", {}).get("total", 0.0))
            ctx.setdefault("volume", sig.get("volume", 0))
        except Exception:
            pass

    return ctx
