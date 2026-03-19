"""Signal Service — 查询层（Redis L1 缓存 + DB 查询）"""
from __future__ import annotations

import json
from datetime import date

from redis.asyncio import Redis
from sqlalchemy import text

from shared.config import get_settings
from shared.db.session import get_postgres_session
from shared.models.signal import SignalFeatures
from shared.utils import get_logger, today_trading

logger = get_logger("signal_queries")

_CACHE_TTL = 6 * 3600  # 6 hours — signals don't change until next batch run
_CACHE_PREFIX = "signal:features"


def _cache_key(symbol: str, d: date) -> str:
    return f"{_CACHE_PREFIX}:{symbol.upper()}:{d.isoformat()}"


async def _get_redis() -> Redis:
    settings = get_settings()
    return Redis.from_url(settings.redis.url, decode_responses=True)


async def set_signal_cache(symbol: str, target_date: date, data: dict) -> None:
    """Write-through helper: 写库后主动刷新单标的缓存。"""
    key = _cache_key(symbol, target_date)
    redis = await _get_redis()
    await redis.set(key, json.dumps(data, default=str), ex=_CACHE_TTL)


async def delete_signal_cache(symbol: str, target_date: date) -> None:
    """Delete-on-write helper: 写缓存失败时删除旧 key，避免返回陈旧数据。"""
    key = _cache_key(symbol, target_date)
    redis = await _get_redis()
    await redis.delete(key)


# ── Unified query ──────────────────────────────────────────


async def query_signals(
    *,
    symbols: list[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    bypass_cache: bool = False,
    volatility_regime: str | None = None,
    trend: str | None = None,
    sort_by: str | None = None,
    sort_order: str = "asc",
    limit: int = 500,
    offset: int = 0,
) -> dict:
    """Unified signal query — supports single/batch, date ranges, and filters.

    Returns ``{"data": [...], "total": N, "limit": N, "offset": N, "filters_applied": {...}}``.
    """
    # Default date range to today if nothing specified
    if start_date is None and end_date is None:
        start_date = end_date = today_trading()
    elif start_date is None:
        start_date = end_date
    elif end_date is None:
        end_date = start_date

    # Normalise symbols
    upper_symbols: list[str] | None = None
    if symbols:
        upper_symbols = list(dict.fromkeys(s.strip().upper() for s in symbols if s.strip()))

    # ── Fast path: single symbol + single date → try cache first ──
    single_mode = (
        upper_symbols is not None
        and len(upper_symbols) == 1
        and start_date == end_date
    )
    if single_mode and not bypass_cache:
        cached = await _try_cache(upper_symbols[0], start_date)  # type: ignore[arg-type]
        if cached is not None:
            # Apply in-memory filters on the single cached result
            items = _apply_json_filters([cached], volatility_regime=volatility_regime, trend=trend)
            return _paginated(items, total=len(items), limit=limit, offset=offset,
                              filters=_describe_filters(upper_symbols, start_date, end_date,
                                                        bypass_cache, volatility_regime, trend))

    # ── DB query ──
    conditions = ["date >= :start_date", "date <= :end_date"]
    params: dict = {"start_date": start_date, "end_date": end_date}

    if upper_symbols:
        conditions.append("symbol = ANY(:symbols)")
        params["symbols"] = upper_symbols

    where = " AND ".join(conditions)

    # Collect total count
    count_sql = f"SELECT COUNT(*) FROM signal_features WHERE {where}"
    data_sql = (
        f"SELECT symbol, date, features_json FROM signal_features "
        f"WHERE {where} ORDER BY date, symbol"
    )

    async with get_postgres_session() as session:
        total_row = await session.execute(text(count_sql), params)
        total = total_row.scalar() or 0

        result = await session.execute(text(data_sql), params)
        rows = result.fetchall()

    # Parse features_json
    items: list[dict] = []
    for row in rows:
        data = row[2] if isinstance(row[2], dict) else json.loads(row[2])
        items.append(data)

    # Apply JSON-level filters
    items = _apply_json_filters(items, volatility_regime=volatility_regime, trend=trend)

    # Sort
    if sort_by and items:
        reverse = sort_order.lower() == "desc"
        items.sort(key=lambda x: x.get(sort_by, ""), reverse=reverse)

    filtered_total = len(items)

    # Populate cache for single-date results (background, best-effort)
    if start_date == end_date and not bypass_cache:
        for item in items:
            sym = item.get("symbol")
            if sym:
                try:
                    await _set_cache(sym, start_date, item)
                except Exception:
                    pass

    return _paginated(items, total=filtered_total, limit=limit, offset=offset,
                      filters=_describe_filters(upper_symbols, start_date, end_date,
                                                bypass_cache, volatility_regime, trend))


# ── Cache helpers ──────────────────────────────────────────


async def _try_cache(symbol: str, d: date) -> dict | None:
    key = _cache_key(symbol, d)
    try:
        redis = await _get_redis()
        cached = await redis.get(key)
        if cached:
            data = json.loads(cached)
            data["_from_cache"] = True
            return data
    except Exception:
        logger.debug("signal_query.redis_miss", symbol=symbol)
    return None


async def _set_cache(symbol: str, d: date, data: dict) -> None:
    key = _cache_key(symbol, d)
    redis = await _get_redis()
    await redis.set(key, json.dumps(data, default=str), ex=_CACHE_TTL)


# ── Filtering helpers ──────────────────────────────────────


def _apply_json_filters(
    items: list[dict],
    *,
    volatility_regime: str | None = None,
    trend: str | None = None,
) -> list[dict]:
    """Filter items by values inside the JSON payload."""
    filtered = items
    if volatility_regime:
        filtered = [i for i in filtered if i.get("volatility_regime") == volatility_regime]
    if trend:
        filtered = [
            i for i in filtered
            if i.get("stock_indicators", {}).get("trend") == trend
        ]
    return filtered


def _describe_filters(
    symbols: list[str] | None,
    start_date: date,
    end_date: date,
    bypass_cache: bool,
    volatility_regime: str | None,
    trend: str | None,
) -> dict:
    f: dict = {
        "start_date": str(start_date),
        "end_date": str(end_date),
    }
    if symbols:
        f["symbols"] = symbols
    if bypass_cache:
        f["bypass_cache"] = True
    if volatility_regime:
        f["volatility_regime"] = volatility_regime
    if trend:
        f["trend"] = trend
    return f


def _paginated(items: list[dict], *, total: int, limit: int, offset: int, filters: dict) -> dict:
    page = items[offset: offset + limit]
    return {
        "data": page,
        "total": total,
        "limit": limit,
        "offset": offset,
        "count": len(page),
        "filters_applied": filters,
    }
