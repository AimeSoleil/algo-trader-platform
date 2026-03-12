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


async def query_signal_features(symbol: str, date_str: str | None = None) -> dict:
    """从 Redis / DB 查询单个标的的信号特征"""
    target_date = date.fromisoformat(date_str) if date_str else today_trading()
    key = _cache_key(symbol, target_date)

    # L1: Redis
    try:
        redis = await _get_redis()
        cached = await redis.get(key)
        if cached:
            return json.loads(cached)
    except Exception:
        logger.debug("signal_query.redis_miss", symbol=symbol)

    # L2: Postgres
    async with get_postgres_session() as session:
        result = await session.execute(
            text(
                "SELECT features_json FROM signal_features "
                "WHERE symbol = :symbol AND date = :date"
            ),
            {"symbol": symbol.upper(), "date": target_date},
        )
        row = result.fetchone()

    if not row:
        return {"error": f"No signals for {symbol} on {target_date}"}

    data = row[0] if isinstance(row[0], dict) else json.loads(row[0])

    # Populate cache
    try:
        redis = await _get_redis()
        await redis.set(key, json.dumps(data, default=str), ex=_CACHE_TTL)
    except Exception:
        pass

    return data


async def query_batch_signal_features(date_str: str | None = None) -> list[dict]:
    """查询当日所有标的的信号特征（支持批量）"""
    target_date = date.fromisoformat(date_str) if date_str else today_trading()

    async with get_postgres_session() as session:
        result = await session.execute(
            text(
                "SELECT symbol, features_json FROM signal_features "
                "WHERE date = :date ORDER BY symbol"
            ),
            {"date": target_date},
        )
        rows = result.fetchall()

    features: list[dict] = []
    for row in rows:
        data = row[1] if isinstance(row[1], dict) else json.loads(row[1])
        features.append(data)

    return features
