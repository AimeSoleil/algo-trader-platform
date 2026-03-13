"""统一时间工具 — UTC 存储 + 配置时区 trading date

Design:
  - DB 列全部为 TIMESTAMPTZ（PostgreSQL 底层 UTC）
  - 代码中 datetime 统一用 UTC 生成 → ``now_utc()``
  - trading date 按配置时区（默认 America/New_York）计算 → ``today_trading()``
  - 需要展示/比较美东时间时 → ``to_market_tz()``
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from functools import lru_cache
from zoneinfo import ZoneInfo

# ── Constants ──────────────────────────────────────────────

UTC = timezone.utc


# ── Core helpers ───────────────────────────────────────────


def now_utc() -> datetime:
    """返回当前 UTC 时间（timezone-aware）。

    替代所有 ``datetime.now()`` / ``datetime.utcnow()``。
    """
    return datetime.now(UTC)


@lru_cache(maxsize=1)
def _trading_tz() -> ZoneInfo:
    """从配置读取交易时区（懒加载 + 缓存）。"""
    from shared.config import get_settings

    return ZoneInfo(get_settings().trading.timezone)


def market_tz() -> ZoneInfo:
    """返回配置中的交易时区 ZoneInfo 实例。"""
    return _trading_tz()


def today_trading() -> date:
    """按配置交易时区计算"今天"的日期。

    在 UTC 服务器上 16:00–00:00 ET 期间，``date.today()``
    返回的日期比美东日期多一天。本函数始终返回正确的美东日期。
    """
    return datetime.now(_trading_tz()).date()


def to_market_tz(dt: datetime) -> datetime:
    """将任意 tz-aware datetime 转为配置交易时区。

    Parameters
    ----------
    dt : datetime
        必须是 tz-aware（推荐 UTC）。

    Raises
    ------
    ValueError
        如果传入 naive datetime。
    """
    if dt.tzinfo is None:
        raise ValueError(
            "to_market_tz() requires a tz-aware datetime. "
            "Use now_utc() instead of datetime.now()."
        )
    return dt.astimezone(_trading_tz())


def ensure_utc(dt: datetime) -> datetime:
    """确保 datetime 为 UTC tz-aware。

    - 已有 tzinfo → ``astimezone(UTC)``
    - naive → 假定为 UTC 并附加 tzinfo
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def next_trading_day(from_date: date | None = None) -> date:
    """返回下一个交易日（跳过周末，不含节假日）。"""
    from datetime import timedelta

    d = (from_date or today_trading()) + timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def previous_trading_day(from_date: date | None = None) -> date:
    """返回上一个交易日（跳过周末，不含节假日）。"""
    from datetime import timedelta

    d = (from_date or today_trading()) - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d
